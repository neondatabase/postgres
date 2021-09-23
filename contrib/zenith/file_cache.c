/*
 *
 * file_cache.c
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/zenith/file_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include <sys/file.h>
#include <unistd.h>
#include <fcntl.h>

#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pagestore_client.h"
#include "postmaster/bgworker.h"
#include "storage/relfilenode.h"
#include "storage/buf_internals.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/dynahash.h"
#include "utils/guc.h"
#include "storage/fd.h"
#include <storage/buf_internals.h>

typedef struct FileCacheEntry {
	BufferTag key;
	uint64 off;
	/* LRU list */
	struct FileCacheEntry* next;
	struct FileCacheEntry* prev;
} FileCacheEntry;

typedef struct FileCacheControl {
	uint64 used;
} FileCacheControl;

static HTAB* lfc_hash;
static int   lfc_desc;
static LWLockId lfc_lock;
static int   lfc_size;
static char* lfc_path;
static  FileCacheControl* lfc_ctl;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static FileCacheEntry lfc_lru = {.next = &lfc_lru, .prev=&lfc_lru};

static void
lfc_shmem_startup(void)
{
	bool found;
	static HASHCTL info;

	if (prev_shmem_startup_hook) {
		prev_shmem_startup_hook();
    }

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	lfc_ctl = (FileCacheControl*)ShmemInitStruct("lfc", sizeof(FileCacheControl), &found);
	if (!found)
	{
		lfc_lock = (LWLockId)GetNamedLWLockTranche("lfc");
		info.keysize = sizeof(BufferTag);
		info.entrysize = sizeof(FileCacheEntry);
		lfc_hash = ShmemInitHash("lfc",
								  lfc_size, lfc_size,
								  &info,
								  HASH_ELEM | HASH_BLOBS);
		lfc_ctl->used = 0;
	}
	LWLockRelease(AddinShmemInitLock);
}

void
lfc_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.
	 */
	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "Zenith module should be loafed via shared_preload_libraries");

	DefineCustomIntVariable("zenith.file_cache_size",
                            "Size of zenith local file cache (blocks)",
							NULL,
							&lfc_size,
							1024*1024, /* 8GB */
							0,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("zenith.file_cache_path",
							   "Path to local file cache (can be raw device)",
							   NULL,
							   &lfc_path,
							   "file.cache",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	if (lfc_size == 0)
		return;

	RequestAddinShmemSpace(hash_estimate_size(lfc_size, sizeof(FileCacheEntry)));
	RequestNamedLWLockTranche("file_cache", 1);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = lfc_shmem_startup;
}

static void
lfc_unlink(FileCacheEntry* entry)
{
	entry->next->prev = entry->prev;
	entry->prev->next = entry->next;
	entry->next = entry->prev = entry;
}

static void
lfc_link(FileCacheEntry* entry)
{
	entry->next = lfc_lru.next;
	entry->prev = &lfc_lru;
	lfc_lru.next->prev = entry;
	lfc_lru.next = entry;
}

static bool
lfc_is_empty(FileCacheEntry* entry)
{
	return entry->next == entry;
}

bool
lfc_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno,
				char *buffer)
{
	BufferTag tag;
	FileCacheEntry* entry;
	ssize_t rc;

	if (lfc_size == 0)
		return false;

	tag.rnode = reln->smgr_rnode.node;
	tag.forkNum = forkNum;
	tag.blockNum = blkno;

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	entry = hash_search(lfc_hash, &tag, HASH_FIND, NULL);
	if (entry == NULL) {
		LWLockRelease(lfc_lock);
		return false;
	}
	/* Unlink entry from LRU list to pin it for the duration of IO operation */
	lfc_unlink(entry);
	LWLockRelease(lfc_lock);

	if (lfc_desc == 0)
 	{
		lfc_desc = BasicOpenFile(lfc_path, O_RDWR|O_CREAT);
		if (lfc_desc < 0) {
			elog(LOG, "Failed to open file cache %s: %m", lfc_path);
			lfc_size = 0; /* disable file cache */
			return false;
		}
	}

	rc = pread(lfc_desc, buffer, BLCKSZ, entry->off);
	if (rc != BLCKSZ)
	{
		elog(INFO, "Failed to read file cache: %m");
		lfc_size = 0; /* disable file cache */
		return false;
	}

	/* Place entry to the head of LRU list */
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	lfc_link(entry);
	LWLockRelease(lfc_lock);

	return true;
}

void
lfc_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno,
				 char *buffer)
{
	BufferTag tag;
	FileCacheEntry* entry;
	ssize_t rc;
	bool found;

	if (lfc_size == 0)
		return;

	tag.rnode = reln->smgr_rnode.node;
	tag.forkNum = forkNum;
	tag.blockNum = blkno;

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	entry = hash_search(lfc_hash, &tag, HASH_ENTER, &found);

	if (found)
	{
		/* Unlink entry from LRU list to pin it for the duration of IO operation */
		lfc_unlink(entry);
	}
	else
	{
		if (lfc_ctl->used >= (uint64)lfc_size && !lfc_is_empty(&lfc_lru))
		{
			/* Cache overflow: evict something */
			FileCacheEntry* victim = lfc_lru.prev;
			lfc_unlink(victim);
			entry->off = victim->off; /* grab victim's chunk */
			hash_search(lfc_hash, &victim->key, HASH_REMOVE, NULL);
		}
		else
			entry->off = BLCKSZ*lfc_ctl->used++;
	}
	LWLockRelease(lfc_lock);

	if (lfc_desc == 0)
 	{
		lfc_desc = BasicOpenFile(lfc_path, O_RDWR|O_CREAT);
		if (lfc_desc < 0) {
			elog(LOG, "Failed to open file cache %s: %m", lfc_path);
			lfc_size = 0; /* disable file cache */
			return;
		}
	}

	rc = pwrite(lfc_desc, buffer, BLCKSZ, entry->off);
	if (rc != BLCKSZ)
	{
		elog(INFO, "Failed to write file cache: %m");
		lfc_size = 0; /* disable file cache */
		return;
	}

	/* Place entry to the head of LRU list */
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	lfc_link(entry);
	LWLockRelease(lfc_lock);
}
