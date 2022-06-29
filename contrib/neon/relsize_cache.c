/*-------------------------------------------------------------------------
 *
 * relsize_cache.c
 *      Relation size cache for better zentih performance.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/neon/relsize_cache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pagestore_client.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "catalog/pg_tablespace_d.h"
#include "utils/dynahash.h"
#include "utils/guc.h"


typedef struct
{
	RelFileNode rnode;
	ForkNumber	forknum;
} RelTag;

typedef struct
{
	RelTag		tag;
	BlockNumber size;
} RelSizeEntry;

static HTAB *relsize_hash;
static LWLockId relsize_lock;
static int	relsize_hash_size;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/*
 * Size of a cache entry is 20 bytes. So this default will take about 1.2 MB,
 * which seems reasonable.
 */
#define DEFAULT_RELSIZE_HASH_SIZE (64 * 1024)

static void
neon_smgr_shmem_startup(void)
{
	static HASHCTL info;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	relsize_lock = (LWLockId) GetNamedLWLockTranche("neon_relsize");
	info.keysize = sizeof(RelTag);
	info.entrysize = sizeof(RelSizeEntry);
	relsize_hash = ShmemInitHash("neon_relsize",
								 relsize_hash_size, relsize_hash_size,
								 &info,
								 HASH_ELEM | HASH_BLOBS);
	LWLockRelease(AddinShmemInitLock);
}

bool
get_cached_relsize(RelFileNode rnode, ForkNumber forknum, BlockNumber *size)
{
	bool		found = false;

	if (relsize_hash_size > 0)
	{
		RelTag		tag;
		RelSizeEntry *entry;

		tag.rnode = rnode;
		tag.forknum = forknum;
		LWLockAcquire(relsize_lock, LW_SHARED);
		entry = hash_search(relsize_hash, &tag, HASH_FIND, NULL);
		if (entry != NULL)
		{
			*size = entry->size;
			found = true;
		}
		LWLockRelease(relsize_lock);
	}
	return found;
}

void
set_cached_relsize(RelFileNode rnode, ForkNumber forknum, BlockNumber size)
{
	if (relsize_hash_size > 0)
	{
		RelTag		tag;
		RelSizeEntry *entry;

		tag.rnode = rnode;
		tag.forknum = forknum;
		LWLockAcquire(relsize_lock, LW_EXCLUSIVE);
		entry = hash_search(relsize_hash, &tag, HASH_ENTER, NULL);
		entry->size = size;
		LWLockRelease(relsize_lock);
	}
}

void
update_cached_relsize(RelFileNode rnode, ForkNumber forknum, BlockNumber size)
{
	if (relsize_hash_size > 0)
	{
		RelTag		tag;
		RelSizeEntry *entry;
		bool		found;

		tag.rnode = rnode;
		tag.forknum = forknum;
		LWLockAcquire(relsize_lock, LW_EXCLUSIVE);
		entry = hash_search(relsize_hash, &tag, HASH_ENTER, &found);
		if (!found || entry->size < size)
			entry->size = size;
		LWLockRelease(relsize_lock);
	}
}

void
forget_cached_relsize(RelFileNode rnode, ForkNumber forknum)
{
	if (relsize_hash_size > 0)
	{
		RelTag		tag;

		tag.rnode = rnode;
		tag.forknum = forknum;
		LWLockAcquire(relsize_lock, LW_EXCLUSIVE);
		hash_search(relsize_hash, &tag, HASH_REMOVE, NULL);
		LWLockRelease(relsize_lock);
	}
}

void
relsize_hash_init(void)
{
	DefineCustomIntVariable("neon.relsize_hash_size",
							"Sets the maximum number of cached relation sizes for neon",
							NULL,
							&relsize_hash_size,
							DEFAULT_RELSIZE_HASH_SIZE,
							0,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	if (relsize_hash_size > 0)
	{
		RequestAddinShmemSpace(hash_estimate_size(relsize_hash_size, sizeof(RelSizeEntry)));
		RequestNamedLWLockTranche("neon_relsize", 1);

		prev_shmem_startup_hook = shmem_startup_hook;
		shmem_startup_hook = neon_smgr_shmem_startup;
	}
}
