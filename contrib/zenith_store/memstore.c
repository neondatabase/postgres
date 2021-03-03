/*-------------------------------------------------------------------------
 *
 * memstore.c -- simplistic in-memory page storage.
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/zenith/memstore.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/lwlock.h"
#include "utils/hsearch.h"
#include "storage/shmem.h"

#include "memstore.h"

MemStore *memStore;

void
memstore_init()
{
	Size		size = 0;
	size = add_size(size, 1000*1000*1000);
	size = MAXALIGN(size);
	RequestAddinShmemSpace(size);

	RequestNamedLWLockTranche("memStoreLock", 1);
}

void
memstore_init_shmem()
{
	HASHCTL		info;
	bool		found;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(PageWalHashKey);
	info.entrysize = sizeof(PageWalHashEntry);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	memStore = ShmemInitStruct("memStore",
								 sizeof(MemStore),
								 &found);

	if (!found)
		memStore->lock = &(GetNamedLWLockTranche("memStoreLock"))->lock;

	memStore->pages = ShmemInitHash("memStorePAges",
		10*1000, /* minsize */
		20*1000, /* maxsize */
		&info, HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
}

void
memstore_insert(PageWalHashKey key, XLogRecPtr lsn, XLogRecord *record)
{
	bool found;
	PageWalHashEntry *hashEntry;
	XLogRecord *recordInShmem;
	PageWalRecordEntry *listEntry;

	/*
	 * XXX: could be done with one allocation, but that is prototype anyway so
	 * don't bother with offsetoff.
	 */
	listEntry = ShmemAlloc(sizeof(PageWalRecordEntry));
	listEntry->record = ShmemAlloc(record->xl_tot_len);
	memcpy(listEntry->record, record, record->xl_tot_len);

	LWLockAcquire(memStore->lock, LW_EXCLUSIVE);

	hashEntry = (PageWalHashEntry *) hash_search(memStore->pages, &key, HASH_ENTER, &found);

	PageWalRecordEntry *oldHead = hashEntry->next;
	hashEntry->next = listEntry;
	listEntry->next = oldHead;
	if (oldHead) {
		oldHead->prev = listEntry;
	}

	LWLockRelease(memStore->lock);
}


PageWalRecordEntry *
memstore_get_last(PageWalHashKey key)
{
	bool found;
	PageWalHashEntry *hashEntry;
	PageWalRecordEntry *result = NULL;

	LWLockAcquire(memStore->lock, LW_SHARED);

	hashEntry = (PageWalHashEntry *) hash_search(memStore->pages, &key, HASH_FIND, &found);

	if (found) {
		result = hashEntry->next;
	}

	LWLockRelease(memStore->lock);

	/*
	 * For now we never modify records or change their next field, so unlocked
	 * access would be okay.
	 */
	return result;
}
