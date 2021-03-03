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
#include "fmgr.h"

#include "memstore.h"
#include "zenith_store.h"

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
	info.keysize = sizeof(PerPageWalHashKey);
	info.entrysize = sizeof(PerPageWalHashEntry);

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
memstore_insert(PerPageWalHashKey key, XLogRecPtr lsn, XLogRecord *record)
{
	bool found;
	PerPageWalHashEntry *hash_entry;
	PerPageWalRecord *list_entry;

	/*
	 * XXX: could be done with one allocation, but that is prototype anyway so
	 * don't bother with offsetoff.
	 */
	list_entry = ShmemAlloc(sizeof(PerPageWalRecord));
	list_entry->record = ShmemAlloc(record->xl_tot_len);
	list_entry->lsn = lsn;
	list_entry->prev = NULL;
	list_entry->next = NULL;
	memcpy(list_entry->record, record, record->xl_tot_len);

	LWLockAcquire(memStore->lock, LW_EXCLUSIVE);

	hash_entry = (PerPageWalHashEntry *) hash_search(memStore->pages, &key, HASH_ENTER, &found);

	if (!found)
	{
		Assert(!hash_entry->newest);
		Assert(!hash_entry->oldest);
	}

	PerPageWalRecord *prev_newest = hash_entry->newest;
	hash_entry->newest = list_entry;
	list_entry->prev = prev_newest;
	if (prev_newest)
		prev_newest->next = list_entry;
	if (!hash_entry->oldest)
		hash_entry->oldest = list_entry;

	LWLockRelease(memStore->lock);
}


PerPageWalRecord *
memstore_get_oldest(PerPageWalHashKey key)
{
	bool found;
	PerPageWalHashEntry *hash_entry;
	PerPageWalRecord *result = NULL;

	LWLockAcquire(memStore->lock, LW_SHARED);

	hash_entry = (PerPageWalHashEntry *) hash_search(memStore->pages, &key, HASH_FIND, &found);

	if (found) {
		result = hash_entry->oldest;
	}

	LWLockRelease(memStore->lock);

	/*
	 * For now we never modify records or change their next field, so unlocked
	 * access would be okay.
	 */
	return result;
}

PG_FUNCTION_INFO_V1(zenith_store_get_page);

Datum
zenith_store_get_page(PG_FUNCTION_ARGS)
{
	uint64		sysid = PG_GETARG_INT64(0);
	Oid			tspaceno = PG_GETARG_OID(1);
	Oid			dbno = PG_GETARG_OID(2);
	Oid			relno = PG_GETARG_OID(3);
	ForkNumber	forknum = PG_GETARG_INT32(4);
	int64		blkno = PG_GETARG_INT64(5);

	bytea	   *raw_page;
	char	   *raw_page_data;

	/* Initialize buffer to copy to */
	raw_page = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(raw_page, BLCKSZ + VARHDRSZ);
	raw_page_data = VARDATA(raw_page);

	PerPageWalHashKey key;
	memset(&key, '\0', sizeof(PerPageWalHashKey));
	key.system_identifier = sysid;
	key.rnode.spcNode = tspaceno;
	key.rnode.dbNode = dbno;
	key.rnode.relNode = relno;
	key.forknum = forknum;
	key.blkno = blkno;

	PerPageWalRecord *record = memstore_get_oldest(key);

	/* do recovery here */
	int n = 0;
	for (; record; record = record->next)
	{
		n++;
	}

	zenith_log(RequestTrace, "Got request: chain len is %d", n);

	PG_RETURN_BYTEA_P(raw_page);
}
