/*-------------------------------------------------------------------------
 *
 * memstore.h
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/zenith/memstore.h
 *-------------------------------------------------------------------------
 */
#ifndef _MEMSTORE_H
#define _MEMSTORE_H

#include "postgres.h"

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "access/xlogrecord.h"
#include "storage/lwlock.h"
#include "utils/hsearch.h"

typedef struct
{
	LWLock	   *lock;
	HTAB	   *pages;
} MemStore;

/*
 * NB: this struct is used as hash key, so memset '\0' before hash insert
 * may be beneficial.
 */
typedef struct
{
	uint64		system_identifier;
	RelFileNode rnode;
	BlockNumber blkno;
} PageWalHashKey;

struct PageWalRecordEntry;
typedef struct PageWalRecordEntry PageWalRecordEntry;

typedef struct PageWalRecordEntry
{
	PageWalRecordEntry *next;
	PageWalRecordEntry *prev;
	XLogRecPtr lsn;
	XLogRecord *record;
} PageWalRecordEntry;

typedef struct PageWalHashEntry
{
	PageWalHashKey key;
	struct PageWalRecordEntry *next;
} PageWalHashEntry;

void memstore_init(void);
void memstore_init_shmem(void);
void memstore_insert(PageWalHashKey key, XLogRecPtr lsn, XLogRecord *record);
PageWalRecordEntry *memstore_get_last(PageWalHashKey key);

#endif							/* _MEMSTORE_H */
