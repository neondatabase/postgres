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
	ForkNumber	forknum;
	BlockNumber blkno;
} PerPageWalHashKey;

struct PerPageWalRecord;
typedef struct PerPageWalRecord PerPageWalRecord;
typedef struct PerPageWalRecord
{
	PerPageWalRecord *next; /* to greater lsn */
	PerPageWalRecord *prev; /* to lower lsn */
	XLogRecPtr lsn;
	XLogRecord *record;
} PerPageWalRecord;

typedef struct PerPageWalHashEntry
{
	PerPageWalHashKey key;
	PerPageWalRecord *newest;
	PerPageWalRecord *oldest;
} PerPageWalHashEntry;

void memstore_init(void);
void memstore_init_shmem(void);
void memstore_insert(PerPageWalHashKey key, XLogRecPtr lsn, XLogRecord *record);
PerPageWalRecord *memstore_get_oldest(PerPageWalHashKey *key);

#endif							/* _MEMSTORE_H */
