/*-------------------------------------------------------------------------
 *
 * buf_table.c
 *	  routines for mapping BufferTags to buffer indexes.
 *
 * Note: the routines in this file do no locking of their own.  The caller
 * must hold a suitable lock on the appropriate BufMappingLock, as specified
 * in the comments.  We can't do the locking inside these functions because
 * in most cases the caller needs to adjust the buffer header contents
 * before the lock is released (see notes in README).
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "storage/buf_internals.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "utils/rel.h"
#include "utils/builtins.h"

/* entry for buffer lookup hashtable */
typedef struct
{
	BufferTag	key;			/* Tag of a disk page */
	int			id;				/* Associated buffer ID */
} BufferLookupEnt;

static HTAB *SharedBufHash;


/*
 * Estimate space needed for mapping hashtable
 *		size is the desired hash table size (possibly more than NBuffers)
 */
Size
BufTableShmemSize(int size)
{
	return hash_estimate_size(size, sizeof(BufferLookupEnt));
}

/*
 * Initialize shmem hash table for mapping buffers
 *		size is the desired hash table size (possibly more than NBuffers)
 */
void
InitBufTable(int size)
{
	HASHCTL		info;

	/* assume no locking is needed yet */

	/* BufferTag maps to Buffer */
	info.keysize = sizeof(BufferTag);
	info.entrysize = sizeof(BufferLookupEnt);
	info.num_partitions = NUM_BUFFER_PARTITIONS;

	/*
	 * The shared buffer look up table is set up only once with maximum possible
	 * entries considering maximum size of the buffer pool. It is not resized
	 * after that even if the buffer pool is resized. Hence it is allocated in
	 * the main shared memory segment and not in a resizeable shared memory
	 * segment.
	 */
	SharedBufHash = ShmemInitHashInSegment("Shared Buffer Lookup Table",
								  size, size,
								  &info,
								  HASH_ELEM | HASH_BLOBS | HASH_PARTITION | HASH_FIXED_SIZE,
								  MAIN_SHMEM_SEGMENT);
}

/*
 * BufTableHashCode
 *		Compute the hash code associated with a BufferTag
 *
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 */
uint32
BufTableHashCode(BufferTag *tagPtr)
{
	return get_hash_value(SharedBufHash, tagPtr);
}

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 */
int
BufTableLookup(BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result;

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_FIND,
									NULL);

	if (!result)
		return -1;

	return result->id;
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
int
BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id)
{
	BufferLookupEnt *result;
	bool		found;

	Assert(buf_id >= 0);		/* -1 is reserved for not-in-table */
	Assert(tagPtr->blockNum != P_NEW);	/* invalid tag */

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_ENTER,
									&found);

	if (found)					/* found something already in the table */
		return result->id;

	result->id = buf_id;

	return -1;
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
void
BufTableDelete(BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result;

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_REMOVE,
									NULL);

	if (!result)				/* shouldn't happen */
		elog(ERROR, "shared buffer hash table corrupted");
}

/*
 * BufTableGetContents
 *		Fill the given tuplestore with contents of the shared buffer lookup table
 *
 * This function is used by pg_buffercache extension to expose buffer lookup
 * table contents via SQL. The caller is responsible for setting up the
 * tuplestore and result set info.
 */
void
BufTableGetContents(Tuplestorestate *tupstore, TupleDesc tupdesc)
{
/* Expected number of attributes of the buffer lookup table entry. */
#define BUFTABLE_CONTENTS_COLS 6

	HASH_SEQ_STATUS hstat;
	BufferLookupEnt *ent;
	Datum		values[BUFTABLE_CONTENTS_COLS];
	bool		nulls[BUFTABLE_CONTENTS_COLS];
	int			i;

	memset(nulls, 0, sizeof(nulls));

	Assert(tupdesc->natts == BUFTABLE_CONTENTS_COLS);

	/*
	 * Lock all buffer mapping partitions to ensure a consistent view of the
	 * hash table during the scan. Must grab LWLocks in partition-number order
	 * to avoid LWLock deadlock.
	 */
	for (i = 0; i < NUM_BUFFER_PARTITIONS; i++)
		LWLockAcquire(BufMappingPartitionLockByIndex(i), LW_SHARED);

	hash_seq_init(&hstat, SharedBufHash);
	while ((ent = (BufferLookupEnt *) hash_seq_search(&hstat)) != NULL)
	{
		values[0] = ObjectIdGetDatum(ent->key.spcOid);
		values[1] = ObjectIdGetDatum(ent->key.dbOid);
		values[2] = ObjectIdGetDatum(ent->key.relNumber);
		values[3] = ObjectIdGetDatum(ent->key.forkNum);
		values[4] = Int64GetDatum(ent->key.blockNum);
		values[5] = Int32GetDatum(ent->id);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/*
	 * Release all buffer mapping partition locks in the reverse order so as
	 * to avoid LWLock deadlock.
	 */
	for (i = NUM_BUFFER_PARTITIONS - 1; i >= 0; i--)
		LWLockRelease(BufMappingPartitionLockByIndex(i));
}
