/*-------------------------------------------------------------------------
 *
 * buf_table_flat.c
 *	  Index-based shared buffer mapping table: one chain node per buffer
 *	  slot, flat bucket head array, integer successor links.
 *
 * See buf_table.c for locking contract.  Used when buffer_mapping_flat is on.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_table_flat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "storage/buf_table_flat.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"
#include "utils/rel.h"

#define BUF_TABLE_CHAIN_END		(-1)
#define BUF_TABLE_NOT_IN_CHAIN	(-2)

typedef struct BufTableEntry
{
	BufferTag	key;
	int			next;
} BufTableEntry;

static int *BufTableBucketHeads;
static BufTableEntry *BufTableEntries;
static int	BufTableNumBuckets;
static int	BufTableNumEntries;

/*
 * Bucket count mirrors dynahash init_htab: next power of two >= nelem hint,
 * then at least NUM_BUFFER_PARTITIONS (partition lock alignment).
 */
static int
buf_table_flat_compute_nbuckets(int nelem_hint)
{
	uint32		nbuckets;

	if (nelem_hint < 1)
		nelem_hint = 1;

	nbuckets = 1;
	while ((long) nbuckets < (long) nelem_hint)
		nbuckets <<= 1;

	while (nbuckets < (uint32) NUM_BUFFER_PARTITIONS)
		nbuckets <<= 1;

	Assert((nbuckets & (nbuckets - 1)) == 0);

	return (int) nbuckets;
}

static inline int
buf_table_flat_bucket_index(uint32 hashcode)
{
	return (int) (hashcode & (uint32) (BufTableNumBuckets - 1));
}

Size
BufTableFlat_ShmemSize(int size)
{
	int			nbuckets;
	Size		bucket_bytes;
	Size		entry_bytes;

	nbuckets = buf_table_flat_compute_nbuckets(size);
	bucket_bytes = MAXALIGN(sizeof(int) * (Size) nbuckets);
	entry_bytes = MAXALIGN(sizeof(BufTableEntry) * (Size) MaxNBuffers);

	return add_size(bucket_bytes, entry_bytes);
}

void
BufTableFlat_Init(int size)
{
	bool		found;
	char	   *base;
	Size		bucket_bytes;
	Size		entry_bytes;
	Size		total;
	int			i;

	BufTableNumEntries = MaxNBuffers;
	BufTableNumBuckets = buf_table_flat_compute_nbuckets(size);

	bucket_bytes = MAXALIGN(sizeof(int) * (Size) BufTableNumBuckets);
	entry_bytes = MAXALIGN(sizeof(BufTableEntry) * (Size) BufTableNumEntries);
	total = add_size(bucket_bytes, entry_bytes);

	base = (char *) ShmemInitStruct("Shared Buffer Lookup Table", total, &found);

	BufTableBucketHeads = (int *) base;
	BufTableEntries = (BufTableEntry *) (base + bucket_bytes);

	if (!found)
	{
		for (i = 0; i < BufTableNumBuckets; i++)
			BufTableBucketHeads[i] = BUF_TABLE_CHAIN_END;

		MemSet(BufTableEntries, 0, sizeof(BufTableEntry) * BufTableNumEntries);

		for (i = 0; i < BufTableNumEntries; i++)
			BufTableEntries[i].next = BUF_TABLE_NOT_IN_CHAIN;
	}
}

uint32
BufTableFlat_HashCode(BufferTag *tagPtr)
{
	return tag_hash(tagPtr, sizeof(BufferTag));
}

int
BufTableFlat_Lookup(BufferTag *tagPtr, uint32 hashcode)
{
	int			bucket;
	int			id;

	bucket = buf_table_flat_bucket_index(hashcode);

	for (id = BufTableBucketHeads[bucket];
		 id != BUF_TABLE_CHAIN_END;
		 id = BufTableEntries[id].next)
	{
		if (BufferTagsEqual(tagPtr, &BufTableEntries[id].key))
			return id;
	}

	return -1;
}

int
BufTableFlat_Insert(BufferTag *tagPtr, uint32 hashcode, int buf_id)
{
	int			bucket;
	int			id;

	Assert(buf_id >= 0);
	Assert(buf_id < BufTableNumEntries);
	Assert(tagPtr->blockNum != P_NEW);

	bucket = buf_table_flat_bucket_index(hashcode);

	for (id = BufTableBucketHeads[bucket];
		 id != BUF_TABLE_CHAIN_END;
		 id = BufTableEntries[id].next)
	{
		if (BufferTagsEqual(tagPtr, &BufTableEntries[id].key))
			return id;
	}

	if (BufTableEntries[buf_id].next != BUF_TABLE_NOT_IN_CHAIN)
		elog(ERROR, "shared buffer hash table corrupted");

	BufTableEntries[buf_id].key = *tagPtr;
	BufTableEntries[buf_id].next = BufTableBucketHeads[bucket];
	BufTableBucketHeads[bucket] = buf_id;

	return -1;
}

void
BufTableFlat_Delete(BufferTag *tagPtr, uint32 hashcode)
{
	int			bucket;
	int			id;
	int			prev = BUF_TABLE_CHAIN_END;

	bucket = buf_table_flat_bucket_index(hashcode);

	id = BufTableBucketHeads[bucket];
	while (id != BUF_TABLE_CHAIN_END)
	{
		if (BufferTagsEqual(tagPtr, &BufTableEntries[id].key))
		{
			if (prev == BUF_TABLE_CHAIN_END)
				BufTableBucketHeads[bucket] = BufTableEntries[id].next;
			else
				BufTableEntries[prev].next = BufTableEntries[id].next;

			BufTableEntries[id].next = BUF_TABLE_NOT_IN_CHAIN;
			ClearBufferTag(&BufTableEntries[id].key);
			return;
		}

		prev = id;
		id = BufTableEntries[id].next;
	}

	elog(ERROR, "shared buffer hash table corrupted");
}

void
BufTableFlat_GetContents(Tuplestorestate *tupstore, TupleDesc tupdesc)
{
#define BUFTABLE_CONTENTS_COLS 6

	Datum		values[BUFTABLE_CONTENTS_COLS];
	bool		nulls[BUFTABLE_CONTENTS_COLS];
	int			b;
	int			id;

	memset(nulls, 0, sizeof(nulls));

	Assert(tupdesc->natts == BUFTABLE_CONTENTS_COLS);

	for (b = 0; b < NUM_BUFFER_PARTITIONS; b++)
		LWLockAcquire(BufMappingPartitionLockByIndex(b), LW_SHARED);

	for (b = 0; b < BufTableNumBuckets; b++)
	{
		for (id = BufTableBucketHeads[b];
			 id != BUF_TABLE_CHAIN_END;
			 id = BufTableEntries[id].next)
		{
			BufTableEntry *ent = &BufTableEntries[id];

			values[0] = ObjectIdGetDatum(ent->key.spcOid);
			values[1] = ObjectIdGetDatum(ent->key.dbOid);
			values[2] = ObjectIdGetDatum(ent->key.relNumber);
			values[3] = ObjectIdGetDatum(ent->key.forkNum);
			values[4] = Int64GetDatum(ent->key.blockNum);
			values[5] = Int32GetDatum(id);

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	for (b = NUM_BUFFER_PARTITIONS - 1; b >= 0; b--)
		LWLockRelease(BufMappingPartitionLockByIndex(b));
}
