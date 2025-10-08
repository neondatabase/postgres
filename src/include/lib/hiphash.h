/*-------------------------------------------------------------------------
 *
 * hiphash.h
 *	  Concurrent hash table backed by a single shared memory allocation.
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/include/lib/hiphash.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef HIPHASH_H
#define HIPHASH_H
#include "storage/lwlock.h"

#define NUM_HIP_PARTITIONS NUM_BUFFER_PARTITIONS

typedef struct HIPHashHeader HIPHashHeader;
typedef int32 HIPEntryIndex;

Size HIPGetSize(int nelements);
void HIPInit(HIPHashHeader *header, int nelements, int locktranche);

/* Get this element from the table. No locks involved. */
HIPEntryIndex HIPGetElementUnchecked(HIPHashHeader *header, uint32 hash);
/*
 * Find this exact element in the provided elemsarray with element size
 * of stride and compare prefix of cmpsize, offset by HIPEntryIndex.
 *
 * Requires lock. Returns NULL when not found.
 */
void * HIPGetElementChecked(HIPHashHeader *header, uint32 hash,
							void *searchelem, void *elemsarray,
							Size stride, Size cmpsize);

/*
 * Insert this entry into the HIP hash table.
 *
 * Caller is responsible for locking.
 */
HIPEntryIndex HIPInsertElement(HIPHashHeader *header, uint32 hash,
							   HIPEntryIndex entry);

/*
 * Delete this element from the hash table.
 *
 * Caller is responsible for locking.
 */
void HIPRemoveElement(HIPHashHeader *header, uint32 hash,
					  HIPEntryIndex entry);

#endif /* HIPHASH_H */
