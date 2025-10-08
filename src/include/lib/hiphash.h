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

#define HIPNotPresent (-1)
#define HIPTryWithLocks (-2)

Size HIPGetSize(int nelements);
void HIPInit(HIPHashHeader *header, int nelements, int locktranche,
			 void *refarray, Size refstride, Size refcmpsz);

/*
 * Get this element from the table, without locking.
 *
 * Returns HIPNotPresent when the element is definitely not present.
 * Can return HIPTryWithLocks if the bucket isn't empty, but absence
 * of the hashed element could not be guaranteed due to e.g. potential
 * recent changes to the bucket.
 */
HIPEntryIndex HIPGetElementUnchecked(HIPHashHeader *header, uint32 hash,
									 void *searchelem);

/*
 * Find this exact element, with locking.
 *
 * Returns HIPNotPresent when the element is not found.
 */
HIPEntryIndex HIPGetElementChecked(HIPHashHeader *header, uint32 hash,
								   void *searchelem);

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
