/*-------------------------------------------------------------------------
 *
 * shmem_internal.h
 *	  Declarations shared between shmem.c and shmem_inspection.c.
 *
 * Not a stable extension API; keep use limited to the shared memory module.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/storage/shmem_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHMEM_INTERNAL_H
#define SHMEM_INTERNAL_H

#include "storage/pg_shmem.h"
#include "utils/hsearch.h"

typedef struct ShmemSegment
{
	PGShmemHeader *ShmemSegHdr;
	slock_t	   *ShmemLock;
	void	   *ShmemBase;
	const char *ShmemSegmentName;
} ShmemSegment;

extern ShmemSegment Segments[NUM_MEMORY_MAPPINGS];

extern HTAB *ShmemIndex;

extern bool firstNumaTouch;

#endif							/* SHMEM_INTERNAL_H */
