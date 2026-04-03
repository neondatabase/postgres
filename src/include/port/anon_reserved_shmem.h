/*-------------------------------------------------------------------------
 *
 * anon_reserved_shmem.h
 *	  memfd-backed shared segments with reserved address space (buffer pool)
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * src/include/port/anon_reserved_shmem.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANON_RESERVED_SHMEM_H
#define ANON_RESERVED_SHMEM_H

#include "storage/pg_shmem.h"

extern void ReservedAnonymousSegmentCreate(int segment_id,
											MemoryMappingSizes *mapping);
extern bool ReservedAnonymousSegmentResize(int segment_id,
											MemoryMappingSizes *mapping,
											bool expanding);

#endif
