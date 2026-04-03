/*-------------------------------------------------------------------------
 *
 * shmem.h
 *	  shared memory management structures
 *
 * Historical note:
 * A long time ago, Postgres' shared memory region was allowed to be mapped
 * at a different address in each process, and shared memory "pointers" were
 * passed around as offsets relative to the start of the shared memory region.
 * That is no longer the case: each process must map the shared memory region
 * at the same address.  This means shared memory pointers can be passed
 * around directly between different processes.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/shmem.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHMEM_H
#define SHMEM_H

#include "storage/spin.h"
#include "utils/hsearch.h"


/* shmem.c */
typedef struct PGShmemHeader PGShmemHeader; /* avoid including
											 * storage/pg_shmem.h here */

/*
 * Main-segment API matches community PostgreSQL.  Multi-segment variants
 * use *InSegment() and an explicit segment id.
 */
extern void InitShmemAccess(PGShmemHeader *seghdr);
extern void InitShmemAccessInSegment(int segment_id, PGShmemHeader *seghdr,
									 slock_t *passedShmemLock);
extern slock_t *InitShmemAllocation(void);
extern slock_t *InitShmemAllocationInSegment(int segment_id);
extern void *ShmemAlloc(Size size);
extern void *ShmemAllocInSegment(int segment_id, Size size);
extern void *ShmemAllocNoError(Size size);
extern void *ShmemAllocUnlocked(Size size);
extern void *ShmemAllocUnlockedInSegment(int segment_id, Size size);
extern bool ShmemAddrIsValid(const void *addr);
extern bool ShmemAddrIsValidInSegment(int segment_id, const void *addr);

/* Spinlock protecting main-segment ShmemAlloc; same as main segment's lock */
extern PGDLLIMPORT slock_t *ShmemLock;
extern void InitShmemIndex(void);
extern HTAB *ShmemInitHash(const char *name, long init_size, long max_size,
						   HASHCTL *infoP, int hash_flags);
extern void *ShmemInitStruct(const char *name, Size size, bool *foundPtr);
extern void *ShmemInitStructInSegment(const char *name, Size size,
									  bool *foundPtr, int segment_id);
extern void *ShmemResizeStructInSegment(const char *name, Size size,
										bool *foundPtr, int segment_id);
extern Size add_size(Size s1, Size s2);
extern Size mul_size(Size s1, Size s2);

extern PGDLLIMPORT Size pg_get_shmem_pagesize(void);


/* ipci.c */
extern void RequestAddinShmemSpace(Size size);

/* size constants for the shmem index table */
 /* max size of data structure string name */
#define SHMEM_INDEX_KEYSIZE		 (48)
 /* estimated size of the shmem index table (not a hard limit) */
#define SHMEM_INDEX_SIZE		 (64)

/* this is a hash bucket in the shmem index table */
typedef struct
{
	char		key[SHMEM_INDEX_KEYSIZE];	/* string name */
	void	   *location;		/* location in shared mem */
	Size		size;			/* # bytes requested for the structure */
	Size		allocated_size; /* # bytes actually allocated */
	int			segment_id;		/* segment in which the structure is allocated */
} ShmemIndexEnt;

#endif							/* SHMEM_H */
