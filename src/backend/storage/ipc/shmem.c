/*-------------------------------------------------------------------------
 *
 * shmem.c
 *	  create shared memory and initialize shared memory data structures.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/shmem.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * POSTGRES processes share one or more regions of shared memory.
 * The shared memory is created by a postmaster and is inherited
 * by each backend via fork() (or, in some ports, via other OS-specific
 * methods).  The routines in this file are used for allocating and
 * binding to shared memory data structures.
 *
 * NOTES:
 *		(a) There are three kinds of shared memory data structures
 *	available to POSTGRES: fixed-size structures, queues and hash
 *	tables.  Fixed-size structures contain things like global variables
 *	for a module and should never be allocated after the shared memory
 *	initialization phase.  Hash tables have a fixed maximum size, but
 *	their actual size can vary dynamically.  When entries are added
 *	to the table, more space is allocated.  Queues link data structures
 *	that have been allocated either within fixed-size structures or as hash
 *	buckets.  Each shared data structure has a string name to identify
 *	it (assigned in the module that declares it).
 *
 *		(b) During initialization, each module looks for its
 *	shared data structures in a hash table called the "Shmem Index".
 *	If the data structure is not present, the caller can allocate
 *	a new one and initialize it.  If the data structure is present,
 *	the caller "attaches" to the structure by initializing a pointer
 *	in the local address space.
 *		The shmem index has two purposes: first, it gives us
 *	a simple model of how the world looks when a backend process
 *	initializes.  If something is present in the shmem index,
 *	it is initialized.  If it is not, it is uninitialized.  Second,
 *	the shmem index allows us to allocate shared memory on demand
 *	instead of trying to preallocate structures and hard-wire the
 *	sizes and locations in header files.  If you are using a lot
 *	of shared memory in a lot of different places (and changing
 *	things during development), this is important.
 *
 *		(c) In standard Unix-ish environments, individual backends do not
 *	need to re-establish their local pointers into shared memory, because
 *	they inherit correct values of those variables via fork() from the
 *	postmaster.  However, this does not work in the EXEC_BACKEND case.
 *	In ports using EXEC_BACKEND, new backends have to set up their local
 *	pointers using the method described in (b) above.
 *
 *		(d) memory allocation model: shared memory can never be
 *	freed, once allocated.   Each hash table has its own free list,
 *	so hash buckets can be reused when an item is deleted.  However,
 *	if one hash table grows very large and then shrinks, its space
 *	cannot be redistributed to other tables.  We could build a simple
 *	hash bucket garbage collector if need be.  Right now, it seems
 *	unnecessary.
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port/pg_numa.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/injection_point.h"
#include "utils/wait_event.h"

/* Structure managing one shared memory segment. */
typedef struct ShmemSegment
{
	PGShmemHeader *ShmemSegHdr; /* shared mem segment header */
	slock_t    *ShmemLock;		/* spinlock for shared memory and LWLock
								 * allocation */
	void	   *ShmemBase;		/* start address of shared memory */
	const char *ShmemSegmentName;	/* name of the segment for logging */
} ShmemSegment;

ShmemSegment Segments[NUM_MEMORY_MAPPINGS];

/*
 * OSS compatibility: points at main segment allocator lock (same as
 * InhShmemSegs[MAIN_SHMEM_SEGMENT].ShmemLock after startup).
 */
slock_t    *ShmemLock = NULL;

static void *ShmemAllocRaw(ShmemSegment *segment, Size size, Size *allocated_size);
static void *ShmemAllocUnlockedInternal(ShmemSegment *segment, Size size);

/*
 * Primary index hashtable for shmem, for simplicity we use a single for all
 * shared memory segments. There can be performance consequences of that, and
 * an alternative option would be to have one index per shared memory segments.
 */
static HTAB *ShmemIndex = NULL;

/* To get reliable results for NUMA inquiry we need to "touch pages" once */
static bool firstNumaTouch = true;

Datum		pg_numa_available(PG_FUNCTION_ARGS);

/*
 *	InitShmemAccess() --- set up basic pointers in the given shared memory segment.
 *
 * These addresses are expected to be stable throughout the life of the process
 * even if the underlying segments get resized.
 */
void
InitShmemAccess(PGShmemHeader *seghdr)
{
	InitShmemAccessInSegment(MAIN_SHMEM_SEGMENT, seghdr, NULL);
}

void
InitShmemAccessInSegment(int segment_id, PGShmemHeader *seghdr,
						 slock_t *passedShmemLock)
{
	ShmemSegment *segment;

	Assert(segment_id >= 0 && segment_id < NUM_MEMORY_MAPPINGS);

	/*
	 * When called from Postmaster code after creating shared memory segment
	 * passedShmemLock is expected to be NULL; it will be created later. But a
	 * backend initialized under EXEC_BACKEND inherits already initialized
	 * lock.
	 */
	Assert((!IsUnderPostmaster && !passedShmemLock) ||
		   (IsUnderPostmaster && passedShmemLock));

	segment = &Segments[segment_id];

	segment->ShmemSegHdr = seghdr;
	segment->ShmemBase = (void *) seghdr;
	segment->ShmemLock = passedShmemLock;
	segment->ShmemSegmentName = MappingName(segment_id);

	/*
	 * EXEC_BACKEND children attach main lock before InitShmemAllocation runs.
	 */
	if (segment_id == MAIN_SHMEM_SEGMENT && segment->ShmemLock != NULL)
		ShmemLock = segment->ShmemLock;
}

slock_t *
InitShmemAllocation(void)
{
	return InitShmemAllocationInSegment(MAIN_SHMEM_SEGMENT);
}

/*
 *	InitShmemAllocation() --- set up shared-memory space allocation.
 *
 * This should be called only in the postmaster or a standalone backend.
 *
 * The function initializes the ShmemLock spinlock in the given segment, and
 * returns it.
 */
slock_t *
InitShmemAllocationInSegment(int segment_id)
{
	ShmemSegment *segment;
	PGShmemHeader *shmhdr;
	char	   *aligned;

	Assert(!IsUnderPostmaster);
	Assert(segment_id >= 0 && segment_id < NUM_MEMORY_MAPPINGS);

	segment = &Segments[segment_id];
	shmhdr = segment->ShmemSegHdr;

	/* This function should be called only once for every segment. */
	Assert(shmhdr != NULL);
	Assert(!segment->ShmemLock);

	/*
	 * Initialize the spinlock used by ShmemAlloc.  We must use
	 * ShmemAllocUnlocked, since obviously ShmemAlloc can't be called yet.
	 * Pass it back to the caller through inhseg, so that it can be shared
	 * with backends.
	 */
	segment->ShmemLock = (slock_t *) ShmemAllocUnlockedInternal(segment, sizeof(slock_t));

	SpinLockInit(segment->ShmemLock);

	if (segment_id == MAIN_SHMEM_SEGMENT)
		ShmemLock = segment->ShmemLock;

	/*
	 * Allocations after this point should go through ShmemAlloc, which
	 * expects to allocate everything on cache line boundaries.  Make sure the
	 * first allocation begins on a cache line boundary.
	 */
	aligned = (char *)
		(CACHELINEALIGN((((char *) shmhdr) + shmhdr->freeoffset)));
	shmhdr->freeoffset = aligned - (char *) shmhdr;

	/* ShmemIndex can't be set up yet (need LWLocks first) */
	shmhdr->index = NULL;
	Assert(!ShmemIndex);

	return segment->ShmemLock;
}

/*
 * ShmemAlloc --
 * 		allocate max-aligned chunk from given shared memory segment
 *
 * Throws error if request cannot be satisfied.
 *
 * Assumes ShmemLock and ShmemSegHdr in the given segment are initialized.
 */

static void *
ShmemAllocInternal(ShmemSegment *segment, Size size)
{
	void	   *newSpace;
	Size		allocated_size;

	newSpace = ShmemAllocRaw(segment, size, &allocated_size);
	if (!newSpace)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory in segment %s (%zu bytes requested)",
						segment->ShmemSegmentName, size)));
	return newSpace;
}

void *
ShmemAlloc(Size size)
{
	return ShmemAllocInSegment(MAIN_SHMEM_SEGMENT, size);
}

void *
ShmemAllocInSegment(int segment_id, Size size)
{
	Assert(segment_id >= 0 && segment_id < NUM_MEMORY_MAPPINGS);

	return ShmemAllocInternal(&Segments[segment_id], size);
}

/*
 * ShmemAllocNoError -- allocate max-aligned chunk from shared memory
 *
 * As ShmemAlloc, but returns NULL if out of space, rather than erroring.
 *
 * This is used as a memory allocation callback for hash tables created using
 * dynahash.c APIs. It's a bit of work to make the callback specify the segment
 * where to allocate the memory. For now, there is not need to create shared
 * memory hash tables in shared memory segments other than main memory segment.
 * Hence we do not support segment_id parameter here.
 */
void *
ShmemAllocNoError(Size size)
{
	Size		allocated_size;

	return ShmemAllocRaw(&Segments[MAIN_SHMEM_SEGMENT], size, &allocated_size);
}

/*
 * ShmemAllocRaw -- allocate align chunk and return allocated size
 *
 * Also sets *allocated_size to the number of bytes allocated, which will
 * be equal to the number requested plus any padding we choose to add.
 */
static void *
ShmemAllocRaw(ShmemSegment *segment, Size size, Size *allocated_size)
{
	Size		newStart;
	Size		newFree;
	void	   *newSpace;
	PGShmemHeader *shmhdr = segment->ShmemSegHdr;

	/*
	 * Ensure all space is adequately aligned.  We used to only MAXALIGN this
	 * space but experience has proved that on modern systems that is not good
	 * enough.  Many parts of the system are very sensitive to critical data
	 * structures getting split across cache line boundaries.  To avoid that,
	 * attempt to align the beginning of the allocation to a cache line
	 * boundary.  The calling code will still need to be careful about how it
	 * uses the allocated space - e.g. by padding each element in an array of
	 * structures out to a power-of-two size - but without this, even that
	 * won't be sufficient.
	 */
	size = CACHELINEALIGN(size);
	*allocated_size = size;

	Assert(shmhdr != NULL);

	SpinLockAcquire(segment->ShmemLock);

	newStart = shmhdr->freeoffset;
	newFree = newStart + size;
	if (newFree <= shmhdr->totalsize)
	{
		newSpace = (char *) segment->ShmemBase + newStart;
		shmhdr->freeoffset = newFree;
	}
	else
		newSpace = NULL;

	SpinLockRelease(segment->ShmemLock);

	/* note this assert is okay with newSpace == NULL */
	Assert(newSpace == (void *) CACHELINEALIGN(newSpace));

	return newSpace;
}

/*
 * ShmemAllocUnlockedInternal
 * 		allocate max-aligned chunk from given shared memory segment
 *
 * Internal version that takes a ShmemSegment pointer directly.
 *
 * We consider maxalign, rather than cachealign, sufficient here.
 */
static void *
ShmemAllocUnlockedInternal(ShmemSegment *segment, Size size)
{
	Size		newStart;
	Size		newFree;
	void	   *newSpace;
	PGShmemHeader *shmhdr = segment->ShmemSegHdr;

	/*
	 * Ensure allocated space is adequately aligned.
	 */
	size = MAXALIGN(size);

	Assert(shmhdr != NULL);

	newStart = shmhdr->freeoffset;

	newFree = newStart + size;
	if (newFree > shmhdr->totalsize)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory in segment %s (%zu bytes requested)",
						segment->ShmemSegmentName, size)));
	shmhdr->freeoffset = newFree;

	newSpace = (char *) segment->ShmemBase + newStart;

	Assert(newSpace == (void *) MAXALIGN(newSpace));

	return newSpace;
}

/*
 * ShmemAllocUnlocked
 * 		allocate max-aligned chunk from given shared memory segment
 *
 * Allocate space without locking ShmemLock.  This should be used for,
 * and only for, allocations that must happen before ShmemLock is ready.
 *
 * We consider maxalign, rather than cachealign, sufficient here.
 */
void *
ShmemAllocUnlocked(Size size)
{
	return ShmemAllocUnlockedInSegment(MAIN_SHMEM_SEGMENT, size);
}

void *
ShmemAllocUnlockedInSegment(int segment_id, Size size)
{
	ShmemSegment *segment;

	Assert(segment_id >= 0 && segment_id < NUM_MEMORY_MAPPINGS);
	segment = &Segments[segment_id];

	return ShmemAllocUnlockedInternal(segment, size);
}

/*
 * ShmemAddrIsValid
 * 		test if an address refers to the main shared memory segment.
 */
bool
ShmemAddrIsValid(const void *addr)
{
	return ShmemAddrIsValidInSegment(MAIN_SHMEM_SEGMENT, addr);
}

/*
 * ShmemAddrIsValidInSegment
 * 		test if an address refers to the given shared memory segment.
 */
bool
ShmemAddrIsValidInSegment(int segment_id, const void *addr)
{
	ShmemSegment *segment;
	void	   *shmemEnd;

	Assert(segment_id >= 0 && segment_id < NUM_MEMORY_MAPPINGS);

	segment = &Segments[segment_id];
	shmemEnd = (char *) segment->ShmemBase + segment->ShmemSegHdr->totalsize;

	return (addr >= segment->ShmemBase) && (addr < shmemEnd);
}

/*
 *	InitShmemIndex() --- set up or attach to shmem index table.
 */
void
InitShmemIndex(void)
{
	HASHCTL		info;

	/*
	 * Create the shared memory shmem index.
	 *
	 * Since ShmemInitHash calls ShmemInitStruct, which expects the ShmemIndex
	 * hashtable to exist already, we have a bit of a circularity problem in
	 * initializing the ShmemIndex itself.  The special "ShmemIndex" hash
	 * table name will tell ShmemInitStruct to fake it.
	 */
	info.keysize = SHMEM_INDEX_KEYSIZE;
	info.entrysize = sizeof(ShmemIndexEnt);

	ShmemIndex = ShmemInitHash("ShmemIndex",
							   SHMEM_INDEX_SIZE, SHMEM_INDEX_SIZE,
							   &info,
							   HASH_ELEM | HASH_STRINGS);
}

/*
 * ShmemInitHash -- Create and initialize, or attach to, a
 *		shared memory hash table.
 *
 * We assume caller is doing some kind of synchronization
 * so that two processes don't try to create/initialize the same
 * table at once.  (In practice, all creations are done in the postmaster
 * process; child processes should always be attaching to existing tables.)
 *
 * max_size is the estimated maximum number of hashtable entries.  This is
 * not a hard limit, but the access efficiency will degrade if it is
 * exceeded substantially (since it's used to compute directory size and
 * the hash table buckets will get overfull).
 *
 * init_size is the number of hashtable entries to preallocate.  For a table
 * whose maximum size is certain, this should be equal to max_size; that
 * ensures that no run-time out-of-shared-memory failures can occur.
 *
 * *infoP and hash_flags must specify at least the entry sizes and key
 * comparison semantics (see hash_create()).  Flag bits and values specific
 * to shared-memory hash tables are added here, except that callers may
 * choose to specify HASH_PARTITION and/or HASH_FIXED_SIZE.
 *
 * Note: before Postgres 9.0, this function returned NULL for some failure
 * cases.  Now, it always throws error instead, so callers need not check
 * for NULL.
 *
 * See prologue of ShmemAllocNoError for explanation about lack of segment_id
 * parameter.
 */
HTAB *
ShmemInitHash(const char *name,		/* table string name for shmem index */
			  long init_size,	/* initial table size */
			  long max_size,	/* max size of the table */
			  HASHCTL *infoP,	/* info about key and bucket size */
			  int hash_flags)	/* info about infoP */
{
	bool		found;
	void	   *location;

	/*
	 * Hash tables allocated in shared memory have a fixed directory; it can't
	 * grow or other backends wouldn't be able to find it. So, make sure we
	 * make it big enough to start with.
	 *
	 * The shared memory allocator must be specified too.
	 */
	infoP->dsize = infoP->max_dsize = hash_select_dirsize(max_size);
	infoP->alloc = ShmemAllocNoError;
	hash_flags |= HASH_SHARED_MEM | HASH_ALLOC | HASH_DIRSIZE;

	/* look it up in the shmem index */
	location = ShmemInitStructInSegment(name,
										hash_get_shared_size(infoP, hash_flags),
										&found, MAIN_SHMEM_SEGMENT);

	/*
	 * if it already exists, attach to it rather than allocate and initialize
	 * new space
	 */
	if (found)
		hash_flags |= HASH_ATTACH;

	/* Pass location of hashtable header to hash_create */
	infoP->hctl = (HASHHDR *) location;

	return hash_create(name, init_size, infoP, hash_flags);
}

/*
 * ShmemInitStruct -- Create/attach to a structure in shared memory.
 *
 *		This is called during initialization to find or allocate
 *		a data structure in shared memory.  If no other process
 *		has created the structure, this routine allocates space
 *		for it.  If it exists already, a pointer to the existing
 *		structure is returned.
 *
 *	Returns: pointer to the object.  *foundPtr is set true if the object was
 *		already in the shmem index (hence, already initialized).
 *
 *	Note: before Postgres 9.0, this function returned NULL for some failure
 *	cases.  Now, it always throws error instead, so callers need not check
 *	for NULL.
 */
void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	return ShmemInitStructInSegment(name, size, foundPtr, MAIN_SHMEM_SEGMENT);
}

void *
ShmemInitStructInSegment(const char *name, Size size, bool *foundPtr, int segment_id)
{
	ShmemIndexEnt *result;
	void	   *structPtr;
	ShmemSegment *segment;

	Assert(segment_id >= 0 && segment_id < NUM_MEMORY_MAPPINGS);

	segment = &Segments[segment_id];

	LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);

	if (!ShmemIndex)
	{
		PGShmemHeader *shmhdr = segment->ShmemSegHdr;

		/*
		 * Must be trying to create/attach to ShmemIndex itself in the main
		 * shared memory segment.
		 */
		Assert(segment_id == MAIN_SHMEM_SEGMENT);
		Assert(strcmp(name, "ShmemIndex") == 0);

		if (IsUnderPostmaster)
		{
			/* Must be initializing a (non-standalone) backend */
			Assert(shmhdr->index != NULL);
			structPtr = shmhdr->index;
			*foundPtr = true;
		}
		else
		{
			/*
			 * If the shmem index doesn't exist, we are bootstrapping: we must
			 * be trying to init the shmem index itself.
			 *
			 * Notice that the ShmemIndexLock is released before the shmem
			 * index has been initialized.  This should be OK because no other
			 * process can be accessing shared memory yet.
			 */
			Assert(shmhdr->index == NULL);
			structPtr = ShmemAllocInternal(segment, size);
			shmhdr->index = structPtr;
			*foundPtr = false;
		}
		LWLockRelease(ShmemIndexLock);
		return structPtr;
	}

	/* look it up in the shmem index */
	result = (ShmemIndexEnt *)
		hash_search(ShmemIndex, name, HASH_ENTER_NULL, foundPtr);

	if (!result)
	{
		LWLockRelease(ShmemIndexLock);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("could not create ShmemIndex entry for data structure \"%s\" in segment %d",
						name, segment_id)));
	}

	if (*foundPtr)
	{
		/*
		 * Structure is in the shmem index so someone else has allocated it
		 * already. The size better be the same as the size we are trying to
		 */
		if (result->size != size)
		{
			LWLockRelease(ShmemIndexLock);
			ereport(ERROR,
					(errmsg("ShmemIndex entry size is wrong for data structure"
							" \"%s\": expected %zu, actual %zu",
							name, size, result->size)));
		}

		structPtr = result->location;
	}
	else
	{
		Size		allocated_size;

		/* It isn't in the table yet. allocate and initialize it */
		structPtr = ShmemAllocRaw(segment, size, &allocated_size);
		if (structPtr == NULL)
		{
			/* out of memory; remove the failed ShmemIndex entry */
			hash_search(ShmemIndex, name, HASH_REMOVE, NULL);
			LWLockRelease(ShmemIndexLock);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("not enough shared memory for data structure"
							" \"%s\" (%zu bytes requested) (%zu bytes allocated)",
							name, size, allocated_size)));
		}
		result->size = size;
		result->allocated_size = allocated_size;
		result->location = structPtr;
		result->segment_id = segment_id;
	}

	LWLockRelease(ShmemIndexLock);

	Assert(ShmemAddrIsValidInSegment(segment_id, structPtr));

	Assert(structPtr == (void *) CACHELINEALIGN(structPtr));

	return structPtr;
}

/*
 * ShmemResizeStructInSegment -- Resize the given structure in shared memory.
 *
 * This function resizes an existing shared memory structure while preserving
 * the existing memory location.
 *
 * Returns: pointer to the existing structure location, if the resize is
 * successful, otherwise NULL.
 */
void *
ShmemResizeStructInSegment(const char *name, Size size, bool *foundPtr,
						   int segment_id)
{
	ShmemIndexEnt *result;
	void	   *structPtr;
	ShmemSegment *segment;
	PGShmemHeader *shmhdr;
	Size		allocated_size;
	Size		newFree;

	Assert(segment_id >= 0 && segment_id < NUM_MEMORY_MAPPINGS);
	Assert(segment_id != MAIN_SHMEM_SEGMENT);	/* main segment structures not
												 * resizable */
	Assert(ShmemIndex);
	Assert(size > 0);
	segment = &Segments[segment_id];
	shmhdr = segment->ShmemSegHdr;
	Assert(shmhdr != NULL);

	LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
	/* Look up the structure in the shmem index */
	result = (ShmemIndexEnt *)
		hash_search(ShmemIndex, name, HASH_FIND, foundPtr);

	Assert(*foundPtr);
	Assert(result);
	Assert(result->segment_id == segment_id);

	/* Save the existing structure pointer to be returned. */
	structPtr = result->location;

	/* Cachealign new size */
	allocated_size = CACHELINEALIGN(size);

	if (allocated_size == result->allocated_size)
	{
		result->size = size;
		/* No need to resize if the existing allocated size is sufficient */
		LWLockRelease(ShmemIndexLock);
		return structPtr;
	}

	SpinLockAcquire(segment->ShmemLock);

	/*
	 * The resizable structures are placed in their own segment after the
	 * header and the spinlock. Hence the memory location where they end are
	 * same as the start of free memory in that segment.
	 */
	Assert((char *) segment->ShmemBase + shmhdr->freeoffset == (char *) result->location + result->allocated_size);
	newFree = shmhdr->freeoffset + (allocated_size - result->allocated_size);
	if (newFree > shmhdr->totalsize)
	{
		structPtr = NULL;
	}
	else
	{
		shmhdr->freeoffset = newFree;
		result->size = size;
		result->allocated_size = allocated_size;
	}

	/*
	 * End of the structure should still be same as the start of free memory
	 * in the segment
	 */
	Assert((char *) segment->ShmemBase + shmhdr->freeoffset == (char *) result->location + result->allocated_size);

	SpinLockRelease(segment->ShmemLock);
	LWLockRelease(ShmemIndexLock);

	/* note this assert is okay with structPtr == NULL */
	Assert(structPtr == (void *) CACHELINEALIGN(structPtr));

	return structPtr;
}



/*
 * Add two Size values, checking for overflow
 */
Size
add_size(Size s1, Size s2)
{
	Size		result;

	result = s1 + s2;
	/* We are assuming Size is an unsigned type here... */
	if (result < s1 || result < s2)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested shared memory size overflows size_t")));
	return result;
}

/*
 * Multiply two Size values, checking for overflow
 */
Size
mul_size(Size s1, Size s2)
{
	Size		result;

	if (s1 == 0 || s2 == 0)
		return 0;
	result = s1 * s2;
	/* We are assuming Size is an unsigned type here... */
	if (result / s2 != s1)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested shared memory size overflows size_t")));
	return result;
}

/* SQL SRF showing allocated shared memory */
Datum
pg_get_shmem_allocations(PG_FUNCTION_ARGS)
{
#define PG_GET_SHMEM_SIZES_COLS 5
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	HASH_SEQ_STATUS hstat;
	ShmemIndexEnt *ent;
	Size		named_allocated[NUM_MEMORY_MAPPINGS] = {0};
	Datum		values[PG_GET_SHMEM_SIZES_COLS];
	bool		nulls[PG_GET_SHMEM_SIZES_COLS];
	int			i;

	InitMaterializedSRF(fcinfo, 0);

	LWLockAcquire(ShmemIndexLock, LW_SHARED);

	hash_seq_init(&hstat, ShmemIndex);

	/* output all allocated entries */
	memset(nulls, 0, sizeof(nulls));
	while ((ent = (ShmemIndexEnt *) hash_seq_search(&hstat)) != NULL)
	{
		ShmemSegment *segment = &Segments[ent->segment_id];
		PGShmemHeader *shmhdr = segment->ShmemSegHdr;

		values[0] = CStringGetTextDatum(ent->key);
		values[1] = CStringGetTextDatum(segment->ShmemSegmentName);
		values[2] = Int64GetDatum((char *) ent->location - (char *) shmhdr);
		values[3] = Int64GetDatum(ent->size);
		values[4] = Int64GetDatum(ent->allocated_size);
		named_allocated[ent->segment_id] += ent->allocated_size;

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	/* output shared memory allocated but not counted via the shmem index */
	for (i = 0; i < NUM_MEMORY_MAPPINGS; i++)
	{
		ShmemSegment *segment = &Segments[i];
		PGShmemHeader *shmhdr = segment->ShmemSegHdr;

		values[0] = CStringGetTextDatum("<anonymous>");
		values[1] = CStringGetTextDatum(segment->ShmemSegmentName);
		nulls[2] = true;
		values[3] = Int64GetDatum(shmhdr->freeoffset - named_allocated[i]);
		values[4] = values[3];
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	/* output as-of-yet unused shared memory */
	memset(nulls, 0, sizeof(nulls));

	for (i = 0; i < NUM_MEMORY_MAPPINGS; i++)
	{
		ShmemSegment *segment = &Segments[i];
		PGShmemHeader *shmhdr = segment->ShmemSegHdr;

		nulls[0] = true;
		values[1] = CStringGetTextDatum(segment->ShmemSegmentName);
		values[2] = Int64GetDatum(shmhdr->freeoffset);
		values[3] = Int64GetDatum(shmhdr->totalsize - shmhdr->freeoffset);
		values[4] = values[3];
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	LWLockRelease(ShmemIndexLock);

	return (Datum) 0;
}

/*
 * SQL SRF showing NUMA memory nodes for allocated shared memory
 *
 * Compared to pg_get_shmem_allocations(), this function does not return
 * information about shared anonymous allocations and unused shared memory.
 */
Datum
pg_get_shmem_allocations_numa(PG_FUNCTION_ARGS)
{
#define PG_GET_SHMEM_NUMA_SIZES_COLS 3
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	HASH_SEQ_STATUS hstat;
	ShmemIndexEnt *ent;
	Datum		values[PG_GET_SHMEM_NUMA_SIZES_COLS];
	bool		nulls[PG_GET_SHMEM_NUMA_SIZES_COLS];
	Size		os_page_size;
	void	  **page_ptrs;
	int		   *pages_status;
	uint64		shm_total_page_count = 0,
				shm_ent_page_count,
				max_nodes;
	Size	   *nodes;

	if (pg_numa_init() == -1)
		elog(ERROR, "libnuma initialization failed or NUMA is not supported on this platform");

	InitMaterializedSRF(fcinfo, 0);

	max_nodes = pg_numa_get_max_node();
	nodes = palloc(sizeof(Size) * (max_nodes + 2));

	/*
	 * Shared memory allocations can vary in size and may not align with OS
	 * memory page boundaries, while NUMA queries work on pages.
	 *
	 * To correctly map each allocation to NUMA nodes, we need to: 1.
	 * Determine the OS memory page size. 2. Align each allocation's start/end
	 * addresses to page boundaries. 3. Query NUMA node information for all
	 * pages spanning the allocation.
	 */
	os_page_size = pg_get_shmem_pagesize();

	/*
	 * Allocate memory for page pointers and status based on total shared
	 * memory size. This simplified approach allocates enough space for all
	 * pages in shared memory rather than calculating the exact requirements
	 * for each segment.
	 *
	 * Add 1, because we don't know how exactly the segments align to OS
	 * pages, so the allocation might use one more memory page. In practice
	 * this is not very likely, and moreover we have more entries, each of
	 * them using only fraction of the total pages.
	 */
	for (int segment = 0; segment < NUM_MEMORY_MAPPINGS; segment++)
	{
		PGShmemHeader *shmhdr = Segments[segment].ShmemSegHdr;

		shm_total_page_count += (shmhdr->totalsize / os_page_size) + 1;
	}
	page_ptrs = palloc0(sizeof(void *) * shm_total_page_count);
	pages_status = palloc(sizeof(int) * shm_total_page_count);

	if (firstNumaTouch)
		elog(DEBUG1, "NUMA: page-faulting shared memory segments for proper NUMA readouts");

	LWLockAcquire(ShmemIndexLock, LW_SHARED);

	hash_seq_init(&hstat, ShmemIndex);

	/* output all allocated entries */
	while ((ent = (ShmemIndexEnt *) hash_seq_search(&hstat)) != NULL)
	{
		int			i;
		char	   *startptr,
				   *endptr;
		Size		total_len;

		/*
		 * Calculate the range of OS pages used by this segment. The segment
		 * may start / end half-way through a page, we want to count these
		 * pages too. So we align the start/end pointers down/up, and then
		 * calculate the number of pages from that.
		 */
		startptr = (char *) TYPEALIGN_DOWN(os_page_size, ent->location);
		endptr = (char *) TYPEALIGN(os_page_size,
									(char *) ent->location + ent->allocated_size);
		total_len = (endptr - startptr);

		shm_ent_page_count = total_len / os_page_size;

		/*
		 * If we ever get 0xff (-1) back from kernel inquiry, then we probably
		 * have a bug in mapping buffers to OS pages.
		 */
		memset(pages_status, 0xff, sizeof(int) * shm_ent_page_count);

		/*
		 * Setup page_ptrs[] with pointers to all OS pages for this segment,
		 * and get the NUMA status using pg_numa_query_pages.
		 *
		 * In order to get reliable results we also need to touch memory
		 * pages, so that inquiry about NUMA memory node doesn't return -2
		 * (ENOENT, which indicates unmapped/unallocated pages).
		 */
		for (i = 0; i < shm_ent_page_count; i++)
		{
			page_ptrs[i] = startptr + (i * os_page_size);

			if (firstNumaTouch)
				pg_numa_touch_mem_if_required(page_ptrs[i]);

			CHECK_FOR_INTERRUPTS();
		}

		if (pg_numa_query_pages(0, shm_ent_page_count, page_ptrs, pages_status) == -1)
			elog(ERROR, "failed NUMA pages inquiry status: %m");

		/* Count number of NUMA nodes used for this shared memory entry */
		memset(nodes, 0, sizeof(Size) * (max_nodes + 2));

		for (i = 0; i < shm_ent_page_count; i++)
		{
			int			s = pages_status[i];

			/* Ensure we are adding only valid index to the array */
			if (s >= 0 && s <= max_nodes)
			{
				/* valid NUMA node */
				nodes[s]++;
				continue;
			}
			else if (s == -2)
			{
				/* -2 means ENOENT (e.g. page was moved to swap) */
				nodes[max_nodes + 1]++;
				continue;
			}

			elog(ERROR, "invalid NUMA node id outside of allowed range "
				 "[0, " UINT64_FORMAT "]: %d", max_nodes, s);
		}

		/* no NULLs for regular nodes */
		memset(nulls, 0, sizeof(nulls));

		/*
		 * Add one entry for each NUMA node, including those without allocated
		 * memory for this segment.
		 */
		for (i = 0; i <= max_nodes; i++)
		{
			values[0] = CStringGetTextDatum(ent->key);
			values[1] = i;
			values[2] = Int64GetDatum(nodes[i] * os_page_size);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								 values, nulls);
		}

		/* The last entry is used for pages without a NUMA node. */
		nulls[1] = true;
		values[0] = CStringGetTextDatum(ent->key);
		values[2] = Int64GetDatum(nodes[max_nodes + 1] * os_page_size);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	LWLockRelease(ShmemIndexLock);
	firstNumaTouch = false;

	return (Datum) 0;
}

/*
 * Determine the memory page size used for the shared memory segment.
 *
 * If the shared segment was allocated using huge pages, returns the size of
 * a huge page. Otherwise returns the size of regular memory page.
 *
 * This should be used only after the server is started.
 */
Size
pg_get_shmem_pagesize(void)
{
	Size		os_page_size;
#ifdef WIN32
	SYSTEM_INFO sysinfo;

	GetSystemInfo(&sysinfo);
	os_page_size = sysinfo.dwPageSize;
#else
	os_page_size = sysconf(_SC_PAGESIZE);
#endif

	Assert(IsUnderPostmaster);
	Assert(huge_pages_status != HUGE_PAGES_UNKNOWN);

	if (huge_pages_status == HUGE_PAGES_ON)
		GetHugePageSize(&os_page_size, NULL, NULL);

	return os_page_size;
}

Datum
pg_numa_available(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(pg_numa_init() != -1);
}

/* SQL SRF showing shared memory segments */
Datum
pg_get_shmem_segments(PG_FUNCTION_ARGS)
{
#define PG_GET_SHMEM_SEGS_COLS 5
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[PG_GET_SHMEM_SEGS_COLS];
	bool		nulls[PG_GET_SHMEM_SEGS_COLS];
	int			i;

	InitMaterializedSRF(fcinfo, 0);

	/* output all allocated entries */
	for (i = 0; i < NUM_MEMORY_MAPPINGS; i++)
	{
		ShmemSegment *segment = &Segments[i];
		PGShmemHeader *shmhdr = segment->ShmemSegHdr;
		int			j;

		if (shmhdr == NULL)
		{
			for (j = 0; j < PG_GET_SHMEM_SEGS_COLS; j++)
				nulls[j] = true;
		}
		else
		{
			memset(nulls, 0, sizeof(nulls));
			values[0] = Int32GetDatum(i);
			values[1] = CStringGetTextDatum(segment->ShmemSegmentName);
			values[2] = Int64GetDatum(shmhdr->totalsize);
			values[3] = Int64GetDatum(shmhdr->freeoffset);
			values[4] = Int64GetDatum(shmhdr->ReservedSize);
		}

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}
