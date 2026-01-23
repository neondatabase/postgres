/*-------------------------------------------------------------------------
 *
 * pg_shmem.h
 *	  Platform-independent API for shared memory support.
 *
 * Every port is expected to support shared memory with approximately
 * SysV-ish semantics; in particular, a memory block is not anonymous
 * but has an ID, and we must be able to tell whether there are any
 * remaining processes attached to a block of a specified ID.
 *
 * To simplify life for the SysV implementation, the ID is assumed to
 * consist of two unsigned long values (these are key and ID in SysV
 * terms).  Other platforms may ignore the second value if they need
 * only one ID number.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pg_shmem.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SHMEM_H
#define PG_SHMEM_H

#include "port/atomics.h"
#include "storage/barrier.h"
#include "storage/dsm_impl.h"
#include "storage/procsignal.h"
#include "storage/spin.h"
#include "storage/shmem.h"
#include "utils/guc.h"

typedef struct MemoryMappingSizes
{
	Size shmem_req_size;		/* Required size of the segment */
	Size shmem_reserved; 		/* Required size of the reserved address space. */
} MemoryMappingSizes;

typedef struct PGShmemHeader	/* standard header for all Postgres shmem */
{
	int32		magic;			/* magic # to identify Postgres segments */
#define PGShmemMagic  679834894
	pid_t		creatorPID;		/* PID of creating process (set but unread) */
	Size		totalsize;		/* total size of segment */
	Size		freeoffset;		/* offset to first free space */
	dsm_handle	dsm_control;	/* ID of dynamic shared memory control seg */
	void	   *index;			/* pointer to ShmemIndex table */
#ifndef WIN32					/* Windows doesn't have useful inode#s */
	dev_t		device;			/* device data directory is on */
	ino_t		inode;			/* inode number of data directory */
#endif
} PGShmemHeader;

typedef struct ShmemSegment
{
	PGShmemHeader *ShmemSegHdr; 	/* shared mem segment header */
	void *ShmemBase; 				/* start address of shared memory */
	void *ShmemEnd; 				/* end+1 address of shared memory */
	slock_t    *ShmemLock; 			/* spinlock for shared memory and LWLock
									 * allocation */
	int segment_fd; 			/* fd for the backing anon file */
	unsigned long seg_id; 		/* IPC key */
	int shmem_segment;			/* TODO: Do we really need it? */
	Size shmem_size; 			/* Size of the actually used memory */
	Size shmem_reserved; 		/* Size of the reserved mapping */
	Pointer shmem; 				/* Pointer to the start of the mapped memory */
	Pointer seg_addr; 			/* SysV shared memory for the header */
} ShmemSegment;

/* Number of available segments for anonymous memory mappings */
#define NUM_MEMORY_MAPPINGS 5

extern PGDLLIMPORT ShmemSegment Segments[NUM_MEMORY_MAPPINGS];

/*
 * ShmemControl is shared between backends and helps to coordinate shared
 * memory resize.
 *
 * TODO: I think we need a lock to protect this structure. If we do so, do we
 * need to use atomic integers?
 */
typedef struct
{
	pg_atomic_flag		resize_in_progress; /* true if resizing is in progress. false otherwise. */
	pg_atomic_uint32	currentNBuffers; /* Original NBuffers value before resize started */
	pg_atomic_uint32	targetNBuffers;
	pid_t				coordinator;
} ShmemControl;

extern PGDLLIMPORT ShmemControl *ShmemCtrl;

/* The phases for shared memory resizing, used by for ProcSignal barrier. */
#define SHMEM_RESIZE_REQUESTED			0
#define SHMEM_RESIZE_START				1
#define SHMEM_RESIZE_DONE				2

/* GUC variables */
extern PGDLLIMPORT int shared_memory_type;
extern PGDLLIMPORT int huge_pages;
extern PGDLLIMPORT int huge_page_size;
extern PGDLLIMPORT int huge_pages_status;
extern PGDLLIMPORT bool finalMaxNBuffers;
extern PGDLLIMPORT int MaxNBuffers;

/* Possible values for huge_pages and huge_pages_status */
typedef enum
{
	HUGE_PAGES_OFF,
	HUGE_PAGES_ON,
	HUGE_PAGES_TRY,				/* only for huge_pages */
	HUGE_PAGES_UNKNOWN,			/* only for huge_pages_status */
}			HugePagesType;

/* Possible values for shared_memory_type */
typedef enum
{
	SHMEM_TYPE_WINDOWS,
	SHMEM_TYPE_SYSV,
	SHMEM_TYPE_MMAP,
}			PGShmemType;

#ifndef WIN32
extern PGDLLIMPORT unsigned long UsedShmemSegID;
#else
extern PGDLLIMPORT HANDLE UsedShmemSegID;
extern PGDLLIMPORT void *ShmemProtectiveRegion;
#endif
extern PGDLLIMPORT void *UsedShmemSegAddr;

#if !defined(WIN32) && !defined(EXEC_BACKEND)
#define DEFAULT_SHARED_MEMORY_TYPE SHMEM_TYPE_MMAP
#elif !defined(WIN32)
#define DEFAULT_SHARED_MEMORY_TYPE SHMEM_TYPE_SYSV
#else
#define DEFAULT_SHARED_MEMORY_TYPE SHMEM_TYPE_WINDOWS
#endif

#ifdef EXEC_BACKEND
extern void PGSharedMemoryReAttach(void);
extern void PGSharedMemoryNoReAttach(void);
#endif

/*
 * round off mapping size to a multiple of a typical page size.
 */
static inline void
round_off_mapping_sizes(MemoryMappingSizes *mapping_sizes)
{
	mapping_sizes->shmem_req_size = add_size(mapping_sizes->shmem_req_size, 8192 - (mapping_sizes->shmem_req_size % 8192));
	mapping_sizes->shmem_reserved = add_size(mapping_sizes->shmem_reserved, 8192 - (mapping_sizes->shmem_reserved % 8192));
}


extern PGShmemHeader *PGSharedMemoryCreate(MemoryMappingSizes *mapping_sizes, int segment_id,
										   PGShmemHeader **shim);
extern bool PGSharedMemoryIsInUse(unsigned long id1, unsigned long id2);
extern void PGSharedMemoryDetach(void);
extern const char *MappingName(int shmem_segment);
extern void GetHugePageSize(Size *hugepagesize, int *mmap_flags,
							int *memfd_flags);
void PrepareHugePages(void);

bool ProcessBarrierShmemResize(Barrier *barrier);
const char *show_shared_buffers(void);
bool check_shared_buffers(int *newval, void **extra, GucSource source);
void AdjustShmemSize(void);
extern void WaitOnShmemBarrier(void);
extern void ShmemControlInit(void);

/*
 * To be able to dynamically resize largest parts of the data stored in shared
 * memory, we split it into multiple shared memory mappings segments. Each
 * segment contains only certain part of the data, which size depends on
 * NBuffers.
 */

/* The main segment, contains everything except buffer blocks and related data. */
#define MAIN_SHMEM_SEGMENT 0

/* Buffer blocks */
#define BUFFERS_SHMEM_SEGMENT 1

/* Buffer descriptors */
#define BUFFER_DESCRIPTORS_SHMEM_SEGMENT 2

/* Condition variables for buffers */
#define BUFFER_IOCV_SHMEM_SEGMENT 3

/* Checkpoint BufferIds */
#define CHECKPOINT_BUFFERS_SHMEM_SEGMENT 4

#endif							/* PG_SHMEM_H */
