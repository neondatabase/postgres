/*-------------------------------------------------------------------------
 *
 * ipci.c
 *	  POSTGRES inter-process communication initialization code.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/ipci.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/subtrans.h"
#include "access/syncscan.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xlogprefetcher.h"
#include "access/xlogrecovery.h"
#include "commands/async.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/walsummarizer.h"
#include "replication/logicallauncher.h"
#include "replication/origin.h"
#include "replication/slot.h"
#include "replication/slotsync.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#if PG_MAJORVERSION_NUM >= 18
#include "storage/aio_subsys.h"
#include "storage/aio.h"
#endif
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/dsm_registry.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "utils/guc.h"
#include "utils/injection_point.h"

/* GUCs */
int			shared_memory_type = DEFAULT_SHARED_MEMORY_TYPE;

shmem_startup_hook_type shmem_startup_hook = NULL;

static Size total_addin_request = 0;

static void CreateOrAttachShmemStructs(void);

/*
 * RequestAddinShmemSpace
 *		Request that extra shmem space be allocated for use by
 *		a loadable module.
 *
 * This may only be called via the shmem_request_hook of a library that is
 * loaded into the postmaster via shared_preload_libraries.  Calls from
 * elsewhere will fail.
 */
void
RequestAddinShmemSpace(Size size)
{
	if (!process_shmem_requests_in_progress)
		elog(FATAL, "cannot request additional shared memory outside shmem_request_hook");
	total_addin_request = add_size(total_addin_request, size);
}

/*
 * CalculateShmemSize
 * 		Calculates the amount of shared memory needed.
 *
 * The amount of shared memory required per segment is saved in mapping_sizes,
 * which is expected to be an array of size NUM_MEMORY_MAPPINGS. The total
 * amount of memory needed across all the segments is returned. For the memory
 * mappings which reserve address space for future expansion, the required
 * amount of reserved space is saved in mapping_sizes of those segments.
 * This memory is not included in the returned value.
 */
Size
CalculateShmemSize(MemoryMappingSizes *mapping_sizes)
{
	Size		size;

	/*
	 * Size of the Postgres shared-memory block is estimated via moderately-
	 * accurate estimates for the big hogs, plus 100K for the stuff that's too
	 * small to bother with estimating.
	 *
	 * We take some care to ensure that the total size request doesn't
	 * overflow size_t.  If this gets through, we don't need to be so careful
	 * during the actual allocation phase.
	 */
	size = 100000;
	size = add_size(size, PGSemaphoreShmemSize(ProcGlobalSemas()));
	size = add_size(size, hash_estimate_size(SHMEM_INDEX_SIZE,
											 sizeof(ShmemIndexEnt)));
	size = add_size(size, dsm_estimate_size());
	size = add_size(size, DSMRegistryShmemSize());

	/*
	 * Buffer manager adds estimates for memory requirements for every shared
	 * memory segment that it uses in the corresponding AnonymousMappings.
	 * Consider size required from only the main shared memory segment here.
	 */
	size = add_size(size, BufferManagerShmemSize(mapping_sizes));
	size = add_size(size, LockManagerShmemSize());
	size = add_size(size, PredicateLockShmemSize());
	size = add_size(size, ProcGlobalShmemSize());
	size = add_size(size, XLogPrefetchShmemSize());
	size = add_size(size, VarsupShmemSize());
	size = add_size(size, XLOGShmemSize());
	size = add_size(size, XLogRecoveryShmemSize());
	size = add_size(size, CLOGShmemSize());
	size = add_size(size, CommitTsShmemSize());
	size = add_size(size, SUBTRANSShmemSize());
	size = add_size(size, TwoPhaseShmemSize());
	size = add_size(size, BackgroundWorkerShmemSize());
	size = add_size(size, MultiXactShmemSize());
	size = add_size(size, LWLockShmemSize());
	size = add_size(size, ProcArrayShmemSize());
	size = add_size(size, BackendStatusShmemSize());
	size = add_size(size, SharedInvalShmemSize());
	size = add_size(size, PMSignalShmemSize());
	size = add_size(size, ProcSignalShmemSize());
	size = add_size(size, CheckpointerShmemSize());
	size = add_size(size, AutoVacuumShmemSize());
	size = add_size(size, ReplicationSlotsShmemSize());
	size = add_size(size, ReplicationOriginShmemSize());
	size = add_size(size, WalSndShmemSize());
	size = add_size(size, WalRcvShmemSize());
	size = add_size(size, WalSummarizerShmemSize());
	size = add_size(size, PgArchShmemSize());
	size = add_size(size, ApplyLauncherShmemSize());
	size = add_size(size, BTreeShmemSize());
	size = add_size(size, SyncScanShmemSize());
	size = add_size(size, AsyncShmemSize());
	size = add_size(size, StatsShmemSize());
	size = add_size(size, WaitEventCustomShmemSize());
	size = add_size(size, InjectionPointShmemSize());
	size = add_size(size, SlotSyncShmemSize());
	size = add_size(size, AioShmemSize());

	/*
	 * XXX: For some reason slightly more memory is needed for larger
	 * shared_buffers, but this size is enough for any large value I've tested
	 * with. Is it a mistake in how slots are split, or there was a hidden
	 * inconsistency in shmem calculation?
	 */
	size = add_size(size, 1024 * 1024 * 100);

	/* include additional requested shmem from preload libraries */
	size = add_size(size, total_addin_request);

	/*
	 * All the shared memory allocations considered so far happen in the main
	 * shared memory segment.
	 */
	mapping_sizes[MAIN_SHMEM_SEGMENT].shmem_req_size = size;
	mapping_sizes[MAIN_SHMEM_SEGMENT].shmem_reserved = size;

	size = 0;
	/* might as well round it off to a multiple of a typical page size */
	for (int segment = 0; segment < NUM_MEMORY_MAPPINGS; segment++)
	{
		round_off_mapping_sizes(&mapping_sizes[segment]);
		/* Compute the total size of all segments */
		size = size + mapping_sizes[segment].shmem_req_size;
	}

	return size;
}

#ifdef EXEC_BACKEND
/*
 * AttachSharedMemoryStructs
 *		Initialize a postmaster child process's access to shared memory
 *      structures.
 *
 * In !EXEC_BACKEND mode, we inherit everything through the fork, and this
 * isn't needed.
 */
void
AttachSharedMemoryStructs(void)
{
	/* InitProcess must've been called already */
	Assert(MyProc != NULL);
	Assert(IsUnderPostmaster);

	/*
	 * In EXEC_BACKEND mode, backends don't inherit the number of fast-path
	 * groups we calculated before setting the shmem up, so recalculate it.
	 */
	InitializeFastPathLocks();

	CreateOrAttachShmemStructs();

	/*
	 * Now give loadable modules a chance to set up their shmem allocations
	 */
	if (shmem_startup_hook)
		shmem_startup_hook();
}
#endif

/*
 * CreateSharedMemoryAndSemaphores
 *		Creates and initializes shared memory and semaphores.
 */
void
CreateSharedMemoryAndSemaphores(void)
{
	PGShmemHeader *shim;
	PGShmemHeader *seghdr;
	MemoryMappingSizes mapping_sizes[NUM_MEMORY_MAPPINGS];

	Assert(!IsUnderPostmaster);

	CalculateShmemSize(mapping_sizes);

	/* Decide if we use huge pages or regular size pages */
	PrepareHugePages();

	for(int segment = 0; segment < NUM_MEMORY_MAPPINGS; segment++)
	{
		MemoryMappingSizes *mapping = &mapping_sizes[segment];

		/* Compute the size of the shared-memory block */
		elog(DEBUG3, "invoking IpcMemoryCreate(segment %s, size=%zu, reserved address space=%zu)",
			 MappingName(segment), mapping->shmem_req_size, mapping->shmem_reserved);

		/*
		 * Create the shmem segment.
		 *
		 * XXX: Do multiple shims are needed, one per segment?
		 */
		seghdr = PGSharedMemoryCreate(mapping, segment, &shim);

		/*
		 * Make sure that huge pages are never reported as "unknown" while the
		 * server is running.
		 */
		Assert(strcmp("unknown",
					  GetConfigOption("huge_pages_status", false, false)) != 0);

		InitShmemAccessInSegment(seghdr, segment);

		/*
		 * Set up shared memory allocation mechanism
		 */
		InitShmemAllocationInSegment(segment);
	}

	/* Initialize subsystems */
	CreateOrAttachShmemStructs();

	/* Initialize dynamic shared memory facilities. */
	dsm_postmaster_startup(shim);

	/*
	 * Now give loadable modules a chance to set up their shmem allocations
	 */
	if (shmem_startup_hook)
		shmem_startup_hook();
}

/*
 * Initialize various subsystems, setting up their data structures in
 * shared memory.
 *
 * This is called by the postmaster or by a standalone backend.
 * It is also called by a backend forked from the postmaster in the
 * EXEC_BACKEND case.  In the latter case, the shared memory segment
 * already exists and has been physically attached to, but we have to
 * initialize pointers in local memory that reference the shared structures,
 * because we didn't inherit the correct pointer values from the postmaster
 * as we do in the fork() scenario.  The easiest way to do that is to run
 * through the same code as before.  (Note that the called routines mostly
 * check IsUnderPostmaster, rather than EXEC_BACKEND, to detect this case.
 * This is a bit code-wasteful and could be cleaned up.)
 */
static void
CreateOrAttachShmemStructs(void)
{
	/*
	 * Now initialize LWLocks, which do shared memory allocation and are
	 * needed for InitShmemIndex.
	 */
	CreateLWLocks();

	/*
	 * Set up shmem.c index hashtable
	 */
	InitShmemIndex();

	dsm_shmem_init();
	DSMRegistryShmemInit();

	/*
	 * Set up xlog, clog, and buffers
	 */
	VarsupShmemInit();
	XLOGShmemInit();
	XLogPrefetchShmemInit();
	XLogRecoveryShmemInit();
	CLOGShmemInit();
	CommitTsShmemInit();
	SUBTRANSShmemInit();
	MultiXactShmemInit();
	/* TODO: This should be part of BufferManagerShmemInit() */
	ShmemControlInit();
	BufferManagerShmemInit();

	/*
	 * Set up lock manager
	 */
	LockManagerShmemInit();

	/*
	 * Set up predicate lock manager
	 */
	PredicateLockShmemInit();

	/*
	 * Set up process table
	 */
	if (!IsUnderPostmaster)
		InitProcGlobal();
	ProcArrayShmemInit();
	BackendStatusShmemInit();
	TwoPhaseShmemInit();
	BackgroundWorkerShmemInit();

	/*
	 * Set up shared-inval messaging
	 */
	SharedInvalShmemInit();

	/*
	 * Set up interprocess signaling mechanisms
	 */
	PMSignalShmemInit();
	ProcSignalShmemInit();
	CheckpointerShmemInit();
	AutoVacuumShmemInit();
	ReplicationSlotsShmemInit();
	ReplicationOriginShmemInit();
	WalSndShmemInit();
	WalRcvShmemInit();
	WalSummarizerShmemInit();
	PgArchShmemInit();
	ApplyLauncherShmemInit();
	SlotSyncShmemInit();

	/*
	 * Set up other modules that need some shared memory space
	 */
	BTreeShmemInit();
	SyncScanShmemInit();
	AsyncShmemInit();
	StatsShmemInit();
	WaitEventCustomShmemInit();
	InjectionPointShmemInit();
	AioShmemInit();
}

/*
 * InitializeShmemGUCs
 *
 * This function initializes runtime-computed GUCs related to the amount of
 * shared memory required for the current configuration. It assumes that the
 * memory required by the shared memory segments is already calculated and is
 * available in AnonymousMappings.
 */
void
InitializeShmemGUCs(void)
{
	char		buf[64];
	Size		size_b;
	Size		size_mb;
	Size		hp_size;
	int			num_semas;
	MemoryMappingSizes mapping_sizes[NUM_MEMORY_MAPPINGS];


	/*
	 * Calculate the shared memory size and round up to the nearest megabyte.
	 */
	size_b = CalculateShmemSize(mapping_sizes);
	size_mb = add_size(size_b, (1024 * 1024) - 1) / (1024 * 1024);
	sprintf(buf, "%zu", size_mb);
	SetConfigOption("shared_memory_size", buf,
					PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);

	/*
	 * Calculate the number of huge pages required.
	 */
	GetHugePageSize(&hp_size, NULL, NULL);
	if (hp_size != 0)
	{
		Size		hp_required;

		hp_required = add_size(size_b / hp_size, 1);
		sprintf(buf, "%zu", hp_required);
		SetConfigOption("shared_memory_size_in_huge_pages", buf,
						PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);
	}

	/* Calculate the number of semaphores needed */
	num_semas = ProcGlobalSemas();
	sprintf(buf, "%d", num_semas);
	SetConfigOption("num_os_semaphores", buf, PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);
}
