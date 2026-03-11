/*-------------------------------------------------------------------------
 *
 * buf_resize.c
 *	  shared buffer pool resizing functionality
 *
 * This module contains the implementation of shared buffer pool resizing,
 * including the main resize coordination function and barrier processing
 * functions that synchronize all backends during resize operations.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_resize.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "utils/fmgrprotos.h"
#include "utils/injection_point.h"
#include "utils/builtins.h"

#define PG_RESIZE_SHMEM_BUFFERS_COLS 2

/*
 * Emit one (phase, elapsed_sec) row into the result set.
 * elapsed_sec is in seconds; set elapsed_null true to emit NULL for elapsed time.
 */
static void
EmitResizePhaseRow(ReturnSetInfo *rsinfo, const char *phase, double elapsed_sec,
				   bool elapsed_null)
{
	Datum		values[PG_RESIZE_SHMEM_BUFFERS_COLS];
	bool		nulls[PG_RESIZE_SHMEM_BUFFERS_COLS];

	values[0] = CStringGetTextDatum(phase);
	nulls[0] = false;
	if (elapsed_null)
	{
		nulls[1] = true;
		values[1] = (Datum) 0;
	}
	else
	{
		nulls[1] = false;
		/* Round to 2 decimal places for stable regression output */
		values[1] = Float8GetDatum(round(elapsed_sec * 100.0) / 100.0);
	}
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

/*
 * Prepare ShmemCtrl for resizing the shared buffer pool.
 */
static void
MarkBufferResizingStart(int targetNBuffers, int currentNBuffers)
{
	Assert(!pg_atomic_unlocked_test_flag(&ShmemCtrl->resize_in_progress));

	Assert(pg_atomic_read_u32(&ShmemCtrl->currentNBuffers) == currentNBuffers);

	pg_atomic_write_u32(&ShmemCtrl->targetNBuffers, targetNBuffers);
	ShmemCtrl->coordinator = MyProcPid;
}

/*
 * Reset ShmemCtrl after resizing the shared buffer pool is done.
 */
static void
MarkBufferResizingEnd(int newNBuffers)
{
	Assert(!pg_atomic_unlocked_test_flag(&ShmemCtrl->resize_in_progress));

	Assert(pg_atomic_read_u32(&ShmemCtrl->currentNBuffers) == newNBuffers);

	/*
	 * TODO: should we leave targetNBuffers as is? We are setting it to
	 * NBuffers in BufferManagerShmemInit().
	 */
	pg_atomic_write_u32(&ShmemCtrl->targetNBuffers, 0);
	ShmemCtrl->coordinator = -1;
}

/*
 * Communicate given buffer pool resize barrier to all other backends and the Postmaster.
 *
 * ProcSignalBarrier is not sent to the Postmaster but we need the Postmaster to
 * update its knowledge about the buffer pool so that it can be inherited by the
 * child processes.
 */
static void
SharedBufferResizeBarrier(ProcSignalBarrierType barrier, const char *barrier_name)
{
	WaitForProcSignalBarrier(EmitProcSignalBarrier(barrier));
	elog(LOG, "all backends acknowledged %s barrier", barrier_name);

#ifdef USE_INJECTION_POINTS
	/* Injection point specific to this barrier type */
	switch (barrier)
	{
		case PROCSIGNAL_BARRIER_SHBUF_SHRINK:
			INJECTION_POINT("pgrsb-shrink-barrier-sent", NULL);
			break;
		case PROCSIGNAL_BARRIER_SHBUF_RESIZE_MAP_AND_MEM:
			INJECTION_POINT("pgrsb-resize-barrier-sent", NULL);
			break;
		case PROCSIGNAL_BARRIER_SHBUF_EXPAND:
			INJECTION_POINT("pgrsb-expand-barrier-sent", NULL);
			break;
		case PROCSIGNAL_BARRIER_SHBUF_RESIZE_FAILED:
			/* TODO: Add an injection point here. */
			break;
		case PROCSIGNAL_BARRIER_SMGRRELEASE:
			/*
			 * Not relevant in this function but it's here so that the
			 * compiler can detect any missing shared buffer resizing barrier
			 * enum here.
			 */
			break;
	}
#endif							/* USE_INJECTION_POINTS */
}

/*
 * Perform the entire shrink path: Phase 1 (SHBUF_SHRINK, evict, shrink
 * structures) then Phase 2 (remap shared memory segments). Emits phase rows
 * with elapsed time for each.
 */
static void
DoShrink(ReturnSetInfo *rsinfo, int currentNBuffers, int targetNBuffers,
		 MemoryMappingSizes *mapping_sizes)
{
	instr_time	shrink_start;
	instr_time	shrink_end;

	instr_time	phase_start;
	instr_time	phase_end;
	int			i;

	/* Phase 1: Shrinking */
	elog(LOG, "Phase 1: Shrinking buffer pool, restricting allocations to %d buffers", targetNBuffers);
	INSTR_TIME_SET_CURRENT(phase_start);
	INSTR_TIME_SET_CURRENT(shrink_start);
	SharedBufferResizeBarrier(PROCSIGNAL_BARRIER_SHBUF_SHRINK, CppAsString(PROCSIGNAL_BARRIER_SHBUF_SHRINK));
	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizePhaseRow(rsinfo, "Phase 1: Barrier-1", INSTR_TIME_GET_DOUBLE(phase_end), false);

	elog(LOG, "evicting buffers %u..%u", targetNBuffers + 1, currentNBuffers);
	INSTR_TIME_SET_CURRENT(phase_start);
	while (!EvictExtraBuffers(targetNBuffers, currentNBuffers))
	{
		pg_usleep(1000);
		// elog(WARNING, "failed to evict extra buffers during shrinking");
		// SharedBufferResizeBarrier(PROCSIGNAL_BARRIER_SHBUF_RESIZE_FAILED, CppAsString(PROCSIGNAL_BARRIER_SHBUF_RESIZE_FAILED));
		// MarkBufferResizingEnd(currentNBuffers);
		// pg_atomic_clear_flag(&ShmemCtrl->resize_in_progress);
		// PG_RETURN_BOOL(false);
	}
	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizePhaseRow(rsinfo, "Phase 1: Evicting", INSTR_TIME_GET_DOUBLE(phase_end), false);

	INSTR_TIME_SET_CURRENT(phase_start);
	BufferManagerShmemResize(currentNBuffers, targetNBuffers);
	pg_atomic_write_u32(&ShmemCtrl->currentNBuffers, targetNBuffers);
	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizePhaseRow(rsinfo, "Phase 1: ShmemResize", INSTR_TIME_GET_DOUBLE(phase_end), false);

	/* Phase 2: Remapping */
	elog(LOG, "Phase 2: Remapping shared memory segments and updating structures");
	INSTR_TIME_SET_CURRENT(phase_start);
	for (i = 0; i < NUM_MEMORY_MAPPINGS; i++)
	{
		if (i == MAIN_SHMEM_SEGMENT)
			continue;
		if (!PGSharedMemoryResize(i, &mapping_sizes[i]))
			elog(PANIC, "failed to resize anonymous shared memory");
	}
	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizePhaseRow(rsinfo, "Phase 2: ftruncate", INSTR_TIME_GET_DOUBLE(phase_end), false);

	INJECTION_POINT("pgrsb-after-shmem-resize", NULL);
	INSTR_TIME_SET_CURRENT(phase_start);
	SharedBufferResizeBarrier(PROCSIGNAL_BARRIER_SHBUF_RESIZE_MAP_AND_MEM, CppAsString(PROCSIGNAL_BARRIER_SHBUF_RESIZE_MAP_AND_MEM));
	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizePhaseRow(rsinfo, "Phase 2: barrier-2", INSTR_TIME_GET_DOUBLE(phase_end), false);

	INSTR_TIME_SET_CURRENT(shrink_end);
	INSTR_TIME_SUBTRACT(shrink_end, shrink_start);
	EmitResizePhaseRow(rsinfo, "Total Shrink", INSTR_TIME_GET_DOUBLE(shrink_end), false);
}

/*
 * Perform the entire expand path: Phase 1 (remap shared memory segments) then
 * Phase 2 (expand structures, SHBUF_EXPAND). Emits phase rows with elapsed
 * time for each.
 */
static void
DoExpand(ReturnSetInfo *rsinfo, int currentNBuffers, int targetNBuffers,
		 MemoryMappingSizes *mapping_sizes)
{
	instr_time	phase_start;
	instr_time	phase_end;
	int			i;

	/* Phase 1: Remapping */
	elog(LOG, "Phase 1: Remapping shared memory segments and updating structures");
	INSTR_TIME_SET_CURRENT(phase_start);
	for (i = 0; i < NUM_MEMORY_MAPPINGS; i++)
	{
		if (i == MAIN_SHMEM_SEGMENT)
			continue;
		if (!PGSharedMemoryResize(i, &mapping_sizes[i]))
			elog(PANIC, "failed to resize anonymous shared memory");
	}
	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizePhaseRow(rsinfo, "Phase 1: fallocate", INSTR_TIME_GET_DOUBLE(phase_end), false);

	INJECTION_POINT("pgrsb-after-shmem-resize", NULL);
	INSTR_TIME_SET_CURRENT(phase_start);
	SharedBufferResizeBarrier(PROCSIGNAL_BARRIER_SHBUF_RESIZE_MAP_AND_MEM, CppAsString(PROCSIGNAL_BARRIER_SHBUF_RESIZE_MAP_AND_MEM));
	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizePhaseRow(rsinfo, "Phase 2: barrier-1", INSTR_TIME_GET_DOUBLE(phase_end), false);

	/* Phase 2: Expanding */
	elog(LOG, "Phase 2: Expanding buffer pool, enabling allocations up to %d buffers", targetNBuffers);
	INSTR_TIME_SET_CURRENT(phase_start);
	BufferManagerShmemResize(currentNBuffers, targetNBuffers);
	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizePhaseRow(rsinfo, "Phase 2: ShmemResize", INSTR_TIME_GET_DOUBLE(phase_end), false);
	
	pg_atomic_write_u32(&ShmemCtrl->currentNBuffers, targetNBuffers);
	INSTR_TIME_SET_CURRENT(phase_start);
	SharedBufferResizeBarrier(PROCSIGNAL_BARRIER_SHBUF_EXPAND, CppAsString(PROCSIGNAL_BARRIER_SHBUF_EXPAND));
	INSTR_TIME_SET_CURRENT(phase_end);
	INSTR_TIME_SUBTRACT(phase_end, phase_start);
	EmitResizePhaseRow(rsinfo, "Phase 2: barrier-2", INSTR_TIME_GET_DOUBLE(phase_end), false);
}

/*
 * C implementation of SQL interface to update the shared buffers according to
 * the current values of shared_buffers GUCs.
 *
 * The current boundaries of the buffer pool are given by two ranges.
 *
 * - [1, StrategyControl::activeNBuffers] is the range of buffers from which new
 * allocations can happen at any time.
 *
 * - [1, ShmemCtrl::currentNBuffers] is the range of valid buffers at any given
 * time.
 *
 * Let's assume that before resizing, the number of buffers in the buffer pool is
 * NBuffersOld. After resizing it is NBuffersNew. Before resizing
 * StrategyControl::activeNBuffers == ShmemCtrl::currentNBuffers == NBuffersOld.
 * After the resizing finishes StrategyControl::activeNBuffers ==
 * ShmemCtrl::currentNBuffers == NBuffersNew. Thus when no resizing happens these
 * two ranges are same.
 *
 * Following steps are performed by the coordinator during resizing.
 *
 * 1. Marks resizing in progress to avoid multiple concurrent invocations of this
 * function.
 *
 * 2. When shrinking the shared buffer pool, the coordinator sends SHBUF_SHRINK
 * ProcSignalBarrier. In response to this barrier background writer is expected
 * to set StrategyControl::activeNBuffers = NBuffersNew to restrict the new
 * buffer allocations only to the new buffer pool size and also reset its
 * internal state. Once every backend has acknowledged the barrier, the
 * coordinator can be sure that new allocations will not happen in the buffer
 * pool area being shrunk. Then it evicts the buffers in that area.  Note that
 * ShmemCtrl::currentNBuffers is still NBuffersOld, since backend may still
 * access buffers allocated before the resizing started. Buffer eviction may fail
 * if a buffer being evicted is pinned and the resizing operatino is aborted.
 * Once the eviction is finished, the extra memory can be freed in the next step.
 *
 * 2. This step is executed in both cases, when expanding the buffer pool or
 * shrinking the buffer pool. The anonymous file backing each of the shared
 * memory segment containg the buffer pool shared data structures is resized to
 * the amount of memory required for the new buffer pool size. When expanding the
 * expanded portion of memory is initialized appropriately.
 * ShmemCtrl::currentNBuffers is set to NBuffersNew to indicate new range of
 * valid shared buffers. Every backend is sent SHBUF_RESIZE_MAP_AND_MEM barrier.
 * All the backends validate that their pointers to the shared buffers structure
 * are valid and have the right size. Once every backend has acknowledged the
 * barrier, this step finishes.
 *
 * 3. When expanding the buffer pool, the coordinator sends SHBUF_EXPAND barrier
 * to signal end of expansion. When expadning the background writer, in response
 * to StrategyControl::activeNBuffers = NBufferNew so that new allocations can
 * use expanded range of buffer pool.
 *
 * TODO: Handle the case when the backend executing this function dies or the
 * query is cancelled or it hits an error while resizing.
 */
Datum
pg_resize_shared_buffers(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			currentNBuffers = pg_atomic_read_u32(&ShmemCtrl->currentNBuffers);
	int			targetNBuffers = NBuffersPending;
	MemoryMappingSizes mapping_sizes[NUM_MEMORY_MAPPINGS];

	InitMaterializedSRF(fcinfo, 0);

	if (currentNBuffers == targetNBuffers)
	{
		elog(LOG, "shared buffers are already at %d, no need to resize", currentNBuffers);
		EmitResizePhaseRow(rsinfo, "no resize", 0.0, false);
		return (Datum) 0;
	}

	if (!pg_atomic_test_set_flag(&ShmemCtrl->resize_in_progress))
	{
		elog(LOG, "shared buffer resizing already in progress");
		EmitResizePhaseRow(rsinfo, "resize already in progress", 0.0, true);
		return (Datum) 0;
	}

	/*
	 * TODO: NBuffersPending may change after it was sampled above, thus
	 * leading to wrong memory size estimates. Find a way to pass
	 * targetNBuffers value to BufferManagerShmemSize().
	 */
	BufferManagerShmemSize(mapping_sizes);
	/* Round it off to a multiple of a typical page size */
	for (int i = 0; i < NUM_MEMORY_MAPPINGS; i++)
	{
		/* Structures in main memory segment are never resized. */
		if (i == MAIN_SHMEM_SEGMENT)
			continue;

		round_off_mapping_sizes(&mapping_sizes[i]);
	}

	/*
	 * TODO: What if the NBuffersPending value seen here is not the desired
	 * one because somebody did a pg_reload_conf() between the last
	 * pg_reload_conf() and execution of this function?
	 */
	MarkBufferResizingStart(targetNBuffers, currentNBuffers);
	elog(LOG, "resizing shared buffers from %d to %d", currentNBuffers, targetNBuffers);

	INJECTION_POINT("pg-resize-shared-buffers-flag-set", NULL);

	if (targetNBuffers < currentNBuffers)
		DoShrink(rsinfo, currentNBuffers, targetNBuffers, mapping_sizes);
	else
		DoExpand(rsinfo, currentNBuffers, targetNBuffers, mapping_sizes);

	/*
	 * Reset buffer resize control area.
	 */
	MarkBufferResizingEnd(targetNBuffers);

	pg_atomic_clear_flag(&ShmemCtrl->resize_in_progress);

	elog(LOG, "successfully resized shared buffers to %d", targetNBuffers);

	return (Datum) 0;
}

bool
ProcessBarrierShmemShrink(void)
{
	int			targetNBuffers = pg_atomic_read_u32(&ShmemCtrl->targetNBuffers);

	Assert(!pg_atomic_unlocked_test_flag(&ShmemCtrl->resize_in_progress));

	/*
	 * Delay adjusting the new active size of buffer pool till this process
	 * becomes ready to resize buffers.
	 */
	if (delay_shmem_resize)
	{
		elog(LOG, "Phase 1: Delaying SHBUF_SHRINK barrier - restricting allocations to %d buffers, coordinator is %d",
			 targetNBuffers, ShmemCtrl->coordinator);

		return false;
	}

	if (ShmemCtrl->coordinator == MyProcPid)
		return true;

	if (MyBackendType == B_BG_WRITER)
	{
		elog(LOG, "B_BG_WRITER BgBufferSyncReset: %d, %d", NBuffers, targetNBuffers);
		/*
		 * We have to reset the background writer's buffer allocation
		 * statistics and the strategy control together so that background
		 * writer doesn't go out of sync with ClockSweepTick().
		 *
		 * TODO: But in case the background writer is not running, nobody
		 * would reset the strategy control area. So we can't rely on
		 * background worker to do that. So find a better way.
		 */
		BgBufferSyncReset(NBuffers, targetNBuffers);
		/* Reset strategy control to new size */
		StrategyReset(targetNBuffers);
	}
	else
	{
		/*
		 * Only acknowledge once the bgwriter has updated activeNBuffers, so we
		 * don't allocate from the shrinking range after acking.  The
		 * coordinator returns true above; only regular backends reach here.
		 */
		if (StrategyGetActiveNBuffers() != targetNBuffers) {
			elog(LOG, "Backend %d waiting for bgwriter to update activeNBuffers to %d", MyProcPid, targetNBuffers);
			return false;
		}
	}

	elog(LOG, "Phase 1: Processing SHBUF_SHRINK barrier - target buffer pool size = %d, coordinator is %d",
		 targetNBuffers, ShmemCtrl->coordinator);

	return true;
}

bool
ProcessBarrierShmemResizeMapAndMem(void)
{
	int			targetNBuffers = pg_atomic_read_u32(&ShmemCtrl->targetNBuffers);
	int			currentNBuffers = pg_atomic_read_u32(&ShmemCtrl->currentNBuffers);

	Assert(!pg_atomic_unlocked_test_flag(&ShmemCtrl->resize_in_progress));

	/*
	 * If this process is in the middle of BufferSync (e.g. checkpointer), do
	 * not process the barrier yet.  The coordinator has already remapped;
	 * if we validated or used buffer pointers now we could touch memory that
	 * was unmapped/remapped and get SIGBUS.  Defer until BufferSync completes.
	 */
	if (delay_shmem_resize)
	{
		elog(LOG, "Phase 2: Delaying SHBUF_RESIZE_MAP_AND_MEM barrier - checkpoint/buffer sync in progress, coordinator is %d",
			 ShmemCtrl->coordinator);
		return false;
	}

	/*
	 * If buffer pool is being shrunk, we are already working with a smaller
	 * buffer pool, so shrinking address space and shared structures should
	 * not be a problem. When expanding, expanding the address space and
	 * shared structures beyond the current boundaries is not going to be a
	 * problem since we are not accessing that memory yet. So there is no
	 * reason to delay processing this barrier.
	 */

	/*
	 * Coordinator has already adjusted its address map and also updated sizes
	 * of the shared buffer structures, no further validation needed.
	 */
	if (ShmemCtrl->coordinator == MyProcPid)
		return true;

	if (targetNBuffers < currentNBuffers)
	{
		/*
		 * When shrinking, shared data structures have been resized at this
		 * point.  Validate that their pointers to shared buffer structures
		 * are still valid and have the correct size after resizing.
		 *
		 * TODO: Do want to do this only in assert enabled builds?
		 */
		BufferManagerShmemValidate(targetNBuffers);
		elog(LOG, "Backend %d successfully validated structure pointers after resize", MyProcPid);
	}

	return true;
}

bool
ProcessBarrierShmemExpand(void)
{
	int			targetNBuffers = pg_atomic_read_u32(&ShmemCtrl->targetNBuffers);

	Assert(!pg_atomic_unlocked_test_flag(&ShmemCtrl->resize_in_progress));

	/*
	 * Delay adjusting the new active size of buffer pool till this process
	 * becomes ready to resize buffers.
	 */
	if (delay_shmem_resize)
	{
		elog(LOG, "Phase 3: delaying SHBUF_EXPAND barrier - enabling allocations up to %d buffers, coordinator is %d",
			 targetNBuffers, ShmemCtrl->coordinator);
		return false;
	}

	if (MyBackendType == B_BG_WRITER)
	{
		elog(LOG, "B_BG_WRITER BgBufferSyncReset: %d, %d", NBuffers, targetNBuffers);
		/*
		 * We have to reset the background writer's buffer allocation
		 * statistics and the strategy control together so that background
		 * writer doesn't go out of sync with ClockSweepTick().
		 *
		 * TODO: But in case the background writer is not running, nobody
		 * would reset the strategy control area. So we can't rely on
		 * background worker to do that. So find a better way.
		 */
		BgBufferSyncReset(NBuffers, targetNBuffers);
		StrategyReset(targetNBuffers);
	}

	/*
	 * Shared data structures must have been resized by now. Validate that
	 * their pointers to shared buffer structures are still valid and have the
	 * correct size after resizing.
	 *
	 * TODO: Do want to do this only in assert enabled builds?
	 */
	BufferManagerShmemValidate(targetNBuffers);
	elog(LOG, "Backend %d successfully validated structure pointers after resize", MyProcPid);

	elog(LOG, "Phase 3: Processing SHBUF_EXPAND barrier - targetNBuffers = %d, ShmemCtrl->coordinator = %d", targetNBuffers, ShmemCtrl->coordinator);

	return true;
}

bool
ProcessBarrierShmemResizeFailed(void)
{
	int			currentNBuffers = pg_atomic_read_u32(&ShmemCtrl->currentNBuffers);
	int			targetNBuffers = pg_atomic_read_u32(&ShmemCtrl->targetNBuffers);

	Assert(!pg_atomic_unlocked_test_flag(&ShmemCtrl->resize_in_progress));

	if (MyBackendType == B_BG_WRITER)
	{
		elog(LOG, "B_BG_WRITER BgBufferSyncReset: %d, %d", NBuffers, currentNBuffers);
		/*
		 * We have to reset the background writer's buffer allocation
		 * statistics and the strategy control together so that background
		 * writer doesn't go out of sync with ClockSweepTick().
		 *
		 * TODO: But in case the background writer is not running, nobody
		 * would reset the strategy control area. So we can't rely on
		 * background worker to do that. So find a better way.
		 */
		BgBufferSyncReset(NBuffers, currentNBuffers);
		/* Reset strategy control to new size */
		StrategyReset(currentNBuffers);
	}

	elog(LOG, "received proc signal indicating failure to resize shared buffers from %d to %d, restoring to %d, coordinator is %d",
		 currentNBuffers, targetNBuffers, currentNBuffers, ShmemCtrl->coordinator);

	return true;
}

/*
 * TODO: add progress report facility if required.
 */
