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

#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "utils/injection_point.h"


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
MarkBufferResizingEnd(int NBuffers)
{
	Assert(!pg_atomic_unlocked_test_flag(&ShmemCtrl->resize_in_progress));

	Assert(pg_atomic_read_u32(&ShmemCtrl->currentNBuffers) == NBuffers);
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
			 * Not relevant in this function but it's here so that the compiler
			 * can detect any missing shared buffer resizing barrier enum here.
			 */
			break;
	}
#endif /* USE_INJECTION_POINTS */
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
	bool result = true;
	int currentNBuffers = pg_atomic_read_u32(&ShmemCtrl->currentNBuffers);
	int targetNBuffers = NBuffersPending;

	if (currentNBuffers == targetNBuffers)
	{
		elog(LOG, "shared buffers are already at %d, no need to resize", currentNBuffers);
		PG_RETURN_BOOL(true);
	}

	if (!pg_atomic_test_set_flag(&ShmemCtrl->resize_in_progress))
	{
		elog(LOG, "shared buffer resizing already in progress");
		PG_RETURN_BOOL(false);
	}

	/*
	 * TODO: What if the NBuffersPending value seen here is not the desired one
	 * because somebody did a pg_reload_conf() between the last pg_reload_conf()
	 * and execution of this function?
	 */
	MarkBufferResizingStart(targetNBuffers, currentNBuffers);
	elog(LOG, "resizing shared buffers from %d to %d", currentNBuffers, targetNBuffers);

	INJECTION_POINT("pg-resize-shared-buffers-flag-set", NULL);

	/* Phase 1: SHBUF_SHRINK - Only for shrinking buffer pool */
	if (targetNBuffers < currentNBuffers)
	{
		/*
		 * Phase 1: Shrinking - send SHBUF_SHRINK barrier
		 * Every backend sets activeNBuffers = NewNBuffers to restrict
		 * buffer pool allocations to the new size
		 */
		elog(LOG, "Phase 1: Shrinking buffer pool, restricting allocations to %d buffers", targetNBuffers);

		SharedBufferResizeBarrier(PROCSIGNAL_BARRIER_SHBUF_SHRINK, CppAsString(PROCSIGNAL_BARRIER_SHBUF_SHRINK));

		/* Evict buffers in the area being shrunk */
		elog(LOG, "evicting buffers %u..%u", targetNBuffers + 1, currentNBuffers);
		if (!EvictExtraBuffers(targetNBuffers, currentNBuffers))
		{
			elog(WARNING, "failed to evict extra buffers during shrinking");
			SharedBufferResizeBarrier(PROCSIGNAL_BARRIER_SHBUF_RESIZE_FAILED, CppAsString(PROCSIGNAL_BARRIER_SHBUF_RESIZE_FAILED));
			MarkBufferResizingEnd(currentNBuffers);
			pg_atomic_clear_flag(&ShmemCtrl->resize_in_progress);
			PG_RETURN_BOOL(false);
		}

		/* Update the current NBuffers. */
		pg_atomic_write_u32(&ShmemCtrl->currentNBuffers, targetNBuffers);
	}

	/* Phase 2: SHBUF_RESIZE_MAP_AND_MEM - Both expanding and shrinking */
	elog(LOG, "Phase 2: Remapping shared memory segments and updating structures");
	if (!AnonymousShmemResize())
	{
		/*
		 * This should never fail since address map should already be reserved.
		 * So the failure should be treated as PANIC.
		 */
		elog(PANIC, "failed to resize anonymous shared memory");
	}

	/* Update structure pointers and sizes */
	BufferManagerShmemResize(currentNBuffers, targetNBuffers);

	INJECTION_POINT("pgrsb-after-shmem-resize", NULL);

	SharedBufferResizeBarrier(PROCSIGNAL_BARRIER_SHBUF_RESIZE_MAP_AND_MEM, CppAsString(PROCSIGNAL_BARRIER_SHBUF_RESIZE_MAP_AND_MEM));

	/* Phase 3: SHBUF_EXPAND - Only for expanding buffer pool */
	if (targetNBuffers > currentNBuffers)
	{
		/*
		 * Phase 3: Expanding - send SHBUF_EXPAND barrier
		 * Backends set activeNBuffers = NewNBuffers and start allocating
		 * buffers from the expanded range
		 */
		elog(LOG, "Phase 3: Expanding buffer pool, enabling allocations up to %d buffers", targetNBuffers);
		pg_atomic_write_u32(&ShmemCtrl->currentNBuffers, targetNBuffers);

		SharedBufferResizeBarrier(PROCSIGNAL_BARRIER_SHBUF_EXPAND, CppAsString(PROCSIGNAL_BARRIER_SHBUF_EXPAND));
	}

	/*
	 * Reset buffer resize control area.
	 */
	MarkBufferResizingEnd(targetNBuffers);

	pg_atomic_clear_flag(&ShmemCtrl->resize_in_progress);

	elog(LOG, "successfully resized shared buffers to %d", targetNBuffers);

	PG_RETURN_BOOL(result);
}

bool
ProcessBarrierShmemShrink(void)
{
	int targetNBuffers = pg_atomic_read_u32(&ShmemCtrl->targetNBuffers);

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

	if (MyBackendType == B_BG_WRITER)
	{
		/*
		 * We have to reset the background writer's buffer allocation statistics
		 * and the strategy control together so that background writer doesn't go
		 * out of sync with ClockSweepTick().
		 *
		 * TODO: But in case the background writer is not running, nobody would
		 * reset the strategy control area. So we can't rely on background
		 * worker to do that. So find a better way.
		 */
		BgBufferSyncReset(NBuffers, targetNBuffers);
		/* Reset strategy control to new size */
		StrategyReset(targetNBuffers);
	}

	elog(LOG, "Phase 1: Processing SHBUF_SHRINK barrier - NBuffers = %d, coordinator is %d",
		 NBuffers, ShmemCtrl->coordinator);

	return true;
}

bool
ProcessBarrierShmemResizeMapAndMem(void)
{
	int targetNBuffers = pg_atomic_read_u32(&ShmemCtrl->targetNBuffers);

	Assert(!pg_atomic_unlocked_test_flag(&ShmemCtrl->resize_in_progress));

	/*
	 * If buffer pool is being shrunk, we are already working with a smaller
	 * buffer pool, so shrinking address space and shared structures should not
	 * be a problem. When expanding, expanding the address space and shared
	 * structures beyond the current boundaries is not going to be a problem
	 * since we are not accessing that memory yet. So there is no reason to
	 * delay processing this barrier.
	 */

	/*
	 * Coordinator has already adjusted its address map and also updated sizes
	 * of the shared buffer structures, no further validation needed.
	 */
	if (ShmemCtrl->coordinator == MyProcPid)
		return true;

	/*
	 * Backends validate that their pointers to shared buffer structures are
	 * still valid and have the correct size after memory remapping.
	 *
	 * TODO: Do want to do this only in assert enabled builds?
	 */
	BufferManagerShmemValidate(targetNBuffers);

	elog(LOG, "Backend %d successfully validated structure pointers after resize", MyProcPid);

	return true;
}

bool
ProcessBarrierShmemExpand(void)
{
	int targetNBuffers = pg_atomic_read_u32(&ShmemCtrl->targetNBuffers);

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
		/*
		 * We have to reset the background writer's buffer allocation statistics
		 * and the strategy control together so that background writer doesn't go
		 * out of sync with ClockSweepTick().
		 *
		 * TODO: But in case the background writer is not running, nobody would
		 * reset the strategy control area. So we can't rely on background
		 * worker to do that. So find a better way.
		 */
		BgBufferSyncReset(NBuffers, targetNBuffers);
		StrategyReset(targetNBuffers);
	}

	elog(LOG, "Phase 3: Processing SHBUF_EXPAND barrier - targetNBuffers = %d, ShmemCtrl->coordinator = %d", targetNBuffers, ShmemCtrl->coordinator);

	return true;
}

bool
ProcessBarrierShmemResizeFailed(void)
{
	int currentNBuffers = pg_atomic_read_u32(&ShmemCtrl->currentNBuffers);
	int targetNBuffers = pg_atomic_read_u32(&ShmemCtrl->targetNBuffers);

	Assert(!pg_atomic_unlocked_test_flag(&ShmemCtrl->resize_in_progress));

	if (MyBackendType == B_BG_WRITER)
	{
		/*
		 * We have to reset the background writer's buffer allocation statistics
		 * and the strategy control together so that background writer doesn't go
		 * out of sync with ClockSweepTick().
		 *
		 * TODO: But in case the background writer is not running, nobody would
		 * reset the strategy control area. So we can't rely on background
		 * worker to do that. So find a better way.
		 */
		BgBufferSyncReset(NBuffers, currentNBuffers);
		/* Reset strategy control to new size */
		StrategyReset(currentNBuffers);
	}

	elog(LOG, "received proc signal indicating failure to resize shared buffers from %d to %d, restoring to %d, coordinator is %d",
			NBuffers, targetNBuffers, currentNBuffers, ShmemCtrl->coordinator);

	return true;
}