/*-------------------------------------------------------------------------
 *
 * buf_init.c
 *	  buffer manager initialization routines
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_init.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/aio.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "utils/guc.h"

BufferDescPadded *BufferDescriptors;
char	   *BufferBlocks;
ConditionVariableMinimallyPadded *BufferIOCVArray;
WritebackContext BackendWritebackContext;
CkptSortItem *CkptBufferIds;

/*
 * Buffer with target WAL redo page.
 * We must not evict this page from the buffer pool, but we cannot just keep it pinned because
 * some WAL redo functions expect the page to not be pinned. So we have a special check in
 * localbuf.c to prevent this buffer from being evicted.
 */
Buffer		wal_redo_buffer;
bool		am_wal_redo_postgres = false;

/*
 * Data Structures:
 *		buffers live in a freelist and a lookup data structure.
 *
 *
 * Buffer Lookup:
 *		Two important notes.  First, the buffer has to be
 *		available for lookup BEFORE an IO begins.  Otherwise
 *		a second process trying to read the buffer will
 *		allocate its own copy and the buffer pool will
 *		become inconsistent.
 *
 * Buffer Replacement:
 *		see freelist.c.  A buffer cannot be replaced while in
 *		use either by data manager or during IO.
 *
 *
 * Synchronization/Locking:
 *
 * IO_IN_PROGRESS -- this is a flag in the buffer descriptor.
 *		It must be set when an IO is initiated and cleared at
 *		the end of the IO.  It is there to make sure that one
 *		process doesn't start to use a buffer while another is
 *		faulting it in.  see WaitIO and related routines.
 *
 * refcount --	Counts the number of processes holding pins on a buffer.
 *		A buffer is pinned during IO and immediately after a BufferAlloc().
 *		Pins must be released before end of transaction.  For efficiency the
 *		shared refcount isn't increased if an individual backend pins a buffer
 *		multiple times. Check the PrivateRefCount infrastructure in bufmgr.c.
 */


/*
 * Initialize shared buffer pool
 *
 * This is called once during shared-memory initialization.
 * TODO: Restore this function to it's initial form. This function should see no
 * change in buffer resize patches, except may be use of NBuffersPending.
 *
 * No locks are taking in this function, it is the caller responsibility to
 * make sure only one backend can work with new buffers.
 */
void
BufferManagerShmemInit(void)
{
	bool		foundBufs,
				foundDescs,
				foundIOCV,
				foundBufCkpt;
	int			i;

	/* Align descriptors to a cacheline boundary. */
	BufferDescriptors = (BufferDescPadded *)
		ShmemInitStructInSegment("Buffer Descriptors",
						NBuffersPending * sizeof(BufferDescPadded),
						&foundDescs, BUFFER_DESCRIPTORS_SHMEM_SEGMENT);

	/* Align buffer pool on IO page size boundary. */
	BufferBlocks = (char *)
		TYPEALIGN(PG_IO_ALIGN_SIZE,
				  ShmemInitStructInSegment("Buffer Blocks",
								  NBuffersPending * (Size) BLCKSZ + PG_IO_ALIGN_SIZE,
								  &foundBufs, BUFFERS_SHMEM_SEGMENT));

	/* Align condition variables to cacheline boundary. */
	BufferIOCVArray = (ConditionVariableMinimallyPadded *)
		ShmemInitStructInSegment("Buffer IO Condition Variables",
						NBuffersPending * sizeof(ConditionVariableMinimallyPadded),
						&foundIOCV, BUFFER_IOCV_SHMEM_SEGMENT);

	/*
	 * The array used to sort to-be-checkpointed buffer ids is located in
	 * shared memory, to avoid having to allocate significant amounts of
	 * memory at runtime. As that'd be in the middle of a checkpoint, or when
	 * the checkpointer is restarted, memory allocation failures would be
	 * painful.
	 */
	CkptBufferIds = (CkptSortItem *)
		ShmemInitStructInSegment("Checkpoint BufferIds",
						NBuffersPending * sizeof(CkptSortItem), &foundBufCkpt,
						CHECKPOINT_BUFFERS_SHMEM_SEGMENT);

	if (foundDescs || foundBufs || foundIOCV || foundBufCkpt)
	{
		/* should find all of these, or none of them */
		Assert(foundDescs && foundBufs && foundIOCV && foundBufCkpt);
		/*
		 * note: this path is only taken in EXEC_BACKEND case when initializing
		 * shared memory.
		 */
	}

	/*
	 * Initialize all the buffer headers.
	 */
	for (i = 0; i < NBuffersPending; i++)
		{
			BufferDesc *buf = GetBufferDescriptor(i);

			ClearBufferTag(&buf->tag);

			pg_atomic_init_u32(&buf->state, 0);
			buf->wait_backend_pgprocno = INVALID_PROC_NUMBER;

			buf->buf_id = i;

			pgaio_wref_clear(&buf->io_wref);

			/*
			 * Initially link all the buffers together as unused. Subsequent
			 * management of this list is done by freelist.c.
			 */
			buf->freeNext = i + 1;

			LWLockInitialize(BufferDescriptorGetContentLock(buf),
							 LWTRANCHE_BUFFER_CONTENT);

		ConditionVariableInit(BufferDescriptorGetIOCV(buf));
	}

	/* Correct last entry of linked list */
	GetBufferDescriptor(NBuffersPending - 1)->freeNext = FREENEXT_END_OF_LIST;

	/*
	 * Init other shared buffer-management stuff.
	 */
	StrategyInitialize(!foundDescs);

	/* Initialize per-backend file flush context */
	WritebackContextInit(&BackendWritebackContext,
						 &backend_flush_after);

	/* Declare the size of current buffer pool. */
	NBuffers = NBuffersPending;
	pg_atomic_write_u32(&ShmemCtrl->currentNBuffers, NBuffers);
	pg_atomic_write_u32(&ShmemCtrl->targetNBuffers, NBuffers);
}

/*
 * BufferManagerShmemSize
 *
 * compute the size of shared memory for the buffer pool including
 * data pages, buffer descriptors, hash tables, etc. based on the
 * shared memory segment. The main segment must not allocate anything
 * related to buffers, every other segment will receive part of the
 * data.
 *
 * Also sets the shmem_reserved field for each segment based on MaxNBuffers.
 */
Size
BufferManagerShmemSize(MemoryMappingSizes *mapping_sizes)
{
	size_t size;

	/* size of buffer descriptors, plus alignment padding */
	size = add_size(0, mul_size(NBuffersPending, sizeof(BufferDescPadded)));
	size = add_size(size, PG_CACHE_LINE_SIZE);
	mapping_sizes[BUFFER_DESCRIPTORS_SHMEM_SEGMENT].shmem_req_size = size;
	size = add_size(0, mul_size(MaxNBuffers, sizeof(BufferDescPadded)));
	size = add_size(size, PG_CACHE_LINE_SIZE);
	mapping_sizes[BUFFER_DESCRIPTORS_SHMEM_SEGMENT].shmem_reserved = size;

	/* size of data pages, plus alignment padding */
	size = add_size(0, PG_IO_ALIGN_SIZE);
	size = add_size(size, mul_size(NBuffersPending, BLCKSZ));
	mapping_sizes[BUFFERS_SHMEM_SEGMENT].shmem_req_size = size;
	size = add_size(0, PG_IO_ALIGN_SIZE);
	size = add_size(size, mul_size(MaxNBuffers, BLCKSZ));
	mapping_sizes[BUFFERS_SHMEM_SEGMENT].shmem_reserved = size;

	/* size of I/O condition variables, plus alignment padding */
	size = add_size(0, mul_size(NBuffersPending,
								   sizeof(ConditionVariableMinimallyPadded)));
	size = add_size(size, PG_CACHE_LINE_SIZE);
	mapping_sizes[BUFFER_IOCV_SHMEM_SEGMENT].shmem_req_size = size;
	size = add_size(0, mul_size(MaxNBuffers,
								   sizeof(ConditionVariableMinimallyPadded)));
	size = add_size(size, PG_CACHE_LINE_SIZE);
	mapping_sizes[BUFFER_IOCV_SHMEM_SEGMENT].shmem_reserved = size;

	/* size of checkpoint sort array in bufmgr.c */
	mapping_sizes[CHECKPOINT_BUFFERS_SHMEM_SEGMENT].shmem_req_size = mul_size(NBuffersPending, sizeof(CkptSortItem));
	mapping_sizes[CHECKPOINT_BUFFERS_SHMEM_SEGMENT].shmem_reserved = mul_size(MaxNBuffers, sizeof(CkptSortItem));

	/* Allocations in the main memory segment, at the end. */

	/* size of stuff controlled by freelist.c */
	size = add_size(0, StrategyShmemSize());

	return size;
}

/*
 * Reinitialize shared buffer manager structures when resizing the buffer pool.
 *
 * This function is called in the backend which coordinates buffer resizing
 * operation.
 *
 * TODO: Avoid code duplication with BufferManagerShmemInit() and also assess
 * which functionality in the latter is required in this function.
 */
void
BufferManagerShmemResize(int currentNBuffers, int targetNBuffers)
{
	bool found;
	int			i;
	void *tmpPtr;

	tmpPtr = (BufferDescPadded *)
		ShmemUpdateStructInSegment("Buffer Descriptors",
						targetNBuffers * sizeof(BufferDescPadded),
						&found, BUFFER_DESCRIPTORS_SHMEM_SEGMENT);
	if (BufferDescriptors != tmpPtr || !found)
		elog(FATAL, "resizing buffer descriptors failed: expected pointer %p, got %p, found=%d",
			 BufferDescriptors, tmpPtr, found);

	tmpPtr = (ConditionVariableMinimallyPadded *)
		ShmemUpdateStructInSegment("Buffer IO Condition Variables",
						targetNBuffers * sizeof(ConditionVariableMinimallyPadded),
						&found, BUFFER_IOCV_SHMEM_SEGMENT);
	if (BufferIOCVArray != tmpPtr || !found)
		elog(FATAL, "resizing buffer IO condition variables failed: expected pointer %p, got %p, found=%d",
			 BufferIOCVArray, tmpPtr, found);

	tmpPtr = (CkptSortItem *)
		ShmemUpdateStructInSegment("Checkpoint BufferIds",
						targetNBuffers * sizeof(CkptSortItem), &found,
						CHECKPOINT_BUFFERS_SHMEM_SEGMENT);
	if (CkptBufferIds != tmpPtr || !found)
		elog(FATAL, "resizing checkpoint buffer IDs failed: expected pointer %p, got %p, found=%d",
			 CkptBufferIds, tmpPtr, found);

	tmpPtr = (char *)
		TYPEALIGN(PG_IO_ALIGN_SIZE,
				  ShmemUpdateStructInSegment("Buffer Blocks",
								  targetNBuffers * (Size) BLCKSZ + PG_IO_ALIGN_SIZE,
								  &found, BUFFERS_SHMEM_SEGMENT));
	if (BufferBlocks != tmpPtr || !found)
		elog(FATAL, "resizing buffer blocks failed: expected pointer %p, got %p, found=%d",
			 BufferBlocks, tmpPtr, found);

	/*
	 * Initialize the headers for new buffers. If we are shrinking the
	 * buffers, currentNBuffers >= targetNBuffers, thus this loop doesn't execute.
	 */
	for (i = currentNBuffers; i < targetNBuffers; i++)
	{
		BufferDesc *buf = GetBufferDescriptor(i);

		ClearBufferTag(&buf->tag);

		pg_atomic_init_u32(&buf->state, 0);
		buf->wait_backend_pgprocno = INVALID_PROC_NUMBER;

		buf->buf_id = i;

		LWLockInitialize(BufferDescriptorGetContentLock(buf),
						 LWTRANCHE_BUFFER_CONTENT);

		ConditionVariableInit(BufferDescriptorGetIOCV(buf));
	}

	/*
	 * We do not touch StrategyControl here. Instead it is done by background
	 * writer when handling PROCSIGNAL_BARRIER_SHBUF_EXPAND or
	 * PROCSIGNAL_BARRIER_SHBUF_SHRINK barrier.
	 */
}

/*
 * BufferManagerShmemValidate
 *		Validate that buffer manager shared memory structures have correct
 *		pointers and sizes after a resize operation.
 *
 * This function is called by backends during ProcessBarrierShmemResizeStruct
 * to ensure their view of the buffer structures is consistent after memory
 * remapping.
 */
void
BufferManagerShmemValidate(int targetNBuffers)
{
	bool found;
	void *tmpPtr;

	/* Validate Buffer Descriptors */
	tmpPtr = (BufferDescPadded *)
		ShmemInitStructInSegment("Buffer Descriptors",
						targetNBuffers * sizeof(BufferDescPadded),
						&found, BUFFER_DESCRIPTORS_SHMEM_SEGMENT);
	if (!found || BufferDescriptors != tmpPtr)
		elog(FATAL, "validating buffer descriptors failed: expected pointer %p, got %p, found=%d",
			 BufferDescriptors, tmpPtr, found);

	/* Validate Buffer IO Condition Variables */
	tmpPtr = (ConditionVariableMinimallyPadded *)
		ShmemInitStructInSegment("Buffer IO Condition Variables",
						targetNBuffers * sizeof(ConditionVariableMinimallyPadded),
						&found, BUFFER_IOCV_SHMEM_SEGMENT);
	if (!found || BufferIOCVArray != tmpPtr)
		elog(FATAL, "validating buffer IO condition variables failed: expected pointer %p, got %p, found=%d",
			 BufferIOCVArray, tmpPtr, found);

	/* Validate Checkpoint BufferIds */
	tmpPtr = (CkptSortItem *)
		ShmemInitStructInSegment("Checkpoint BufferIds",
						targetNBuffers * sizeof(CkptSortItem), &found,
						CHECKPOINT_BUFFERS_SHMEM_SEGMENT);
	if (!found || CkptBufferIds != tmpPtr)
		elog(FATAL, "validating checkpoint buffer IDs failed: expected pointer %p, got %p, found=%d",
			 CkptBufferIds, tmpPtr, found);

	/* Validate Buffer Blocks */
	tmpPtr = (char *)
		TYPEALIGN(PG_IO_ALIGN_SIZE,
				  ShmemInitStructInSegment("Buffer Blocks",
								  targetNBuffers * (Size) BLCKSZ + PG_IO_ALIGN_SIZE,
								  &found, BUFFERS_SHMEM_SEGMENT));
	if (!found || BufferBlocks != tmpPtr)
		elog(FATAL, "validating buffer blocks failed: expected pointer %p, got %p, found=%d",
			 BufferBlocks, tmpPtr, found);
}

/*
 * check_shared_buffers
 *		GUC check_hook for shared_buffers
 *
 * When reloading the configuration, shared_buffers should not be set to a value
 * higher than max_shared_buffers fixed at the boot time.
 */
bool
check_shared_buffers(int *newval, void **extra, GucSource source)
{
	if (finalMaxNBuffers && *newval > MaxNBuffers)
	{
		GUC_check_errdetail("\"shared_buffers\" must be less than \"max_shared_buffers\".");
		return false;
	}
	return true;
}

/*
 * show_shared_buffers
 *		GUC show_hook for shared_buffers
 *
 * Shows both current and pending buffer counts with proper unit formatting.
 */
const char *
show_shared_buffers(void)
{
	static char buffer[128];
	int64 current_value, pending_value;
	const char *current_unit, *pending_unit;
	int currentNBuffers = pg_atomic_read_u32(&ShmemCtrl->currentNBuffers);

	if (currentNBuffers == NBuffersPending)
	{
		/* No buffer pool resizing pending. */
		convert_int_from_base_unit(currentNBuffers, GUC_UNIT_BLOCKS, &current_value, &current_unit);
		snprintf(buffer, sizeof(buffer), INT64_FORMAT "%s", current_value, current_unit);
	}
	else
	{
		/*
		 * New value for NBuffers is loaded but not applied yet, show both
		 * current and pending.
		 */
		convert_int_from_base_unit(currentNBuffers, GUC_UNIT_BLOCKS, &current_value, &current_unit);
		convert_int_from_base_unit(NBuffersPending, GUC_UNIT_BLOCKS, &pending_value, &pending_unit);
		snprintf(buffer, sizeof(buffer), INT64_FORMAT "%s (pending: " INT64_FORMAT "%s)",
				 current_value, current_unit, pending_value, pending_unit);
	}

	return buffer;
}
