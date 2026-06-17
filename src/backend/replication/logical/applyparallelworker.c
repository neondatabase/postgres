/*-------------------------------------------------------------------------
 * applyparallelworker.c
 *	   Support routines for applying xact by parallel apply worker
 *
 * Copyright (c) 2023-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/applyparallelworker.c
 *
 * This file contains the code to launch, set up, and teardown a parallel apply
 * worker which receives the changes from the leader worker and invokes routines
 * to apply those on the subscriber database. Additionally, this file contains
 * routines that are intended to support setting up, using, and tearing down a
 * ParallelApplyWorkerInfo which is required so the leader worker and parallel
 * apply workers can communicate with each other.
 *
 * Streaming transactions
 * ======================
 *
 * The parallel apply workers are assigned (if available) as soon as xact's
 * first stream is received for subscriptions that have set their 'streaming'
 * option as parallel. The leader apply worker will send changes to this new
 * worker via shared memory. We keep this worker assigned till the transaction
 * commit is received and also wait for the worker to finish at commit. This
 * preserves commit ordering and avoid file I/O in most cases, although we
 * still need to spill to a file if there is no worker available. See comments
 * atop logical/worker to know more about streamed xacts whose changes are
 * spilled to disk. It is important to maintain commit order to avoid failures
 * due to: (a) transaction dependencies - say if we insert a row in the first
 * transaction and update it in the second transaction on publisher then
 * allowing the subscriber to apply both in parallel can lead to failure in the
 * update; (b) deadlocks - allowing transactions that update the same set of
 * rows/tables in the opposite order to be applied in parallel can lead to
 * deadlocks.
 *
 * A worker pool is used to avoid restarting workers for each streaming
 * transaction. We maintain each worker's information (ParallelApplyWorkerInfo)
 * in the ParallelApplyWorkerPool. After successfully launching a new worker,
 * its information is added to the ParallelApplyWorkerPool. Once the worker
 * finishes applying the transaction, it is marked as available for re-use.
 * Now, before starting a new worker to apply the streaming transaction, we
 * check the list for any available worker. Note that we retain a maximum of
 * half the max_parallel_apply_workers_per_subscription workers in the pool and
 * after that, we simply exit the worker after applying the transaction.
 *
 * XXX This worker pool threshold is arbitrary and we can provide a GUC
 * variable for this in the future if required.
 *
 * The leader apply worker will create a separate dynamic shared memory segment
 * when each parallel apply worker starts. The reason for this design is that
 * we cannot predict how many workers will be needed. It may be possible to
 * allocate enough shared memory in one segment based on the maximum number of
 * parallel apply workers (max_parallel_apply_workers_per_subscription), but
 * this would waste memory if no process is actually started.
 *
 * The dynamic shared memory segment contains: (a) a shm_mq that is used to
 * send changes in the transaction from leader apply worker to parallel apply
 * worker; (b) another shm_mq that is used to send errors (and other messages
 * reported via elog/ereport) from the parallel apply worker to leader apply
 * worker; (c) necessary information to be shared among parallel apply workers
 * and the leader apply worker (i.e. members of ParallelApplyWorkerShared).
 *
 * Locking Considerations
 * ----------------------
 * We have a risk of deadlock due to concurrently applying the transactions in
 * parallel mode that were independent on the publisher side but became
 * dependent on the subscriber side due to the different database structures
 * (like schema of subscription tables, constraints, etc.) on each side. This
 * can happen even without parallel mode when there are concurrent operations
 * on the subscriber. In order to detect the deadlocks among leader (LA) and
 * parallel apply (PA) workers, we used lmgr locks when the PA waits for the
 * next stream (set of changes) and LA waits for PA to finish the transaction.
 * An alternative approach could be to not allow parallelism when the schema of
 * tables is different between the publisher and subscriber but that would be
 * too restrictive and would require the publisher to send much more
 * information than it is currently sending.
 *
 * Consider a case where the subscribed table does not have a unique key on the
 * publisher and has a unique key on the subscriber. The deadlock can happen in
 * the following ways:
 *
 * 1) Deadlock between the leader apply worker and a parallel apply worker
 *
 * Consider that the parallel apply worker (PA) is executing TX-1 and the
 * leader apply worker (LA) is executing TX-2 concurrently on the subscriber.
 * Now, LA is waiting for PA because of the unique key constraint of the
 * subscribed table while PA is waiting for LA to send the next stream of
 * changes or transaction finish command message.
 *
 * In order for lmgr to detect this, we have LA acquire a session lock on the
 * remote transaction (by pa_lock_stream()) and have PA wait on the lock before
 * trying to receive the next stream of changes. Specifically, LA will acquire
 * the lock in AccessExclusive mode before sending the STREAM_STOP and will
 * release it if already acquired after sending the STREAM_START, STREAM_ABORT
 * (for toplevel transaction), STREAM_PREPARE, and STREAM_COMMIT. The PA will
 * acquire the lock in AccessShare mode after processing STREAM_STOP and
 * STREAM_ABORT (for subtransaction) and then release the lock immediately
 * after acquiring it.
 *
 * The lock graph for the above example will look as follows:
 * LA (waiting to acquire the lock on the unique index) -> PA (waiting to
 * acquire the stream lock) -> LA
 *
 * This way, when PA is waiting for LA for the next stream of changes, we can
 * have a wait-edge from PA to LA in lmgr, which will make us detect the
 * deadlock between LA and PA.
 *
 * 2) Deadlock between the leader apply worker and parallel apply workers
 *
 * This scenario is similar to the first case but TX-1 and TX-2 are executed by
 * two parallel apply workers (PA-1 and PA-2 respectively). In this scenario,
 * PA-2 is waiting for PA-1 to complete its transaction while PA-1 is waiting
 * for subsequent input from LA. Also, LA is waiting for PA-2 to complete its
 * transaction in order to preserve the commit order. There is a deadlock among
 * the three processes.
 *
 * In order for lmgr to detect this, we have PA acquire a session lock (this is
 * a different lock than referred in the previous case, see
 * pa_lock_transaction()) on the transaction being applied and have LA wait on
 * the lock before proceeding in the transaction finish commands. Specifically,
 * PA will acquire this lock in AccessExclusive mode before executing the first
 * message of the transaction and release it at the xact end. LA will acquire
 * this lock in AccessShare mode at transaction finish commands and release it
 * immediately.
 *
 * The lock graph for the above example will look as follows:
 * LA (waiting to acquire the transaction lock) -> PA-2 (waiting to acquire the
 * lock due to unique index constraint) -> PA-1 (waiting to acquire the stream
 * lock) -> LA
 *
 * This way when LA is waiting to finish the transaction end command to preserve
 * the commit order, we will be able to detect deadlock, if any.
 *
 * One might think we can use XactLockTableWait(), but XactLockTableWait()
 * considers PREPARED TRANSACTION as still in progress which means the lock
 * won't be released even after the parallel apply worker has prepared the
 * transaction.
 *
 * 3) Deadlock when the shm_mq buffer is full
 *
 * In the previous scenario (ie. PA-1 and PA-2 are executing transactions
 * concurrently), if the shm_mq buffer between LA and PA-2 is full, LA has to
 * wait to send messages, and this wait doesn't appear in lmgr.
 *
 * To avoid this wait, we use a non-blocking write and wait with a timeout. If
 * the timeout is exceeded, the LA will serialize all the pending messages to
 * a file and indicate PA-2 that it needs to read that file for the remaining
 * messages. Then LA will start waiting for commit as in the previous case
 * which will detect deadlock if any. See pa_send_data() and
 * enum TransApplyAction.
 *
 *
 * Non-streaming transactions
 * ======================
 * The handling is similar to streaming transactions, but including few
 * differences:
 *
 * Transaction dependency
 * -------------------------------
 * Before dispatching changes to a parallel worker, the leader verifies if the
 * current modification affects the same row (identitied by replica identity
 * key) as another ongoing transaction (see handle_dependency_on_change for
 * details). If so, the leader sends a list of dependent transaction IDs to the
 * parallel worker, indicating that the parallel apply worker must wait for
 * these transactions to commit before proceeding. Parallel apply workers do not
 * maintain commit order; transactions can be committed at any time provided
 * there are no dependencies.
 *
 * Lock types
 * ----------
 * Both the stream lock and the transaction lock mentioned above are
 * session-level locks because both locks could be acquired outside the
 * transaction, and the stream lock in the leader needs to persist across
 * transaction boundaries i.e. until the end of the streaming transaction.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq/pqformat.h"
#include "libpq/pqmq.h"
#include "pgstat.h"
#include "postmaster/interrupt.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "replication/origin.h"
#include "replication/worker_internal.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#define PG_LOGICAL_APPLY_SHM_MAGIC 0x787ca067

/*
 * DSM keys for parallel apply worker. Unlike other parallel execution code,
 * since we don't need to worry about DSM keys conflicting with plan_node_id we
 * can use small integers.
 */
#define PARALLEL_APPLY_KEY_SHARED		1
#define PARALLEL_APPLY_KEY_MQ			2
#define PARALLEL_APPLY_KEY_ERROR_QUEUE	3

/* Queue size of DSM, 16 MB for now. */
#define DSM_QUEUE_SIZE	(16 * 1024 * 1024)

/*
 * Error queue size of DSM. It is desirable to make it large enough that a
 * typical ErrorResponse can be sent without blocking. That way, a worker that
 * errors out can write the whole message into the queue and terminate without
 * waiting for the user backend.
 */
#define DSM_ERROR_QUEUE_SIZE			(16 * 1024)

/*
 * There are three fields in each message received by the parallel apply
 * worker: start_lsn, end_lsn and send_time. Because we have updated these
 * statistics in the leader apply worker, we can ignore these fields in the
 * parallel apply worker (see function LogicalRepApplyLoop).
 */
#define SIZE_STATS_MESSAGE (2 * sizeof(XLogRecPtr) + sizeof(TimestampTz))

/*
 * The type of session-level lock on a transaction being applied on a logical
 * replication subscriber.
 */
#define PARALLEL_APPLY_LOCK_STREAM	0
#define PARALLEL_APPLY_LOCK_XACT	1

/*
 * Hash table entry to map xid to the parallel apply worker state.
 */
typedef struct ParallelApplyWorkerEntry
{
	TransactionId xid;			/* Hash key -- must be first */
	ParallelApplyWorkerInfo *winfo;
	XLogRecPtr	local_end;
} ParallelApplyWorkerEntry;

/* an entry in the parallelized_txns shared hash table */
typedef struct ParallelizedTxnEntry
{
	TransactionId xid;			/* Hash key */
} ParallelizedTxnEntry;

/*
 * A hash table used to cache the state of streaming transactions being applied
 * by the parallel apply workers.
 */
static HTAB *ParallelApplyTxnHash = NULL;

/*
 * A hash table used to track the parallelized transactions that could be
 * depended on by other transactions.
 */
static dsa_area *parallel_apply_dsa_area = NULL;
static dshash_table *parallelized_txns = NULL;

/* parameters for the parallelized_txns shared hash table */
static const dshash_parameters dsh_params = {
	sizeof(TransactionId),
	sizeof(ParallelizedTxnEntry),
	dshash_memcmp,
	dshash_memhash,
	dshash_memcpy,
	LWTRANCHE_PARALLEL_APPLY_DSA
};

/*
* A list (pool) of active parallel apply workers. The information for
* the new worker is added to the list after successfully launching it. The
* list entry is removed if there are already enough workers in the worker
* pool at the end of the transaction. For more information about the worker
* pool, see comments atop this file.
 */
static List *ParallelApplyWorkerPool = NIL;

/*
 * Information shared between leader apply worker and parallel apply worker.
 */
ParallelApplyWorkerShared *MyParallelShared = NULL;

/*
 * Is there a message sent by a parallel apply worker that the leader apply
 * worker needs to receive?
 */
volatile sig_atomic_t ParallelApplyMessagePending = false;

/*
 * Cache the parallel apply worker information required for applying the
 * current streaming transaction. It is used to save the cost of searching the
 * hash table when applying the changes between STREAM_START and STREAM_STOP.
 */
static ParallelApplyWorkerInfo *stream_apply_worker = NULL;

/* A list to maintain subtransactions, if any. */
static List *subxactlist = NIL;

/* GUC */
bool preserve_commit_order = true;

static void pa_free_worker_info(ParallelApplyWorkerInfo *winfo);
static ParallelTransState pa_get_xact_state(ParallelApplyWorkerShared *wshared);
static PartialFileSetState pa_get_fileset_state(void);
static void pa_attach_parallelized_txn_hash(dsa_handle *pa_dsa_handle,
											dshash_table_handle *pa_dshash_handle);
static void write_internal_relation(StringInfo s, LogicalRepRelation *rel);

/*
 * Returns true if it is OK to start a parallel apply worker, false otherwise.
 */
static bool
pa_can_start(void)
{
	/* Only leader apply workers can start parallel apply workers. */
	if (!am_leader_apply_worker())
		return false;

	/*
	 * It is good to check for any change in the subscription parameter to
	 * avoid the case where for a very long time the change doesn't get
	 * reflected. This can happen when there is a constant flow of streaming
	 * transactions that are handled by parallel apply workers.
	 *
	 * It is better to do it before the below checks so that the latest values
	 * of subscription can be used for the checks.
	 */
	maybe_reread_subscription();

	/*
	 * Don't start a new parallel apply worker if the subscription is not
	 * using parallel streaming mode, or if the publisher does not support
	 * parallel apply.
	 */
	if (!MyLogicalRepWorker->parallel_apply)
		return false;

	/*
	 * Don't start a new parallel worker if user has set skiplsn as it's
	 * possible that they want to skip the streaming transaction. For
	 * streaming transactions, we need to serialize the transaction to a file
	 * so that we can get the last LSN of the transaction to judge whether to
	 * skip before starting to apply the change.
	 *
	 * One might think that we could allow parallelism if the first lsn of the
	 * transaction is greater than skiplsn, but we don't send it with the
	 * STREAM START message, and it doesn't seem worth sending the extra eight
	 * bytes with the STREAM START to enable parallelism for this case.
	 */
	if (!XLogRecPtrIsInvalid(MySubscription->skiplsn))
		return false;

	/*
	 * For streaming transactions that are being applied using a parallel
	 * apply worker, we cannot decide whether to apply the change for a
	 * relation that is not in the READY state (see
	 * should_apply_changes_for_rel) as we won't know remote_final_lsn by that
	 * time. So, we don't start the new parallel apply worker in this case.
	 */
	if (!AllTablesyncsReady())
		return false;

	return true;
}

/*
 * Set up a dynamic shared memory segment.
 *
 * We set up a control region that contains a fixed-size worker info
 * (ParallelApplyWorkerShared), a message queue, and an error queue.
 *
 * Returns true on success, false on failure.
 */
static bool
pa_setup_dsm(ParallelApplyWorkerInfo *winfo)
{
	shm_toc_estimator e;
	Size		segsize;
	dsm_segment *seg;
	shm_toc    *toc;
	ParallelApplyWorkerShared *shared;
	shm_mq	   *mq;
	Size		queue_size = DSM_QUEUE_SIZE;
	Size		error_queue_size = DSM_ERROR_QUEUE_SIZE;
	dsa_handle	parallel_apply_dsa_handle;
	dshash_table_handle parallelized_txns_handle;

	pa_attach_parallelized_txn_hash(&parallel_apply_dsa_handle,
									&parallelized_txns_handle);

	if (parallel_apply_dsa_handle == DSA_HANDLE_INVALID ||
		parallelized_txns_handle == DSHASH_HANDLE_INVALID)
		return false;

	/*
	 * Estimate how much shared memory we need.
	 *
	 * Because the TOC machinery may choose to insert padding of oddly-sized
	 * requests, we must estimate each chunk separately.
	 *
	 * We need one key to register the location of the header, and two other
	 * keys to track the locations of the message queue and the error message
	 * queue.
	 */
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(ParallelApplyWorkerShared));
	shm_toc_estimate_chunk(&e, queue_size);
	shm_toc_estimate_chunk(&e, error_queue_size);

	shm_toc_estimate_keys(&e, 3);
	segsize = shm_toc_estimate(&e);

	/* Create the shared memory segment and establish a table of contents. */
	seg = dsm_create(shm_toc_estimate(&e), 0);
	if (!seg)
		return false;

	toc = shm_toc_create(PG_LOGICAL_APPLY_SHM_MAGIC, dsm_segment_address(seg),
						 segsize);

	/* Set up the header region. */
	shared = shm_toc_allocate(toc, sizeof(ParallelApplyWorkerShared));
	SpinLockInit(&shared->mutex);
	shared->xid = InvalidTransactionId;
	shared->xact_state = PARALLEL_TRANS_UNKNOWN;
	pg_atomic_init_u32(&(shared->pending_stream_count), 0);
	shared->last_commit_end = InvalidXLogRecPtr;
	shared->fileset_state = FS_EMPTY;
	shared->parallel_apply_dsa_handle = parallel_apply_dsa_handle;
	shared->parallelized_txns_handle = parallelized_txns_handle;
	shared->has_dependent_txn = false;

	shm_toc_insert(toc, PARALLEL_APPLY_KEY_SHARED, shared);

	/* Set up message queue for the worker. */
	mq = shm_mq_create(shm_toc_allocate(toc, queue_size), queue_size);
	shm_toc_insert(toc, PARALLEL_APPLY_KEY_MQ, mq);
	shm_mq_set_sender(mq, MyProc);

	/* Attach the queue. */
	winfo->mq_handle = shm_mq_attach(mq, seg, NULL);

	/* Set up error queue for the worker. */
	mq = shm_mq_create(shm_toc_allocate(toc, error_queue_size),
					   error_queue_size);
	shm_toc_insert(toc, PARALLEL_APPLY_KEY_ERROR_QUEUE, mq);
	shm_mq_set_receiver(mq, MyProc);

	/* Attach the queue. */
	winfo->error_mq_handle = shm_mq_attach(mq, seg, NULL);

	/* Return results to caller. */
	winfo->dsm_seg = seg;
	winfo->shared = shared;

	return true;
}

/*
 * Try to get a parallel apply worker from the pool. If none is available then
 * start a new one.
 */
static ParallelApplyWorkerInfo *
pa_launch_parallel_worker(void)
{
	MemoryContext oldcontext;
	bool		launched;
	ParallelApplyWorkerInfo *winfo;
	dsa_handle	pa_dsa_handle;
	dshash_table_handle pa_dshash_handle;
	ListCell   *lc;

	/* Try to get an available parallel apply worker from the worker pool. */
	foreach(lc, ParallelApplyWorkerPool)
	{
		winfo = (ParallelApplyWorkerInfo *) lfirst(lc);

		if (!winfo->stream_txn &&
			pa_get_xact_state(winfo->shared) == PARALLEL_TRANS_FINISHED)
		{
			/*
			 * Save the local commit LSN of the last transaction applied by this
			 * worker before reusing it for another transaction. This WAL
			 * position is crucial for determining the flush position in
			 * responses to the publisher (see get_flush_position()).
			 */
			(void) pa_get_last_commit_end(winfo->shared->xid, false, NULL);
			return winfo;
		}

		if (winfo->stream_txn && !winfo->in_use)
			return winfo;
	}

	pa_attach_parallelized_txn_hash(&pa_dsa_handle, &pa_dshash_handle);

	/*
	 * Return if the number of parallel apply workers has reached the maximum
	 * limit.
	 */
	if (list_length(ParallelApplyWorkerPool) ==
		max_parallel_apply_workers_per_subscription)
		return NULL;

	/*
	 * Start a new parallel apply worker.
	 *
	 * The worker info can be used for the lifetime of the worker process, so
	 * create it in a permanent context.
	 */
	oldcontext = MemoryContextSwitchTo(ApplyContext);

	winfo = (ParallelApplyWorkerInfo *) palloc0(sizeof(ParallelApplyWorkerInfo));

	/* Setup shared memory. */
	if (!pa_setup_dsm(winfo))
	{
		MemoryContextSwitchTo(oldcontext);
		pfree(winfo);
		return NULL;
	}

	launched = logicalrep_worker_launch(WORKERTYPE_PARALLEL_APPLY,
										MyLogicalRepWorker->dbid,
										MySubscription->oid,
										MySubscription->name,
										MyLogicalRepWorker->userid,
										InvalidOid,
										dsm_segment_handle(winfo->dsm_seg));

	if (!launched)
	{
		MemoryContextSwitchTo(oldcontext);
		pa_free_worker_info(winfo);
		return NULL;
	}

	ParallelApplyWorkerPool = lappend(ParallelApplyWorkerPool, winfo);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Send all existing remote relation information to the parallel apply
	 * worker. This allows the parallel worker to initialize the
	 * LogicalRepRelMapEntry locally before applying remote changes.
	 */
	if (logicalrep_get_num_rels())
	{
		StringInfoData out;
		initStringInfo(&out);

		write_internal_relation(&out, NULL);
		pa_send_data(winfo, out.len, out.data);
	}

	return winfo;
}

/*
 * Allocate a parallel apply worker that will be used for the specified xid.
 *
 * We first try to get an available worker from the pool, if any and then try
 * to launch a new worker. On successful allocation, remember the worker
 * information in the hash table so that we can get it later for processing the
 * streaming changes.
 */
void
pa_allocate_worker(TransactionId xid, bool stream_txn)
{
	bool		found;
	ParallelApplyWorkerInfo *winfo = NULL;
	ParallelApplyWorkerEntry *entry;

	if (!pa_can_start())
		return;

	winfo = pa_launch_parallel_worker();
	if (!winfo)
		return;

	/* First time through, initialize parallel apply worker state hashtable. */
	if (!ParallelApplyTxnHash)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(TransactionId);
		ctl.entrysize = sizeof(ParallelApplyWorkerEntry);
		ctl.hcxt = ApplyContext;

		ParallelApplyTxnHash = hash_create("logical replication parallel apply workers hash",
										   16, &ctl,
										   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/* Create an entry for the requested transaction. */
	entry = hash_search(ParallelApplyTxnHash, &xid, HASH_ENTER, &found);
	if (found)
		elog(ERROR, "hash table corrupted");

	/* Update the transaction information in shared memory. */
	SpinLockAcquire(&winfo->shared->mutex);
	winfo->shared->xact_state = PARALLEL_TRANS_UNKNOWN;
	winfo->shared->xid = xid;
	winfo->shared->has_dependent_txn = false;
	SpinLockRelease(&winfo->shared->mutex);

	winfo->in_use = true;
	winfo->serialize_changes = false;
	winfo->stream_txn = stream_txn;
	entry->winfo = winfo;
	entry->local_end = InvalidXLogRecPtr;
}

/*
 * Find the assigned worker for the given transaction, if any.
 */
ParallelApplyWorkerInfo *
pa_find_worker(TransactionId xid)
{
	bool		found;
	ParallelApplyWorkerEntry *entry;

	if (!TransactionIdIsValid(xid))
		return NULL;

	if (!ParallelApplyTxnHash)
		return NULL;

	/* Return the cached parallel apply worker if valid. */
	if (stream_apply_worker)
		return stream_apply_worker;

	/* Find an entry for the requested transaction. */
	entry = hash_search(ParallelApplyTxnHash, &xid, HASH_FIND, &found);
	if (found)
	{
		/* The worker must not have exited.  */
		Assert(entry->winfo->in_use);
		return entry->winfo;
	}

	return NULL;
}

/*
 * Makes the worker available for reuse.
 *
 * This removes the parallel apply worker entry from the hash table so that it
 * can't be used. If there are enough workers in the pool, it stops the worker
 * and frees the corresponding info. Otherwise it just marks the worker as
 * available for reuse.
 *
 * For more information about the worker pool, see comments atop this file.
 */
static void
pa_free_worker(ParallelApplyWorkerInfo *winfo)
{
	Assert(!am_parallel_apply_worker());
	Assert(winfo->in_use);
	Assert(!winfo->stream_txn ||
		   pa_get_xact_state(winfo->shared) == PARALLEL_TRANS_FINISHED);

	if (!hash_search(ParallelApplyTxnHash, &winfo->shared->xid, HASH_REMOVE, NULL))
		elog(ERROR, "hash table corrupted");

	/*
	 * Stop the worker if there are enough workers in the pool.
	 *
	 * XXX Additionally, we also stop the worker if the leader apply worker
	 * serialize part of the transaction data due to a send timeout. This is
	 * because the message could be partially written to the queue and there
	 * is no way to clean the queue other than resending the message until it
	 * succeeds. Instead of trying to send the data which anyway would have
	 * been serialized and then letting the parallel apply worker deal with
	 * the spurious message, we stop the worker.
	 */
	if (winfo->serialize_changes)
	{
		logicalrep_pa_worker_stop(winfo);
		pa_free_worker_info(winfo);

		return;
	}

	winfo->in_use = false;
	winfo->serialize_changes = false;
}

/*
 * Free the parallel apply worker information and unlink the files with
 * serialized changes if any.
 */
static void
pa_free_worker_info(ParallelApplyWorkerInfo *winfo)
{
	Assert(winfo);

	if (winfo->mq_handle)
		shm_mq_detach(winfo->mq_handle);

	if (winfo->error_mq_handle)
		shm_mq_detach(winfo->error_mq_handle);

	/* Unlink the files with serialized changes. */
	if (winfo->serialize_changes)
		stream_cleanup_files(MyLogicalRepWorker->subid, winfo->shared->xid);

	if (winfo->dsm_seg)
		dsm_detach(winfo->dsm_seg);

	/* Remove from the worker pool. */
	ParallelApplyWorkerPool = list_delete_ptr(ParallelApplyWorkerPool, winfo);

	pfree(winfo);
}

/*
 * Detach the error queue for all parallel apply workers.
 */
void
pa_detach_all_error_mq(void)
{
	ListCell   *lc;

	foreach(lc, ParallelApplyWorkerPool)
	{
		ParallelApplyWorkerInfo *winfo = (ParallelApplyWorkerInfo *) lfirst(lc);

		if (winfo->error_mq_handle)
		{
			shm_mq_detach(winfo->error_mq_handle);
			winfo->error_mq_handle = NULL;
		}
	}
}

/*
 * Check if there are any pending spooled messages.
 */
static bool
pa_has_spooled_message_pending()
{
	PartialFileSetState fileset_state;

	fileset_state = pa_get_fileset_state();

	return (fileset_state != FS_EMPTY);
}

/*
 * Replay the spooled messages once the leader apply worker has finished
 * serializing changes to the file.
 *
 * Returns false if there aren't any pending spooled messages, true otherwise.
 */
static bool
pa_process_spooled_messages_if_required(void)
{
	PartialFileSetState fileset_state;

	fileset_state = pa_get_fileset_state();

	if (fileset_state == FS_EMPTY)
		return false;

	/*
	 * If the leader apply worker is busy serializing the partial changes then
	 * acquire the stream lock now and wait for the leader worker to finish
	 * serializing the changes. Otherwise, the parallel apply worker won't get
	 * a chance to receive a STREAM_STOP (and acquire the stream lock) until
	 * the leader had serialized all changes which can lead to undetected
	 * deadlock.
	 *
	 * Note that the fileset state can be FS_SERIALIZE_DONE once the leader
	 * worker has finished serializing the changes.
	 */
	if (fileset_state == FS_SERIALIZE_IN_PROGRESS)
	{
		pa_lock_stream(MyParallelShared->xid, AccessShareLock);
		pa_unlock_stream(MyParallelShared->xid, AccessShareLock);

		fileset_state = pa_get_fileset_state();
	}

	/*
	 * We cannot read the file immediately after the leader has serialized all
	 * changes to the file because there may still be messages in the memory
	 * queue. We will apply all spooled messages the next time we call this
	 * function and that will ensure there are no messages left in the memory
	 * queue.
	 */
	if (fileset_state == FS_SERIALIZE_DONE)
	{
		pa_set_fileset_state(MyParallelShared, FS_READY);
	}
	else if (fileset_state == FS_READY)
	{
		apply_spooled_messages(&MyParallelShared->fileset,
							   MyParallelShared->xid,
							   InvalidXLogRecPtr);
		pa_set_fileset_state(MyParallelShared, FS_EMPTY);
	}

	return true;
}

/*
 * Get the local end LSN for a transaction applied by a parallel apply worker.
 *
 * Set delete_entry to true if you intend to remove the transaction from the
 * ParallelApplyTxnHash after collecting its LSN.
 *
 * If the parallel apply worker did not write any changes during the transaction
 * application due to situations like update/delete_missing or a before trigger,
 * the *skipped_write will be set to true.
 */
XLogRecPtr
pa_get_last_commit_end(TransactionId xid, bool delete_entry, bool *skipped_write)
{
	bool		found;
	ParallelApplyWorkerEntry *entry;
	ParallelApplyWorkerInfo *winfo;

	Assert(TransactionIdIsValid(xid));

	if (skipped_write)
		*skipped_write = false;

	/* Find an entry for the requested transaction. */
	entry = hash_search(ParallelApplyTxnHash, &xid, HASH_FIND, &found);

	if (!found)
		return InvalidXLogRecPtr;

	/*
	 * If worker info is NULL, it indicates that the worker has been reused for
	 * handling other transactions. Consequently, the local end LSN has already
	 * been collected and saved in entry->local_end.
	 */
	winfo = entry->winfo;
	if (winfo == NULL)
	{
		if (delete_entry &&
			!hash_search(ParallelApplyTxnHash, &xid, HASH_REMOVE, NULL))
			elog(ERROR, "hash table corrupted");

		if (skipped_write)
			*skipped_write = XLogRecPtrIsInvalid(entry->local_end);

		return entry->local_end;
	}

	/* Return InvalidXLogRecPtr if the transaction is still in progress */
	if (pa_get_xact_state(winfo->shared) != PARALLEL_TRANS_FINISHED)
		return InvalidXLogRecPtr;

	/* Collect the local end LSN from the worker's shared memory area */
	entry->local_end = winfo->shared->last_commit_end;
	entry->winfo = NULL;

	if (skipped_write)
		*skipped_write = XLogRecPtrIsInvalid(entry->local_end);

	elog(DEBUG1, "store local commit %X/%X end to txn entry: %u",
		 LSN_FORMAT_ARGS(entry->local_end), xid);

	if (delete_entry &&
		!hash_search(ParallelApplyTxnHash, &xid, HASH_REMOVE, NULL))
		elog(ERROR, "hash table corrupted");

	return entry->local_end;
}

/*
 * Wait for the remote transaction associated with the specified remote xid to
 * complete.
 */
static void
pa_wait_for_transaction(TransactionId wait_for_xid)
{
	if (!am_leader_apply_worker())
		return;

	if (!TransactionIdIsValid(wait_for_xid))
		return;

	elog(DEBUG1, "plan to wait for remote_xid %u to finish",
		 wait_for_xid);

	for (;;)
	{
		if (pa_transaction_committed(wait_for_xid))
			break;

		pa_lock_transaction(wait_for_xid, AccessShareLock);
		pa_unlock_transaction(wait_for_xid, AccessShareLock);

		/* An interrupt may have occurred while we were waiting. */
		CHECK_FOR_INTERRUPTS();
	}

	elog(DEBUG1, "finished wait for remote_xid %u to finish",
		 wait_for_xid);
}

/*
 * Interrupt handler for main loop of parallel apply worker.
 */
static void
ProcessParallelApplyInterrupts(void)
{
	CHECK_FOR_INTERRUPTS();

	if (ShutdownRequestPending)
	{
		ereport(LOG,
				(errmsg("logical replication parallel apply worker for subscription \"%s\" has finished",
						MySubscription->name)));

		proc_exit(0);
	}

	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}
}

/* Parallel apply worker main loop. */
static void
LogicalParallelApplyLoop(shm_mq_handle *mqh)
{
	shm_mq_result shmq_res;
	ErrorContextCallback errcallback;
	MemoryContext oldcxt = CurrentMemoryContext;

	/*
	 * Init the ApplyMessageContext which we clean up after each replication
	 * protocol message.
	 */
	ApplyMessageContext = AllocSetContextCreate(ApplyContext,
												"ApplyMessageContext",
												ALLOCSET_DEFAULT_SIZES);

	/*
	 * Push apply error context callback. Fields will be filled while applying
	 * a change.
	 */
	errcallback.callback = apply_error_callback;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	for (;;)
	{
		void	   *data;
		Size		len;

		ProcessParallelApplyInterrupts();

		/* Ensure we are reading the data into our memory context. */
		MemoryContextSwitchTo(ApplyMessageContext);

		shmq_res = shm_mq_receive(mqh, &len, &data, true);

		if (shmq_res == SHM_MQ_SUCCESS)
		{
			StringInfoData s;
			int			c;

			if (len == 0)
				elog(ERROR, "invalid message length");

			initReadOnlyStringInfo(&s, data, len);

			/*
			 * The first byte of messages sent from leader apply worker to
			 * parallel apply workers can only be 'w'.
			 */
			c = pq_getmsgbyte(&s);
			if (c == 'w')
			{
				/*
				 * Ignore statistics fields that have been updated by the
				 * leader apply worker.
				 *
				 * XXX We can avoid sending the statistics fields from the
				 * leader apply worker but for that, it needs to rebuild the
				 * entire message by removing these fields which could be more
				 * work than simply ignoring these fields in the parallel
				 * apply worker.
				 */
				s.cursor += SIZE_STATS_MESSAGE;

				apply_dispatch(&s);
			}
			else if (c == PARALLEL_APPLY_INTERNAL_MESSAGE)
			{
				apply_dispatch(&s);
			}
			else
			{
				/*
				 * The first byte of messages sent from leader apply worker to
				 * parallel apply workers can only be 'w' or 'i'.
				 */
				elog(ERROR, "unexpected message \"%c\"", c);
			}
		}
		else if (shmq_res == SHM_MQ_WOULD_BLOCK)
		{
			/* Replay the changes from the file, if any. */
			if (!pa_process_spooled_messages_if_required())
			{
				int			rc;

				/* Wait for more work. */
				rc = WaitLatch(MyLatch,
							   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							   1000L,
							   WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN);

				if (rc & WL_LATCH_SET)
					ResetLatch(MyLatch);

				if (!IsTransactionState())
					pgstat_report_stat(true);
			}
		}
		else
		{
			Assert(shmq_res == SHM_MQ_DETACHED);

			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("lost connection to the logical replication apply worker")));
		}

		MemoryContextReset(ApplyMessageContext);
		MemoryContextSwitchTo(oldcxt);
	}

	/* Pop the error context stack. */
	error_context_stack = errcallback.previous;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Make sure the leader apply worker tries to read from our error queue one more
 * time. This guards against the case where we exit uncleanly without sending
 * an ErrorResponse, for example because some code calls proc_exit directly.
 *
 * Also explicitly detach from dsm segment to invoke on_dsm_detach callbacks,
 * if any. See ParallelWorkerShutdown for details.
 */
static void
pa_shutdown(int code, Datum arg)
{
	SendProcSignal(MyLogicalRepWorker->leader_pid,
				   PROCSIG_PARALLEL_APPLY_MESSAGE,
				   INVALID_PROC_NUMBER);

	dsm_detach((dsm_segment *) DatumGetPointer(arg));

	if (parallel_apply_dsa_area)
		dsa_detach(parallel_apply_dsa_area);
}

/*
 * Parallel apply worker entry point.
 */
void
ParallelApplyWorkerMain(Datum main_arg)
{
	ParallelApplyWorkerShared *shared;
	dsm_handle	handle;
	dsm_segment *seg;
	shm_toc    *toc;
	shm_mq	   *mq;
	shm_mq_handle *mqh;
	shm_mq_handle *error_mqh;
	dsa_handle	pa_dsa_handle;
	dshash_table_handle pa_dshash_handle;
	RepOriginId originid;
	int			worker_slot = DatumGetInt32(main_arg);
	char		originname[NAMEDATALEN];

	InitializingApplyWorker = true;

	/*
	 * Setup signal handling.
	 *
	 * Note: We intentionally used SIGUSR2 to trigger a graceful shutdown
	 * initiated by the leader apply worker. This helps to differentiate it
	 * from the case where we abort the current transaction and exit on
	 * receiving SIGTERM.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	pqsignal(SIGUSR2, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	/*
	 * Attach to the dynamic shared memory segment for the parallel apply, and
	 * find its table of contents.
	 *
	 * Like parallel query, we don't need resource owner by this time. See
	 * ParallelWorkerMain.
	 */
	memcpy(&handle, MyBgworkerEntry->bgw_extra, sizeof(dsm_handle));
	seg = dsm_attach(handle);
	if (!seg)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not map dynamic shared memory segment")));

	toc = shm_toc_attach(PG_LOGICAL_APPLY_SHM_MAGIC, dsm_segment_address(seg));
	if (!toc)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid magic number in dynamic shared memory segment")));

	/* Look up the shared information. */
	shared = shm_toc_lookup(toc, PARALLEL_APPLY_KEY_SHARED, false);
	MyParallelShared = shared;

	/*
	 * Attach to the message queue.
	 */
	mq = shm_toc_lookup(toc, PARALLEL_APPLY_KEY_MQ, false);
	shm_mq_set_receiver(mq, MyProc);
	mqh = shm_mq_attach(mq, seg, NULL);

	/*
	 * Primary initialization is complete. Now, we can attach to our slot.
	 * This is to ensure that the leader apply worker does not write data to
	 * the uninitialized memory queue.
	 */
	logicalrep_worker_attach(worker_slot);

	/*
	 * Register the shutdown callback after we are attached to the worker
	 * slot. This is to ensure that MyLogicalRepWorker remains valid when this
	 * callback is invoked.
	 */
	before_shmem_exit(pa_shutdown, PointerGetDatum(seg));

	SpinLockAcquire(&MyParallelShared->mutex);
	MyParallelShared->logicalrep_worker_generation = MyLogicalRepWorker->generation;
	MyParallelShared->logicalrep_worker_slot_no = worker_slot;
	SpinLockRelease(&MyParallelShared->mutex);

	/*
	 * Attach to the error queue.
	 */
	mq = shm_toc_lookup(toc, PARALLEL_APPLY_KEY_ERROR_QUEUE, false);
	shm_mq_set_sender(mq, MyProc);
	error_mqh = shm_mq_attach(mq, seg, NULL);

	pq_redirect_to_shm_mq(seg, error_mqh);
	pq_set_parallel_leader(MyLogicalRepWorker->leader_pid,
						   INVALID_PROC_NUMBER);

	MyLogicalRepWorker->last_send_time = MyLogicalRepWorker->last_recv_time =
		MyLogicalRepWorker->reply_time = 0;

	InitializeLogRepWorker();

	InitializingApplyWorker = false;

	pa_attach_parallelized_txn_hash(&pa_dsa_handle, &pa_dshash_handle);

	/* Setup replication origin tracking. */
	StartTransactionCommand();
	ReplicationOriginNameForLogicalRep(MySubscription->oid, InvalidOid,
									   originname, sizeof(originname));
	originid = replorigin_by_name(originname, false);

	/*
	 * The parallel apply worker doesn't need to monopolize this replication
	 * origin which was already acquired by its leader process.
	 */
	replorigin_session_setup(originid, MyLogicalRepWorker->leader_pid);
	replorigin_session_origin = originid;
	CommitTransactionCommand();

	/*
	 * Setup callback for syscache so that we know when something changes in
	 * the subscription relation state.
	 */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONRELMAP,
								  invalidate_syncing_table_states,
								  (Datum) 0);

	set_apply_error_context_origin(originname);

	LogicalParallelApplyLoop(mqh);

	/*
	 * The parallel apply worker must not get here because the parallel apply
	 * worker will only stop when it receives a SIGTERM or SIGUSR2 from the
	 * leader, or SIGINT from itself, or when there is an error. None of these
	 * cases will allow the code to reach here.
	 */
	Assert(false);
}

/*
 * Handle receipt of an interrupt indicating a parallel apply worker message.
 *
 * Note: this is called within a signal handler! All we can do is set a flag
 * that will cause the next CHECK_FOR_INTERRUPTS() to invoke
 * ProcessParallelApplyMessages().
 */
void
HandleParallelApplyMessageInterrupt(void)
{
	InterruptPending = true;
	ParallelApplyMessagePending = true;
	SetLatch(MyLatch);
}

/*
 * Process a single protocol message received from a single parallel apply
 * worker.
 */
static void
ProcessParallelApplyMessage(StringInfo msg)
{
	char		msgtype;

	msgtype = pq_getmsgbyte(msg);

	switch (msgtype)
	{
		case 'E':				/* ErrorResponse */
			{
				ErrorData	edata;

				/* Parse ErrorResponse. */
				pq_parse_errornotice(msg, &edata);

				/*
				 * If desired, add a context line to show that this is a
				 * message propagated from a parallel apply worker. Otherwise,
				 * it can sometimes be confusing to understand what actually
				 * happened.
				 */
				if (edata.context)
					edata.context = psprintf("%s\n%s", edata.context,
											 _("logical replication parallel apply worker"));
				else
					edata.context = pstrdup(_("logical replication parallel apply worker"));

				/*
				 * Context beyond that should use the error context callbacks
				 * that were in effect in LogicalRepApplyLoop().
				 */
				error_context_stack = apply_error_context_stack;

				/*
				 * The actual error must have been reported by the parallel
				 * apply worker.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("logical replication parallel apply worker exited due to error"),
						 errcontext("%s", edata.context)));
			}

			/*
			 * Don't need to do anything about NoticeResponse and
			 * NotifyResponse as the logical replication worker doesn't need
			 * to send messages to the client.
			 */
		case 'N':
		case 'A':
			break;

		default:
			elog(ERROR, "unrecognized message type received from logical replication parallel apply worker: %c (message length %d bytes)",
				 msgtype, msg->len);
	}
}

/*
 * Handle any queued protocol messages received from parallel apply workers.
 */
void
ProcessParallelApplyMessages(void)
{
	ListCell   *lc;
	MemoryContext oldcontext;

	static MemoryContext hpam_context = NULL;

	/*
	 * This is invoked from ProcessInterrupts(), and since some of the
	 * functions it calls contain CHECK_FOR_INTERRUPTS(), there is a potential
	 * for recursive calls if more signals are received while this runs. It's
	 * unclear that recursive entry would be safe, and it doesn't seem useful
	 * even if it is safe, so let's block interrupts until done.
	 */
	HOLD_INTERRUPTS();

	/*
	 * Moreover, CurrentMemoryContext might be pointing almost anywhere. We
	 * don't want to risk leaking data into long-lived contexts, so let's do
	 * our work here in a private context that we can reset on each use.
	 */
	if (!hpam_context)			/* first time through? */
		hpam_context = AllocSetContextCreate(TopMemoryContext,
											 "ProcessParallelApplyMessages",
											 ALLOCSET_DEFAULT_SIZES);
	else
		MemoryContextReset(hpam_context);

	oldcontext = MemoryContextSwitchTo(hpam_context);

	ParallelApplyMessagePending = false;

	foreach(lc, ParallelApplyWorkerPool)
	{
		shm_mq_result res;
		Size		nbytes;
		void	   *data;
		ParallelApplyWorkerInfo *winfo = (ParallelApplyWorkerInfo *) lfirst(lc);

		/*
		 * The leader will detach from the error queue and set it to NULL
		 * before preparing to stop all parallel apply workers, so we don't
		 * need to handle error messages anymore. See
		 * logicalrep_worker_detach.
		 */
		if (!winfo->error_mq_handle)
			continue;

		res = shm_mq_receive(winfo->error_mq_handle, &nbytes, &data, true);

		if (res == SHM_MQ_WOULD_BLOCK)
			continue;
		else if (res == SHM_MQ_SUCCESS)
		{
			StringInfoData msg;

			initStringInfo(&msg);
			appendBinaryStringInfo(&msg, data, nbytes);
			ProcessParallelApplyMessage(&msg);
			pfree(msg.data);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("lost connection to the logical replication parallel apply worker")));
	}

	MemoryContextSwitchTo(oldcontext);

	/* Might as well clear the context on our way out */
	MemoryContextReset(hpam_context);

	RESUME_INTERRUPTS();
}

/*
 * Send the data to the specified parallel apply worker via shared-memory
 * queue.
 *
 * Returns false if the attempt to send data via shared memory times out, true
 * otherwise.
 */
bool
pa_send_data(ParallelApplyWorkerInfo *winfo, Size nbytes, const void *data)
{
	int			rc;
	shm_mq_result result;
	TimestampTz startTime = 0;

	Assert(!winfo->serialize_changes);

	/*
	 * We don't try to send data to parallel worker for 'immediate' mode. This
	 * is primarily used for testing purposes.
	 */
	if (unlikely(debug_logical_replication_streaming == DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE))
		return false;

/*
 * This timeout is a bit arbitrary but testing revealed that it is sufficient
 * to send the message unless the parallel apply worker is waiting on some
 * lock or there is a serious resource crunch. See the comments atop this file
 * to know why we are using a non-blocking way to send the message.
 */
#define SHM_SEND_RETRY_INTERVAL_MS 1000
#define SHM_SEND_TIMEOUT_MS		(10000 - SHM_SEND_RETRY_INTERVAL_MS)

	for (;;)
	{
		result = shm_mq_send(winfo->mq_handle, nbytes, data, true, true);

		if (result == SHM_MQ_SUCCESS)
			return true;
		else if (result == SHM_MQ_DETACHED)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("could not send data to shared-memory queue")));

		Assert(result == SHM_MQ_WOULD_BLOCK);

		/* Wait before retrying. */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   SHM_SEND_RETRY_INTERVAL_MS,
					   WAIT_EVENT_LOGICAL_APPLY_SEND_DATA);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		if (startTime == 0)
			startTime = GetCurrentTimestamp();
		else if (TimestampDifferenceExceeds(startTime, GetCurrentTimestamp(),
											SHM_SEND_TIMEOUT_MS))
			return false;
	}
}

/*
 * Distribute remote relation information to all active parallel apply workers
 * that require it.
 */
void
pa_distribute_schema_changes_to_workers(LogicalRepRelation *rel)
{
	List	   *workers_stopped = NIL;
	StringInfoData out;

	if (!ParallelApplyWorkerPool)
		return;

	initStringInfo(&out);

	write_internal_relation(&out, rel);

	foreach_ptr(ParallelApplyWorkerInfo, winfo, ParallelApplyWorkerPool)
	{
		/*
		 * Skip the worker responsible for the current transaction, as the
		 * relation information has already been sent to it.
		 */
		if (winfo == stream_apply_worker)
			continue;

		/*
		 * Skip the worker that is in serialize mode, as they will soon stop
		 * once they finish applying the transaction.
		 */
		if (winfo->serialize_changes)
			continue;

		elog(DEBUG1, "distributing schema changes to pa workers");

		if (pa_send_data(winfo, out.len, out.data))
			continue;

		elog(DEBUG1, "failed to distribute, will stop that worker instead");

		/*
		 * Distribution to this worker failed due to a sending timeout. Wait for
		 * the worker to complete its transaction and then stop it. This is
		 * consistent with the handling of workers in serialize mode (see
		 * pa_free_worker() for details).
		 */
		pa_wait_for_transaction(winfo->shared->xid);

		pa_get_last_commit_end(winfo->shared->xid, false, NULL);

		logicalrep_pa_worker_stop(winfo);

		workers_stopped = lappend(workers_stopped, winfo);
	}

	pfree(out.data);

	foreach_ptr(ParallelApplyWorkerInfo, winfo, workers_stopped)
		pa_free_worker_info(winfo);
}

/*
 * Switch to PARTIAL_SERIALIZE mode for the current transaction -- this means
 * that the current data and any subsequent data for this transaction will be
 * serialized to a file. This is done to prevent possible deadlocks with
 * another parallel apply worker (refer to the comments atop this file).
 */
void
pa_switch_to_partial_serialize(ParallelApplyWorkerInfo *winfo,
							   bool stream_locked)
{
	ereport(LOG,
			(errmsg("logical replication apply worker will serialize the remaining changes of remote transaction %u to a file",
					winfo->shared->xid)));

	/*
	 * The parallel apply worker could be stuck for some reason (say waiting
	 * on some lock by other backend), so stop trying to send data directly to
	 * it and start serializing data to the file instead.
	 */
	winfo->serialize_changes = true;

	/* Initialize the stream fileset. */
	stream_start_internal(winfo->shared->xid, true);

	/*
	 * Acquires the stream lock if not already to make sure that the parallel
	 * apply worker will wait for the leader to release the stream lock until
	 * the end of the transaction.
	 */
	if (!stream_locked)
		pa_lock_stream(winfo->shared->xid, AccessExclusiveLock);

	pa_set_fileset_state(winfo->shared, FS_SERIALIZE_IN_PROGRESS);
}

/*
 * Wait until the parallel apply worker's transaction state has reached or
 * exceeded the given xact_state.
 */
static void
pa_wait_for_xact_state(ParallelApplyWorkerInfo *winfo,
					   ParallelTransState xact_state)
{
	for (;;)
	{
		/*
		 * Stop if the transaction state has reached or exceeded the given
		 * xact_state.
		 */
		if (pa_get_xact_state(winfo->shared) >= xact_state)
			break;

		/* Wait to be signalled. */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 10L,
						 WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE);

		/* Reset the latch so we don't spin. */
		ResetLatch(MyLatch);

		/* An interrupt may have occurred while we were waiting. */
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * Wait until the parallel apply worker's transaction finishes.
 */
static void
pa_wait_for_xact_finish(ParallelApplyWorkerInfo *winfo)
{
	/*
	 * Wait until the parallel apply worker set the state to
	 * PARALLEL_TRANS_STARTED which means it has acquired the transaction
	 * lock. This is to prevent leader apply worker from acquiring the
	 * transaction lock earlier than the parallel apply worker.
	 */
	pa_wait_for_xact_state(winfo, PARALLEL_TRANS_STARTED);

	/*
	 * Wait for the transaction lock to be released. This is required to
	 * detect detect deadlock among leader and parallel apply workers. Refer
	 * to the comments atop this file.
	 */
	pa_lock_transaction(winfo->shared->xid, AccessShareLock);
	pa_unlock_transaction(winfo->shared->xid, AccessShareLock);

	/*
	 * Check if the state becomes PARALLEL_TRANS_FINISHED in case the parallel
	 * apply worker failed while applying changes causing the lock to be
	 * released.
	 */
	if (pa_get_xact_state(winfo->shared) != PARALLEL_TRANS_FINISHED)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("lost connection to the logical replication parallel apply worker")));

}

/*
 * Set the transaction state for a given parallel apply worker.
 */
void
pa_set_xact_state(ParallelApplyWorkerShared *wshared,
				  ParallelTransState xact_state)
{
	SpinLockAcquire(&wshared->mutex);
	wshared->xact_state = xact_state;
	SpinLockRelease(&wshared->mutex);
}

/*
 * Get the transaction state for a given parallel apply worker.
 */
static ParallelTransState
pa_get_xact_state(ParallelApplyWorkerShared *wshared)
{
	ParallelTransState xact_state;

	SpinLockAcquire(&wshared->mutex);
	xact_state = wshared->xact_state;
	SpinLockRelease(&wshared->mutex);

	return xact_state;
}

/*
 * Cache the parallel apply worker information.
 */
void
pa_set_stream_apply_worker(ParallelApplyWorkerInfo *winfo)
{
	stream_apply_worker = winfo;
}

/*
 * Form a unique savepoint name for the streaming transaction.
 *
 * Note that different subscriptions for publications on different nodes can
 * receive same remote xid, so we need to use subscription id along with it.
 *
 * Returns the name in the supplied buffer.
 */
static void
pa_savepoint_name(Oid suboid, TransactionId xid, char *spname, Size szsp)
{
	snprintf(spname, szsp, "pg_sp_%u_%u", suboid, xid);
}

/*
 * Define a savepoint for a subxact in parallel apply worker if needed.
 *
 * The parallel apply worker can figure out if a new subtransaction was
 * started by checking if the new change arrived with a different xid. In that
 * case define a named savepoint, so that we are able to rollback to it
 * if required.
 */
void
pa_start_subtrans(TransactionId current_xid, TransactionId top_xid)
{
	if (!TransactionIdIsValid(top_xid))
		return;

	if (current_xid != top_xid &&
		!list_member_xid(subxactlist, current_xid))
	{
		MemoryContext oldctx;
		char		spname[NAMEDATALEN];

		pa_savepoint_name(MySubscription->oid, current_xid,
						  spname, sizeof(spname));

		elog(DEBUG1, "defining savepoint %s in logical replication parallel apply worker", spname);

		/* We must be in transaction block to define the SAVEPOINT. */
		if (!IsTransactionBlock())
		{
			if (!IsTransactionState())
				StartTransactionCommand();

			BeginTransactionBlock();
			CommitTransactionCommand();
		}

		DefineSavepoint(spname);

		/*
		 * CommitTransactionCommand is needed to start a subtransaction after
		 * issuing a SAVEPOINT inside a transaction block (see
		 * StartSubTransaction()).
		 */
		CommitTransactionCommand();

		oldctx = MemoryContextSwitchTo(TopTransactionContext);
		subxactlist = lappend_xid(subxactlist, current_xid);
		MemoryContextSwitchTo(oldctx);
	}
}

/* Reset the list that maintains subtransactions. */
void
pa_reset_subtrans(void)
{
	/*
	 * We don't need to free this explicitly as the allocated memory will be
	 * freed at the transaction end.
	 */
	subxactlist = NIL;
}

/*
 * Handle STREAM ABORT message when the transaction was applied in a parallel
 * apply worker.
 */
void
pa_stream_abort(LogicalRepStreamAbortData *abort_data)
{
	TransactionId xid = abort_data->xid;
	TransactionId subxid = abort_data->subxid;

	/*
	 * Update origin state so we can restart streaming from correct position
	 * in case of crash.
	 */
	replorigin_session_origin_lsn = abort_data->abort_lsn;
	replorigin_session_origin_timestamp = abort_data->abort_time;

	/*
	 * If the two XIDs are the same, it's in fact abort of toplevel xact, so
	 * just free the subxactlist.
	 */
	if (subxid == xid)
	{
		pa_set_xact_state(MyParallelShared, PARALLEL_TRANS_FINISHED);

		/*
		 * Release the lock as we might be processing an empty streaming
		 * transaction in which case the lock won't be released during
		 * transaction rollback.
		 *
		 * Note that it's ok to release the transaction lock before aborting
		 * the transaction because even if the parallel apply worker dies due
		 * to crash or some other reason, such a transaction would still be
		 * considered aborted.
		 */
		pa_unlock_transaction(xid, AccessExclusiveLock);

		AbortCurrentTransaction();

		if (IsTransactionBlock())
		{
			EndTransactionBlock(false);
			CommitTransactionCommand();
		}

		pa_reset_subtrans();

		pgstat_report_activity(STATE_IDLE, NULL);
	}
	else
	{
		/* OK, so it's a subxact. Rollback to the savepoint. */
		int			i;
		char		spname[NAMEDATALEN];

		pa_savepoint_name(MySubscription->oid, subxid, spname, sizeof(spname));

		elog(DEBUG1, "rolling back to savepoint %s in logical replication parallel apply worker", spname);

		/*
		 * Search the subxactlist, determine the offset tracked for the
		 * subxact, and truncate the list.
		 *
		 * Note that for an empty sub-transaction we won't find the subxid
		 * here.
		 */
		for (i = list_length(subxactlist) - 1; i >= 0; i--)
		{
			TransactionId xid_tmp = lfirst_xid(list_nth_cell(subxactlist, i));

			if (xid_tmp == subxid)
			{
				RollbackToSavepoint(spname);
				CommitTransactionCommand();
				subxactlist = list_truncate(subxactlist, i);
				break;
			}
		}
	}
}

/*
 * Set the fileset state for a particular parallel apply worker. The fileset
 * will be set once the leader worker serialized all changes to the file
 * so that it can be used by parallel apply worker.
 */
void
pa_set_fileset_state(ParallelApplyWorkerShared *wshared,
					 PartialFileSetState fileset_state)
{
	SpinLockAcquire(&wshared->mutex);
	wshared->fileset_state = fileset_state;

	if (fileset_state == FS_SERIALIZE_DONE)
	{
		Assert(am_leader_apply_worker());
		Assert(MyLogicalRepWorker->stream_fileset);
		wshared->fileset = *MyLogicalRepWorker->stream_fileset;
	}

	SpinLockRelease(&wshared->mutex);
}

/*
 * Get the fileset state for the current parallel apply worker.
 */
static PartialFileSetState
pa_get_fileset_state(void)
{
	PartialFileSetState fileset_state;

	Assert(am_parallel_apply_worker());

	SpinLockAcquire(&MyParallelShared->mutex);
	fileset_state = MyParallelShared->fileset_state;
	SpinLockRelease(&MyParallelShared->mutex);

	return fileset_state;
}

/*
 * Helper functions to acquire and release a lock for each stream block.
 *
 * Set locktag_field4 to PARALLEL_APPLY_LOCK_STREAM to indicate that it's a
 * stream lock.
 *
 * Refer to the comments atop this file to see how the stream lock is used.
 */
void
pa_lock_stream(TransactionId xid, LOCKMODE lockmode)
{
	LockApplyTransactionForSession(MyLogicalRepWorker->subid, xid,
								   PARALLEL_APPLY_LOCK_STREAM, lockmode);
}

void
pa_unlock_stream(TransactionId xid, LOCKMODE lockmode)
{
	UnlockApplyTransactionForSession(MyLogicalRepWorker->subid, xid,
									 PARALLEL_APPLY_LOCK_STREAM, lockmode);
}

/*
 * Helper functions to acquire and release a lock for each local transaction
 * apply.
 *
 * Set locktag_field4 to PARALLEL_APPLY_LOCK_XACT to indicate that it's a
 * transaction lock.
 *
 * Note that all the callers must pass a remote transaction ID instead of a
 * local transaction ID as xid. This is because the local transaction ID will
 * only be assigned while applying the first change in the parallel apply but
 * it's possible that the first change in the parallel apply worker is blocked
 * by a concurrently executing transaction in another parallel apply worker. We
 * can only communicate the local transaction id to the leader after applying
 * the first change so it won't be able to wait after sending the xact finish
 * command using this lock.
 *
 * Refer to the comments atop this file to see how the transaction lock is
 * used.
 */
void
pa_lock_transaction(TransactionId xid, LOCKMODE lockmode)
{
	LockApplyTransactionForSession(MyLogicalRepWorker->subid, xid,
								   PARALLEL_APPLY_LOCK_XACT, lockmode);
}

void
pa_unlock_transaction(TransactionId xid, LOCKMODE lockmode)
{
	UnlockApplyTransactionForSession(MyLogicalRepWorker->subid, xid,
									 PARALLEL_APPLY_LOCK_XACT, lockmode);
}

/*
 * Decrement the number of pending streaming blocks and wait on the stream lock
 * if there is no pending block available.
 */
void
pa_decr_and_wait_stream_block(void)
{
	Assert(am_parallel_apply_worker());

	/*
	 * It is only possible to not have any pending stream chunks when we are
	 * applying spooled messages.
	 */
	if (pg_atomic_read_u32(&MyParallelShared->pending_stream_count) == 0)
	{
		if (pa_has_spooled_message_pending())
			return;

		elog(ERROR, "invalid pending streaming chunk 0");
	}

	if (pg_atomic_sub_fetch_u32(&MyParallelShared->pending_stream_count, 1) == 0)
	{
		pa_lock_stream(MyParallelShared->xid, AccessShareLock);
		pa_unlock_stream(MyParallelShared->xid, AccessShareLock);
	}
}

/*
 * Finish processing the streaming transaction in the leader apply worker.
 */
void
pa_xact_finish(ParallelApplyWorkerInfo *winfo, XLogRecPtr remote_lsn)
{
	XLogRecPtr	local_lsn = InvalidXLogRecPtr;
	TransactionId pa_remote_xid = winfo->shared->xid;

	Assert(am_leader_apply_worker());

	/*
	 * Unlock the shared object lock taken for streaming transactions so that
	 * parallel apply worker can continue to receive and apply changes.
	 */
	if (winfo->stream_txn)
		pa_unlock_stream(winfo->shared->xid, AccessExclusiveLock);

	/*
	 * Wait for that worker for streaming transaction to finish. This is
	 * necessary to maintain commit order which avoids failures due to
	 * transaction dependencies and deadlocks.
	 *
	 * For non-streaming transaction but in partial seralize mode, wait for stop
	 * as well as the worker is anyway cannot be reused anymore (see
	 * pa_free_worker() for details).
	 */
	if (winfo->serialize_changes || winfo->stream_txn)
	{
		pa_wait_for_xact_finish(winfo);

		local_lsn = winfo->shared->last_commit_end;
		pa_remote_xid = InvalidTransactionId;

		pa_free_worker(winfo);
	}

	if (!XLogRecPtrIsInvalid(remote_lsn))
		store_flush_position(remote_lsn, local_lsn, pa_remote_xid);

	pa_set_stream_apply_worker(NULL);
}

bool
pa_transaction_committed(TransactionId xid)
{
	bool		found;
	ParallelApplyWorkerEntry *entry;

	Assert(TransactionIdIsValid(xid));

	/* Find an entry for the requested transaction */
	entry = hash_search(ParallelApplyTxnHash, &xid, HASH_FIND, &found);

	if (!found)
		return true;

	if (!entry->winfo)
		return true;

	return pa_get_xact_state(entry->winfo->shared) == PARALLEL_TRANS_FINISHED;
}

/*
 * Attach to the shared hash table for parallelized transactions.
 */
static void
pa_attach_parallelized_txn_hash(dsa_handle *pa_dsa_handle,
								dshash_table_handle *pa_dshash_handle)
{
	MemoryContext oldctx;

	if (parallelized_txns)
	{
		Assert(parallel_apply_dsa_area);
		*pa_dsa_handle = dsa_get_handle(parallel_apply_dsa_area);
		*pa_dshash_handle = dshash_get_hash_table_handle(parallelized_txns);
		return;
	}

	/* Be sure any local memory allocated by DSA routines is persistent. */
	oldctx = MemoryContextSwitchTo(ApplyContext);

	if (am_leader_apply_worker())
	{
		/* Initialize dynamic shared hash table for last-start times. */
		parallel_apply_dsa_area = dsa_create(LWTRANCHE_PARALLEL_APPLY_DSA);
		dsa_pin(parallel_apply_dsa_area);
		dsa_pin_mapping(parallel_apply_dsa_area);
		parallelized_txns = dshash_create(parallel_apply_dsa_area, &dsh_params, NULL);

		/* Store handles in shared memory for other backends to use. */
		*pa_dsa_handle = dsa_get_handle(parallel_apply_dsa_area);
		*pa_dshash_handle = dshash_get_hash_table_handle(parallelized_txns);
	}
	else if (am_parallel_apply_worker())
	{
		/* Attach to existing dynamic shared hash table. */
		parallel_apply_dsa_area = dsa_attach(MyParallelShared->parallel_apply_dsa_handle);
		dsa_pin_mapping(parallel_apply_dsa_area);
		parallelized_txns = dshash_attach(parallel_apply_dsa_area, &dsh_params,
										  MyParallelShared->parallelized_txns_handle,
										  NULL);
	}

	MemoryContextSwitchTo(oldctx);
}

/*
 * Record in-progress transactions from the given list that are being depended
 * on into the shared hash table.
 */
void
pa_record_dependency_on_transactions(List *depends_on_xids)
{
	foreach_xid(xid, depends_on_xids)
	{
		bool		found;
		ParallelApplyWorkerEntry *winfo_entry;
		ParallelApplyWorkerInfo *winfo;
		ParallelizedTxnEntry *txn_entry;

		winfo_entry = hash_search(ParallelApplyTxnHash, &xid, HASH_FIND, &found);
		winfo = winfo_entry->winfo;

		if (winfo->shared->has_dependent_txn)
			continue;

		txn_entry = dshash_find_or_insert(parallelized_txns, &xid, &found);

		if (found)
			elog(ERROR, "hash table corrupted");

		winfo->shared->has_dependent_txn = true;

		/*
		 * If the transaction has been committed now, remove the entry,
		 * otherwise the parallel apply worker will remove the entry once
		 * committed the transaction.
		 */
		if (pa_get_xact_state(winfo->shared) == PARALLEL_TRANS_FINISHED)
			dshash_delete_entry(parallelized_txns, txn_entry);
		else
			dshash_release_lock(parallelized_txns, txn_entry);
	}
}

/*
 * Mark the transaction state as finished and remove the shared hash entry if
 * there are dependent transactions waiting for this transaction to complete.
 */
void
pa_commit_transaction(void)
{
	TransactionId xid = MyParallelShared->xid;
	bool		has_dependent_txn;

	SpinLockAcquire(&MyParallelShared->mutex);
	MyParallelShared->xact_state = PARALLEL_TRANS_FINISHED;
	has_dependent_txn = MyParallelShared->has_dependent_txn;
	SpinLockRelease(&MyParallelShared->mutex);

	if (!has_dependent_txn)
		return;

	dshash_delete_key(parallelized_txns, &xid);
	elog(DEBUG1, "depended xid %u committed", xid);
}

/*
 * Wait for the given transaction to finish.
 */
void
pa_wait_for_depended_transaction(TransactionId xid)
{
	elog(DEBUG1, "wait for depended xid %u", xid);

	for (;;)
	{
		ParallelizedTxnEntry *txn_entry;

		txn_entry = dshash_find(parallelized_txns, &xid, false);

		/* The entry is removed only if the transaction is committed */
		if (txn_entry == NULL)
			break;

		dshash_release_lock(parallelized_txns, txn_entry);

		pa_lock_transaction(xid, AccessShareLock);
		pa_unlock_transaction(xid, AccessShareLock);

		CHECK_FOR_INTERRUPTS();
	}

	elog(DEBUG1, "finish waiting for depended xid %u", xid);
}

/*
 * Write internal relation description to the output stream.
 */
static void
write_internal_relation(StringInfo s, LogicalRepRelation *rel)
{
	pq_sendbyte(s, PARALLEL_APPLY_INTERNAL_MESSAGE);
	pq_sendbyte(s, LOGICAL_REP_MSG_INTERNAL_RELATION);

	if (rel)
	{
		pq_sendint(s, 1, 4);
		logicalrep_write_internal_rel(s, rel);
	}
	else
	{
		pq_sendint(s, logicalrep_get_num_rels(), 4);
		logicalrep_write_all_rels(s);
	}
}

#include "postmaster/bgworker_internals.h"

typedef struct
{
	ConditionVariable cv;
	slock_t mutex;
	size_t head;
	size_t tail;
	TransactionId ring[MAX_PARALLEL_WORKER_LIMIT];
} ParallelApplyShmem;

static ParallelApplyShmem* pa_shmem;

void
pa_commit(TransactionId xid)
{
	SpinLockAcquire(&pa_shmem->mutex);
	pa_shmem->ring[pa_shmem->head++ % MAX_PARALLEL_WORKER_LIMIT] = xid;
	SpinLockRelease(&pa_shmem->mutex);
	ConditionVariableBroadcast(&pa_shmem->cv);
}


void
pa_before_apply_commit(void)
{
	TransactionId xid = MyParallelShared->xid;

	if (!preserve_commit_order)
		return;

	while (true)
	{
		SpinLockAcquire(&pa_shmem->mutex);
		if (pa_shmem->head > pa_shmem->tail && pa_shmem->ring[pa_shmem->tail % MAX_PARALLEL_WORKER_LIMIT] == xid)
		{
			SpinLockRelease(&pa_shmem->mutex);
			break;
		}
		SpinLockRelease(&pa_shmem->mutex);
		ConditionVariableSleep(&pa_shmem->cv, WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN);
	}
	ConditionVariableCancelSleep();
}

void
pa_after_apply_commit(void)
{
	SpinLockAcquire(&pa_shmem->mutex);
	pa_shmem->tail += 1;
	SpinLockRelease(&pa_shmem->mutex);
	ConditionVariableBroadcast(&pa_shmem->cv);
}

Size
ParallelApplyShmemSize(void)
{
	return sizeof(ParallelApplyShmem);
}

void
ParallelApplyShmemInit(void)
{
	bool		found;

	pa_shmem = (ParallelApplyShmem*)ShmemInitStruct("Parallel worker shmem", sizeof(ParallelApplyShmem), &found);
	if (!found)
	{
		pa_shmem->head = pa_shmem->tail = 0;
		ConditionVariableInit(&pa_shmem->cv);
		SpinLockInit(&pa_shmem->mutex);
	}
}
