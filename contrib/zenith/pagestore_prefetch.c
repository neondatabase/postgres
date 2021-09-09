/*-------------------------------------------------------------------------
 *
 * pagestore_prefetch.c
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/zenith/pagestore_prefetch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pagestore_client.h"
#include "postmaster/bgworker.h"
#include "storage/relfilenode.h"
#include "storage/buf_internals.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/dynahash.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "pagestore_client.h"

typedef enum {
	QUEUED,
	IN_PROGRESS,
	COMPLETED,
	CANCELED,
} PrefetchState;

typedef struct PrefetchEntry {
	BufferTag     tag;
	XLogRecPtr    lsn;
	uint32        index;  /* index of entry i prefetch cyclic buffer */
	PrefetchState state;
	Latch*        wait_latch; /* latch to be signaled when block is fetched */
} PrefetchEntry;

typedef struct PrefetchControl {
	size_t curr; /* position in cyclic buffer */
	Latch* go_latch; /* latch to wakeup prefetcher */
	PrefetchEntry* entries[1]; /* cyclic buffer with size == prefetch_buffer_size */
} PrefetchControl;

static int prefetch_buffer_size;
static PrefetchControl* prefetch_control;
static char* prefetch_buffer;
static HTAB* prefetch_hash;
static LWLockId prefetch_lock;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static bool prefetch_cancel;

size_t n_prefetch_requests;
size_t n_prefetch_hits;
size_t max_merged_prefetch_requests;

static void
zenith_prefetch_shmem_startup(void)
{
	bool found;

	if (prev_shmem_startup_hook) {
		prev_shmem_startup_hook();
    }

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	prefetch_buffer = ShmemInitStruct("zenithr_prefetch",
									  sizeof(PrefetchControl) +
									  (BLCKSZ+sizeof(PrefetchEntry*))*prefetch_buffer_size,
									  &found);
	prefetch_control = (PrefetchControl*)(prefetch_buffer + BLCKSZ*(size_t)prefetch_buffer_size);
	if (!found)
	{
		static HASHCTL info;
		prefetch_lock = (LWLockId)GetNamedLWLockTranche("zenith_prefetch");
		info.keysize = sizeof(BufferTag);
		info.entrysize = sizeof(PrefetchEntry);
		prefetch_hash = ShmemInitHash("zenith_prefetch",
									  prefetch_buffer_size, prefetch_buffer_size,
									  &info,
									  HASH_ELEM | HASH_BLOBS);
		prefetch_control->curr = 0;
		prefetch_control->go_latch = NULL;
		memset(prefetch_control->entries, 0, prefetch_buffer_size*sizeof(PrefetchEntry*));
	}
	LWLockRelease(AddinShmemInitLock);
}

void
zenith_prefetch_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.
	 */
	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "Zenith module should be loaed via shared_preload_libraries");

	DefineCustomIntVariable("zenith.prefetch_buffer_size",
                            "Size of zenith prefetch buffer",
							NULL,
							&prefetch_buffer_size,
							1024, /* 8Mb */
							0,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	if (prefetch_buffer_size == 0)
		return;

	RequestAddinShmemSpace((size_t)prefetch_buffer_size*(BLCKSZ+sizeof(PrefetchEntry*))
						   + sizeof(PrefetchControl)
						   + hash_estimate_size(prefetch_buffer_size, sizeof(PrefetchEntry)));
	RequestNamedLWLockTranche("zenith_prefetch", 1);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = zenith_prefetch_shmem_startup;
}


bool zenith_find_prefetched_buffer(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, XLogRecPtr lsn, char* buffer)
{
	PrefetchEntry* entry;
	BufferTag tag;

	if (prefetch_buffer_size == 0)
		return false;

	tag.rnode = reln->smgr_rnode.node;
	tag.forkNum = forknum;
	tag.blockNum = blocknum;

	while (true)
	{
		LWLockAcquire(prefetch_lock, LW_EXCLUSIVE);
		entry = hash_search(prefetch_hash, &tag, HASH_FIND, NULL);
		if (entry != NULL) {
			if (entry->state == QUEUED || entry->lsn < lsn)
			{
				/* We have not sent this prefetch request and page is already requested.
				 * So just concel this prefetch request.
				 */
				prefetch_control->entries[entry->index] = NULL;
				hash_search(prefetch_hash, &tag, HASH_REMOVE, NULL);
				if (entry->wait_latch)
					SetLatch(entry->wait_latch);
				LWLockRelease(prefetch_lock);
				return false;
			}
			if (entry->state == COMPLETED) {
				prefetch_log("%lu: prefetch hit for block %d of relation %d",
							 GetCurrentTimestamp(), blocknum, tag.rnode.relNode);
				memcpy(buffer, prefetch_buffer + entry->index*BLCKSZ, BLCKSZ);
				LWLockRelease(prefetch_lock);
				n_prefetch_hits += 1;
				return true;
			}
			else
			{
				/* Prefetch is still in progress */

				prefetch_log("%lu: wait completion of prefetch for block %d of relation %d",
							 GetCurrentTimestamp(), blocknum, tag.rnode.relNode);

				/* Two concurrent reads of the same buffers are not possible */
				Assert(entry->wait_latch == NULL || entry->wait_latch == MyLatch);
				entry->wait_latch = MyLatch;

				LWLockRelease(prefetch_lock);

				/* wait latch to be signaled */
				(void)WaitLatch(MyLatch, WL_EXIT_ON_PM_DEATH|WL_LATCH_SET, 0, PG_WAIT_EXTENSION);
				ResetLatch(MyLatch);
			}
			continue;
		}
		LWLockRelease(prefetch_lock);
		return false;
	}
}

static void
start_prefetch_worker()
{
	/* Lazy start of prefetch bacnhround worker */
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	pid_t bgw_pid;

	MemSet(&worker, 0, sizeof(worker));
	strncpy(worker.bgw_name, "zenith_prefetch", sizeof(worker.bgw_name));
	strncpy(worker.bgw_type, "zenith_prefetch", sizeof(worker.bgw_type));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	strcpy(worker.bgw_function_name, "zenith_prefetch_main");
	strcpy(worker.bgw_library_name, "zenith");
	worker.bgw_notify_pid = MyProcPid;
	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		elog(ERROR, "zenith: failed to start prefetch background worker");
	}
	if (WaitForBackgroundWorkerStartup(handle, &bgw_pid) != BGWH_STARTED)
	{
		elog(ERROR, "zenith: startup of prefetch background worker is failed");
	}
	for (int n_attempts = 0; prefetch_control->go_latch == NULL && n_attempts < 100; n_attempts++)
	{
		pg_usleep(10000); /* wait background worker to be registered in procarray */
	}
	if (prefetch_control->go_latch == NULL)
	{
		elog(ERROR, "zenith: background prefetch worker %d is crashed", bgw_pid);
	}
}

void
zenith_prefetch_buffer(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, XLogRecPtr lsn)
{
	PrefetchEntry* entry;
	bool found;
	BufferTag tag;

	if (prefetch_buffer_size == 0
		|| reln->smgr_rnode.node.dbNode == 0
		|| reln->smgr_rnode.node.relNode < FirstNormalObjectId)
	{
		/* disable prefetch for system tables */
		return;
	}

	tag.rnode = reln->smgr_rnode.node;
	tag.forkNum = forknum;
	tag.blockNum = blocknum;

	LWLockAcquire(prefetch_lock, LW_EXCLUSIVE);
	entry = hash_search(prefetch_hash, &tag, HASH_ENTER, &found);

	if (!found)
	{
		size_t curr = prefetch_control->curr;
		PrefetchEntry* victim = prefetch_control->entries[curr];
		if (victim != NULL)
		{
			/* remove old entry */
			if (victim->wait_latch)
				SetLatch(victim->wait_latch);
			hash_search(prefetch_hash, &victim->tag, HASH_REMOVE, NULL);
		}
		entry->state = QUEUED;
		entry->wait_latch = NULL;
		entry->lsn = lsn;
		entry->index = curr;
		prefetch_control->entries[curr] = entry;
		prefetch_control->curr = (curr + 1) % prefetch_buffer_size;
		n_prefetch_requests += 1;
		prefetch_log("%lu: prefetch request for block %d of relation %d",
					 GetCurrentTimestamp(), blocknum, tag.rnode.relNode);

		if (prefetch_control->go_latch == NULL)
		{
			/* lazy start of prefetch background worker */
			start_prefetch_worker();
		}
		LWLockRelease(prefetch_lock);
		SetLatch(prefetch_control->go_latch);
	} else
		LWLockRelease(prefetch_lock);
}

/* Cancel merger bgwroker */
static void
zenith_prefetch_cancel(int sig)
{
	prefetch_cancel = true;
	SetLatch(MyLatch);
}

/* Main function of prefetch bgwroker */
void
zenith_prefetch_main(Datum arg)
{
	size_t curr = 0;
	PrefetchEntry* prefetch_entries = (PrefetchEntry*)palloc(prefetch_buffer_size*sizeof(PrefetchEntry));

	pqsignal(SIGINT,  zenith_prefetch_cancel);
	pqsignal(SIGQUIT, zenith_prefetch_cancel);
	pqsignal(SIGTERM, zenith_prefetch_cancel);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	prefetch_control->go_latch = MyLatch;

	while (!prefetch_cancel)
	{
		size_t from, till;
		size_t n_prefetched = 0;
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		LWLockAcquire(prefetch_lock, LW_EXCLUSIVE);
		till = prefetch_control->curr;
		for (from = curr; !prefetch_cancel && from != till;  from = (from + 1) % prefetch_buffer_size)
		{
			PrefetchEntry* entry = prefetch_control->entries[from];

			if (entry == NULL) /* prefetch request was withdrawn */
			{
				prefetch_entries[from].state = CANCELED;
				continue;
			}
			Assert(entry->state == QUEUED);
			entry->state = IN_PROGRESS;
			prefetch_entries[from] = *entry;

			prefetch_log("%lu: send prefetch request for block %d of relation %d",
						 GetCurrentTimestamp(), entry->tag.blockNum, entry->tag.rnode.relNode);

			page_server->send((ZenithRequest) {
				.tag = T_ZenithReadRequest,
				.page_key = {
					 .rnode = entry->tag.rnode,
					 .forknum = entry->tag.forkNum,
					 .blkno = entry->tag.blockNum
				},
				.lsn = entry->lsn
		    });
			n_prefetched += 1;
		}

		if (n_prefetched)
		{
			page_server->flush();
			if (n_prefetched > max_merged_prefetch_requests)
				max_merged_prefetch_requests = n_prefetched;
		}

		for (from = curr; !prefetch_cancel && from != till;  from = (from + 1) % prefetch_buffer_size)
		{
			ZenithResponse *resp;
			PrefetchEntry* entry;

			if (prefetch_entries[from].state == CANCELED)
				continue;

			/* Release lock to load buffer */
			LWLockRelease(prefetch_lock);
			resp = page_server->receive();
			LWLockAcquire(prefetch_lock, LW_EXCLUSIVE);

			entry = prefetch_control->entries[from];
			if (entry != NULL
				&& entry->lsn == prefetch_entries[from].lsn
				&& memcmp(&entry->tag, &prefetch_entries[from].tag, sizeof(entry->tag)) == 0)
			{
				/* entry was not replaced in cyclic buffer */
				Assert(entry->state == IN_PROGRESS);

				if (!resp->ok)
				{
					ereport(LOG,
							(errcode(ERRCODE_IO_ERROR),
							 errmsg("could not prefetch block %u in rel %u/%u/%u.%u from page server at lsn %X/%08X",
									entry->tag.blockNum,
									entry->tag.rnode.spcNode,
									entry->tag.rnode.dbNode,
									entry->tag.rnode.relNode,
									entry->tag.forkNum,
									LSN_FORMAT_ARGS(entry->lsn))));
				}
				else if (!PageIsNew(resp->page)) /* page server returns zero page if requested cblock is not found */
				{
					prefetch_log("%lu: receive prefetched data for block %d of relation %d",
								 GetCurrentTimestamp(), entry->tag.blockNum, entry->tag.rnode.relNode);
					entry->state = COMPLETED;
					((PageHeader)resp->page)->pd_flags &= ~PD_WAL_LOGGED; /* Clear PD_WAL_LOGGED bit stored in WAL record */
					memcpy(prefetch_buffer + entry->index*BLCKSZ, resp->page, BLCKSZ);
				}
				if (entry->wait_latch)
				{
					SetLatch(entry->wait_latch);
					entry->wait_latch = NULL;
				}
			}
			pfree(resp);
		}
		curr = till;
		LWLockRelease(prefetch_lock);
	}
}
