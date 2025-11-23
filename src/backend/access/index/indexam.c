/*-------------------------------------------------------------------------
 *
 * indexam.c
 *	  general index access method routines
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/index/indexam.c
 *
 * INTERFACE ROUTINES
 *		index_open		- open an index relation by relation OID
 *		index_close		- close an index relation
 *		index_beginscan - start a scan of an index with amgettuple
 *		index_beginscan_bitmap - start a scan of an index with amgetbitmap
 *		index_rescan	- restart a scan of an index
 *		index_endscan	- end a scan
 *		index_insert	- insert an index tuple into a relation
 *		index_markpos	- mark a scan position
 *		index_restrpos	- restore a scan position
 *		index_parallelscan_estimate - estimate shared memory for parallel scan
 *		index_parallelscan_initialize - initialize parallel scan
 *		index_parallelrescan  - (re)start a parallel scan of an index
 *		index_beginscan_parallel - join parallel index scan
 *		index_getnext_tid	- get the next TID from a scan
 *		index_fetch_heap		- get the scan's next heap tuple
 *		index_getnext_slot	- get the next tuple from a scan
 *		index_getbitmap - get all tuples from a scan
 *		index_bulk_delete	- bulk deletion of index tuples
 *		index_vacuum_cleanup	- post-deletion cleanup of an index
 *		index_can_return	- does index support index-only scans?
 *		index_getprocid - get a support procedure OID
 *		index_getprocinfo - get a support procedure's lookup info
 *
 * NOTES
 *		This file contains the index_ routines which used
 *		to be a scattered collection of stuff in access/genam.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/nbtree.h"		/* XXX for MaxTIDsPerBTreePage (should remove) */
#include "access/relation.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "nodes/execnodes.h"
#include "optimizer/cost.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/memdebug.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"


/* ----------------------------------------------------------------
 *					macros used in index_ routines
 *
 * Note: the ReindexIsProcessingIndex() check in RELATION_CHECKS is there
 * to check that we don't try to scan or do retail insertions into an index
 * that is currently being rebuilt or pending rebuild.  This helps to catch
 * things that don't work when reindexing system catalogs, as well as prevent
 * user errors like index expressions that access their own tables.  The check
 * doesn't prevent the actual rebuild because we don't use RELATION_CHECKS
 * when calling the index AM's ambuild routine, and there is no reason for
 * ambuild to call its subsidiary routines through this file.
 * ----------------------------------------------------------------
 */
#define RELATION_CHECKS \
do { \
	Assert(RelationIsValid(indexRelation)); \
	Assert(PointerIsValid(indexRelation->rd_indam)); \
	if (unlikely(ReindexIsProcessingIndex(RelationGetRelid(indexRelation)))) \
		ereport(ERROR, \
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
				 errmsg("cannot access index \"%s\" while it is being reindexed", \
						RelationGetRelationName(indexRelation)))); \
} while(0)

#define SCAN_CHECKS \
( \
	AssertMacro(IndexScanIsValid(scan)), \
	AssertMacro(RelationIsValid(scan->indexRelation)), \
	AssertMacro(PointerIsValid(scan->indexRelation->rd_indam)) \
)

#define CHECK_REL_PROCEDURE(pname) \
do { \
	if (indexRelation->rd_indam->pname == NULL) \
		elog(ERROR, "function \"%s\" is not defined for index \"%s\"", \
			 CppAsString(pname), RelationGetRelationName(indexRelation)); \
} while(0)

#define CHECK_SCAN_PROCEDURE(pname) \
do { \
	if (scan->indexRelation->rd_indam->pname == NULL) \
		elog(ERROR, "function \"%s\" is not defined for index \"%s\"", \
			 CppAsString(pname), RelationGetRelationName(scan->indexRelation)); \
} while(0)

static IndexScanDesc index_beginscan_internal(Relation indexRelation,
											  int nkeys, int norderbys, Snapshot snapshot,
											  ParallelIndexScanDesc pscan, bool temp_snap);
static ItemPointer index_batch_getnext_tid(IndexScanDesc scan, ScanDirection direction);
static ItemPointer index_retail_getnext_tid(IndexScanDesc scan, ScanDirection direction);
static inline void validate_relation_kind(Relation r);

/* index batching */
static void index_batch_init(IndexScanDesc scan);
static void index_batch_reset(IndexScanDesc scan, bool complete);
static void index_batch_end(IndexScanDesc scan);
static bool index_batch_getnext(IndexScanDesc scan, ScanDirection direction);
static void index_batch_free(IndexScanDesc scan, IndexScanBatch batch);

static BlockNumber index_scan_stream_read_next(ReadStream *stream,
											   void *callback_private_data,
											   void *per_buffer_data);

static pg_attribute_always_inline bool index_batch_pos_advance(IndexScanDesc scan,
															   IndexScanBatchPos *pos,
															   ScanDirection direction);
static void index_batch_pos_reset(IndexScanDesc scan, IndexScanBatchPos *pos);
static void index_batch_kill_item(IndexScanDesc scan);

static void AssertCheckBatchPosValid(IndexScanDesc scan, IndexScanBatchPos *pos);
static void AssertCheckBatch(IndexScanDesc scan, IndexScanBatch batch);
static void AssertCheckBatches(IndexScanDesc scan);


/*
 * Maximum number of batches (leaf pages) we can keep in memory.
 *
 * The value 64 value is arbitrary, it's about 1MB of data with 8KB pages
 * (512kB for pages, and then a bit of overhead). We should not really need
 * this many batches in most cases, though. The read stream looks ahead just
 * enough to queue enough IOs, adjusting the distance (TIDs, but ultimately
 * the number of future batches) to meet that.
 *
 * In most cases an index leaf page has many (hundreds) index tuples, and
 * it's enough to read one or maybe two leaf pages ahead to satisfy the
 * distance.
 *
 * But there are cases where this may not quite work, for example:
 *
 * a) bloated index - many pages only have a single index item, so that
 *    achieving the distance requires too many leaf pages
 *
 * b) correlated index - duplicate blocks are skipped (the callback does not
 *    even return those, thanks to currentPrefetchBlock optimization), and are
 *    mostly ignored in the distance heuristics (read stream does not even see
 *    those TIDs, and there's no I/O either)
 *
 * c) index-only scan - the callback skips TIDs from all-visible blocks (not
 *    reading those is the whole point of index-only scans), and so it's
 *    invisible to the distance / IO heuristics (similarly to duplicates)
 *
 * In these cases we might need to read a significant number of batches to
 * find the first block to return to the read stream. It's not clear if
 * looking this far ahead is worth it - it's a lot of work / synchronous
 * I/O, and the query may terminate before reaching those TIDs (e.g. due to
 * a LIMIT clause).
 *
 * Currently, there's no way to "pause" a read stream - stop looking ahead
 * for a while, but then resume the work when a batch gets freed. To simulate
 * this, the read stream is terminated (as if there were no more data), and
 * then reset after draining all the queued blocks in order to resume work.
 * This works, but it "stalls" the I/O queue. If it happens very often, it
 * can be a serious performance bottleneck.

 * XXX Maybe 64 is too high? It also defines the maximum amount of overhead
 * allowed. In the worst case, reading a single row might trigger reading this
 * many leaf pages (e.g. with IOS, if most pages are all-visible). Which might
 * be an issue with LIMIT queries, when we actually won't get that far.
 */
#define INDEX_SCAN_MAX_BATCHES	64

/*
 * Thresholds controlling when we cancel use of a read stream to do
 * prefetching
 */
#define INDEX_SCAN_MIN_DISTANCE_NBATCHES	20
#define INDEX_SCAN_MIN_TUPLE_DISTANCE		7

#define INDEX_SCAN_BATCH_COUNT(scan) \
	((scan)->batchState->nextBatch - (scan)->batchState->headBatch)

/* Did we already load batch with the requested index? */
/* XXX shouldn't this also compare headBatch? maybe the batch was freed? */
#define INDEX_SCAN_BATCH_LOADED(scan, idx) \
	((idx) < (scan)->batchState->nextBatch)

/* Have we loaded the maximum number of batches? */
#define INDEX_SCAN_BATCH_FULL(scan) \
	(INDEX_SCAN_BATCH_COUNT(scan) == scan->batchState->maxBatches)

/* Return batch for the provided index. */
/* XXX Should this have an assert to enforce the batch is loaded? Maybe the
 * index is too far back, but there happens to be a batch in the right slot?
 * Could easily happen if we have to keep many batches around.
 */
#define INDEX_SCAN_BATCH(scan, idx)	\
		((scan)->batchState->batches[(idx) % INDEX_SCAN_MAX_BATCHES])

/* Is the position invalid/undefined? */
#define INDEX_SCAN_POS_INVALID(pos) \
		(((pos)->batch == -1) && ((pos)->index == -1))

#ifdef INDEXAM_DEBUG
#define DEBUG_LOG(...) elog(AmRegularBackendProcess() ? NOTICE : DEBUG2, __VA_ARGS__)
#else
#define DEBUG_LOG(...)
#endif

/* debug: print info about current batches */
static void
index_batch_print(const char *label, IndexScanDesc scan)
{
#ifdef INDEXAM_DEBUG
	IndexScanBatchState *batches = scan->batchState;

	if (!scan->batchState)
		return;

	if (!AmRegularBackendProcess())
		return;
	if (IsCatalogRelation(scan->indexRelation))
		return;

	DEBUG_LOG("%s: batches headBatch %d nextBatch %d maxBatches %d",
			  label,
			  batches->headBatch, batches->nextBatch, batches->maxBatches);

	for (int i = batches->headBatch; i < batches->nextBatch; i++)
	{
		IndexScanBatchData *batch = INDEX_SCAN_BATCH(scan, i);
		BTScanPos	pos = (BTScanPos) batch->pos;

		DEBUG_LOG("    batch %d currPage %u %p firstItem %d lastItem %d killed %d",
				  i, pos->currPage, batch, batch->firstItem, batch->lastItem,
				  batch->numKilled);
	}
#endif
}

/* ----------------------------------------------------------------
 *				   index_ interface functions
 * ----------------------------------------------------------------
 */

/* ----------------
 *		index_open - open an index relation by relation OID
 *
 *		If lockmode is not "NoLock", the specified kind of lock is
 *		obtained on the index.  (Generally, NoLock should only be
 *		used if the caller knows it has some appropriate lock on the
 *		index already.)
 *
 *		An error is raised if the index does not exist.
 *
 *		This is a convenience routine adapted for indexscan use.
 *		Some callers may prefer to use relation_open directly.
 * ----------------
 */
Relation
index_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	r = relation_open(relationId, lockmode);

	validate_relation_kind(r);

	return r;
}

/* ----------------
 *		try_index_open - open an index relation by relation OID
 *
 *		Same as index_open, except return NULL instead of failing
 *		if the relation does not exist.
 * ----------------
 */
Relation
try_index_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	r = try_relation_open(relationId, lockmode);

	/* leave if index does not exist */
	if (!r)
		return NULL;

	validate_relation_kind(r);

	return r;
}

/* ----------------
 *		index_close - close an index relation
 *
 *		If lockmode is not "NoLock", we then release the specified lock.
 *
 *		Note that it is often sensible to hold a lock beyond index_close;
 *		in that case, the lock is released automatically at xact end.
 * ----------------
 */
void
index_close(Relation relation, LOCKMODE lockmode)
{
	LockRelId	relid = relation->rd_lockInfo.lockRelId;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* The relcache does the real work... */
	RelationClose(relation);

	if (lockmode != NoLock)
		UnlockRelationId(&relid, lockmode);
}

/* ----------------
 *		validate_relation_kind - check the relation's kind
 *
 *		Make sure relkind is an index or a partitioned index.
 * ----------------
 */
static inline void
validate_relation_kind(Relation r)
{
	if (r->rd_rel->relkind != RELKIND_INDEX &&
		r->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index",
						RelationGetRelationName(r))));
}


/* ----------------
 *		index_insert - insert an index tuple into a relation
 * ----------------
 */
bool
index_insert(Relation indexRelation,
			 Datum *values,
			 bool *isnull,
			 ItemPointer heap_t_ctid,
			 Relation heapRelation,
			 IndexUniqueCheck checkUnique,
			 bool indexUnchanged,
			 IndexInfo *indexInfo)
{
	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(aminsert);

	if (!(indexRelation->rd_indam->ampredlocks))
		CheckForSerializableConflictIn(indexRelation,
									   (ItemPointer) NULL,
									   InvalidBlockNumber);

	return indexRelation->rd_indam->aminsert(indexRelation, values, isnull,
											 heap_t_ctid, heapRelation,
											 checkUnique, indexUnchanged,
											 indexInfo);
}

/* -------------------------
 *		index_insert_cleanup - clean up after all index inserts are done
 * -------------------------
 */
void
index_insert_cleanup(Relation indexRelation,
					 IndexInfo *indexInfo)
{
	RELATION_CHECKS;

	if (indexRelation->rd_indam->aminsertcleanup)
		indexRelation->rd_indam->aminsertcleanup(indexRelation, indexInfo);
}

/*
 * index_beginscan - start a scan of an index with amgettuple
 *
 * Caller must be holding suitable locks on the heap and the index.
 */
IndexScanDesc
index_beginscan(Relation heapRelation,
				Relation indexRelation,
				Snapshot snapshot,
				IndexScanInstrumentation *instrument,
				int nkeys, int norderbys)
{
	IndexScanDesc scan;

	Assert(snapshot != InvalidSnapshot);

	scan = index_beginscan_internal(indexRelation, nkeys, norderbys, snapshot, NULL, false);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by RelationGetIndexScan.
	 */
	scan->heapRelation = heapRelation;
	scan->xs_snapshot = snapshot;
	scan->instrument = instrument;

	if (indexRelation->rd_indam->amgetbatch != NULL)
		index_batch_init(scan);

	/* prepare to fetch index matches from table */
	scan->xs_heapfetch = table_index_fetch_begin(heapRelation);

	return scan;
}

/*
 * index_beginscan_bitmap - start a scan of an index with amgetbitmap
 *
 * As above, caller had better be holding some lock on the parent heap
 * relation, even though it's not explicitly mentioned here.
 */
IndexScanDesc
index_beginscan_bitmap(Relation indexRelation,
					   Snapshot snapshot,
					   IndexScanInstrumentation *instrument,
					   int nkeys)
{
	IndexScanDesc scan;

	Assert(snapshot != InvalidSnapshot);

	scan = index_beginscan_internal(indexRelation, nkeys, 0, snapshot, NULL, false);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by RelationGetIndexScan.
	 */
	scan->xs_snapshot = snapshot;
	scan->instrument = instrument;

	return scan;
}

/*
 * index_beginscan_internal --- common code for index_beginscan variants
 */
static IndexScanDesc
index_beginscan_internal(Relation indexRelation,
						 int nkeys, int norderbys, Snapshot snapshot,
						 ParallelIndexScanDesc pscan, bool temp_snap)
{
	IndexScanDesc scan;

	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(ambeginscan);

	if (!(indexRelation->rd_indam->ampredlocks))
		PredicateLockRelation(indexRelation, snapshot);

	/*
	 * We hold a reference count to the relcache entry throughout the scan.
	 */
	RelationIncrementReferenceCount(indexRelation);

	/*
	 * Tell the AM to open a scan.
	 */
	scan = indexRelation->rd_indam->ambeginscan(indexRelation, nkeys,
												norderbys);
	/* Initialize information for parallel scan. */
	scan->parallel_scan = pscan;
	scan->xs_temp_snap = temp_snap;

	return scan;
}

/* ----------------
 *		index_rescan  - (re)start a scan of an index
 *
 * During a restart, the caller may specify a new set of scankeys and/or
 * orderbykeys; but the number of keys cannot differ from what index_beginscan
 * was told.  (Later we might relax that to "must not exceed", but currently
 * the index AMs tend to assume that scan->numberOfKeys is what to believe.)
 * To restart the scan without changing keys, pass NULL for the key arrays.
 * (Of course, keys *must* be passed on the first call, unless
 * scan->numberOfKeys is zero.)
 * ----------------
 */
void
index_rescan(IndexScanDesc scan,
			 ScanKey keys, int nkeys,
			 ScanKey orderbys, int norderbys)
{
	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amrescan);

	Assert(nkeys == scan->numberOfKeys);
	Assert(norderbys == scan->numberOfOrderBys);

	/* Release resources (like buffer pins) from table accesses */
	if (scan->xs_heapfetch)
		table_index_fetch_reset(scan->xs_heapfetch);

	scan->kill_prior_tuple = false; /* for safety */
	scan->xs_heap_continue = false;

	/*
	 * Reset the batching. This makes it look like there are no batches,
	 * discards reads already scheduled within the read stream, etc.
	 */
	index_batch_reset(scan, true);

	scan->indexRelation->rd_indam->amrescan(scan, keys, nkeys,
											orderbys, norderbys);
}

/* ----------------
 *		index_endscan - end a scan
 * ----------------
 */
void
index_endscan(IndexScanDesc scan)
{
	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amendscan);

	/* Cleanup batching, so that the AM can release pins and so on. */
	index_batch_end(scan);

	/* Release resources (like buffer pins) from table accesses */
	if (scan->xs_heapfetch)
	{
		table_index_fetch_end(scan->xs_heapfetch);
		scan->xs_heapfetch = NULL;
	}

	/* End the AM's scan */
	scan->indexRelation->rd_indam->amendscan(scan);

	/* Release index refcount acquired by index_beginscan */
	RelationDecrementReferenceCount(scan->indexRelation);

	if (scan->xs_temp_snap)
		UnregisterSnapshot(scan->xs_snapshot);

	/* Release the scan data structure itself */
	IndexScanEnd(scan);
}

/* ----------------
 *		index_markpos  - mark a scan position
 * ----------------
 */
void
index_markpos(IndexScanDesc scan)
{
	IndexScanBatchState *batchState = scan->batchState;
	IndexScanBatchPos *markPos = &batchState->markPos;
	IndexScanBatchData *markBatch = batchState->markBatch;

	SCAN_CHECKS;

	/*
	 * FIXME this should probably check there actually is a batch state. For
	 * now it works the only AM with mark/restore support is btree, and that
	 * has batching. But we should not rely on that, right?
	 */

	/*
	 * Free the previous mark batch (if any), but only if the batch is no
	 * longer valid (in the current head/next range). This means that if we're
	 * marking the same batch (different item), we don't really do anything.
	 *
	 * XXX Should have some macro for this check, I guess.
	 */
	if (markBatch != NULL && (markPos->batch < batchState->headBatch ||
							  markPos->batch >= batchState->nextBatch))
	{
		batchState->markBatch = NULL;
		index_batch_free(scan, markBatch);
	}

	/* just copy the read position (which has to be valid) */
	batchState->markPos = batchState->readPos;
	batchState->markBatch = INDEX_SCAN_BATCH(scan, batchState->markPos.batch);

	/*
	 * FIXME we need to make sure the batch does not get freed during the
	 * regular advances.
	 */

	AssertCheckBatchPosValid(scan, &batchState->markPos);
}

/* ----------------
 *		index_restrpos	- restore a scan position
 *
 * NOTE: this only restores the internal scan state of the index AM.  See
 * comments for ExecRestrPos().
 *
 * NOTE: For heap, in the presence of HOT chains, mark/restore only works
 * correctly if the scan's snapshot is MVCC-safe; that ensures that there's at
 * most one returnable tuple in each HOT chain, and so restoring the prior
 * state at the granularity of the index AM is sufficient.  Since the only
 * current user of mark/restore functionality is nodeMergejoin.c, this
 * effectively means that merge-join plans only work for MVCC snapshots.  This
 * could be fixed if necessary, but for now it seems unimportant.
 * ----------------
 */
void
index_restrpos(IndexScanDesc scan)
{
	IndexScanBatchState *batchState;
	IndexScanBatchPos *markPos;
	IndexScanBatchData *markBatch;

	Assert(IsMVCCSnapshot(scan->xs_snapshot));

	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amgetbatch);
	CHECK_SCAN_PROCEDURE(amposreset);

	/*
	 * release resources (like buffer pins) from table accesses
	 *
	 * XXX: Currently, the distance is always remembered across any
	 * read_stream_reset calls (to work around the scan->batchState->reset
	 * behavior of resetting the stream to deal with running out of batches).
	 * We probably _should_ be forgetting the distance when we reset the
	 * stream here (through our table_index_fetch_reset call), though.
	 */
	if (scan->xs_heapfetch)
		table_index_fetch_reset(scan->xs_heapfetch);

	scan->kill_prior_tuple = false; /* for safety */
	scan->xs_heap_continue = false;

	/*
	 * FIXME this should probably check there actually is a batch state. For
	 * now it works the only AM with mark/restore support is btree, and that
	 * has batching. But we should not rely on that, right?
	 */

	batchState = scan->batchState;
	markPos = &batchState->markPos;
	markBatch = scan->batchState->markBatch;

	/*
	 * Call amposreset to let index AM know to invalidate any private state
	 * that independently tracks the scan's progress
	 */
	scan->indexRelation->rd_indam->amposreset(scan, markBatch);

	/*
	 * Reset the batching state, except for the marked batch, and make it look
	 * like we have a single batch -- the marked one.
	 */
	index_batch_reset(scan, false);

	batchState->markPos = *markPos;
	batchState->readPos = *markPos;
	batchState->headBatch = markPos->batch;
	batchState->nextBatch = (batchState->headBatch + 1);

	INDEX_SCAN_BATCH(scan, batchState->markPos.batch) = markBatch;
	batchState->markBatch = markBatch;	/* also remember this */
}

/*
 * index_parallelscan_estimate - estimate shared memory for parallel scan
 *
 * When instrument=true, estimate includes SharedIndexScanInstrumentation
 * space.  When parallel_aware=true, estimate includes whatever space the
 * index AM's amestimateparallelscan routine requested when called.
 */
Size
index_parallelscan_estimate(Relation indexRelation, int nkeys, int norderbys,
							Snapshot snapshot, bool instrument,
							bool parallel_aware, int nworkers)
{
	Size		nbytes;

	Assert(instrument || parallel_aware);

	RELATION_CHECKS;

	nbytes = offsetof(ParallelIndexScanDescData, ps_snapshot_data);
	nbytes = add_size(nbytes, EstimateSnapshotSpace(snapshot));
	nbytes = MAXALIGN(nbytes);

	if (instrument)
	{
		Size		sharedinfosz;

		sharedinfosz = offsetof(SharedIndexScanInstrumentation, winstrument) +
			nworkers * sizeof(IndexScanInstrumentation);
		nbytes = add_size(nbytes, sharedinfosz);
		nbytes = MAXALIGN(nbytes);
	}

	/*
	 * If parallel scan index AM interface can't be used (or index AM provides
	 * no such interface), assume there is no AM-specific data needed
	 */
	if (parallel_aware &&
		indexRelation->rd_indam->amestimateparallelscan != NULL)
		nbytes = add_size(nbytes,
						  indexRelation->rd_indam->amestimateparallelscan(indexRelation,
																		  nkeys,
																		  norderbys));

	return nbytes;
}

/*
 * index_parallelscan_initialize - initialize parallel scan
 *
 * We initialize both the ParallelIndexScanDesc proper and the AM-specific
 * information which follows it.
 *
 * This function calls access method specific initialization routine to
 * initialize am specific information.  Call this just once in the leader
 * process; then, individual workers attach via index_beginscan_parallel.
 */
void
index_parallelscan_initialize(Relation heapRelation, Relation indexRelation,
							  Snapshot snapshot, bool instrument,
							  bool parallel_aware, int nworkers,
							  SharedIndexScanInstrumentation **sharedinfo,
							  ParallelIndexScanDesc target)
{
	Size		offset;

	Assert(instrument || parallel_aware);

	RELATION_CHECKS;

	offset = add_size(offsetof(ParallelIndexScanDescData, ps_snapshot_data),
					  EstimateSnapshotSpace(snapshot));
	offset = MAXALIGN(offset);

	target->ps_locator = heapRelation->rd_locator;
	target->ps_indexlocator = indexRelation->rd_locator;
	target->ps_offset_ins = 0;
	target->ps_offset_am = 0;
	SerializeSnapshot(snapshot, target->ps_snapshot_data);

	if (instrument)
	{
		Size		sharedinfosz;

		target->ps_offset_ins = offset;
		sharedinfosz = offsetof(SharedIndexScanInstrumentation, winstrument) +
			nworkers * sizeof(IndexScanInstrumentation);
		offset = add_size(offset, sharedinfosz);
		offset = MAXALIGN(offset);

		/* Set leader's *sharedinfo pointer, and initialize stats */
		*sharedinfo = (SharedIndexScanInstrumentation *)
			OffsetToPointer(target, target->ps_offset_ins);
		memset(*sharedinfo, 0, sharedinfosz);
		(*sharedinfo)->num_workers = nworkers;
	}

	/* aminitparallelscan is optional; assume no-op if not provided by AM */
	if (parallel_aware && indexRelation->rd_indam->aminitparallelscan != NULL)
	{
		void	   *amtarget;

		target->ps_offset_am = offset;
		amtarget = OffsetToPointer(target, target->ps_offset_am);
		indexRelation->rd_indam->aminitparallelscan(amtarget);
	}
}

/* ----------------
 *		index_parallelrescan  - (re)start a parallel scan of an index
 * ----------------
 */
void
index_parallelrescan(IndexScanDesc scan)
{
	SCAN_CHECKS;

	if (scan->xs_heapfetch)
		table_index_fetch_reset(scan->xs_heapfetch);

	/*
	 * Reset the batching. This makes it look like there are no batches,
	 * discards reads already scheduled to the read stream, etc.
	 *
	 * XXX We do this before calling amparallelrescan, so that it could
	 * reinitialize everything (this probably does not matter very much, now
	 * that we've moved all the batching logic to indexam.c, it was more
	 * important when the index AM was responsible for more of it).
	 */
	index_batch_reset(scan, true);

	/* amparallelrescan is optional; assume no-op if not provided by AM */
	if (scan->indexRelation->rd_indam->amparallelrescan != NULL)
		scan->indexRelation->rd_indam->amparallelrescan(scan);
}

/*
 * index_beginscan_parallel - join parallel index scan
 *
 * Caller must be holding suitable locks on the heap and the index.
 */
IndexScanDesc
index_beginscan_parallel(Relation heaprel, Relation indexrel,
						 IndexScanInstrumentation *instrument,
						 int nkeys, int norderbys,
						 ParallelIndexScanDesc pscan)
{
	Snapshot	snapshot;
	IndexScanDesc scan;

	Assert(RelFileLocatorEquals(heaprel->rd_locator, pscan->ps_locator));
	Assert(RelFileLocatorEquals(indexrel->rd_locator, pscan->ps_indexlocator));

	snapshot = RestoreSnapshot(pscan->ps_snapshot_data);
	RegisterSnapshot(snapshot);
	scan = index_beginscan_internal(indexrel, nkeys, norderbys, snapshot,
									pscan, true);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by index_beginscan_internal.
	 */
	scan->heapRelation = heaprel;
	scan->xs_snapshot = snapshot;
	scan->instrument = instrument;

	if (indexrel->rd_indam->amgetbatch != NULL)
		index_batch_init(scan);

	/* prepare to fetch index matches from table */
	scan->xs_heapfetch = table_index_fetch_begin(heaprel);

	return scan;
}

/* ----------------
 * index_getnext_tid - get the next TID from a scan
 *
 * The result is the next TID satisfying the scan keys,
 * or NULL if no more matching tuples exist.
 * ----------------
 */
ItemPointer
index_getnext_tid(IndexScanDesc scan, ScanDirection direction)
{
	SCAN_CHECKS;

	/* XXX: we should assert that a snapshot is pushed or registered */
	Assert(TransactionIdIsValid(RecentXmin));

	/*
	 * Index AMs that support plain index scans must provide exactly one of
	 * either the amgetbatch or amgettuple callbacks
	 */
	Assert(!(scan->indexRelation->rd_indam->amgettuple != NULL &&
			 scan->indexRelation->rd_indam->amgetbatch != NULL));

	if (scan->batchState != NULL)
		return index_batch_getnext_tid(scan, direction);
	else
		return index_retail_getnext_tid(scan, direction);
}

/* ----------------
 *		index_getnext_batch_tid - amgetbatch index_getnext_tid implementation
 *
 * If we advance to the next batch, we release the previous one (unless it's
 * tracked for mark/restore).
 *
 * If the scan direction changes, we release all batches except the current
 * one (per readPos), to make it look it's the only batch we loaded.
 *
 * Returns the first/next TID, or NULL if no more items.
 *
 * FIXME This only sets xs_heaptid and xs_itup (if requested). Not sure if
 * we need to do something with xs_hitup. Should this set xs_hitup?
 *
 * XXX Maybe if we advance the position to the next batch, we could keep the
 * batch for a bit more, in case the scan direction changes (as long as it
 * fits into maxBatches)? But maybe that's unnecessary complexity for too
 * little gain, we'd need to be careful about releasing the batches lazily.
 * ----------------
 */
static ItemPointer
index_batch_getnext_tid(IndexScanDesc scan, ScanDirection direction)
{
	IndexScanBatchState *batchState = scan->batchState;
	IndexScanBatchPos *readPos;

	CHECK_SCAN_PROCEDURE(amgetbatch);

	/* shouldn't get here without batching */
	AssertCheckBatches(scan);

	/* Initialize direction on first call */
	if (batchState->direction == NoMovementScanDirection)
		batchState->direction = direction;

	/*
	 * Handle cancelling the use of the read stream for prefetching
	 */
	else if (unlikely(batchState->disabled && scan->xs_heapfetch->rs))
	{
		index_batch_pos_reset(scan, &batchState->streamPos);

		read_stream_reset(scan->xs_heapfetch->rs);
		scan->xs_heapfetch->rs = NULL;
	}

	/*
	 * Handle change of scan direction (reset stream, ...).
	 *
	 * Release future batches properly, to make it look like the current batch
	 * is the only one we loaded. Also reset the stream position, as if we are
	 * just starting the scan.
	 */
	else if (unlikely(batchState->direction != direction))
	{
		/* release "future" batches in the wrong direction */
		while (batchState->nextBatch > batchState->headBatch + 1)
		{
			IndexScanBatch fbatch;

			batchState->nextBatch--;
			fbatch = INDEX_SCAN_BATCH(scan, batchState->nextBatch);
			index_batch_free(scan, fbatch);
		}

		/*
		 * Remember the new direction, and make sure the scan is not marked as
		 * "finished" (we might have already read the last batch, but now we
		 * need to start over). Do this before resetting the stream - it
		 * should not invoke the callback until the first read, but it may
		 * seem a bit confusing otherwise.
		 */
		batchState->direction = direction;
		batchState->finished = false;
		batchState->currentPrefetchBlock = InvalidBlockNumber;

		index_batch_pos_reset(scan, &batchState->streamPos);
		if (scan->xs_heapfetch->rs)
			read_stream_reset(scan->xs_heapfetch->rs);
	}

	/* shortcut for the read position, for convenience */
	readPos = &batchState->readPos;

	DEBUG_LOG("index_batch_getnext_tid readPos %d %d direction %d",
			  readPos->batch, readPos->index, direction);

	/*
	 * Try advancing the batch position. If that doesn't succeed, it means we
	 * don't have more items in the current batch, and there's no future batch
	 * loaded. So try loading another batch, and maybe retry.
	 *
	 * FIXME This loop shouldn't happen more than twice. Maybe we should have
	 * some protection against infinite loops? To detect cases when the
	 * advance/getnext functions get to disagree?
	 */
	while (true)
	{
		/*
		 * If we manage to advance to the next items, return it and we're
		 * done. Otherwise try loading another batch.
		 */
		if (index_batch_pos_advance(scan, readPos, direction))
		{
			IndexScanBatchData *readBatch = INDEX_SCAN_BATCH(scan, readPos->batch);

			/* set the TID / itup for the scan */
			scan->xs_heaptid = readBatch->items[readPos->index].heapTid;
			if (scan->xs_want_itup)
				scan->xs_itup =
					(IndexTuple) (readBatch->currTuples +
								  readBatch->items[readPos->index].tupleOffset);

			DEBUG_LOG("readBatch %p firstItem %d lastItem %d readPos %d/%d TID (%u,%u)",
					  readBatch, readBatch->firstItem, readBatch->lastItem,
					  readPos->batch, readPos->index,
					  ItemPointerGetBlockNumber(&scan->xs_heaptid),
					  ItemPointerGetOffsetNumber(&scan->xs_heaptid));

			/*
			 * If we advanced to the next batch, release the batch we no
			 * longer need. The positions is the "read" position, and we can
			 * compare it to headBatch.
			 */
			if (unlikely(readPos->batch != batchState->headBatch))
			{
				IndexScanBatchData *headBatch = INDEX_SCAN_BATCH(scan,
																 batchState->headBatch);

				/*
				 * XXX When advancing readPos, the streamPos may get behind as
				 * we're only advancing it when actually requesting heap
				 * blocks. But we may not do that often enough - e.g. IOS may
				 * not need to access all-visible heap blocks, so the
				 * read_next callback does not get invoked for a long time.
				 * It's possible the stream gets so far behind the position
				 * that is becomes invalid, as we already removed the batch.
				 * But that means we don't need any heap blocks until the
				 * current read position -- if we did, we would not be in this
				 * situation (or it's a sign of a bug, as those two places are
				 * expected to be in sync). So if the streamPos still points
				 * at the batch we're about to free, reset the position --
				 * we'll set it to readPos in the read_next callback later on.
				 *
				 * XXX This can happen after the queue gets full, we "pause"
				 * the stream, and then reset it to continue. But I think that
				 * just increases the probability of hitting the issue, it's
				 * just more chance to to not advance the streamPos, which
				 * depends on when we try to fetch the first heap block after
				 * calling read_stream_reset().
				 *
				 * FIXME Simplify/clarify/shorten this comment. Can it
				 * actually happen, if we never pull from the stream in IOS?
				 * We probably don't look ahead for the first call.
				 */
				if (unlikely(batchState->streamPos.batch == batchState->headBatch))
				{
					DEBUG_LOG("index_batch_pos_reset called early (streamPos.batch == headBatch)");
					index_batch_pos_reset(scan, &batchState->streamPos);
				}

				DEBUG_LOG("index_batch_getnext_tid free headBatch %p headBatch %d nextBatch %d",
						  headBatch, batchState->headBatch, batchState->nextBatch);

				/* Free the head batch (except when it's markBatch) */
				index_batch_free(scan, headBatch);

				/*
				 * In any case, remove the batch from the regular queue, even
				 * if we kept it for mark/restore.
				 */
				batchState->headBatch++;

				DEBUG_LOG("index_batch_getnext_tid batch freed headBatch %d nextBatch %d",
						  batchState->headBatch, batchState->nextBatch);

				index_batch_print("index_batch_getnext_tid / free old batch", scan);

				/* we can't skip any batches */
				Assert(batchState->headBatch == readPos->batch);
			}

			pgstat_count_index_tuples(scan->indexRelation, 1);
			return &scan->xs_heaptid;
		}

		/*
		 * We failed to advance, i.e. we ran out of currently loaded batches.
		 * So if we filled the queue, this is a good time to reset the stream
		 * (before we try loading the next batch).
		 */
		if (unlikely(batchState->reset))
		{
			DEBUG_LOG("resetting read stream readPos %d,%d",
					  readPos->batch, readPos->index);

			batchState->reset = false;
			batchState->currentPrefetchBlock = InvalidBlockNumber;

			/*
			 * Need to reset the stream position, it might be too far behind.
			 * Ultimately we want to set it to readPos, but we can't do that
			 * yet - readPos still point sat the old batch, so just reset it
			 * and we'll init it to readPos later in the callback.
			 */
			index_batch_pos_reset(scan, &batchState->streamPos);

			if (scan->xs_heapfetch->rs)
				read_stream_reset(scan->xs_heapfetch->rs);
		}

		/*
		 * Failed to advance the read position, so try reading the next batch.
		 * If this fails, we're done - there's nothing more to load.
		 *
		 * Most of the batches should be loaded from read_stream_next_buffer,
		 * but we need to call index_batch_getnext here too, for two reasons.
		 * First, the read_stream only gets working after we try fetching the
		 * first heap tuple, so we need to load the initial batch (the head).
		 * Second, while most batches will be preloaded by the stream thanks
		 * to prefetching, it's possible to set effective_io_concurrency=0,
		 * and in that case all the batches get loaded from here.
		 */
		if (!index_batch_getnext(scan, direction))
			break;

		DEBUG_LOG("loaded next batch, retry to advance position");
	}

	/*
	 * If we get here, we failed to advance the position and there are no more
	 * batches, so we're done.
	 */
	DEBUG_LOG("no more batches to process");

	/*
	 * Reset the position - we must not keep the last valid position, in case
	 * we change direction of the scan and start scanning again. If we kept
	 * the position, we'd skip the first item.
	 *
	 * XXX This is a bit strange. Do we really need to reset the position
	 * after returning the last item? I wonder if it means the API is not
	 * quite right.
	 */
	index_batch_pos_reset(scan, readPos);

	return NULL;
}

/* ----------------
 *		index_retail_getnext_tid - amgettuple index_getnext_tid implementation
 *
 * Returns the first/next TID, or NULL if no more items.
 * ----------------
 */
static ItemPointer
index_retail_getnext_tid(IndexScanDesc scan, ScanDirection direction)
{
	bool		found;

	CHECK_SCAN_PROCEDURE(amgettuple);

	/*
	 * The AM's amgettuple proc finds the next index entry matching the scan
	 * keys, and puts the TID into scan->xs_heaptid.  It should also set
	 * scan->xs_recheck and possibly scan->xs_itup/scan->xs_hitup, though we
	 * pay no attention to those fields here.
	 */
	found = scan->indexRelation->rd_indam->amgettuple(scan, direction);

	/* Reset kill flag immediately for safety */
	scan->kill_prior_tuple = false;
	scan->xs_heap_continue = false;

	/* If we're out of index entries, we're done */
	if (!found)
	{
		/* release resources (like buffer pins) from table accesses */
		if (scan->xs_heapfetch)
			table_index_fetch_reset(scan->xs_heapfetch);

		return NULL;
	}
	Assert(ItemPointerIsValid(&scan->xs_heaptid));

	pgstat_count_index_tuples(scan->indexRelation, 1);

	/* Return the TID of the tuple we found. */
	return &scan->xs_heaptid;
}

/* ----------------
 *		index_fetch_heap - get the scan's next heap tuple
 *
 * The result is a visible heap tuple associated with the index TID most
 * recently fetched by index_getnext_tid, or NULL if no more matching tuples
 * exist.  (There can be more than one matching tuple because of HOT chains,
 * although when using an MVCC snapshot it should be impossible for more than
 * one such tuple to exist.)
 *
 * On success, the buffer containing the heap tup is pinned (the pin will be
 * dropped in a future index_getnext_tid, index_fetch_heap or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */
bool
index_fetch_heap(IndexScanDesc scan, TupleTableSlot *slot)
{
	bool		all_dead = false;
	bool		found;

	found = table_index_fetch_tuple(scan->xs_heapfetch, &scan->xs_heaptid,
									scan->xs_snapshot, slot,
									&scan->xs_heap_continue, &all_dead);

	if (found)
		pgstat_count_heap_fetch(scan->indexRelation);

	/*
	 * If we scanned a whole HOT chain and found only dead tuples, tell index
	 * AM to kill its entry for that TID (this will take effect in the next
	 * amgettuple call, in index_getnext_tid).  We do not do this when in
	 * recovery because it may violate MVCC to do so.  See comments in
	 * RelationGetIndexScan().
	 *
	 * XXX For scans using batching, record the flag in the batch (we will
	 * pass it to the AM later, when freeing it). Otherwise just pass it to
	 * the AM using the kill_prior_tuple field.
	 */
	if (!scan->xactStartedInRecovery)
	{
		if (scan->batchState == NULL)
			scan->kill_prior_tuple = all_dead;
		else if (all_dead)
			index_batch_kill_item(scan);
	}

	return found;
}

/* ----------------
 *		index_getnext_slot - get the next tuple from a scan
 *
 * The result is true if a tuple satisfying the scan keys and the snapshot was
 * found, false otherwise.  The tuple is stored in the specified slot.
 *
 * On success, resources (like buffer pins) are likely to be held, and will be
 * dropped by a future index_getnext_tid, index_fetch_heap or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */
bool
index_getnext_slot(IndexScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	for (;;)
	{
		if (!scan->xs_heap_continue)
		{
			ItemPointer tid;

			/* Time to fetch the next TID from the index */
			tid = index_getnext_tid(scan, direction);

			/* If we're out of index entries, we're done */
			if (tid == NULL)
				break;

			Assert(ItemPointerEquals(tid, &scan->xs_heaptid));
		}

		/*
		 * Fetch the next (or only) visible heap tuple for this index entry.
		 * If we don't find anything, loop around and grab the next TID from
		 * the index.
		 */
		Assert(ItemPointerIsValid(&scan->xs_heaptid));
		if (index_fetch_heap(scan, slot))
			return true;
	}

	return false;
}

/* ----------------
 *		index_getbitmap - get all tuples at once from an index scan
 *
 * Adds the TIDs of all heap tuples satisfying the scan keys to a bitmap.
 * Since there's no interlock between the index scan and the eventual heap
 * access, this is only safe to use with MVCC-based snapshots: the heap
 * item slot could have been replaced by a newer tuple by the time we get
 * to it.
 *
 * Returns the number of matching tuples found.  (Note: this might be only
 * approximate, so it should only be used for statistical purposes.)
 * ----------------
 */
int64
index_getbitmap(IndexScanDesc scan, TIDBitmap *bitmap)
{
	int64		ntids;

	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amgetbitmap);

	/* just make sure this is false... */
	scan->kill_prior_tuple = false;

	/*
	 * have the am's getbitmap proc do all the work.
	 */
	ntids = scan->indexRelation->rd_indam->amgetbitmap(scan, bitmap);

	pgstat_count_index_tuples(scan->indexRelation, ntids);

	return ntids;
}

/* ----------------
 *		index_bulk_delete - do mass deletion of index entries
 *
 *		callback routine tells whether a given main-heap tuple is
 *		to be deleted
 *
 *		return value is an optional palloc'd struct of statistics
 * ----------------
 */
IndexBulkDeleteResult *
index_bulk_delete(IndexVacuumInfo *info,
				  IndexBulkDeleteResult *istat,
				  IndexBulkDeleteCallback callback,
				  void *callback_state)
{
	Relation	indexRelation = info->index;

	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(ambulkdelete);

	return indexRelation->rd_indam->ambulkdelete(info, istat,
												 callback, callback_state);
}

/* ----------------
 *		index_vacuum_cleanup - do post-deletion cleanup of an index
 *
 *		return value is an optional palloc'd struct of statistics
 * ----------------
 */
IndexBulkDeleteResult *
index_vacuum_cleanup(IndexVacuumInfo *info,
					 IndexBulkDeleteResult *istat)
{
	Relation	indexRelation = info->index;

	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(amvacuumcleanup);

	return indexRelation->rd_indam->amvacuumcleanup(info, istat);
}

/* ----------------
 *		index_can_return
 *
 *		Does the index access method support index-only scans for the given
 *		column?
 * ----------------
 */
bool
index_can_return(Relation indexRelation, int attno)
{
	RELATION_CHECKS;

	/* amcanreturn is optional; assume false if not provided by AM */
	if (indexRelation->rd_indam->amcanreturn == NULL)
		return false;

	return indexRelation->rd_indam->amcanreturn(indexRelation, attno);
}

/* ----------------
 *		index_getprocid
 *
 *		Index access methods typically require support routines that are
 *		not directly the implementation of any WHERE-clause query operator
 *		and so cannot be kept in pg_amop.  Instead, such routines are kept
 *		in pg_amproc.  These registered procedure OIDs are assigned numbers
 *		according to a convention established by the access method.
 *		The general index code doesn't know anything about the routines
 *		involved; it just builds an ordered list of them for
 *		each attribute on which an index is defined.
 *
 *		As of Postgres 8.3, support routines within an operator family
 *		are further subdivided by the "left type" and "right type" of the
 *		query operator(s) that they support.  The "default" functions for a
 *		particular indexed attribute are those with both types equal to
 *		the index opclass' opcintype (note that this is subtly different
 *		from the indexed attribute's own type: it may be a binary-compatible
 *		type instead).  Only the default functions are stored in relcache
 *		entries --- access methods can use the syscache to look up non-default
 *		functions.
 *
 *		This routine returns the requested default procedure OID for a
 *		particular indexed attribute.
 * ----------------
 */
RegProcedure
index_getprocid(Relation irel,
				AttrNumber attnum,
				uint16 procnum)
{
	RegProcedure *loc;
	int			nproc;
	int			procindex;

	nproc = irel->rd_indam->amsupport;

	Assert(procnum > 0 && procnum <= (uint16) nproc);

	procindex = (nproc * (attnum - 1)) + (procnum - 1);

	loc = irel->rd_support;

	Assert(loc != NULL);

	return loc[procindex];
}

/* ----------------
 *		index_getprocinfo
 *
 *		This routine allows index AMs to keep fmgr lookup info for
 *		support procs in the relcache.  As above, only the "default"
 *		functions for any particular indexed attribute are cached.
 *
 * Note: the return value points into cached data that will be lost during
 * any relcache rebuild!  Therefore, either use the callinfo right away,
 * or save it only after having acquired some type of lock on the index rel.
 * ----------------
 */
FmgrInfo *
index_getprocinfo(Relation irel,
				  AttrNumber attnum,
				  uint16 procnum)
{
	FmgrInfo   *locinfo;
	int			nproc;
	int			optsproc;
	int			procindex;

	nproc = irel->rd_indam->amsupport;
	optsproc = irel->rd_indam->amoptsprocnum;

	Assert(procnum > 0 && procnum <= (uint16) nproc);

	procindex = (nproc * (attnum - 1)) + (procnum - 1);

	locinfo = irel->rd_supportinfo;

	Assert(locinfo != NULL);

	locinfo += procindex;

	/* Initialize the lookup info if first time through */
	if (locinfo->fn_oid == InvalidOid)
	{
		RegProcedure *loc = irel->rd_support;
		RegProcedure procId;

		Assert(loc != NULL);

		procId = loc[procindex];

		/*
		 * Complain if function was not found during IndexSupportInitialize.
		 * This should not happen unless the system tables contain bogus
		 * entries for the index opclass.  (If an AM wants to allow a support
		 * function to be optional, it can use index_getprocid.)
		 */
		if (!RegProcedureIsValid(procId))
			elog(ERROR, "missing support function %d for attribute %d of index \"%s\"",
				 procnum, attnum, RelationGetRelationName(irel));

		fmgr_info_cxt(procId, locinfo, irel->rd_indexcxt);

		if (procnum != optsproc)
		{
			/* Initialize locinfo->fn_expr with opclass options Const */
			bytea	  **attoptions = RelationGetIndexAttOptions(irel, false);
			MemoryContext oldcxt = MemoryContextSwitchTo(irel->rd_indexcxt);

			set_fn_opclass_options(locinfo, attoptions[attnum - 1]);

			MemoryContextSwitchTo(oldcxt);
		}
	}

	return locinfo;
}

/* ----------------
 *		index_store_float8_orderby_distances
 *
 *		Convert AM distance function's results (that can be inexact)
 *		to ORDER BY types and save them into xs_orderbyvals/xs_orderbynulls
 *		for a possible recheck.
 * ----------------
 */
void
index_store_float8_orderby_distances(IndexScanDesc scan, Oid *orderByTypes,
									 IndexOrderByDistance *distances,
									 bool recheckOrderBy)
{
	int			i;

	Assert(distances || !recheckOrderBy);

	scan->xs_recheckorderby = recheckOrderBy;

	for (i = 0; i < scan->numberOfOrderBys; i++)
	{
		if (orderByTypes[i] == FLOAT8OID)
		{
#ifndef USE_FLOAT8_BYVAL
			/* must free any old value to avoid memory leakage */
			if (!scan->xs_orderbynulls[i])
				pfree(DatumGetPointer(scan->xs_orderbyvals[i]));
#endif
			if (distances && !distances[i].isnull)
			{
				scan->xs_orderbyvals[i] = Float8GetDatum(distances[i].value);
				scan->xs_orderbynulls[i] = false;
			}
			else
			{
				scan->xs_orderbyvals[i] = (Datum) 0;
				scan->xs_orderbynulls[i] = true;
			}
		}
		else if (orderByTypes[i] == FLOAT4OID)
		{
			/* convert distance function's result to ORDER BY type */
			if (distances && !distances[i].isnull)
			{
				scan->xs_orderbyvals[i] = Float4GetDatum((float4) distances[i].value);
				scan->xs_orderbynulls[i] = false;
			}
			else
			{
				scan->xs_orderbyvals[i] = (Datum) 0;
				scan->xs_orderbynulls[i] = true;
			}
		}
		else
		{
			/*
			 * If the ordering operator's return value is anything else, we
			 * don't know how to convert the float8 bound calculated by the
			 * distance function to that.  The executor won't actually need
			 * the order by values we return here, if there are no lossy
			 * results, so only insist on converting if the *recheck flag is
			 * set.
			 */
			if (scan->xs_recheckorderby)
				elog(ERROR, "ORDER BY operator must return float8 or float4 if the distance function is lossy");
			scan->xs_orderbynulls[i] = true;
		}
	}
}

/* ----------------
 *      index_opclass_options
 *
 *      Parse opclass-specific options for index column.
 * ----------------
 */
bytea *
index_opclass_options(Relation indrel, AttrNumber attnum, Datum attoptions,
					  bool validate)
{
	int			amoptsprocnum = indrel->rd_indam->amoptsprocnum;
	Oid			procid = InvalidOid;
	FmgrInfo   *procinfo;
	local_relopts relopts;

	/* fetch options support procedure if specified */
	if (amoptsprocnum != 0)
		procid = index_getprocid(indrel, attnum, amoptsprocnum);

	if (!OidIsValid(procid))
	{
		Oid			opclass;
		Datum		indclassDatum;
		oidvector  *indclass;

		if (!DatumGetPointer(attoptions))
			return NULL;		/* ok, no options, no procedure */

		/*
		 * Report an error if the opclass's options-parsing procedure does not
		 * exist but the opclass options are specified.
		 */
		indclassDatum = SysCacheGetAttrNotNull(INDEXRELID, indrel->rd_indextuple,
											   Anum_pg_index_indclass);
		indclass = (oidvector *) DatumGetPointer(indclassDatum);
		opclass = indclass->values[attnum - 1];

		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("operator class %s has no options",
						generate_opclass_name(opclass))));
	}

	init_local_reloptions(&relopts, 0);

	procinfo = index_getprocinfo(indrel, attnum, amoptsprocnum);

	(void) FunctionCall1(procinfo, PointerGetDatum(&relopts));

	return build_local_reloptions(&relopts, attoptions, validate);
}

/*
 * Check that a position (batch,item) is valid with respect to the batches we
 * have currently loaded.
 *
 * XXX The "marked" batch is an exception. The marked batch may get outside
 * the range of current batches, so make sure to never check the position
 * for that.
 */
static void
AssertCheckBatchPosValid(IndexScanDesc scan, IndexScanBatchPos *pos)
{
#ifdef USE_ASSERT_CHECKING
	IndexScanBatchState *batchState = scan->batchState;

	/* make sure the position is valid for currently loaded batches */
	Assert(pos->batch >= batchState->headBatch);
	Assert(pos->batch < batchState->nextBatch);
#endif
}

/*
 * Check a single batch is valid.
 */
static void
AssertCheckBatch(IndexScanDesc scan, IndexScanBatch batch)
{
#ifdef USE_ASSERT_CHECKING
	/* there must be valid range of items */
	Assert(batch->firstItem <= batch->lastItem);
	Assert(batch->firstItem >= 0);

	/* we should have items (buffer and pointers) */
	Assert(batch->items != NULL);

	/*
	 * The number of killed items must be valid, and there must be an array of
	 * indexes if there are items.
	 */
	Assert(batch->numKilled >= 0);
	Assert(!(batch->numKilled > 0 && batch->killedItems == NULL));

	/* XXX can we check some of the other batch fields? */
#endif
}

/*
 * Check invariants on current batches
 *
 * Makes sure the indexes are set as expected, the buffer size is within
 * limits, and so on.
 */
static void
AssertCheckBatches(IndexScanDesc scan)
{
#ifdef USE_ASSERT_CHECKING
	IndexScanBatchState *batchState = scan->batchState;

	/* we should have batches initialized */
	Assert(batchState != NULL);

	/* We should not have too many batches. */
	Assert(batchState->maxBatches > 0 &&
		   batchState->maxBatches <= INDEX_SCAN_MAX_BATCHES);

	/*
	 * The head/next indexes should define a valid range (in the cyclic
	 * buffer, and should not overflow maxBatches.
	 */
	Assert(batchState->headBatch >= 0 &&
		   batchState->headBatch <= batchState->nextBatch);
	Assert(batchState->nextBatch - batchState->headBatch <=
		   batchState->maxBatches);

	/* Check all current batches */
	for (int i = batchState->headBatch; i < batchState->nextBatch; i++)
	{
		IndexScanBatch batch = INDEX_SCAN_BATCH(scan, i);

		AssertCheckBatch(scan, batch);
	}
#endif
}

/*
 * index_batch_pos_advance
 *		Advance the position to the next item, depending on scan direction.
 *
 * Advance the position to the next item, either in the same batch or the
 * following one (if already available).
 *
 * We can advance only if we already have some batches loaded, and there's
 * either enough items in the current batch, or some more items in the
 * subsequent batches.
 *
 * If this is the first advance (right after loading the initial/head batch),
 * position is still undefined. Otherwise we expect the position to be valid.
 *
 * Returns true if the position was advanced, false otherwise.
 *
 * The position is guaranteed to be valid only after a successful advance.
 * If an advance fails (false returned), the position can be invalid.
 *
 * XXX This seems like a good place to enforce some "invariants", e.g.
 * that the positions are always valid. We should never get here with
 * invalid position (so probably should be initialized as part of loading the
 * initial/head batch), and then invalidated if advance fails. Could be tricky
 * for the stream position, though, because it can get "lag" for IOS etc.
 */
static pg_attribute_always_inline bool
index_batch_pos_advance(IndexScanDesc scan, IndexScanBatchPos *pos,
						ScanDirection direction)
{
	IndexScanBatchData *batch;

	/* make sure we have batching initialized and consistent */
	AssertCheckBatches(scan);

	/* should know direction by now */
	Assert(direction == scan->batchState->direction);
	Assert(direction != NoMovementScanDirection);

	/* We can't advance if there are no batches available. */
	if (INDEX_SCAN_BATCH_COUNT(scan) == 0)
		return false;

	/*
	 * If the position has not been advanced yet, it has to be right after we
	 * loaded the initial batch (must be the head batch). In that case just
	 * initialize it to the batch's first item (or its last item, when
	 * scanning backwards).
	 *
	 * XXX Maybe we should just explicitly initialize the position after
	 * loading the initial batch, without having to go through the advance.
	 */
	if (INDEX_SCAN_POS_INVALID(pos))
	{
		/*
		 * We should have loaded the scan's initial batch, or maybe we have
		 * changed the direction of the scan after scanning all the way to the
		 * end (in which case the position is invalid, and we make it look
		 * like there is just one batch). We should have just one batch,
		 * though.
		 *
		 * XXX Actually, could there be more batches? Maybe we prefetched more
		 * batches right away? It doesn't seem to be a substantial invariant.
		 */
		Assert(INDEX_SCAN_BATCH_COUNT(scan) == 1);

		/*
		 * Get the initial batch (which must be the head), and initialize the
		 * position to the appropriate item for the current scan direction
		 */
		batch = INDEX_SCAN_BATCH(scan, scan->batchState->headBatch);

		pos->batch = scan->batchState->headBatch;

		if (ScanDirectionIsForward(direction))
			pos->index = batch->firstItem;
		else
			pos->index = batch->lastItem;

		AssertCheckBatchPosValid(scan, pos);

		return true;
	}

	/*
	 * The position is already defined, so we should have some batches loaded
	 * and the position has to be valid with respect to those.
	 */
	AssertCheckBatchPosValid(scan, pos);

	/*
	 * Advance to the next item in the same batch, if there are more items. If
	 * we're at the last item, we'll try advancing to the next batch later.
	 */
	batch = INDEX_SCAN_BATCH(scan, pos->batch);

	if (ScanDirectionIsForward(direction))
	{
		if (++pos->index <= batch->lastItem)
		{
			AssertCheckBatchPosValid(scan, pos);

			return true;
		}
	}
	else						/* ScanDirectionIsBackward */
	{
		if (--pos->index >= batch->firstItem)
		{
			AssertCheckBatchPosValid(scan, pos);

			return true;
		}
	}

	/*
	 * We couldn't advance within the same batch, try advancing to the next
	 * batch, if it's already loaded.
	 */
	if (INDEX_SCAN_BATCH_LOADED(scan, pos->batch + 1))
	{
		/* advance to the next batch */
		pos->batch++;

		batch = INDEX_SCAN_BATCH(scan, pos->batch);
		Assert(batch != NULL);

		if (ScanDirectionIsForward(direction))
			pos->index = batch->firstItem;
		else
			pos->index = batch->lastItem;

		AssertCheckBatchPosValid(scan, pos);

		return true;
	}

	/* can't advance */
	return false;
}

/*
 * index_batch_pos_reset
 *		Reset the position, so that it looks as if never advanced.
 */
static void
index_batch_pos_reset(IndexScanDesc scan, IndexScanBatchPos *pos)
{
	pos->batch = -1;
	pos->index = -1;
}

/*
 * index_scan_stream_read_next
 *		return the next block to pass to the read stream
 *
 * This assumes the "current" scan direction, requested by the caller.
 *
 * If the direction changes before consuming all blocks, we'll reset the stream
 * and start from scratch. The scan direction change is handled elsewhere. Here
 * we rely on having the correct value in batchState->direction.
 *
 * The position of the read_stream is stored in streamPos, which may be ahead of
 * the current readPos (which is what got consumed by the scan).
 *
 * The streamPos can however also get behind readPos too, when some blocks are
 * skipped and not returned to the read_stream. An example is an index scan on
 * a correlated index, with many duplicate blocks are skipped, or an IOS where
 * all-visible blocks are skipped.
 *
 * The initial batch is always loaded from index_batch_getnext_tid(). We don't
 * get here until the first read_stream_next_buffer() call, when pulling the
 * first heap tuple from the stream. After that, most batches should be loaded
 * by this callback, driven by the read_stream look-ahead distance. However,
 * with disabled prefetching (that is, with effective_io_concurrency=0), all
 * batches will be loaded in index_batch_getnext_tid.
 *
 * It's possible we got here only fairly late in the scan, e.g. if many tuples
 * got skipped in the index-only scan, etc. In this case just use the read
 * position as a streamPos starting point.
 *
 * XXX It seems the readPos/streamPos comments should be placed elsewhere. The
 * read_stream callback does not seem like the right place.
 */
static BlockNumber
index_scan_stream_read_next(ReadStream *stream,
							void *callback_private_data,
							void *per_buffer_data)
{
	IndexScanDesc scan = (IndexScanDesc) callback_private_data;
	IndexScanBatchState *batchState = scan->batchState;
	IndexScanBatchPos *streamPos = &batchState->streamPos;
	ScanDirection direction = batchState->direction;

	/* By now we should know the direction of the scan. */
	Assert(direction != NoMovementScanDirection);

	/*
	 * The read position (readPos) has to be valid.
	 *
	 * We initialize/advance it before even attempting to read the heap tuple,
	 * and it gets invalidated when we reach the end of the scan (but then we
	 * don't invoke the callback again).
	 *
	 * XXX This applies to the readPos. We'll use streamPos to determine which
	 * blocks to pass to the stream, and readPos may be used to initialize it.
	 */
	AssertCheckBatchPosValid(scan, &batchState->readPos);

	/*
	 * Try to advance the streamPos to the next item, and if that doesn't
	 * succeed (if there are no more items in loaded batches), try loading the
	 * next one.
	 *
	 * FIXME Unlike index_batch_getnext_tid, this can loop more than twice. If
	 * many blocks get skipped due to currentPrefetchBlock or all-visibility
	 * (per the "prefetch" callback), we get to load additional batches. In
	 * the worst case we hit the INDEX_SCAN_MAX_BATCHES limit and have to
	 * "pause" the stream.
	 */
	while (true)
	{
		bool		advanced = false;

		/*
		 * If the stream position has not been initialized yet, set it to the
		 * current read position. This is the item the caller is trying to
		 * read, so it's what we should return to the stream.
		 */
		if (INDEX_SCAN_POS_INVALID(streamPos))
		{
			*streamPos = batchState->readPos;
			advanced = true;
		}
		else if (index_batch_pos_advance(scan, streamPos, direction))
		{
			advanced = true;
		}

		/*
		 * FIXME Maybe check the streamPos is not behind readPos?
		 *
		 * FIXME Actually, could streamPos get stale/lagging behind readPos,
		 * and if yes how much. Could it get so far behind to not be valid,
		 * pointing at a freed batch? In that case we can't even advance it,
		 * and we should just initialize it to readPos. We might do that
		 * anyway, I guess, just to save on "pointless" advances (it must
		 * agree with readPos, we can't allow "retroactively" changing the
		 * block sequence).
		 */

		/*
		 * If we advanced the position, either return the block for the TID,
		 * or skip it (and then try advancing again).
		 *
		 * The block may be "skipped" for two reasons. First, the caller may
		 * define a "prefetch" callback that tells us to skip items (IOS does
		 * this to skip all-visible pages). Second, currentPrefetchBlock is
		 * used to skip duplicate block numbers (a sequence of TIDS for the
		 * same block).
		 */
		if (advanced)
		{
			IndexScanBatch streamBatch = INDEX_SCAN_BATCH(scan, streamPos->batch);
			ItemPointer tid = &streamBatch->items[streamPos->index].heapTid;

			DEBUG_LOG("index_scan_stream_read_next: index %d TID (%u,%u)",
					  streamPos->index,
					  ItemPointerGetBlockNumber(tid),
					  ItemPointerGetOffsetNumber(tid));

			/*
			 * If there's a prefetch callback, use it to decide if we need to
			 * read the next block.
			 *
			 * We need to do this before checking currentPrefetchBlock; it's
			 * essential that the VM cache used by index-only scans is
			 * initialized here.
			 */
			if (batchState->prefetch &&
				!batchState->prefetch(scan, batchState->prefetchArg, streamPos))
			{
				DEBUG_LOG("index_scan_stream_read_next: skip block (callback)");
				continue;
			}

			/* same block as before, don't need to read it */
			if (batchState->currentPrefetchBlock == ItemPointerGetBlockNumber(tid))
			{
				DEBUG_LOG("index_scan_stream_read_next: skip block (currentPrefetchBlock)");
				continue;
			}

			batchState->currentPrefetchBlock = ItemPointerGetBlockNumber(tid);

			return batchState->currentPrefetchBlock;
		}

		/*
		 * Couldn't advance the position, no more items in the loaded batches.
		 * Try loading the next batch - if that succeeds, try advancing again
		 * (this time the advance should work, but we may skip all the items).
		 *
		 * If we fail to load the next batch, we're done.
		 */
		if (!index_batch_getnext(scan, direction))
			break;

		/*
		 * Consider disabling prefetching when we can't keep a sufficiently
		 * large "index tuple distance" between readPos and streamPos.
		 *
		 * Only consider doing this when we're not on the scan's initial
		 * batch, when readPos and streamPos share the same batch.
		 */
		if (!batchState->finished && !batchState->prefetchingLockedIn)
		{
			int			indexdiff;

			if (streamPos->batch <= INDEX_SCAN_MIN_DISTANCE_NBATCHES)
			{
				/* Too early to check if prefetching should be disabled */
			}
			else if (batchState->readPos.batch == streamPos->batch)
			{
				IndexScanBatchPos *readPos = &batchState->readPos;

				if (ScanDirectionIsForward(direction))
					indexdiff = streamPos->index - readPos->index;
				else
				{
					IndexScanBatch readBatch =
						INDEX_SCAN_BATCH(scan, readPos->batch);

					indexdiff = (readPos->index - readBatch->firstItem) -
						(streamPos->index - readBatch->firstItem);
				}

				if (indexdiff < INDEX_SCAN_MIN_TUPLE_DISTANCE)
				{
					batchState->disabled = true;
					return InvalidBlockNumber;
				}
				else
				{
					batchState->prefetchingLockedIn = true;
				}
			}
			else
				batchState->prefetchingLockedIn = true;
		}
	}

	/* no more items in this scan */
	return InvalidBlockNumber;
}

/* ----------------
 *		index_batch_getnext - get the next batch of TIDs from a scan
 *
 * Returns true if we managed to read a batch of TIDs, or false if there are no
 * more TIDs in the scan. The load may also return false if we used the maximum
 * number of batches (INDEX_SCAN_MAX_BATCHES), in which case we'll reset the
 * stream and continue the scan later.
 *
 * Returns true if the batch was loaded successfully, false otherwise.
 *
 * This only loads the TIDs and resets the various batch fields to fresh
 * state. It does not set xs_heaptid/xs_itup/xs_hitup, that's the
 * responsibility of the following index_batch_getnext_tid() calls.
 * ----------------
 */
static bool
index_batch_getnext(IndexScanDesc scan, ScanDirection direction)
{
	IndexScanBatchState *batchState = scan->batchState;
	IndexScanBatch priorbatch = NULL,
				batch = NULL;

	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amgetbatch);

	/* XXX: we should assert that a snapshot is pushed or registered */
	Assert(TransactionIdIsValid(RecentXmin));

	/* Did we already read the last batch for this scan? */
	if (batchState->finished)
		return false;

	/*
	 * If we already used the maximum number of batch slots available, it's
	 * pointless to try loading another one. This can happen for various
	 * reasons, e.g. for index-only scans on all-visible table, or skipping
	 * duplicate blocks on perfectly correlated indexes, etc.
	 *
	 * We could enlarge the array to allow more batches, but that's futile, we
	 * can always construct a case using more memory. Not only it would risk
	 * OOM, it'd also be inefficient because this happens early in the scan
	 * (so it'd interfere with LIMIT queries).
	 */
	if (INDEX_SCAN_BATCH_FULL(scan))
	{
		DEBUG_LOG("index_batch_getnext: ran out of space for batches");
		scan->batchState->reset = true;
		return false;
	}

	index_batch_print("index_batch_getnext / start", scan);

	/*
	 * Check if there's an existing batch that amgetbatch has to pick things
	 * up from
	 */
	if (batchState->headBatch < batchState->nextBatch)
		priorbatch = INDEX_SCAN_BATCH(scan, batchState->nextBatch - 1);

	batch = scan->indexRelation->rd_indam->amgetbatch(scan, priorbatch,
													  direction);
	if (batch != NULL)
	{
		/*
		 * We got the batch from the AM, but we need to add it to the queue.
		 * Maybe that should be part of the "batch allocation" that happens in
		 * the AM?
		 */
		int			batchIndex = batchState->nextBatch;

		INDEX_SCAN_BATCH(scan, batchIndex) = batch;

		batchState->nextBatch++;

		DEBUG_LOG("index_batch_getnext headBatch %d nextBatch %d batch %p",
				  batchState->headBatch, batchState->nextBatch, batch);

		/* Delay initializing stream until reading from scan's second batch */
		if (priorbatch && !scan->xs_heapfetch->rs && !batchState->disabled &&
			enable_indexscan_prefetch)
			scan->xs_heapfetch->rs =
				read_stream_begin_relation(READ_STREAM_DEFAULT, NULL,
										   scan->heapRelation, MAIN_FORKNUM,
										   index_scan_stream_read_next, scan, 0);
	}
	else
		batchState->finished = true;

	AssertCheckBatches(scan);

	index_batch_print("index_batch_getnext / end", scan);

	return (batch != NULL);
}

/*
 * index_batch_init
 *		Initialize various fields / arrays needed by batching.
 *
 * FIXME This is a bit ad-hoc hodge podge, due to how I was adding more and
 * more pieces. Some of the fields may be not quite necessary, needs cleanup.
 */
static void
index_batch_init(IndexScanDesc scan)
{
	/* init batching info */
	Assert(scan->indexRelation->rd_indam->amgetbatch != NULL);
	Assert(scan->indexRelation->rd_indam->amfreebatch != NULL);

	scan->batchState = palloc(sizeof(IndexScanBatchState));

	/*
	 * Initialize the batch.
	 *
	 * We prefer to eagerly drop leaf page pins before amgetbatch returns.
	 * This avoids making VACUUM wait to acquire a cleanup lock on the page.
	 *
	 * We cannot safely drop leaf page pins during index-only scans due to a
	 * race condition involving VACUUM setting pages all-visible in the VM.
	 * It's also unsafe for plain index scans that use a non-MVCC snapshot.
	 *
	 * When we drop pins eagerly, the mechanism that marks index tuples as
	 * LP_DEAD has to deal with concurrent TID recycling races.  The scheme
	 * used to detect unsafe TID recycling won't work when scanning unlogged
	 * relations (since it involves saving an affected page's LSN).  Opt out
	 * of eager pin dropping during unlogged relation scans for now.
	 */
	scan->batchState->dropPin =
		(!scan->xs_want_itup && IsMVCCSnapshot(scan->xs_snapshot) &&
		 RelationNeedsWAL(scan->indexRelation));
	scan->batchState->finished = false;
	scan->batchState->reset = false;
	scan->batchState->prefetchingLockedIn = false;
	scan->batchState->disabled = false;
	scan->batchState->currentPrefetchBlock = InvalidBlockNumber;
	scan->batchState->direction = NoMovementScanDirection;
	/* positions in the queue of batches */
	index_batch_pos_reset(scan, &scan->batchState->readPos);
	index_batch_pos_reset(scan, &scan->batchState->streamPos);
	index_batch_pos_reset(scan, &scan->batchState->markPos);

	scan->batchState->markBatch = NULL;
	scan->batchState->maxBatches = INDEX_SCAN_MAX_BATCHES;
	scan->batchState->headBatch = 0;	/* initial head batch */
	scan->batchState->nextBatch = 0;	/* initial batch starts empty */

	/* XXX init the cache of batches, capacity 16 is arbitrary */
	scan->batchState->cache.maxbatches = 16;
	scan->batchState->cache.batches = NULL;

	scan->batchState->batches =
		palloc(sizeof(IndexScanBatchData *) * scan->batchState->maxBatches);

	scan->batchState->prefetch = NULL;
	scan->batchState->prefetchArg = NULL;
}

/*
 * index_batch_reset
 *		Reset the batch before reading the next chunk of data.
 *
 * complete - true means we reset even marked batch
 *
  * XXX Should this reset the batch memory context, xs_itup, xs_hitup, etc?
 */
static void
index_batch_reset(IndexScanDesc scan, bool complete)
{
	IndexScanBatchState *batchState = scan->batchState;

	/* bail out if batching not enabled */
	if (!batchState)
		return;

	AssertCheckBatches(scan);

	index_batch_print("index_batch_reset", scan);

	/* With batching enabled, we should have a read stream. Reset it. */
	Assert(scan->xs_heapfetch);
	if (scan->xs_heapfetch->rs)
		read_stream_reset(scan->xs_heapfetch->rs);

	/* reset the positions */
	index_batch_pos_reset(scan, &batchState->readPos);
	index_batch_pos_reset(scan, &batchState->streamPos);

	/*
	 * With "complete" reset, make sure to also free the marked batch, either
	 * by just forgetting it (if it's still in the queue), or by explicitly
	 * freeing it.
	 *
	 * XXX Do this before the loop, so that it calls the amfreebatch().
	 */
	if (complete && unlikely(batchState->markBatch != NULL))
	{
		IndexScanBatchPos *markPos = &batchState->markPos;
		IndexScanBatch markBatch = batchState->markBatch;

		/* always reset the position, forget the marked batch */
		batchState->markBatch = NULL;

		/*
		 * If we've already moved past the marked batch (it's not in the
		 * current queue), free it explicitly. Otherwise it'll be in the freed
		 * later.
		 */
		if (markPos->batch < batchState->headBatch ||
			markPos->batch >= batchState->nextBatch)
			index_batch_free(scan, markBatch);

		/* reset position only after the queue range check */
		index_batch_pos_reset(scan, &batchState->markPos);
	}

	/* release all currently loaded batches */
	while (batchState->headBatch < batchState->nextBatch)
	{
		IndexScanBatch batch = INDEX_SCAN_BATCH(scan, batchState->headBatch);

		DEBUG_LOG("freeing batch %d %p", batchState->headBatch, batch);

		index_batch_free(scan, batch);

		/* update the valid range, so that asserts / debugging works */
		batchState->headBatch++;
	}

	/* reset relevant batch state fields */
	Assert(batchState->maxBatches == INDEX_SCAN_MAX_BATCHES);
	batchState->headBatch = 0;	/* initial batch */
	batchState->nextBatch = 0;	/* initial batch is empty */

	batchState->finished = false;
	batchState->reset = false;
	batchState->currentPrefetchBlock = InvalidBlockNumber;

	AssertCheckBatches(scan);
}

static void
index_batch_kill_item(IndexScanDesc scan)
{
	IndexScanBatchPos *readPos = &scan->batchState->readPos;
	IndexScanBatchData *readBatch = INDEX_SCAN_BATCH(scan, readPos->batch);

	AssertCheckBatchPosValid(scan, readPos);

	/*
	 * XXX Maybe we can move the state that indicates if an item has been
	 * killed into IndexScanBatchData.items[] array.
	 *
	 * See:
	 * https://postgr.es/m/CAH2-WznLN7P0i2-YEnv3QGmeA5AMjdcjkraO_nz3H2Va1V1WOA@mail.gmail.com
	 */
	if (readBatch->killedItems == NULL)
		readBatch->killedItems = (int *)
			palloc(MaxTIDsPerBTreePage * sizeof(int));
	if (readBatch->numKilled < MaxTIDsPerBTreePage)
		readBatch->killedItems[readBatch->numKilled++] = readPos->index;
}

static void
index_batch_free(IndexScanDesc scan, IndexScanBatch batch)
{
	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amfreebatch);

	AssertCheckBatch(scan, batch);

	/* don't free the batch that is marked */
	if (batch == scan->batchState->markBatch)
		return;

	scan->indexRelation->rd_indam->amfreebatch(scan, batch);
}

/* */
static void
index_batch_end(IndexScanDesc scan)
{
	index_batch_reset(scan, true);

	/* bail out without batching */
	if (!scan->batchState)
		return;

	/* we can simply free batches thanks to the earlier reset */
	if (scan->batchState->batches)
		pfree(scan->batchState->batches);

	/* also walk the cache of batches, if any */
	if (scan->batchState->cache.batches)
	{
		for (int i = 0; i < scan->batchState->cache.maxbatches; i++)
		{
			if (scan->batchState->cache.batches[i] == NULL)
				continue;

			pfree(scan->batchState->cache.batches[i]);
		}

		pfree(scan->batchState->cache.batches);
	}

	pfree(scan->batchState);
}

/*
 * index_batch_alloc
 *		Allocate a batch that can fit maxitems index tuples.
 *
 * Returns a IndexScanBatch struct with capacity sufficient for maxitems
 * index tuples. It's either newly allocated or loaded from a small cache
 * maintained for individual scans.
 *
 * maxitems determines the minimum size of the batch (it may be larger)
 * want_itup determines whether the bach allocates space for currTuples
 *
 * XXX Both index_batch_alloc() calls in btree use MaxTIDsPerBTreePage,
 * which seems unfortunate - it increases the allocation sizes, even if
 * the index would be fine with smaller arrays. This means all batches
 * exceed ALLOC_CHUNK_LIMIT, forcing a separate malloc (expensive). The
 * cache helps for longer queries, not for queries that only create a
 * single batch, etc.
 */
IndexScanBatch
index_batch_alloc(IndexScanDesc scan, int maxitems, bool want_itup)
{
	IndexScanBatch batch = NULL;

	/*
	 * Try to find a sufficiently large batch in the cache.
	 *
	 * Use the first batch that can fit the requested number of items. We
	 * could be smarter and look for the smallest of such batches. But that
	 * probably won't help very much. We expect batches to be mostly uniform,
	 * with about the same size. And index_batch_release() prefers larger
	 * batches, so we should end up with mostly larger batches in the cache.
	 *
	 * XXX We can get here with batchState==NULL for bitmapscans. Could that
	 * mean bitmapscans have issues with malloc/free on batches too? But the
	 * cache can't help with that, when it's in batchState (because bitmap
	 * scans don't have that).
	 */
	if ((scan->batchState != NULL) &&
		(scan->batchState->cache.batches != NULL))
	{
		/*
		 * try to find a batch in the cache, with maxitems high enough
		 *
		 * XXX Maybe should look for a batch with lowest maxitems? That should
		 * increase probability of cache hits in the future?
		 */
		for (int i = 0; i < scan->batchState->cache.maxbatches; i++)
		{
			if ((scan->batchState->cache.batches[i] != NULL) &&
				(scan->batchState->cache.batches[i]->maxitems >= maxitems))
			{
				batch = scan->batchState->cache.batches[i];
				scan->batchState->cache.batches[i] = NULL;
				break;
			}
		}
	}

	/* found a batch in the cache? */
	if (batch)
	{
		/* for IOS, we expect to already have the currTuples */
		Assert(!(want_itup && (batch->currTuples == NULL)));

		/* XXX maybe we could keep these allocations too */
		Assert(batch->pos == NULL);
		Assert(batch->itemsvisibility == NULL);
	}
	else
	{
		batch = palloc(offsetof(IndexScanBatchData, items) +
					   sizeof(IndexScanBatchPosItem) * maxitems);

		batch->maxitems = maxitems;

		/*
		 * If we are doing an index-only scan, we need a tuple storage
		 * workspace. We allocate BLCKSZ for this, which should always give
		 * the index AM enough space to fit a full page's worth of tuples.
		 */
		batch->currTuples = NULL;
		if (want_itup)
			batch->currTuples = palloc(BLCKSZ);
	}

	/* shared initialization */
	batch->firstItem = -1;
	batch->lastItem = -1;
	batch->killedItems = NULL;
	batch->numKilled = 0;

	batch->buf = InvalidBuffer;
	batch->pos = NULL;
	batch->itemsvisibility = NULL;	/* per-batch IOS visibility */

	return batch;
}

/*
 * Unlock batch->buf.  If batch scan is dropPin, drop the pin, too.  Dropping
 * the pin prevents VACUUM from blocking on acquiring a cleanup lock.
 */
void
index_batch_unlock(Relation rel, bool dropPin, IndexScanBatch batch)
{
	if (!dropPin)
	{
		if (!RelationUsesLocalBuffers(rel))
			VALGRIND_MAKE_MEM_NOACCESS(BufferGetPage(batch->buf), BLCKSZ);

		/* Just drop the lock (not the pin) */
		LockBuffer(batch->buf, BUFFER_LOCK_UNLOCK);
		return;
	}

	/*
	 * Drop both the lock and the pin.
	 *
	 * Have to set batch->lsn so that amfreebatch has a way to detect when
	 * concurrent heap TID recycling by VACUUM might have taken place.  It'll
	 * only be safe to set any index tuple LP_DEAD bits when the page LSN
	 * hasn't advanced.
	 */
	Assert(RelationNeedsWAL(rel));
	batch->lsn = BufferGetLSNAtomic(batch->buf);
	LockBuffer(batch->buf, BUFFER_LOCK_UNLOCK);
	if (!RelationUsesLocalBuffers(rel))
		VALGRIND_MAKE_MEM_NOACCESS(BufferGetPage(batch->buf), BLCKSZ);
	ReleaseBuffer(batch->buf);
	batch->buf = InvalidBuffer; /* defensive */
}

/*
 * index_batch_release
 *		Either stash the batch info a small cache for reuse, or free it.
 */
void
index_batch_release(IndexScanDesc scan, IndexScanBatch batch)
{
	/* custom fields should have been cleaned by amfreebatch */
	Assert(batch->pos == NULL);
	Assert(batch->buf == InvalidBuffer);

	/*
	 * free killedItems / itemsvisibility
	 *
	 * XXX We could keep/reuse those too, I guess.
	 */

	if (batch->killedItems != NULL)
	{
		pfree(batch->killedItems);
		batch->killedItems = NULL;
	}

	if (batch->itemsvisibility != NULL)
	{
		pfree(batch->itemsvisibility);
		batch->itemsvisibility = NULL;
	}

	/*
	 * Try adding the batch to the small cache - find a slot that's either
	 * empty or used by a smaller batch (with smallest maxitems value), and
	 * replace that batch.
	 *
	 * XXX There may be ways to improve this. We could track the number of
	 * empty slots, and minimum maxitems value, which would allow skipping
	 * pointless searches (in cases when should just discard the batch).
	 */
	if (scan->batchState != NULL)
	{
		/* lowest maxitems we found in the cache (to replace with this batch) */
		int			maxitems = batch->maxitems;
		int			slot = scan->batchState->cache.maxbatches;

		/* first time through, initialize the cache */
		if (scan->batchState->cache.batches == NULL)
			scan->batchState->cache.batches
				= palloc0_array(IndexScanBatch,
								scan->batchState->cache.maxbatches);

		/* find am empty or sufficiently large batch */
		for (int i = 0; i < scan->batchState->cache.maxbatches; i++)
		{
			/* found empty slot, we're done */
			if (scan->batchState->cache.batches[i] == NULL)
			{
				scan->batchState->cache.batches[i] = batch;
				return;
			}

			/* found a smaller slot, remember it */
			if (scan->batchState->cache.batches[i]->maxitems < maxitems)
			{
				maxitems = scan->batchState->cache.batches[i]->maxitems;
				slot = i;
			}
		}

		/* found a slot for this batch? */
		if (maxitems < batch->maxitems)
		{
			pfree(scan->batchState->cache.batches[slot]);
			scan->batchState->cache.batches[slot] = batch;
			return;
		}
	}

	/* either no cache or no slot for this batch */
	pfree(batch);
}
