/*-------------------------------------------------------------------------
 *
 * execPrefetch.c
 *	  routines for prefetching heap pages for index scans.
 *
 * The IndexPrefetch node represents an "index prefetcher" which reads TIDs
 * from an index scan, and prefetches the referenced heap pages. The basic
 * API consists of these methods:
 *
 *	IndexPrefetchAlloc - allocate IndexPrefetch with custom callbacks
 *	IndexPrefetchNext - read next TID from the index scan, do prefetches
 *	IndexPrefetchReset - reset state of the prefetcher (for rescans)
 *	IndexPrefetchEnd - release resources held by the prefetcher
 *
 * When allocating a prefetcher, the caller can supply two custom callbacks:
 *
 *	IndexPrefetchNextCB - reads the next TID from the index scan (required)
 *	IndexPrefetchCleanupCB - release private prefetch data (optional)
 *
 * These callbacks allow customizing the behavior for different types of
 * index scans - for exampel index-only scans may inspect visibility map,
 * and adjust prefetches based on that.
 *
 *
 * TID queue
 * ---------
 * The prefetcher maintains a simple queue of TIDs fetched from the index.
 * The length of the queue (number of TIDs) is determined by the prefetch
 * target, i.e. effective_io_concurrency. Adding entries to the queue is
 * the responsibility of IndexPrefetchFillQueue(), depending on the state
 * of the scan etc. It also prefetches the pages, if appropriate.
 *
 * Note: This prefetching applies only to heap pages from the indexed
 * relation, not the internal index pages.
 *
 *
 * pattern detection
 * -----------------
 * For certain access patterns, prefetching is inefficient. In particular,
 * this applies to sequential access (where kernel read-ahead works fine)
 * and for pages that are already in memory (prefetched recently). The
 * prefetcher attempts to identify these two cases - sequential patterns
 * are detected by IndexPrefetchBlockIsSequential, usign a tiny queue of
 * recently prefetched blocks. Recently prefetched blocks are tracked in
 * a "partitioned" LRU cache.
 *
 * Note: These are inherently best-effort heuristics. We don't know what
 * the kernel algorithm/configuration is, or more precisely what already
 * is in page cache.
 *
 *
 * cache of recent prefetches
 * --------------------------
 * Cache of recently prefetched blocks, organized as a hash table of LRU
 * LRU caches. Doesn't need to be perfectly accurate, but we aim to make
 * false positives/negatives reasonably low. For more details see the
 * comments at IndexPrefetchIsCached.
 *
 *
 * prefetch request number
 * -----------------------
 * Prefetching works with the concept of "age" (e.g. "recently prefetched
 * pages"). This relies on a simple prefetch counter, incremented every
 * time a prefetch is issued. This is not exactly the same thing as time,
 * as there may be arbitrary delays, it's good enough for this purpose.
 *
 *
 * auto-tuning / self-adjustment
 * -----------------------------
 *
 * XXX Some ideas how to auto-tune the prefetching, so that unnecessary
 * prefetching does not cause significant regressions (e.g. for nestloop
 * with inner index scan). We could track number of rescans and number of
 * items (TIDs) actually returned from the scan. Then we could calculate
 * rows / rescan and adjust the prefetch target accordingly. That'd help
 * with cases when a scan matches only very few rows, far less than the
 * prefetchTarget, because the unnecessary prefetches are wasted I/O.
 * Imagine a LIMIT on top of index scan, or something like that.
 *
 * XXX Could we tune the cache size based on execution statistics? We have
 * a cache of limited size (PREFETCH_CACHE_SIZE = 1024 by default), but
 * how do we know it's the right size? Ideally, we'd have a cache large
 * enough to track actually cached blocks. If the OS caches 10240 pages,
 * then we may do 90% of prefetch requests unnecessarily. Or maybe there's
 * a lot of contention, blocks are evicted quickly, and 90% of the blocks
 * in the cache are not actually cached anymore? But we do have a concept
 * of sequential request ID (PrefetchCacheEntry->request), which gives us
 * information about "age" of the last prefetch. Now it's used only when
 * evicting entries (to keep the more recent one), but maybe we could also
 * use it when deciding if the page is cached. Right now any block that's
 * in the cache is considered cached and not prefetched, but maybe we could
 * have "max age", and tune it based on feedback from reading the blocks
 * later. For example, if we find the block in cache and decide not to
 * prefetch it, but then later find we have to do I/O, it means our cache
 * is too large. And we could "reduce" the maximum age (measured from the
 * current prefetchRequest value), so that only more recent blocks would
 * be considered cached. Not sure about the opposite direction, where we
 * decide to prefetch a block - AFAIK we don't have a way to determine if
 * I/O was needed or not in this case (so we can't increase the max age).
 * But maybe we could di that somehow speculatively, i.e. increase the
 * value once in a while, and see what happens.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execPrefetch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"
#include "utils/spccache.h"


/*
 * An entry representing a recently prefetched block. For each block we know
 * the request number, assigned sequentially, allowing us to decide how old
 * the request is.
 *
 * XXX Is it enough to keep the request as uint32? This way we can prefetch
 * 32TB of data, and this allows us to fit the whole entry into 64B, i.e.
 * one cacheline. Which seems like a good thing.
 *
 * XXX If we're extra careful / paranoid about uint32, we could reset the
 * cache once the request wraps around.
 */
typedef struct IndexPrefetchCacheEntry
{
	BlockNumber block;
	uint32		request;
} IndexPrefetchCacheEntry;

/*
 * Size of the cache of recently prefetched blocks - shouldn't be too small or
 * too large. 1024 entries seems about right, it covers ~8MB of data. This is
 * rather arbitrary - there's no formula that'd tell us what the optimal size
 * is, and we can't even tune it based on runtime (as it depends on what the
 * other backends do too).
 *
 * A value too small would mean we may issue unnecessary prefetches for pages
 * that have already been prefetched recently (and are still in page cache),
 * incurring costs for unnecessary fadvise() calls.
 *
 * A value too large would mean we do not issue prefetches for pages that have
 * already been evicted from memory (both shared buffers and page cache).
 *
 * Note however that PrefetchBuffer() checks shared buffers before doing the
 * fadvise call, which somewhat limits the risk of a small cache - the page
 * would have to get evicted from shared buffers not yet from page cache.
 * Also, the cost of not issuing a fadvise call (and doing synchronous I/O
 * later) is much higher than the unnecessary fadvise call. For these reasons
 * it's better to keep the cache fairly small.
 *
 * The cache is structured as an array of small LRU caches - you may also
 * imagine it as a hash table of LRU caches. To remember a prefetched block,
 * the block number mapped to a LRU using by hashing. And then in each LRU
 * we organize the entries by age (per request number) - in particular, the
 * age determines which entry gets evicted after the LRU gets full.
 *
 * The LRU needs to be small enough to be searched linearly. At the same
 * time it needs to be sufficiently large to handle collisions when several
 * hot blocks get mapped to the same LRU. For example, if the LRU was only
 * a single entry, and there were two hot blocks mapped to it, that would
 * often give incorrect answer.
 *
 * The 8 entries per LRU seems about right - it's small enough for linear
 * search to work well, but large enough to be adaptive. It's not very
 * likely for 9+ busy blocks (out of 1000 recent requests) to map to the
 * same LRU. Assuming reasonable hash function.
 *
 * XXX Maybe we could consider effective_cache_size when sizing the cache?
 * Not to size the cache for that, ofc, but maybe as a guidance of how many
 * heap pages it might keep. Maybe just a fraction fraction of the value,
 * say Max(8MB, effective_cache_size / max_connections) or something.
 */
#define		PREFETCH_LRU_SIZE		8	/* slots in one LRU */
#define		PREFETCH_LRU_COUNT		128 /* number of LRUs */
#define		PREFETCH_CACHE_SIZE		(PREFETCH_LRU_SIZE * PREFETCH_LRU_COUNT)

/*
 * Size of small sequential queue of most recently prefetched blocks, used
 * to check if the block is exactly the same as the immediately preceding
 * one (in which case prefetching is not needed), and if the blocks are a
 * sequential pattern (in which case the kernel read-ahead is likely going
 * to be more efficient, and we don't want to interfere with it).
 */
#define		PREFETCH_QUEUE_HISTORY	8

/*
 * An index prefetcher, which maintains a queue of TIDs from an index, and
 * issues prefetches (if deemed beneficial and supported by the OS).
 */
typedef struct IndexPrefetch
{
	int			prefetchTarget; /* how far we should be prefetching */
	int			prefetchMaxTarget;	/* maximum prefetching distance */
	int			prefetchReset;	/* reset to this distance on rescan */
	bool		prefetchDone;	/* did we get all TIDs from the index? */
	bool        skipSequential; /* do nt prefetch for sequential access pattern */

	/* runtime statistics, displayed in EXPLAIN etc. */
	uint32		countAll;		/* all prefetch requests (including skipped) */
	uint32		countPrefetch;	/* PrefetchBuffer calls */
	uint32		countSkipSequential;	/* skipped as sequential pattern */
	uint32		countSkipCached;	/* skipped as recently prefetched */

	/*
	 * Queue of TIDs to prefetch.
	 *
	 * XXX Sizing for MAX_IO_CONCURRENCY may be overkill, but it seems simpler
	 * than dynamically adjusting for custom values. However, 1000 entries
	 * means ~16kB, which means an oversized chunk, and thus always a malloc()
	 * call. However, we already have the prefetchCache, which is also large
	 * enough to cause this :-(
	 *
	 * XXX However what about the case without prefetching? In that case it
	 * would be nice to lower the malloc overhead, maybe?
	 */
	IndexPrefetchEntry queueItems[MAX_IO_CONCURRENCY];
	uint32		queueIndex;		/* next TID to prefetch */
	uint32		queueStart;		/* first valid TID in queue */
	uint32		queueEnd;		/* first invalid (empty) TID in queue */

	/*
	 * A couple of last prefetched blocks, used to check for certain access
	 * pattern and skip prefetching - e.g. for sequential access).
	 *
	 * XXX Separate from the main queue, because we only want to compare the
	 * block numbers, not the whole TID. In sequential access it's likely we
	 * read many items from each page, and we don't want to check many items
	 * (as that is much more expensive).
	 */
	BlockNumber blockItems[PREFETCH_QUEUE_HISTORY];
	uint32		blockIndex;		/* index in the block (points to the first
								 * empty entry) */

	/*
	 * Cache of recently prefetched blocks, organized as a hash table of small
	 * LRU caches.
	 */
	uint32		prefetchRequest;
	IndexPrefetchCacheEntry prefetchCache[PREFETCH_CACHE_SIZE];


	/*
	 * Callback to customize the prefetch (decide which block need to be
	 * prefetched, etc.)
	 */
	IndexPrefetchNextCB next_cb;	/* read next TID */
	IndexPrefetchCleanupCB cleanup_cb;	/* cleanup data */

	/*
	 * If a callback is specified, it may store global state (for all TIDs).
	 * For example VM buffer may be kept during IOS. This is similar to the
	 * data field in IndexPrefetchEntry, but that's per-TID.
	 */
	void	   *data;
} IndexPrefetch;

/* small sequential queue of recent blocks */
#define PREFETCH_BLOCK_INDEX(v)	((v) % PREFETCH_QUEUE_HISTORY)

/* access to the main hybrid cache (hash of LRUs) */
#define PREFETCH_LRU_ENTRY(p, lru, idx)	\
	&((p)->prefetchCache[(lru) * PREFETCH_LRU_SIZE + (idx)])

/* access to queue of TIDs (up to MAX_IO_CONCURRENCY elements) */
#define PREFETCH_QUEUE_INDEX(a)	((a) % (MAX_IO_CONCURRENCY))
#define PREFETCH_QUEUE_EMPTY(p)	((p)->queueEnd == (p)->queueIndex)

/*
 * macros to deal with prefetcher state
 *
 * FIXME may need rethinking, easy to confuse PREFETCH_ENABLED/PREFETCH_ACTIVE
 */
#define PREFETCH_ENABLED(p)		((p) && ((p)->prefetchMaxTarget > 0))
#define PREFETCH_QUEUE_FULL(p)		((p)->queueEnd - (p)->queueIndex == (p)->prefetchTarget)
#define PREFETCH_DONE(p)		((p) && ((p)->prefetchDone && PREFETCH_QUEUE_EMPTY(p)))
#define PREFETCH_ACTIVE(p)		(PREFETCH_ENABLED(p) && !(p)->prefetchDone)


/*
 * IndexPrefetchBlockIsSequential
 *		Track the block number and check if the I/O pattern is sequential,
 *		or if the block is the same as the immediately preceding one.
 *
 * This also updates the small sequential cache of blocks.
 *
 * The prefetching overhead is fairly low, but for some access patterns the
 * benefits are small compared to the extra overhead, or the prefetching may
 * even be harmful. In particular, for sequential access the read-ahead
 * performed by the OS is very effective/efficient and our prefetching may
 * be pointless or (worse) even interfere with it.
 *
 * This identifies simple sequential patterns, using a tiny queue of recently
 * prefetched block numbers (PREFETCH_QUEUE_HISTORY blocks). It also checks
 * if the block is exactly the same as any of the blocks in the queue (the
 * main cache has block too, but checking the tiny cache is likely cheaper).
 *
 * The the main prefetch queue is not really useful for this, as it stores
 * full TIDs, but while we only care about block numbers. Consider a nicely
 * clustered table, with a perfectly sequential pattern when accessed through
 * an index. Each heap page may have dozens of TIDs, filling the prefetch
 * queue. But we need to compare block numbers - those may either not be
 * in the queue anymore, or we have to walk many TIDs (making it expensive,
 * and we're in hot path).
 *
 * So a tiny queue of just block numbers seems like a better option.
 *
 * Returns true if the block is in a sequential pattern or was prefetched
 * recently (and so should not be prefetched this time), or false (in which
 * case it should be prefetched).
 */
static bool
IndexPrefetchBlockIsSequential(IndexPrefetch *prefetch, BlockNumber block)
{
	int			idx;

	/*
	 * If the block queue is empty, just store the block and we're done (it's
	 * neither a sequential pattern, neither recently prefetched block).
	 */
	if (prefetch->blockIndex == 0)
	{
		prefetch->blockItems[PREFETCH_BLOCK_INDEX(prefetch->blockIndex)] = block;
		prefetch->blockIndex++;
		return false;
	}

	/*
	 * Check if it's the same as the immediately preceding block. We don't
	 * want to prefetch the same block over and over (which would happen for
	 * well correlated indexes).
	 *
	 * In principle we could rely on IndexPrefetchIsCached doing this using
	 * the full cache, but this check is much cheaper and we need to look at
	 * the preceding block anyway, so we just do it.
	 *
	 * Notice we haven't added the block to the block queue yet, and there
	 * is a preceding block (i.e. blockIndex-1 is valid).
	 */
	if (prefetch->blockItems[PREFETCH_BLOCK_INDEX(prefetch->blockIndex - 1)] == block)
		return true;

	/*
	 * Add the block number to the small queue.
	 *
	 * Done before checking if the pattern is sequential, because we want to
	 * know about the block later, even if we end up skipping the prefetch.
	 * Otherwise we'd not be able to detect longer sequential pattens - we'd
	 * skip one block and then fail to skip the next couple blocks even in a
	 * perfectly sequential pattern. And this ocillation might even prevent
	 * the OS read-ahead from kicking in.
	 */
	prefetch->blockItems[PREFETCH_BLOCK_INDEX(prefetch->blockIndex)] = block;
	prefetch->blockIndex++;

	/*
	 * Are there enough requests to confirm a sequential pattern? We only
	 * consider something to be sequential after finding a sequence of
	 * PREFETCH_QUEUE_HISTORY blocks.
	 */
	if (prefetch->blockIndex < PREFETCH_QUEUE_HISTORY)
		return false;

	/*
	 * Check if the last couple blocks are in a sequential pattern. We look
	 * for a sequential pattern of PREFETCH_QUEUE_HISTORY (8 by default), so
	 * we look for patterns of 8 pages (64kB) including the new block.
	 *
	 * XXX Could it be harmful that we read the queue backwards? Maybe memory
	 * prefetching works better for the forward direction?
	 */
	for (int i = 1; i < PREFETCH_QUEUE_HISTORY; i++)
	{
		/*
		 * Calculate index of the earlier block (we need to do -1 as we
		 * already incremented the index after adding the new block to the
		 * queue). So (blockIndex-1) is the new block.
		 */
		idx = PREFETCH_BLOCK_INDEX(prefetch->blockIndex - i - 1);

		/*
		 * For a sequential pattern, blocks "k" step ago needs to have block
		 * number by "k" smaller compared to the current block.
		 */
		if (prefetch->blockItems[idx] != (block - i))
			return false;

		/* Don't prefetch if the block happens to be the same. */
		if (prefetch->blockItems[idx] == block)
			return false;
	}

	/* not sequential, not recently prefetched */
	return true;
}

/*
 * IndexPrefetchIsCached
 *		Check if the block was prefetched recently, and update the cache.
 *
 * We don't want to prefetch blocks that we already prefetched recently. It's
 * cheap but not free, and the overhead may be quite significant.
 *
 * We want to remember which blocks were prefetched recently, so that we can
 * skip repeated prefetches. We also need to eventually forget these blocks
 * as they may get evicted from memory (particularly page cache, which is
 * outside our control).
 *
 * A simple queue is not a viable option - it would allow expiring requests
 * based on age, but it's very expensive to check (as it requires linear
 * search, and we need fairly large number of entries). Hash table does not
 * work because it does not allow expiring entries by age.
 *
 * The cache does not need to be perfect - false positives/negatives are
 * both acceptable, as long as the rate is reasonably low.
 *
 * We use a hybrid cache that is organized as many small LRU caches. Each
 * block is mapped to a particular LRU by hashing (so it's a bit like a
 * hash table of LRUs). The LRU caches are tiny (e.g. 8 entries), and the
 * expiration happens at the level of a single LRU (using age determined
 * by sequential request number).
 *
 * This allows quick searches and expiration, with false negatives (when a
 * particular LRU has too many collisions with hot blocks, we may end up
 * evicting entries that are more recent than some other LRU).
 *
 * For example, imagine 128 LRU caches, each with 8 entries - that's 1024
 * request in total (these are the default parameters.) representing about
 * 8MB of data.
 *
 * If we want to check if a block was recently prefetched, we calculate
 * (hash(blkno) % 128) and search only LRU at this index, using a linear
 * search. If we want to add the block to the cache, we find either an
 * empty slot or the "oldest" entry in the LRU, and store the block in it.
 * If the block is already in the LRU, we only update the request number.
 *
 * The request age is determined using a prefetch counter, incremented every
 * time we end up prefetching a block. The counter is uint32, so it should
 * not wrap (we'd have to prefetch 32TB).
 *
 * If the request number is not less than PREFETCH_CACHE_SIZE ago, it's
 * considered "recently prefetched". That is, the maximum age is the same
 * as the total capacity of the cache.
 *
 * Returns true if the block was recently prefetched (and thus we don't
 * need to prefetch it again), or false (should do a prefetch).
 *
 * XXX It's a bit confusing these return values are inverse compared to
 * what IndexPrefetchBlockIsSequential does.
 *
 * XXX Should we increase the prefetch counter even if we determine the
 * entry was recently prefetched? Then we might skip some request numbers
 * (there's be no entry with them).
 */
static bool
IndexPrefetchIsCached(IndexPrefetch *prefetch, BlockNumber block)
{
	IndexPrefetchCacheEntry *entry;

	/* map the block number the the LRU */
	int			lru;

	/* age/index of the oldest entry in the LRU, to maybe use */
	uint64		oldestRequest = PG_UINT64_MAX;
	int			oldestIndex = -1;

	/*
	 * First add the block to the (tiny) queue and see if it's part of a
	 * sequential pattern. In this case we just ignore the block and don't
	 * prefetch it - we expect OS read-ahead to do a better job.
	 *
	 * XXX Maybe we should still add the block to the main cache, in case we
	 * happen to access it later. That might help if we happen to scan a lot
	 * of the table sequentially, and then randomly. Not sure that's very
	 * likely with index access, though.
	 */
	if (prefetch->skipSequential && IndexPrefetchBlockIsSequential(prefetch, block))
	{
		prefetch->countSkipSequential++;
		return true;
	}

	/* Which LRU does this block belong to? */
	lru = hash_uint32(block) % PREFETCH_LRU_COUNT;

	/*
	 * Did we prefetch this block recently? Scan the LRU linearly, and while
	 * doing that, track the oldest (or empty) entry, so that we know where to
	 * put the block if we don't find a match.
	 */
	for (int i = 0; i < PREFETCH_LRU_SIZE; i++)
	{
		entry = PREFETCH_LRU_ENTRY(prefetch, lru, i);

		/*
		 * Is this the oldest prefetch request in this LRU?
		 *
		 * Notice that request is uint32, so an empty entry (with request=0)
		 * is automatically oldest one.
		 */
		if (entry->request < oldestRequest)
		{
			oldestRequest = entry->request;
			oldestIndex = i;
		}

		/* Skip unused entries. */
		if (entry->request == 0)
			continue;

		/* Is this entry for the same block as the current request? */
		if (entry->block == block)
		{
			bool		prefetched;

			/*
			 * Is the old request sufficiently recent? If yes, we treat the
			 * block as already prefetched. We need to check before updating
			 * the prefetch request.
			 *
			 * XXX We do add the cache size to the request in order not to
			 * have issues with underflows.
			 */
			prefetched = ((entry->request + PREFETCH_CACHE_SIZE) >= prefetch->prefetchRequest);

			prefetch->countSkipCached += (prefetched) ? 1 : 0;

			/* Update the request number. */
			entry->request = ++prefetch->prefetchRequest;

			return prefetched;
		}
	}

	/*
	 * We didn't find the block in the LRU, so store it the "oldest" prefetch
	 * request in this LRU (which might be an empty entry).
	 */
	Assert((oldestIndex >= 0) && (oldestIndex < PREFETCH_LRU_SIZE));

	entry = PREFETCH_LRU_ENTRY(prefetch, lru, oldestIndex);

	entry->block = block;
	entry->request = ++prefetch->prefetchRequest;

	/* not in the prefetch cache */
	return false;
}

/*
 * IndexPrefetchHeapPage
 *		Prefetch a heap page for the TID, unless it's sequential or was
 *		recently prefetched.
 */
static void
IndexPrefetchHeapPage(IndexScanDesc scan, IndexPrefetch *prefetch, IndexPrefetchEntry *entry)
{
	BlockNumber block = ItemPointerGetBlockNumber(&entry->tid);

	prefetch->countAll++;

	/*
	 * Do not prefetch the same block over and over again, if it's probably
	 * still in memory (page cache).
	 *
	 * This happens e.g. for clustered or naturally correlated indexes (fkey
	 * to a sequence ID). It's not expensive (the block is in page cache
	 * already, so no I/O), but it's not free either.
	 *
	 * If we make a mistake and prefetch a buffer that's still in our shared
	 * buffers, PrefetchBuffer will take care of that. If it's in page cache,
	 * we'll issue an unnecessary prefetch. There's not much we can do about
	 * that, unfortunately.
	 *
	 * XXX Maybe we could check PrefetchBufferResult and adjust countPrefetch
	 * based on that?
	 */
	if (IndexPrefetchIsCached(prefetch, block))
		return;

	prefetch->countPrefetch++;

	PrefetchBuffer(scan->heapRelation, MAIN_FORKNUM, block);
	pgBufferUsage.prefetch.tids += 1;
}

/*
 * IndexPrefetchFillQueue
 *		Fill the prefetch queue and issue necessary prefetch requests.
 *
 * If the prefetching is still active (enabled, not reached end of scan), read
 * TIDs into the queue until we hit the current target.
 *
 * This also ramps-up the prefetch target from 0 to prefetch_max, determined
 * when allocating the prefetcher.
 */
static void
IndexPrefetchFillQueue(IndexScanDesc scan, IndexPrefetch *prefetch, ScanDirection direction)
{
	/* When inactive (not enabled or end of scan reached), we're done. */
	if (!PREFETCH_ACTIVE(prefetch))
		return;

	/*
	 * Ramp up the prefetch distance incrementally.
	 *
	 * Intentionally done as first, before reading the TIDs into the queue, so
	 * that there's always at least one item. Otherwise we might get into a
	 * situation where we start with target=0 and no TIDs loaded.
	 */
	prefetch->prefetchTarget = Min(prefetch->prefetchTarget + 1,
								   prefetch->prefetchMaxTarget);

	/*
	 * Read TIDs from the index until the queue is full (with respect to the
	 * current prefetch target).
	 */
	while (!PREFETCH_QUEUE_FULL(prefetch))
	{
		IndexPrefetchEntry *entry
		= prefetch->next_cb(scan, direction, prefetch->data);

		/* no more entries in this index scan */
		if (entry == NULL)
		{
			prefetch->prefetchDone = true;
			return;
		}

		Assert(ItemPointerEquals(&entry->tid, &scan->xs_heaptid));

		/* store the entry and then maybe issue the prefetch request */
		prefetch->queueItems[PREFETCH_QUEUE_INDEX(prefetch->queueEnd++)] = *entry;

		/* issue the prefetch request? */
		if (entry->prefetch)
			IndexPrefetchHeapPage(scan, prefetch, entry);
	}
}

/*
 * IndexPrefetchNextEntry
 *		Get the next entry from the prefetch queue (or from the index directly).
 *
 * If prefetching is enabled, get next entry from the prefetch queue (unless
 * queue is empty). With prefetching disabled, read an entry directly from the
 * index scan.
 *
 * XXX not sure this correctly handles xs_heap_continue - see index_getnext_slot,
 * maybe nodeIndexscan needs to do something more to handle this? Although, that
 * should be in the indexscan next_cb callback, probably.
 *
 * XXX If xs_heap_continue=true, we need to return the last TID.
 */
static IndexPrefetchEntry *
IndexPrefetchNextEntry(IndexScanDesc scan, IndexPrefetch *prefetch, ScanDirection direction)
{
	IndexPrefetchEntry *entry = NULL;

	/*
	 * With prefetching enabled (even if we already finished reading all TIDs
	 * from the index scan), we need to return a TID from the queue.
	 * Otherwise, we just get the next TID from the scan directly.
	 */
	if (PREFETCH_ENABLED(prefetch))
	{
		/* Did we reach the end of the scan and the queue is empty? */
		if (PREFETCH_DONE(prefetch))
			return NULL;

		entry = palloc(sizeof(IndexPrefetchEntry));

		entry->tid = prefetch->queueItems[PREFETCH_QUEUE_INDEX(prefetch->queueIndex)].tid;
		entry->data = prefetch->queueItems[PREFETCH_QUEUE_INDEX(prefetch->queueIndex)].data;

		prefetch->queueIndex++;

		scan->xs_heaptid = entry->tid;
	}
	else						/* not prefetching, just do the regular work  */
	{
		ItemPointer tid;

		/* Time to fetch the next TID from the index */
		tid = index_getnext_tid(scan, direction);

		/* If we're out of index entries, we're done */
		if (tid == NULL)
			return NULL;

		Assert(ItemPointerEquals(tid, &scan->xs_heaptid));

		entry = palloc(sizeof(IndexPrefetchEntry));

		entry->tid = scan->xs_heaptid;
		entry->data = NULL;
	}

	return entry;
}

/*
 * IndexPrefetchComputeTarget
 *		Calculate prefetch distance for the given heap relation.
 *
 * We disable prefetching when using direct I/O (when there's no page cache
 * to prefetch into), and scans where the prefetch distance may change (e.g.
 * for scrollable cursors).
 *
 * In regular cases we look at effective_io_concurrency for the tablepace
 * (of the heap, not the index), and cap it with plan_rows.
 *
 * XXX We cap the target to plan_rows, becausse it's pointless to prefetch
 * more than we expect to use.
 *
 * XXX Maybe we should reduce the value with parallel workers?
 */
int
IndexPrefetchComputeTarget(Relation heapRel, double plan_rows, bool prefetch)
{
	/*
	 * No prefetching for direct I/O.
	 *
	 * XXX Shouldn't we do prefetching even for direct I/O? We would only
	 * pretend doing it now, ofc, because we'd not do posix_fadvise(), but
	 * once the code starts loading into shared buffers, that'd work.
	 */
	if ((io_direct_flags & IO_DIRECT_DATA) != 0)
		return 0;

	/* disable prefetching (for cursors etc.) */
	if (!prefetch)
		return 0;

	/* regular case, look at tablespace effective_io_concurrency */
	return Min(get_tablespace_io_concurrency(heapRel->rd_rel->reltablespace),
			   plan_rows);
}

/*
 * IndexPrefetchAlloc
 *		Allocate the index prefetcher.
 *
 * The behavior is customized by two callbacks - next_cb, which generates TID
 * values to put into the prefetch queue, and (optional) cleanup_cb which
 * releases resources at the end.
 *
 * prefetch_max specifies the maximum prefetch distance, i.e. how many TIDs
 * ahead to keep in the prefetch queue. prefetch_max=0 means prefetching is
 * disabled.
 *
 * data may point to a custom data, associated with the prefetcher.
 */
IndexPrefetch *
IndexPrefetchAlloc(IndexPrefetchNextCB next_cb, IndexPrefetchCleanupCB cleanup_cb,
				   int prefetch_max, bool skip_sequential, void *data)
{
	IndexPrefetch *prefetch = palloc0(sizeof(IndexPrefetch));

	/* the next_cb callback is required */
	Assert(next_cb);

	/* valid prefetch distance */
	Assert((prefetch_max >= 0) && (prefetch_max <= MAX_IO_CONCURRENCY));

	prefetch->queueIndex = 0;
	prefetch->queueStart = 0;
	prefetch->queueEnd = 0;

	prefetch->prefetchTarget = 0;
	prefetch->prefetchMaxTarget = prefetch_max;
	prefetch->skipSequential = skip_sequential;
	/*
	 * Customize the prefetch to also check visibility map and keep the result
	 * so that IOS does not need to repeat it.
	 */
	prefetch->next_cb = next_cb;
	prefetch->cleanup_cb = cleanup_cb;
	prefetch->data = data;

	return prefetch;
}

/*
 * IndexPrefetchNext
 *		Read the next entry from the prefetch queue.
 *
 * Returns the next TID in the prefetch queue (which might have been prefetched
 * sometime in the past). If needed, it adds more entries to the queue and does
 * the prefetching for them.
 *
 * Returns IndexPrefetchEntry with the TID and optional data associated with
 * the TID in the next_cb callback.
 */
IndexPrefetchEntry *
IndexPrefetchNext(IndexScanDesc scan, IndexPrefetch *prefetch, ScanDirection direction)
{
	/* Do prefetching (if requested/enabled). */
	IndexPrefetchFillQueue(scan, prefetch, direction);

	/* Read the TID from the queue (or directly from the index). */
	return IndexPrefetchNextEntry(scan, prefetch, direction);
}

/*
 * IndexPrefetchReset
 *		Reset the prefetch TID, restart the prefetching.
 *
 * Useful during rescans etc. This also resets the prefetch target, so that
 * each rescan does the initial prefetch ramp-up from target=0 to maximum
 * prefetch distance.
 */
void
IndexPrefetchReset(IndexScanDesc scan, IndexPrefetch *state)
{
	if (!state)
		return;

	state->queueIndex = 0;
	state->queueStart = 0;
	state->queueEnd = 0;

	state->prefetchDone = false;
	state->prefetchTarget = 0;
}

/*
 * IndexPrefetchStats
 *		Log basic runtime debug stats of the prefetcher.
 *
 * FIXME Should be only in debug builds, or something like that.
 */
void
IndexPrefetchStats(IndexScanDesc scan, IndexPrefetch *state)
{
	if (!state)
		return;

	elog(LOG, "index prefetch stats: requests %u prefetches %u (%f) skip cached %u sequential %u",
		 state->countAll,
		 state->countPrefetch,
		 state->countPrefetch * 100.0 / state->countAll,
		 state->countSkipCached,
		 state->countSkipSequential);
}

/*
 * IndexPrefetchEnd
 *		Release resources associated with the prefetcher.
 *
 * This is primarily about the private data the caller might have allocated
 * in the next_cb, and stored in the data field. We don't know what the
 * data might contain (e.g. buffers etc.), requiring additional cleanup, so
 * we call another custom callback.
 *
 * Needs to be called at the end of the executor node.
 *
 * XXX Maybe if there's no callback, we should just pfree the data? Does
 * not seem very useful, though.
 */
void
IndexPrefetchEnd(IndexScanDesc scan, IndexPrefetch *state)
{
	if (!state)
		return;

	if (!state->cleanup_cb)
		return;

	state->cleanup_cb(scan, state->data);
}
