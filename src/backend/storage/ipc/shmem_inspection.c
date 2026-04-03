/*-------------------------------------------------------------------------
 *
 * shmem_inspection.c
 *	  SQL-callable introspection for shared memory (SRFs and helpers).
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/shmem_inspection.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port/pg_numa.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "storage/shmem.h"
#include "storage/shmem_internal.h"
#include "utils/builtins.h"

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
