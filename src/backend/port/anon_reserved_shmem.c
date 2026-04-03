/*-------------------------------------------------------------------------
 *
 * anon_reserved_shmem.c
 *	  memfd + MAP_NORESERVE reserved VMA for resizable buffer shmem segments
 *
 * Main shared memory uses plain anonymous mmap in sysv_shmem.c (OSS-style).
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/port/anon_reserved_shmem.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "miscadmin.h"
#include "port/anon_reserved_shmem.h"
#include "port/pg_bitutils.h"
#include "portability/mem.h"
#include "storage/pg_shmem.h"
#include "utils/guc.h"

/* See sysv_shmem.c / upstream: THP pre-fault path */
#define USE_MADV_POPULATE_WRITE 1

/*
 * Wrapper around posix_fallocate() to allocate memory for a given shared memory
 * segment.
 */
static void
shmem_fallocate(int fd, const char *mapping_name, Size size, int elevel)
{
#if defined(HAVE_POSIX_FALLOCATE) && defined(__linux__)
	int			ret;

	do
	{
		ret = posix_fallocate(fd, 0, size);
	} while (ret == EINTR);

	if (ret != 0)
	{
		ereport(elevel,
				(errmsg("segment[%s]: could not allocate space for anonymous file: %s",
						mapping_name, strerror(ret)),
				 (ret == ENOMEM) ?
				 errhint("This error usually means that PostgreSQL's request "
						 "for a shared memory segment exceeded available memory, "
						 "swap space, or huge pages. To reduce the request size "
						 "(currently %zu bytes), reduce PostgreSQL's shared "
						 "memory usage, perhaps by reducing \"shared_buffers\" or "
						 "\"max_connections\".",
						 size) : 0));
	}
#endif
}

static inline void
round_off_mapping_sizes_for_hugepages(MemoryMappingSizes *mapping, int hugepagesize)
{
	if (hugepagesize == 0)
		return;

	if (mapping->shmem_req_size % hugepagesize != 0)
		mapping->shmem_req_size = add_size(mapping->shmem_req_size,
											hugepagesize - (mapping->shmem_req_size % hugepagesize));

	if (mapping->shmem_reserved % hugepagesize != 0)
		mapping->shmem_reserved = add_size(mapping->shmem_reserved,
										   hugepagesize - (mapping->shmem_reserved % hugepagesize));
}

static const char *
mmap_flags_to_string(int flags, char *buf, size_t buflen)
{
	buf[0] = '\0';

	if (flags & MAP_SHARED)
		strlcat(buf, "MAP_SHARED|", buflen);
#ifdef MAP_HASSEMAPHORE
	if (flags & MAP_HASSEMAPHORE)
		strlcat(buf, "MAP_HASSEMAPHORE|", buflen);
#endif
#ifdef MAP_NORESERVE
	if (flags & MAP_NORESERVE)
		strlcat(buf, "MAP_NORESERVE|", buflen);
#endif
#ifdef MAP_HUGETLB
	if (flags & MAP_HUGETLB)
		strlcat(buf, "MAP_HUGETLB|", buflen);
#endif

	{
		size_t		len = strlen(buf);

		if (len > 0 && buf[len - 1] == '|')
			buf[len - 1] = '\0';
	}

	if (buf[0] == '\0')
		snprintf(buf, buflen, "0x%x", flags);

	return buf;
}

static const char *
memfd_flags_to_string(int flags, char *buf, size_t buflen)
{
	buf[0] = '\0';

	if (flags == 0)
	{
		strlcat(buf, "0", buflen);
		return buf;
	}

#ifdef MFD_CLOEXEC
	if (flags & MFD_CLOEXEC)
		strlcat(buf, "MFD_CLOEXEC|", buflen);
#endif
#ifdef MFD_HUGETLB
	if (flags & MFD_HUGETLB)
		strlcat(buf, "MFD_HUGETLB|", buflen);
#endif

	{
		size_t		len = strlen(buf);

		if (len > 0 && buf[len - 1] == '|')
			buf[len - 1] = '\0';
	}

	if (buf[0] == '\0')
		snprintf(buf, buflen, "0x%x", flags);

	return buf;
}

/*
 * Create a memfd-backed segment with a reserved address range (for ftruncate
 * growth).  Not used for MAIN_SHMEM_SEGMENT.
 */
void
ReservedAnonymousSegmentCreate(int segment_id, MemoryMappingSizes *mapping)
{
	void	   *ptr = MAP_FAILED;
	int			mmap_flags = MAP_SHARED | MAP_HASSEMAPHORE | MAP_NORESERVE;
	AnonShmemSegment *anonseg = &AnonShmemSegs[segment_id];
	const char *segname = MappingName(segment_id);
	int			memfd_flags = 0;

	Assert(segment_id != MAIN_SHMEM_SEGMENT);

#ifndef MAP_HUGETLB
	Assert(huge_pages != HUGE_PAGES_ON && !huge_pages_on);
#else
	if (huge_pages_on)
	{
		Size		hugepagesize;
		int			huge_mmap_flags;
		int			huge_memfd_flags = 0;

		Assert(huge_pages == HUGE_PAGES_ON || huge_pages == HUGE_PAGES_TRY);

		GetHugePageSize(&hugepagesize, &huge_mmap_flags, &huge_memfd_flags);
		round_off_mapping_sizes_for_hugepages(mapping, hugepagesize);

		Assert(mapping->shmem_reserved >= mapping->shmem_req_size);

		mmap_flags = mmap_flags | huge_mmap_flags;
		memfd_flags = memfd_flags | huge_memfd_flags;

		{
			char		mmap_buf[128];
			char		memfd_buf[128];

			elog(LOG, "segment[%s]: huge pages are on, hugepagesize: %zu, mmap_flags: %s, memfd_flags: %s",
				 segname, hugepagesize,
				 mmap_flags_to_string(mmap_flags, mmap_buf, sizeof(mmap_buf)),
				 memfd_flags_to_string(memfd_flags, memfd_buf, sizeof(memfd_buf)));
		}
	}
#endif

	{
		char		mmap_buf[128];
		char		memfd_buf[128];

		elog(LOG, "segment[%s]: mmap_flags: %s, memfd_flags: %s",
			 segname,
			 mmap_flags_to_string(mmap_flags, mmap_buf, sizeof(mmap_buf)),
			 memfd_flags_to_string(memfd_flags, memfd_buf, sizeof(memfd_buf)));
	}

	anonseg->fd = memfd_create(segname, memfd_flags);
	if (anonseg->fd == -1)
		ereport(FATAL,
				(errmsg("segment[%s]: could not create anonymous shared memory file: %m",
						segname)));

	elog(LOG, "segment[%s]: mmap(%zu) reserved, %zu requested",
		 segname, mapping->shmem_reserved, mapping->shmem_req_size);

	ptr = mmap(NULL, mapping->shmem_reserved, PROT_NONE,
			   mmap_flags, anonseg->fd, 0);
	if (ptr == MAP_FAILED)
		ereport(FATAL,
				(errmsg("segment[%s]: could not map anonymous shared memory: %m",
						segname)));

	if (mprotect(ptr, mapping->shmem_reserved, PROT_READ | PROT_WRITE) == -1)
		ereport(FATAL,
				(errmsg("segment[%s]: could not update anonymous shared memory permissions: %m",
						segname)));

	if (ftruncate(anonseg->fd, mapping->shmem_req_size) == -1)
	{
		int			save_errno = errno;

		close(anonseg->fd);
		anonseg->fd = -1;

		errno = save_errno;
		ereport(FATAL,
				(errmsg("segment[%s]: could not truncate anonymous file to size %zu: %m",
						segname, mapping->shmem_req_size),
				 (save_errno == ENOMEM) ?
				 errhint("This error usually means that PostgreSQL's request "
						 "for a shared memory segment exceeded available memory, "
						 "swap space, or huge pages. To reduce the request size "
						 "(currently %zu bytes), reduce PostgreSQL's shared "
						 "memory usage, perhaps by reducing \"shared_buffers\" or "
						 "\"max_connections\".",
						 mapping->shmem_req_size) : 0));
	}

#if defined(MADV_HUGEPAGE) && defined(MADV_POPULATE_WRITE) && USE_MADV_POPULATE_WRITE
	if (madvise(ptr, mapping->shmem_req_size, MADV_HUGEPAGE) == -1)
	{
		elog(LOG, "segment[%s]: madvise(MADV_HUGEPAGE) failed: %m, falling back to fallocate",
			 segname);
		shmem_fallocate(anonseg->fd, segname, mapping->shmem_req_size, FATAL);
	}
	else
	{
		elog(LOG, "segment[%s]: madvise(MADV_HUGEPAGE) succeeded, pre-faulting via MADV_POPULATE_WRITE",
			 segname);
		if (madvise(ptr, mapping->shmem_req_size, MADV_POPULATE_WRITE) == -1)
			elog(FATAL,
				 "segment[%s]: madvise(MADV_POPULATE_WRITE) failed for size %zu: %m",
				 segname, mapping->shmem_req_size);
	}
#else
	shmem_fallocate(anonseg->fd, segname, mapping->shmem_req_size, FATAL);
#endif

	anonseg->addr = ptr;
	anonseg->size = mapping->shmem_reserved;
}

bool
ReservedAnonymousSegmentResize(int segment_id, MemoryMappingSizes *mapping, bool expanding)
{
	Size		hugepagesize;
	AnonShmemSegment *anonseg = &AnonShmemSegs[segment_id];
	PGShmemHeader *hdr = (PGShmemHeader *) anonseg->addr;

	Assert(!pg_atomic_unlocked_test_flag(&ShmemCtrl->resize_in_progress));

	elog(DEBUG1, "Resize shmem segment %s from %zu to %zu",
		 MappingName(segment_id), hdr->totalsize, mapping->shmem_req_size);

	if (anonseg->fd == -1)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("segment[%s]: only anonymous (mmaped) file backed segments can be resized",
						MappingName(segment_id))));

#ifndef MAP_HUGETLB
	Assert(huge_pages != HUGE_PAGES_ON && !huge_pages_on);
#else
	if (huge_pages_on)
	{
		Assert(huge_pages == HUGE_PAGES_ON || huge_pages == HUGE_PAGES_TRY);
		GetHugePageSize(&hugepagesize, NULL, NULL);
		round_off_mapping_sizes_for_hugepages(mapping, hugepagesize);
	}
#endif
	Assert(anonseg->addr);

	Assert(anonseg->size == mapping->shmem_reserved);

	if (ftruncate(anonseg->fd, mapping->shmem_req_size) == -1)
		ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("could not truncate anonymous file segment for \"%s\": %m",
						MappingName(segment_id))));

	if (expanding)
	{
#if defined(MADV_HUGEPAGE) && defined(MADV_POPULATE_WRITE) && USE_MADV_POPULATE_WRITE
		if (madvise(anonseg->addr, mapping->shmem_req_size, MADV_POPULATE_WRITE) == -1)
			ereport(ERROR,
					(errcode(ERRCODE_SYSTEM_ERROR),
					 errmsg("could not populate anonymous file segment for \"%s\": %m",
							MappingName(segment_id))));
#else
		shmem_fallocate(anonseg->fd, MappingName(segment_id), mapping->shmem_req_size, ERROR);
#endif
	}

	return true;
}
