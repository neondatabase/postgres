/*-------------------------------------------------------------------------
 *
 * hiphash.c
 *	  Concurrent hash tables backed by a single consecutive shared memory
 *	  area.
 */

#include "postgres.h"

#include "common/hashfn.h"
#include "lib/hiphash.h"
#include "storage/s_lock.h"
#include "storage/spin.h"
#include "storage/lwlock.h"

typedef pg_atomic_uint32 SlotPtr;
#define InvalidSlotPtr PG_INT32_MIN

#define HIP_CACHE_LINE_SIZE (PG_CACHE_LINE_SIZE / 2)

typedef struct HIPHashElement {
	SlotPtr bucket;
	union {
		/*
		 * When cast to int32, negative values of tag indicate this is a
		 * .free element, while positive values indicate this is .used
		 */
		pg_atomic_uint32 tag;
		struct {
			SlotPtr		next;
			SlotPtr		prev;
		} free;
		struct {
			HIPEntryIndex index;
			uint32		hash;
			SlotPtr		next;
		} used;
	};
} HIPHashElement;

#define HIPElementsPerCacheLine (HIP_CACHE_LINE_SIZE / sizeof(HIPHashElement))

StaticAssertDecl(HIP_CACHE_LINE_SIZE == HIPElementsPerCacheLine *
				 sizeof(HIPHashElement),
				 "HIPElement should be sized to prevent false cache line sharing");

typedef struct HIPPartition {
	LWLock bucketlock;
	slock_t fllock;
	int		flnmembers;
	SlotPtr flstart;
	SlotPtr flend;
} HIPPartition;

struct HIPHashHeader {
	union {
		struct {
			uint32		nelements;
			void	   *refarray;
			Size		refstride;
			Size		refcmpsz;
		};
		char _pad[HIP_CACHE_LINE_SIZE];
	};

	union {
		HIPPartition p;
		char _pad[HIP_CACHE_LINE_SIZE];
	} partitions[NUM_HIP_PARTITIONS];

	HIPHashElement elements[FLEXIBLE_ARRAY_MEMBER];
};

#define HIPSlotToFreeList(num) ((num / HIPElementsPerCacheLine) % NUM_HIP_PARTITIONS)
#define HIPElementIsFree(elem) ((((int32) pg_atomic_read_u32(&elem->tag)) < 0))

static inline int32 HIPHashToBucket(HIPHashHeader *hdr, uint32 hash);
static bool HIPElementMatches(HIPHashHeader *hdr, HIPHashElement *element,
							  uint32 searchhash, void *searchelem,
							  HIPEntryIndex index);

static HIPHashElement * HIPPopFreeListEntry(HIPHashHeader *header,
											HIPHashElement *element,
											HIPEntryIndex value,
											uint32 hash);
static void HIPAppendFreeListEntry(HIPHashHeader *header, HIPHashElement *element, int slotno);

Size
HIPGetSize(int32 nelements)
{
	Size size = offsetof(HIPHashHeader, elements);

	size += ((nelements + HIPElementsPerCacheLine - 1) / HIPElementsPerCacheLine) * HIP_CACHE_LINE_SIZE;

	return size;
}

/*
 * Initialize this HIP hash table.
 */
void
HIPInit(HIPHashHeader *header, int32 nelements, int locktranche,
		void *refarray, Size refstride, Size refcmpsz)
{
	header->nelements = nelements;
	header->refarray = refarray;
	header->refstride = refstride;
	header->refcmpsz = refcmpsz;

	for (int i = 0; i < NUM_HIP_PARTITIONS; i++)
	{
		HIPPartition *part = &header->partitions[i].p;
		memset(&header->partitions[i].pad, 0, HIP_CACHE_LINE_SIZE);
		LWLockInitialize(&part->bucketlock, locktranche);
		SpinLockInit(&part->fllock);
		pg_atomic_unlocked_write_u32(&part->flstart, InvalidSlotPtr);
		pg_atomic_unlocked_write_u32(&part->flend, InvalidSlotPtr);
	}

	for (int32 i = 0; i < nelements; i++)
	{
		HIPHashElement *elem = &header->elements[i];
		pg_atomic_unlocked_write_u32(&elem->bucket, InvalidSlotPtr);
		HIPAppendFreeListEntry(header, elem, i);
	}
}

static void
HIPAppendFreeListEntry(HIPHashHeader *header, HIPHashElement *element, int32 slotno)
{
	int		partid = HIPSlotToFreeList(slotno);
	HIPPartition *part = &header->partitions[partid].p;
	HIPHashElement *lastflelem;
	int32		last;

	SpinLockAcquire(&part->fllock);
	last = pg_atomic_read_u32(&part->flend);
	lastflelem = &header->elements[last];

	pg_atomic_unlocked_write_u32(&element->free.next, (uint32) InvalidSlotPtr);
	pg_atomic_unlocked_write_u32(&element->free.prev, last);
	part->flnmembers++;

	Assert(HIPElementIsFree(lastelem));
	pg_atomic_write_membarrier_u32(&lastflelem->free.next, -slotno);
	SpinLockRelease(&part->fllock);
}

static inline int32
HIPHashToBucket(HIPHashHeader *hdr, uint32 hash)
{
	return hash % hdr->nelements;
}

/*
 * HIPElementMatches - Does this element match the search query?
 *
 * Returns true if it matches, false otherwise.
 */
static bool
HIPElementMatches(HIPHashHeader *hdr, HIPHashElement *element,
				  uint32 searchhash, void *searchelem,
				  HIPEntryIndex index)
{
	uint32			checkidx;
	Size			offset;
	void		   *refelemptr;

	Assert(offset >= 0);

	if (element->used.hash != searchhash)
		return false;

	offset = index * hdr->refstride;
	refelemptr = (char *) (hdr->refarray) + offset;

	if (memcmp(searchelem, refelemptr, hdr->refcmpsz) != 0)
		return false;

	checkidx = pg_atomic_read_membarrier_u32(&element->tag);

	return checkidx == index;
}

HIPEntryIndex
HIPGetElementUnchecked(HIPHashHeader *header, uint32 hash,
					   void *searchelem)
{
	int32		bucketidx = HIPHashToBucket(header, hash);
	HIPHashElement *elem = &header->elements[bucketidx];
	uint32		nextptr = pg_atomic_read_u32(&elem->bucket);

	/* Slot empty? -> Element not present */
	if (nextptr == InvalidSlotPtr)
		return HIPNotPresent;

	/*
	 * We co-allocate elements with their buckets where possible.
	 * By adding this branch, we allow the CPU to speculate past
	 * this memory access; which is likely to succeed.
	 */
	if ((-nextptr) != bucketidx)
		elem = &header->elements[-nextptr];

	/* follow the chain */
	while (nextptr != InvalidSlotPtr)
	{
		HIPEntryIndex idx = pg_atomic_read_membarrier_u32(&elem->tag);
		nextptr = pg_atomic_read_u32(&elem->used.next);

		/* Element is not populated; the list was concurrently modified */
		if (idx < 0)
			return HIPTryWithLocks;

		/* If the element matches, nice! */
		if (HIPElementMatches(header, elem, hash, searchelem, idx))
		{
			return idx;
		}

		/* The element didn't match - follow the link to the next element */
		if (nextptr >= 0)
		{
			elem = &header->elements[-nextptr];
		}
		else
		{
			/*
			 * No next element. In cases of bad luck we can be victim to concurrent
			 * modification, so make the caller retry with locks.
			 */
			return HIPTryWithLocks;
		}
	}

	pg_unreachable();
}

/*
 *
 */
HIPEntryIndex
HIPGetElementChecked(HIPHashHeader *header, uint32 hash,
					   void *searchelem)
{
	int32		bucketidx = HIPHashToBucket(header, hash);
	HIPHashElement *elem = &header->elements[bucketidx];
	uint32		nextptr = pg_atomic_read_u32(&elem->bucket);

	/* Slot empty? -> Element not present */
	if (nextptr == InvalidSlotPtr)
		return HIPNotPresent;

	/*
	 * We co-allocate elements with their buckets where possible.
	 * By adding this branch, we allow the CPU to speculate past
	 * this memory access; which is likely to succeed.
	 */
	if ((-nextptr) != bucketidx)
		elem = &header->elements[-nextptr];

	/* follow the chain */
	while (nextptr != InvalidSlotPtr)
	{
		HIPEntryIndex idx = pg_atomic_read_membarrier_u32(&elem->tag);
		nextptr = pg_atomic_read_u32(&elem->used.next);

		/* Element is not populated; the list was concurrently modified */
		if (idx < 0)
			return HIPTryWithLocks;

		/* If the element matches, nice! */
		if (HIPElementMatches(header, elem, hash, searchelem, idx))
		{
			return idx;
		}

		/* The element didn't match - follow the link to the next element */
		if (nextptr >= 0)
		{
			elem = &header->elements[-nextptr];
		}
		else
		{
			/*
			 * No next element. In cases of bad luck we can be victim to concurrent
			 * modification, so make the caller retry with locks.
			 */
			return HIPTryWithLocks;
		}
	}

	pg_unreachable();
}

