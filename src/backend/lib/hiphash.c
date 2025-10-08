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
				 "Make sure we can properly fit this on cache lines");

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
			int			nelements;
			void	   *elemsarray;
			Size		stride;
			Size		cmpsize;
		};
		char pad[HIP_CACHE_LINE_SIZE];
	};

	union {
		HIPPartition p;
		char pad[HIP_CACHE_LINE_SIZE];
	} partitions[NUM_HIP_PARTITIONS];

	HIPHashElement elements[FLEXIBLE_ARRAY_MEMBER];
};

#define HIPSlotToFreeList(num) ((num / HIPElementsPerCacheLine) % NUM_HIP_PARTITIONS)
#define HIPElementIsFree(elem) ((((int32) pg_atomic_read_u32(&elem->tag)) < 0)

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
HIPInit(HIPHashHeader *header, int32 nelements, int locktranche)
{
	header->nelements = nelements;

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
