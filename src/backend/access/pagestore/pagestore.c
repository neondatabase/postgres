/*
 *
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/xlogutils.h"
#include "access/pagestore_xlog.h"
#include "storage/bufmgr.h"
#include "miscadmin.h"
#include "utils/memutils.h"

typedef struct EmptyPageMarker
{
	RelFileNode rnode;
	ForkNumber forknum;
	BlockNumber blocknum;
	PageStamp stamp;
} EmptyPageMarker;


static void pagestore_extend_relation(XLogReaderState *record);
static void mark_empty_page(Page page, RelFileNode rnode, ForkNumber forknum, BlockNumber blknum);

XLogRecPtr
log_relsize(RelFileNode rnode, ForkNumber forknum, BlockNumber blkno)
{
	XLogRecPtr	lsn;

	XLogBeginInsert();
	XLogRegisterBlock(0, &rnode, forknum, blkno, NULL /* page */, REGBUF_WILL_INIT);
	lsn = XLogInsert(RM_PAGESTORE_ID, XLOG_PAGESTORE_EXTEND_RELATION);

	return lsn;
}

void
pagestore_redo(XLogReaderState *record)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
	case XLOG_PAGESTORE_EXTEND_RELATION:
		pagestore_extend_relation(record);
		break;

	default:
		break;
	}
}

static void
pagestore_extend_relation(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blocknum;
	Page		page;
	Buffer buf;

	/* Should we support multi-page extend here? */
	Assert(record->max_block_id == 1);

	buf = XLogInitBufferForRedo(record, 0);
	page = BufferGetPage(buf);

	XLogRecGetBlockTag(record, 0, &rnode, &forknum, &blocknum);
	mark_empty_page(page, rnode, forknum, blocknum);

	/* Even though the page is new still set an LSN */
	PageSetLSN(page, lsn);
	MarkBufferDirty(buf);
	UnlockReleaseBuffer(buf);
}

bool
validate_marked_page(Page page, RelFileNode rnode, ForkNumber forknum, BlockNumber blknum)
{
	EmptyPageMarker *pm = (EmptyPageMarker *) (page + SizeOfPageHeaderData);
	EmptyPageMarker marker = {
			.rnode = rnode,
		.forknum = forknum,
		.blocknum = blknum
	};

	return memcmp(pm, &marker, sizeof(EmptyPageMarker)) == 0;
}


static void
mark_empty_page(Page page, RelFileNode rnode, ForkNumber forknum, BlockNumber blknum)
{
	char* ptr = page + SizeOfPageHeaderData;
	EmptyPageMarker marker = {
		.rnode = rnode,
		.forknum = forknum,
		.blocknum = blknum
	};

	memcpy(ptr, &marker, sizeof(EmptyPageMarker));
}
