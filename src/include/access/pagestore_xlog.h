/*-------------------------------------------------------------------------
 * TODO: type some smart text here
 *
 * src/include/access/pagestore_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_XLOG_H
#define PAGESTORE_XLOG_H

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "storage/bufpage.h"
#include "utils/rel.h"

#define XLOG_PAGESTORE_EXTEND_RELATION 0x01


typedef enum PageStamp
{
	PAGE_STAMP_UNKNOWN = 0,
	PAGE_STAMP_REDO,
	PAGE_STAMP_BUFMGR
} PageStamp;

extern XLogRecPtr log_relsize(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno);
extern bool validate_marked_page(Page page, RelFileNode rnode, ForkNumber forknum, BlockNumber blknum);

/* functions defined for rmgr */
extern void pagestore_redo(XLogReaderState *record);
extern const char *pagestore_identify(uint8 info);
extern void pagestore_desc(StringInfo buf, XLogReaderState *record);

#endif							/* PAGESTORE_XLOG_H */
