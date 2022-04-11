/*-------------------------------------------------------------------------
 *
 * remotexact.h
 *
 * src/include/access/remotexact.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTEXACT_H
#define REMOTEXACT_H

#include "utils/relcache.h"
#include "storage/itemptr.h"

#define UNKNOWN_REGION -1
#define GLOBAL_REGION 0

#define IsMultiRegion() (current_region != GLOBAL_REGION)
#define RegionIsRemote(r) (r != current_region && r != GLOBAL_REGION && r != UNKNOWN_REGION)

/* GUC variable */
extern int current_region;

typedef struct
{
	void		(*collect_read_tuple) (Relation relation, ItemPointer tid, TransactionId tuple_xid);
	void		(*collect_seq_scan_relation) (Relation relation);
	void		(*collect_index_scan_page) (Relation relation, BlockNumber blkno);
	void		(*clear_rwset) (void);
	void		(*send_rwset_and_wait) (void);
} RemoteXactHook;

void SetRemoteXactHook(const RemoteXactHook *hook);

void CollectReadTuple(Relation relation, ItemPointer tid, TransactionId tuple_xid);
void CollectSeqScanRelation(Relation relation);
void CollectIndexScanPage(Relation relation, BlockNumber blkno);
void SendRwsetAndWait(void);

void AtEOXact_RemoteXact(void);

#endif							/* REMOTEXACT_H */
