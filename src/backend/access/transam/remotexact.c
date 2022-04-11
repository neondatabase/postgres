/*-------------------------------------------------------------------------
 *
 * remotexact.c
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/remotexact.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/remotexact.h"

/* GUC variable */
int current_region;

static const RemoteXactHook *remote_xact_hook = NULL;

void
SetRemoteXactHook(const RemoteXactHook *hook)
{
	Assert(hook != NULL);
	remote_xact_hook = hook;
}

void
CollectReadTuple(Relation relation, ItemPointer tid, TransactionId tuple_xid)
{
	if (remote_xact_hook)
		remote_xact_hook->collect_read_tuple(relation, tid, tuple_xid);
}

void
CollectSeqScanRelation(Relation relation)
{
	if (remote_xact_hook)
		remote_xact_hook->collect_seq_scan_relation(relation);
}

void
CollectIndexScanPage(Relation relation, BlockNumber blkno)
{
	if (remote_xact_hook)
		remote_xact_hook->collect_index_scan_page(relation, blkno);
}

void
SendRwsetAndWait(void)
{
	if (remote_xact_hook)
		remote_xact_hook->send_rwset_and_wait();
}

// TODO: probably can use RegisterXactCallback for this
void
AtEOXact_RemoteXact(void)
{
	if (remote_xact_hook)
		remote_xact_hook->clear_rwset();	
}
