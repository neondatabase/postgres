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

get_region_lsn_hook_type get_region_lsn_hook = NULL;

static const RemoteXactHook *remote_xact_hook = NULL;
#define CallHook(name) \
	if (remote_xact_hook) remote_xact_hook->name

void
SetRemoteXactHook(const RemoteXactHook *hook)
{
	remote_xact_hook = hook;
}

void
CollectReadTuple(Relation relation, ItemPointer tid, TransactionId tuple_xid)
{
	CallHook(collect_read_tuple)(relation, tid, tuple_xid);
}

void
CollectSeqScanRelation(Relation relation)
{
	CallHook(collect_seq_scan_relation)(relation);
}

void
CollectIndexScanPage(Relation relation, BlockNumber blkno)
{
	CallHook(collect_index_scan_page)(relation, blkno);
}

void
CollectInsert(Relation relation, HeapTuple newtuple)
{
	CallHook(collect_insert)(relation, newtuple);
}

void
CollectUpdate(Relation relation, HeapTuple oldtuple, HeapTuple newtuple)
{
	CallHook(collect_update)(relation, oldtuple, newtuple);
}

void
CollectDelete(Relation relation, HeapTuple oldtuple)
{
	CallHook(collect_delete)(relation, oldtuple);
}

void
SendRwsetAndWait(void)
{
	CallHook(send_rwset_and_wait)();
}

// TODO: probably can use RegisterXactCallback for this
void
AtEOXact_RemoteXact(void)
{
	CallHook(clear_rwset)();
}
