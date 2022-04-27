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
CollectRelation(Oid dbid, Oid relid)
{
	CallHook(collect_relation)(dbid, relid);
}

void
CollectPage(Oid dbid, Oid relid, BlockNumber blkno)
{
	CallHook(collect_page)(dbid, relid, blkno);
}

void
CollectTuple(Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber offset)
{
	CallHook(collect_tuple)(dbid, relid, blkno, offset);
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
