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
int			current_region;

get_region_lsn_hook_type get_region_lsn_hook = NULL;

bool		is_surrogate = false;

static const RemoteXactHook *remote_xact_hook = NULL;

/* Only call if a hook is set and is in multi-region mode */
#define CallHook(name) \
	if (remote_xact_hook && IsMultiRegion()) remote_xact_hook->name

void
SetRemoteXactHook(const RemoteXactHook *hook)
{
	remote_xact_hook = hook;
}

void
CollectRegion(Relation relation)
{
	CallHook(collect_region)(relation);
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
PreCommit_ExecuteRemoteXact(void)
{
	if (!is_surrogate)
		CallHook(execute_remote_xact)();

	is_surrogate = false;
}
