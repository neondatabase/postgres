/*-------------------------------------------------------------------------
 *
 * remotexact_default.c
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/remotexact_default.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/remotexact.h"

/* GUC variable */
int current_region;

static void
default_collect_read_tuple(Relation relation, ItemPointer tid, TransactionId tuple_xid)
{
}

static void
default_collect_seq_scan_relation(Relation relation)
{
}

static void
default_collect_index_scan_page(Relation relation, BlockNumber blkno)
{
}

static void
default_clear_rwset(void)
{
}

static void
default_send_rwset_and_wait(void)
{
}

static const RemoteXactHook default_hook = {
	.collect_read_tuple = default_collect_read_tuple,
	.collect_seq_scan_relation = default_collect_seq_scan_relation,
	.collect_index_scan_page = default_collect_index_scan_page,
	.clear_rwset = default_clear_rwset,
	.send_rwset_and_wait = default_send_rwset_and_wait
};

static const RemoteXactHook *remote_xact_hook = &default_hook;

void
SetRemoteXactHook(const RemoteXactHook *hook)
{
	Assert(hook != NULL);
	remote_xact_hook = hook;
}

const RemoteXactHook *
GetRemoteXactHook(void)
{
	return remote_xact_hook;
}

void
AtEOXact_RemoteXact(void)
{
	remote_xact_hook->clear_rwset();
}
