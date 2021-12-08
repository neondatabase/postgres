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

typedef struct
{
	void		(*collect_read_tuple) (Relation relation, ItemPointer tid, TransactionId tuple_xid);
	void		(*collect_seq_scan_relation) (Relation relation);
	void		(*collect_index_scan_page) (Relation relation, BlockNumber blkno);
	void		(*clear_rwset) (void);
	void		(*send_rwset_and_wait) (void);
} RemoteXactHook;

extern void SetRemoteXactHook(const RemoteXactHook *hook);
extern const RemoteXactHook *GetRemoteXactHook(void);

extern void AtEOXact_RemoteXact(void);

#endif							/* REMOTEXACT_H */
