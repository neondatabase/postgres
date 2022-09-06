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

#include "access/htup.h"
#include "access/xlogdefs.h"
#include "utils/relcache.h"
#include "storage/itemptr.h"

#define UNKNOWN_REGION -1
#define GLOBAL_REGION 0

#define IsMultiRegion() (current_region != GLOBAL_REGION)
#define RegionIsValid(r) (r != UNKNOWN_REGION)
#define RegionIsRemote(r) (RegionIsValid(r) && r != current_region && r != GLOBAL_REGION)

/*
 * RelationGetRegion
 *		Fetch relation's region.
 *
 * Returns UnknownRegion if there is no smgr
 */
#define RelationGetRegion(relation) \
	( (relation)->rd_smgr != NULL ? (relation)->rd_smgr->smgr_region : UNKNOWN_REGION )

/* GUC variable */
extern int current_region;

typedef XLogRecPtr (*get_region_lsn_hook_type) (int region);
extern PGDLLIMPORT get_region_lsn_hook_type get_region_lsn_hook;

#define GetRegionLsn(r) (get_region_lsn_hook == NULL ? InvalidXLogRecPtr : (*get_region_lsn_hook)(r))

typedef struct
{
	void		(*collect_region) (Relation relation);
	void		(*collect_relation) (Oid dbid, Oid relid);
	void		(*collect_page) (Oid dbid, Oid relid, BlockNumber blkno);
	void		(*collect_tuple) (Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber offset);
	void		(*collect_insert) (Relation relation, HeapTuple newtuple);
	void		(*collect_update) (Relation relation, HeapTuple oldtuple, HeapTuple newtuple);
	void		(*collect_delete) (Relation relation, HeapTuple oldtuple);
	void		(*clear_rwset) (void);
	void		(*send_rwset_and_wait) (void);
} RemoteXactHook;

extern void SetRemoteXactHook(const RemoteXactHook *hook);

extern void CollectRegion(Relation relation);
extern void CollectRelation(Oid dbid, Oid relid);
extern void CollectPage(Oid dbid, Oid relid, BlockNumber blkno);
extern void CollectTuple(Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber offset);
extern void CollectInsert(Relation relation, HeapTuple newtuple);
extern void CollectUpdate(Relation relation, HeapTuple oldtuple, HeapTuple newtuple);
extern void CollectDelete(Relation relation, HeapTuple oldtuple);
extern void SendRwsetAndWait(void);

extern void AtEOXact_RemoteXact(void);

#endif							/* REMOTEXACT_H */
