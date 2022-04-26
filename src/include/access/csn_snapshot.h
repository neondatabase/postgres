/*-------------------------------------------------------------------------
 *
 * csn_snapshot.h
 *	  Support for cross-node snapshot isolation.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/csn_snapshot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CSN_SNAPSHOT_H
#define CSN_SNAPSHOT_H

#include "port/atomics.h"
#include "storage/lock.h"
#include "utils/snapshot.h"
#include "utils/guc.h"

/*
 * snapshot.h is used in frontend code so atomic variant of SnapshotCSN type
 * is defined here.
 */
typedef pg_atomic_uint64 CSN_atomic;

#define InProgressXidCSN	 	UINT64CONST(0x0)
#define AbortedXidCSN	 		UINT64CONST(0x1)
#define FrozenXidCSN		 	UINT64CONST(0x2)
#define InDoubtXidCSN	 		UINT64CONST(0x3)
#define FirstNormalXidCSN 		UINT64CONST(0x4)

#define XidCSNIsInProgress(csn)	((csn) == InProgressXidCSN)
#define XidCSNIsAborted(csn)		((csn) == AbortedXidCSN)
#define XidCSNIsFrozen(csn)		((csn) == FrozenXidCSN)
#define XidCSNIsInDoubt(csn)		((csn) == InDoubtXidCSN)
#define XidCSNIsNormal(csn)		((csn) >= FirstNormalXidCSN)




extern Size CSNSnapshotShmemSize(void);
extern void CSNSnapshotShmemInit(void);

extern SnapshotCSN GetLastAssignedCSN(bool locked);
extern void SetAssignedCSN(PGPROC *proc, SnapshotCSN csn, bool locked);

extern bool XidInvisibleInCSNSnapshot(TransactionId xid, Snapshot snapshot);

extern XidCSN TransactionIdGetXidCSN(TransactionId xid);

extern void CSNSnapshotAbort(PGPROC *proc, TransactionId xid, int nsubxids,
								TransactionId *subxids);
extern void CSNSnapshotPrecommit(PGPROC *proc, TransactionId xid, int nsubxids,
									TransactionId *subxids);
extern void CSNSnapshotCommit(PGPROC *proc, TransactionId xid, int nsubxids,
									TransactionId *subxids);

#endif							/* CSN_SNAPSHOT_H */
