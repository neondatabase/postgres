/*-------------------------------------------------------------------------
 *
 * walsender.h
 *	  Exports from replication/walsender.c.
 *
 * Portions Copyright (c) 2010-2021, PostgreSQL Global Development Group
 *
 * src/include/replication/walsender.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WALSENDER_H
#define _WALSENDER_H

#include "access/xlog.h"
#include <signal.h>

/*
 * What to do with a snapshot in create replication slot command.
 */
typedef enum
{
	CRS_EXPORT_SNAPSHOT,
	CRS_NOEXPORT_SNAPSHOT,
	CRS_USE_SNAPSHOT
} CRSSnapshotAction;

/* global state */
extern bool am_walsender;
extern bool am_cascading_walsender;
extern bool am_db_walsender;
extern bool wake_wal_senders;

/* user-settable parameters */
extern int	max_wal_senders;
extern int	wal_sender_timeout;
extern bool log_replication_commands;

extern void InitWalSender(void);
extern bool exec_replication_command(const char *query_string);
extern void WalSndErrorCleanup(void);
extern void WalSndResourceCleanup(bool isCommit);
extern void WalSndSignals(void);
extern Size WalSndShmemSize(void);
extern void WalSndShmemInit(void);
extern void WalSndWakeup(void);
extern void WalSndInitStopping(void);
extern void WalSndWaitStopping(void);
extern void HandleWalSndInitStopping(void);
extern void WalSndRqstFileReload(void);

/*
 * Hook to check for WAL receiving backpressure.
 * Return value in microseconds
 */
extern uint64 (*delay_backend_us)(void);

/* expose these so that they can be reused by the neon walproposer extension */
extern void LagTrackerWrite(XLogRecPtr lsn, TimestampTz local_flush_time);
extern TimeOffset LagTrackerRead(int head, XLogRecPtr lsn, TimestampTz now);
extern void ProcessStandbyReply(XLogRecPtr writePtr, XLogRecPtr flushPtr,
								XLogRecPtr applyPtr, TimestampTz replyTime,
								bool replyRequested);
extern void PhysicalConfirmReceivedLocation(XLogRecPtr lsn);
extern void ProcessStandbyHSFeedback(TimestampTz   replyTime,
									 TransactionId feedbackXmin,
									 uint32		feedbackEpoch,
									 TransactionId feedbackCatalogXmin,
									 uint32		feedbackCatalogEpoch);

/*
 * Remember that we want to wakeup walsenders later
 *
 * This is separated from doing the actual wakeup because the writeout is done
 * while holding contended locks.
 */
#define WalSndWakeupRequest() \
	do { wake_wal_senders = true; } while (0)

/*
 * wakeup walsenders if there is work to be done
 */
#define WalSndWakeupProcessRequests()		\
	do										\
	{										\
		if (wake_wal_senders)				\
		{									\
			wake_wal_senders = false;		\
			if (max_wal_senders > 0)		\
				WalSndWakeup();				\
		}									\
	} while (0)

#endif							/* _WALSENDER_H */
