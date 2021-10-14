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

#include <signal.h>
#include "access/xlogdefs.h"
#include "lib/stringinfo.h"

/*
 * What to do with a snapshot in create replication slot command.
 */
typedef enum
{
	CRS_EXPORT_SNAPSHOT,
	CRS_NOEXPORT_SNAPSHOT,
	CRS_USE_SNAPSHOT
} CRSSnapshotAction;

/*
 * Callback for PhysicalReplicationHandler.iteration,
 * and other functions internal to walsender
 */
typedef void (*WalSndSendDataCallback) (void);

/*
 * Struct containing the callbacks that are used in the physical replication
 * API.
 *
 * The WAL sender API does not by itself update the replication slot status:
 * the replication handler should call ProcessStandbyReply regularly to
 * update the replication slot and prevent replication slot timeout errors.
 */
typedef struct PhysicalReplicationHandler
{
	/*
	 * Signals the replication handler that the replication slot is OK,
	 * the WAL sender is initialized, and that we will start processing data.
	 */
	void (*start) (void);
	/*
	 * Prepare a StringInfo buffer for the wal record between startptr and
	 * endptr of size len.
	 *
	 * The buffer starts out empty, and after this call the buffer will be
	 * extended by the WAL record's data, after which send_data is called
	 * and the buffer is reset.
	 */
	void (*prepare_buf) (StringInfo buf, XLogRecPtr startptr,
						 XLogRecPtr endptr, Size len);
	/*
	 * Send the WAL record located between startptr and endptr. The record is
	 * stored in the last 'len' bytes of the buffer.
	 */
	void (*send_data) (StringInfo buf, XLogRecPtr startptr,
					   XLogRecPtr endptr, Size len);
	/*
	 * Called once every iteration of the physical replication loop.
	 *
	 * The handler is expected (but not strictly required) to call send_data
	 * exactly once for each iteration.
	 * If this function returns false, we will stop sending data.
	 */
	bool (*iteration) (WalSndSendDataCallback send_data);
	/*
	 * Register that the current timeline is complete
	 */
	void (*send_timeline_done) (void);
	/*
	 * Signal that we've completed sending data.
	 */
	void (*end) (void);
} PhysicalReplicationHandler;

/* global state */
extern bool am_walsender;
extern bool am_cascading_walsender;
extern bool am_db_walsender;
extern bool wake_wal_senders;
extern PhysicalReplicationHandler *physicalHandler;

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
extern void StartPhysicalReplication(char* slot_name, TimeLineID timeline,
									 XLogRecPtr startpoint,
									 PhysicalReplicationHandler *handler);
extern void ProcessStandbyReply(XLogRecPtr	writePtr,
								XLogRecPtr	flushPtr,
								XLogRecPtr	applyPtr,
								TimestampTz replyTime);

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
