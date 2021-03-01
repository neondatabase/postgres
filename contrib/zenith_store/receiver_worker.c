/*-------------------------------------------------------------------------
 *
 * receiver_worker.c - decode and redistribute WAL per page
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/zenith/receiver_worker.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "storage/latch.h"
#include "pgstat.h"
#include "replication/walreceiver.h"
#include "utils/guc.h"
#include "postmaster/bgworker.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "tcop/tcopprot.h"
#include "libpq-fe.h"

#include "zenith_store.h"
#include "receiver_worker.h"

#define NAPTIME_PER_CYCLE 1000	/* max sleep time between cycles (1s) */

static void receiver_loop(WalReceiverConn *conn, XLogRecPtr last_received);
static void apply_dispatch(StringInfo s);
static void send_feedback(WalReceiverConn *conn, XLogRecPtr recvpos, bool force, bool requestReply);

typedef struct
{
	/* Current connection to the primary, if any */
	PGconn	   *streamConn;
	/* Used to remember if the connection is logical or physical */
	bool		logical;
	/* Buffer for currently read records */
	char	   *recvBuf;
} WalReceiverConnDeopaque;

/*
 * Own reimplementation of walrcv_identify_system() that returns lsn.
 * Another option is to use walrcv_exec() but that one wants db conection.
 */
static char *
zenith_identify_system(WalReceiverConn *opaqueConn, TimeLineID *primary_tli, XLogRecPtr *primary_lsn)
{
	PGresult   *res;
	char	   *primary_sysid;
	WalReceiverConnDeopaque *conn = (WalReceiverConnDeopaque *) opaqueConn;
	/*
	 * Get the system identifier and timeline ID as a DataRow message from the
	 * primary server.
	 */
	res = PQexec(conn->streamConn, "IDENTIFY_SYSTEM");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("could not receive database system identifier and timeline ID from "
						"the primary server: %s",
						pchomp(PQerrorMessage(conn->streamConn)))));
	}
	if (PQnfields(res) < 3 || PQntuples(res) != 1)
	{
		int			ntuples = PQntuples(res);
		int			nfields = PQnfields(res);

		PQclear(res);
		ereport(ERROR,
				(errmsg("invalid response from primary server"),
				 errdetail("Could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields.",
						   ntuples, nfields, 3, 1)));
	}
	primary_sysid = pstrdup(PQgetvalue(res, 0, 0));
	*primary_tli = pg_strtoint32(PQgetvalue(res, 0, 1));

	/* Get LSN start position if necessary */
	if (primary_lsn != NULL)
	{
		uint32		hi, lo;
		if (sscanf(PQgetvalue(res, 0, 2), "%X/%X", &hi, &lo) != 2)
		{
			zenith_log(ERROR, "could not parse write-ahead log location \"%s\"",
						 PQgetvalue(res, 0, 2));

			PQclear(res);
			return false;
		}
		*primary_lsn = ((uint64) hi) << 32 | lo;
	}

	PQclear(res);

	return primary_sysid;
}


void
receiver_main(Datum main_arg)
{
	char		   *err = NULL;
	char		   *system_id = NULL;
	WalRcvStreamOptions options;
	WalReceiverConn *conn;
	TimeLineID		startpointTLI;
	XLogRecPtr		primary_lsn;

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* walrcv_exec() wants database connection */
	// BackgroundWorkerInitializeConnection('postgres', NULL, 0);

	zenith_log(ReceiverTrace, "Starting receiver on '%s'", conn_string);

	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");

	conn = walrcv_connect(conn_string, false /* is_logical */, "zenith", &err);
	if (conn == NULL)
		ereport(ERROR,
				(errmsg("could not connect to the publisher: %s", err)));

	/*
	 * We don't really use the output identify_system for anything but it
	 * does some initializations on the upstream so let's still call it.
	 */
	system_id = zenith_identify_system(conn, &startpointTLI, &primary_lsn);

	zenith_log(ReceiverTrace, "Receiver connected to '%s': timeline=%d, lsn=%X/%x",
		system_id, startpointTLI, (uint32) (primary_lsn >> 32), (uint32) primary_lsn);

	/* Build logical replication streaming options. */
	options.logical = false;
	options.startpoint = primary_lsn;
	options.slotname = "zenith_store";
	options.proto.physical.startpointTLI = startpointTLI;

	/* Start normal logical streaming replication. */
	walrcv_startstreaming(conn, &options);

	receiver_loop(conn, primary_lsn);
}

/*
 * Receiver main loop.
 */
static void
receiver_loop(WalReceiverConn *conn, XLogRecPtr last_received)
{
	TimestampTz last_recv_timestamp = GetCurrentTimestamp();
	bool		ping_sent = false;
	TimeLineID	tli;

	/* This outer loop iterates once per wait. */
	for (;;)
	{
		pgsocket	fd = PGINVALID_SOCKET;
		int			rc;
		int			len;
		char	   *buf = NULL;
		bool		endofstream = false;

		CHECK_FOR_INTERRUPTS();

		len = walrcv_receive(conn, &buf, &fd);

		/* Loop to process all available data (without blocking). */
		for (;;)
		{
			CHECK_FOR_INTERRUPTS();

			if (len == 0)
			{
				break;
			}
			else if (len < 0)
			{
				ereport(LOG,
						(errmsg("data stream from publisher has ended")));
				endofstream = true;
				break;
			}
			else
			{
				int			c;
				StringInfoData s;

				/* Reset timeout. */
				last_recv_timestamp = GetCurrentTimestamp();
				ping_sent = false;

				s.data = buf;
				s.len = len;
				s.cursor = 0;
				s.maxlen = -1;

				c = pq_getmsgbyte(&s);

				if (c == 'w')
				{
					XLogRecPtr	start_lsn;
					XLogRecPtr	end_lsn;

					start_lsn = pq_getmsgint64(&s);
					end_lsn = pq_getmsgint64(&s);
					pq_getmsgint64(&s); /* send_time */

					if (last_received < start_lsn)
						last_received = start_lsn;

					if (last_received < end_lsn)
						last_received = end_lsn;

					apply_dispatch(&s);
				}
				else if (c == 'k')
				{
					XLogRecPtr	end_lsn;
					TimestampTz timestamp;
					bool		reply_requested;

					end_lsn = pq_getmsgint64(&s);
					timestamp = pq_getmsgint64(&s);
					reply_requested = pq_getmsgbyte(&s);

					if (last_received < end_lsn)
						last_received = end_lsn;

					send_feedback(conn, last_received, reply_requested, false);
				}
				/* other message types are purposefully ignored */
			}

			len = walrcv_receive(conn, &buf, &fd);
		}

		/* confirm all writes so far */
		send_feedback(conn, last_received, false, false);

		/* Check if we need to exit the streaming loop. */
		if (endofstream)
			break;


		rc = WaitLatchOrSocket(MyLatch,
							   WL_SOCKET_READABLE | WL_LATCH_SET |
							   WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							   fd, NAPTIME_PER_CYCLE,
							   PG_WAIT_EXTENSION);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}
	}

	/* All done */
	walrcv_endstreaming(conn, &tli);
}

static void
apply_dispatch(StringInfo s)
{
	zenith_log(ReceiverTrace, "Got record len=%d", s->len);
}

static void
send_feedback(WalReceiverConn *conn, XLogRecPtr recvpos, bool force, bool requestReply)
{
	static StringInfo reply_message = NULL;

	TimestampTz now;

	now = GetCurrentTimestamp();
	reply_message = makeStringInfo();
	pq_sendbyte(reply_message, 'r');
	pq_sendint64(reply_message, recvpos);	/* write */
	pq_sendint64(reply_message, recvpos);	/* flush */
	pq_sendint64(reply_message, recvpos);	/* apply */
	pq_sendint64(reply_message, now);		/* sendTime */
	pq_sendbyte(reply_message, requestReply);	/* replyRequested */

	walrcv_send(conn, reply_message->data, reply_message->len);
}
