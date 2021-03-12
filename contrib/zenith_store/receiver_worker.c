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
#include "utils/builtins.h"
#include "tcop/tcopprot.h"
#include "access/xlog_internal.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands_xlog.h"
#include "access/xact.h"
#include "catalog/pg_control.h"

#include "libpq/pqformat.h"
#include "libpq-fe.h"

#include "zenith_store.h"
#include "memstore.h"
#include "receiver_worker.h"

uint64 client_system_id;
bool first_record_found = false;

#define NAPTIME_PER_CYCLE 1000	/* max sleep time between cycles (1s) */

#define WAL_BUFF_RETAIN_SIZE 10*XLOG_BLCKSZ
#define WAL_BUFF_CLEANUP_THRESHOLD 1*XLOG_BLCKSZ

#define LSN_PARTS(lsn) (uint32) ((lsn) >> 32), (uint32) (lsn)

typedef struct
{
	XLogRecPtr first_ever_lsn;
	XLogRecPtr start_lsn; /* LSN of the first entry in .data */
	StringInfoData wal;
} RecvBuffer;

static void receiver_loop(WalReceiverConn *conn, XLogRecPtr last_received);
static void process_records(XLogReaderState *reader_state, RecvBuffer *recv_buffer);
static void send_feedback(WalReceiverConn *conn, XLogRecPtr recvpos, bool force, bool requestReply);
static void handle_record(XLogReaderState *record);

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
 * RmgrNames is an array of resource manager names, to make error messages
 * a bit nicer.
 */
#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup,mask) \
  name,

static const char *RmgrNames[RM_MAX_ID + 1] = {
#include "access/rmgrlist.h"
};

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

	zenith_log(ReceiverLog, "Starting receiver on '%s'", conn_string);

	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		zenith_log(ERROR, "libpqwalreceiver didn't initialize correctly");

	conn = walrcv_connect(conn_string, false /* is_logical */, "zenith", &err);
	if (conn == NULL)
		zenith_log(ERROR, "could not connect to the processing node: %s", err);

	system_id = zenith_identify_system(conn, &startpointTLI, &primary_lsn);

	/*
	 * Always start streaming from beginning of the page -- xlogreader
	 * depends on that.
	 */
	primary_lsn = primary_lsn - (primary_lsn % XLOG_BLCKSZ);

	if (sscanf(system_id, "%lu", &client_system_id) != 1)
		zenith_log(ERROR, "failed to parse client system id");

	zenith_log(ReceiverLog, "Receiver connected to '%s': timeline=%d, lsn=%X/%X",
		system_id, startpointTLI, (uint32) (primary_lsn >> 32), (uint32) primary_lsn);

	/* Build logical replication streaming options. */
	options.logical = false;
	options.startpoint = primary_lsn;
	options.slotname = "zenith_store";
	options.proto.physical.startpointTLI = startpointTLI;

	/* Start streaming replication. */
	walrcv_startstreaming(conn, &options);

	receiver_loop(conn, primary_lsn);
}

static int
read_xlog_page(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
			   XLogRecPtr targetRecPtr, char *readBuff)
{
	RecvBuffer *recv_buffer = (RecvBuffer *) state->private_data;

	Assert(recv_buffer->wal.cursor == 0);
	Assert(recv_buffer->start_lsn % XLOG_BLCKSZ == 0);
	Assert(reqLen <= XLOG_BLCKSZ);

	zenith_log(ReceiverTrace,
		"read_xlog_page: targetPagePtr=%X/%X targetRecPtr=%X/%X, req_len=%d, start_lsn=%X/%X, last_lsn=%X/%X",
		LSN_PARTS(targetPagePtr),
		LSN_PARTS(targetRecPtr),
		reqLen,
		LSN_PARTS(recv_buffer->start_lsn),
		LSN_PARTS(recv_buffer->start_lsn + recv_buffer->wal.len)
	);

	int off = targetPagePtr - recv_buffer->start_lsn;
	Assert(off >= 0);

	int recv_len = recv_buffer->wal.len - off;
	if (recv_len < reqLen)
		return -1;

	int copy_len = recv_len < XLOG_BLCKSZ ? recv_len : XLOG_BLCKSZ;

	memcpy(readBuff, recv_buffer->wal.data + off, copy_len);


	/*
	 * FIXME: now received WAL would just indefinitely grow. XLogReadRecord() may request
	 * pages from the past and I didn't yet understood what correct lookbehind boundary would be.
	 * 10 pages are not enough. Would that boundary be just segsize?
	 */

	// /* free data from time to time */
	// if (targetPagePtr > recv_buffer->start_lsn + WAL_BUFF_RETAIN_SIZE + WAL_BUFF_CLEANUP_THRESHOLD)
	// {
	// 	Assert(recv_buffer->wal.len > WAL_BUFF_RETAIN_SIZE + WAL_BUFF_CLEANUP_THRESHOLD);

	// 	memmove(recv_buffer->wal.data,
	// 		recv_buffer->wal.data + WAL_BUFF_CLEANUP_THRESHOLD,
	// 		recv_buffer->wal.len - WAL_BUFF_CLEANUP_THRESHOLD
	// 	);
	// 	recv_buffer->wal.len -= WAL_BUFF_CLEANUP_THRESHOLD;
	// 	recv_buffer->start_lsn += WAL_BUFF_CLEANUP_THRESHOLD;

	// 	zenith_log(ReceiverTrace,
	// 		"read_xlog_page compaction: old_start=%X/%X new_start=%X/%X",
	// 		LSN_PARTS(recv_buffer->start_lsn - WAL_BUFF_CLEANUP_THRESHOLD),
	// 		LSN_PARTS(recv_buffer->start_lsn)
	// 	);
	// }

	return copy_len;
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

	XLogReaderState *xlogreader;
	XLogReaderRoutine dummy_routine = { &read_xlog_page, NULL, NULL };

	RecvBuffer recv_buffer;
	initStringInfo(&recv_buffer.wal);
	recv_buffer.start_lsn = InvalidXLogRecPtr;
	recv_buffer.first_ever_lsn = InvalidXLogRecPtr;

	bool first = true;

	xlogreader = XLogReaderAllocate(wal_segment_size, NULL, &dummy_routine, &recv_buffer);
	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

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

					zenith_log(ReceiverTrace, "got records pack size=%d, first_lsn=%X/%X",
						s.len - s.cursor,
						(uint32) (start_lsn >> 32), (uint32) start_lsn);

					if (first)
					{
						Assert(start_lsn % XLOG_BLCKSZ == 0);
						recv_buffer.start_lsn = start_lsn;
						recv_buffer.first_ever_lsn = start_lsn;

						// XXX: change that to XLogFindNextRecord()
						start_lsn += XLogPageHeaderSize((XLogPageHeader) (s.data + s.cursor));
						// XLogBeginRead(xlogreader, start_lsn);


						XLogSegNo	targetSegNo;
						XLByteToSeg(recv_buffer.start_lsn, targetSegNo, xlogreader->segcxt.ws_segsize);
						xlogreader->seg.ws_segno = targetSegNo;

						first = false;
					}

					appendBinaryStringInfoNT(&recv_buffer.wal, s.data + s.cursor, s.len - s.cursor);
					process_records(xlogreader, &recv_buffer);
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
process_records(XLogReaderState *reader, RecvBuffer *recv_buffer)
{
	if (!first_record_found)
	{
		XLogRecPtr first_valid_lsn = XLogFindNextRecord(reader, recv_buffer->first_ever_lsn);

		if (first_valid_lsn)
		{
			first_record_found = true;
			zenith_log(ReceiverTrace, "XLogFindNextRecord: first_lsn bumbed to %X/%X",
				(uint32) (first_valid_lsn >> 32), (uint32) first_valid_lsn);
		}
	}
	else
	{
		for (;;)
		{
			XLogRecord *record;
			char	   *err = NULL;

			record = XLogReadRecord(reader, &err);
			if (err)
				zenith_log(ERROR, "%s", err);
			if (!record)
				break; // XXX: check that it is because of insufficient data

			if (DecodeXLogRecord(reader, record, &err))
				handle_record(reader);
			else
				zenith_log(ERROR, "failed to decode WAL record: %s", err);
		}
	}
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

/*
 * Parse the record. For each data file it touches, look up the file_entry_t and
 * write out a copy of the record
 */
void
handle_record(XLogReaderState *record)
{
	/*
	 * Extract information on which blocks the current record modifies.
	 */
	// int			block_id;
	RmgrId		rmid = XLogRecGetRmid(record);
	uint8		info = XLogRecGetInfo(record);
	uint8		rminfo = info & ~XLR_INFO_MASK;
	enum {
		SKIP = 0,
		APPEND_REL_WAL,
		APPEND_NONREL_WAL
	} action;

	zenith_log(ReceiverTrace, "processing parsed WAL record: lsn=%X/%X, rmgr=%s, info=%02X",
		LSN_PARTS(record->ReadRecPtr), RmgrNames[rmid], info);

	/* Is this a special record type that I recognize? */

	if (rmid == RM_DBASE_ID && rminfo == XLOG_DBASE_CREATE)
	{
		/* TODO */
		action = APPEND_NONREL_WAL;
	}
	else if (rmid == RM_DBASE_ID && rminfo == XLOG_DBASE_DROP)
	{
		/* TODO */
		action = APPEND_NONREL_WAL;
	}
	else if (rmid == RM_SMGR_ID && rminfo == XLOG_SMGR_CREATE)
	{
		action = SKIP;
	}
	else if (rmid == RM_SMGR_ID && rminfo == XLOG_SMGR_TRUNCATE)
	{
		/* FIXME: This should probably go to per-rel WAL? */
		action = APPEND_NONREL_WAL;
	}
	else if (rmid == RM_XACT_ID &&
			 ((rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT ||
			  (rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT_PREPARED))
	{
		/* These records can include "dropped rels". */
		xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(record);
		xl_xact_parsed_commit parsed;

		ParseCommitRecord(info, xlrec, &parsed);
		for (int i = 0; i < parsed.nrels; i++)
		{
		}
		action = APPEND_NONREL_WAL;
	}
	else if (rmid == RM_XACT_ID &&
			 ((rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT ||
			  (rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT_PREPARED))
	{
		/* These records can include "dropped rels". */
		xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(record);
		xl_xact_parsed_abort parsed;

		ParseAbortRecord(info, xlrec, &parsed);
		for (int i = 0; i < parsed.nrels; i++)
		{
		}
		action = APPEND_NONREL_WAL;
	}
	else if (info & XLR_SPECIAL_REL_UPDATE)
	{
		/*
		 * This record type modifies a relation file in some special way, but
		 * we don't recognize the type. That's bad - we don't know how to
		 * track that change.
		 */
		zenith_log(ERROR, "WAL record modifies a relation, but record type is not recognized: "
				 "lsn: %X/%X, rmgr: %s, info: %02X",
				 (uint32) (record->ReadRecPtr >> 32), (uint32) (record->ReadRecPtr),
				 RmgrNames[rmid], info);
	}
	else if (rmid == RM_XLOG_ID)
	{
		if (rminfo == XLOG_FPI || rminfo == XLOG_FPI_FOR_HINT)
			action = APPEND_REL_WAL;
		else
			action = APPEND_NONREL_WAL;
	}
	else
	{
		switch (rmid)
		{
			case RM_CLOG_ID:
			case RM_DBASE_ID:
			case RM_TBLSPC_ID:
			case RM_MULTIXACT_ID:
			case RM_RELMAP_ID:
			case RM_STANDBY_ID:
			case RM_SEQ_ID:
			case RM_COMMIT_TS_ID:
			case RM_REPLORIGIN_ID:
			case RM_LOGICALMSG_ID:
				/* These go to the "non-relation" WAL */
				action = APPEND_NONREL_WAL;
				break;

			case RM_SMGR_ID:
			case RM_HEAP2_ID:
			case RM_HEAP_ID:
			case RM_BTREE_ID:
			case RM_HASH_ID:
			case RM_GIN_ID:
			case RM_GIST_ID:
			case RM_SPGIST_ID:
			case RM_BRIN_ID:
			case RM_GENERIC_ID:
				/* These go to the per-relation WAL files */
				action = APPEND_REL_WAL;
				break;

			default:
				zenith_log(ERROR, "unknown resource manager");
		}
	}

	if (action == APPEND_REL_WAL)
	{
		int block_id;

		for (block_id = 0; block_id <= record->max_block_id; block_id++)
		{
			RelFileNode rnode;
			ForkNumber	forknum;
			BlockNumber blkno;

			if (!XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blkno))
				continue;

			zenith_log(ReceiverTrace,
				"record has blkref #%u: rel=%u/%u/%u fork=%s blk=%u, fpw=%d, fpw_verify=%d",
				block_id,
				rnode.spcNode, rnode.dbNode, rnode.relNode,
				forknum == MAIN_FORKNUM ? "main" : forkNames[forknum],
				blkno,
				XLogRecHasBlockImage(record, block_id),
				XLogRecBlockImageApply(record, block_id)
			);

			PerPageWalHashKey key;
			memset(&key, '\0', sizeof(PerPageWalHashKey));
			key.system_identifier = 42; //client_system_id;
			key.rnode = rnode;
			key.forknum = forknum;
			key.blkno = blkno;

			memstore_insert(key, record->EndRecPtr, block_id, record->decoded_record);
		}
	}
}
