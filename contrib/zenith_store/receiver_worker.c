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

#define NAPTIME_PER_CYCLE 1000	/* max sleep time between cycles (1s) */

#define LSN_PARTS(lsn) (uint32) ((lsn) >> 32), (uint32) (lsn)

static void receiver_loop(WalReceiverConn *conn, XLogRecPtr last_received);
static void process_record(XLogRecord *record, XLogRecPtr start_lsn, XLogRecPtr end_lsn);
static void process_records(StringInfo s, XLogRecPtr start_lsn);
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

	/*
	 * We don't really use the output identify_system for anything but it
	 * does some initializations on the upstream so let's still call it.
	 */
	system_id = zenith_identify_system(conn, &startpointTLI, &primary_lsn);

	if (sscanf(system_id, "%lu", &client_system_id) != 1)
		zenith_log(ERROR, "failed to parse client system id");

	zenith_log(ReceiverLog, "Receiver connected to '%s': timeline=%d, lsn=%X/%X",
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

					process_records(&s, start_lsn);
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

static bool
is_valid_record(XLogRecord *record)
{
	pg_crc32c	crc;

	/* Calculate the CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
	/* include the record header last */
	COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(record->xl_crc, crc))
	{
		return false;
	}

	return true;
}

static void
process_records(StringInfo s, XLogRecPtr start_lsn)
{

	zenith_log(ReceiverTrace, "got record pack size=%d, first_lsn=%X/%X",
		s->len - s->cursor,
		(uint32) (start_lsn >> 32), (uint32) start_lsn);

	const XLogRecPtr lsn_base = start_lsn - s->cursor;

	while (s->cursor < s->len)
	{
		// XXX: do XLogRecord sanity check akin to ValidXLogRecordHeader()
		XLogRecord *record = (XLogRecord *) (s->data + s->cursor);
		uint32 total_len = record->xl_tot_len;
		uint32 len_in_block = XLOG_BLCKSZ - (lsn_base + s->cursor) % XLOG_BLCKSZ;

		start_lsn = lsn_base + s->cursor;

		if (total_len <= len_in_block)
		{
			process_record(record, start_lsn, lsn_base + s->cursor + MAXALIGN(record->xl_tot_len));
			s->cursor += MAXALIGN(record->xl_tot_len);
		}
		else
		{
			/* Record does cross a page boundary */
			StringInfoData buf;
			initStringInfo(&buf);
			appendBinaryStringInfoNT(&buf, s->data + s->cursor, len_in_block);
			s->cursor += len_in_block;

			XLogPageHeaderData *pageHeader;
			uint32 gotlen = len_in_block;
			do
			{
				Assert((lsn_base + s->cursor) % XLOG_BLCKSZ == 0);

				pageHeader = (XLogPageHeaderData *) (s->data + s->cursor);
				if (!(pageHeader->xlp_info & XLP_FIRST_IS_CONTRECORD))
					zenith_log(ERROR, "there is no contrecord flag at %X/%X", LSN_PARTS(start_lsn));

				if (pageHeader->xlp_rem_len == 0 ||
					total_len != (pageHeader->xlp_rem_len + gotlen))
				{
					zenith_log(ERROR, "invalid contrecord length %u (expected %lld) at %X/%X",
							pageHeader->xlp_rem_len,
							((long long) total_len) - gotlen,
							LSN_PARTS(lsn_base + s->cursor));
				}

				s->cursor += XLogPageHeaderSize(pageHeader);
				len_in_block = XLOG_BLCKSZ - XLogPageHeaderSize(pageHeader);
				if (pageHeader->xlp_rem_len < len_in_block)
					len_in_block = pageHeader->xlp_rem_len;

				appendBinaryStringInfoNT(&buf, s->data + s->cursor, len_in_block);
				s->cursor += MAXALIGN(len_in_block);
				gotlen += len_in_block;
			} while (gotlen < total_len);


			Assert(record->xl_tot_len == total_len);
			process_record((XLogRecord *) buf.data, start_lsn, lsn_base + s->cursor);
		}
	}
	Assert(s->cursor == s->len);
}

static void
process_record(XLogRecord *decoded_record, XLogRecPtr start_lsn, XLogRecPtr end_lsn)
{
	char	   *errormsg = "";

	zenith_log(ReceiverTrace, "processing record: xid=%d, xl_tot_len=%d, len_before_8k=%lu, lsn=%X/%X",
		decoded_record->xl_xid,
		decoded_record->xl_tot_len,
		XLOG_BLCKSZ - start_lsn % XLOG_BLCKSZ,
		LSN_PARTS(start_lsn));

	if (!is_valid_record(decoded_record))
		zenith_log(ERROR, "CRC check failed for record %X/%X", LSN_PARTS(start_lsn));

	XLogReaderState reader_state = {
		.ReadRecPtr = start_lsn,
		.EndRecPtr = end_lsn,
		.decoded_record = decoded_record
	};

	if (DecodeXLogRecord(&reader_state, decoded_record, &errormsg))
		handle_record(&reader_state);
	else
		zenith_log(ERROR, "failed to decode WAL record: %s", errormsg);
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

			PageWalHashKey key;
			key.system_identifier = client_system_id;
			key.rnode = rnode;
			key.blkno = blkno;

			memstore_insert(key, record->EndRecPtr, record->decoded_record);
		}
	}
}
