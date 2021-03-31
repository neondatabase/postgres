/*-------------------------------------------------------------------------
 *
 * walsender.c - stream WAL from safekeeper to pager/replicas
 *
 * Author: Konstantin Knizhnik (knizhnik@garret.ru)
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/safekeeper/walsender.c
 *-------------------------------------------------------------------------
 */

#include "safekeeper.h"
#include "common/logging.h"
#include "streamutil.h"

#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>

static WalSender walSenders = {&walSenders, &walSenders}; /* L2-List of active WAL senders */
static volatile bool streaming = true;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
static XLogRecPtr commitLsn;

static void
EnqueueWalSender(WalSender* ws)
{
	pthread_mutex_lock(&mutex);
	ws->next = &walSenders;
	ws->prev = walSenders.prev;
	walSenders.prev = ws->prev->next = ws;
	pthread_mutex_unlock(&mutex);
}

static void
DequeueWalSender(WalSender* ws)
{
	pthread_mutex_lock(&mutex);
	ws->next->prev = ws->prev;
	ws->prev->next = ws->next;
	pthread_mutex_unlock(&mutex);
}

void
NotifyWalSenders(XLogRecPtr lsn)
{
	if (lsn > commitLsn)
	{
		commitLsn = lsn;
		pthread_cond_broadcast(&cond);
	}
}

/*
 * WAL sender main loop
 */
static void*
WalSenderMain(void* arg)
{
	WalSender* ws = (WalSender*)arg;
	char*  msg;
	uint32 len;
	char*  startupBuf = NULL;
	char*  query = NULL;
	uint32 w[4];
	XLogRecPtr startpos;
	XLogRecPtr endpos;
	TimeLineID timeline;
	XLogSegNo segno;
	char   hdr[LIBPQ_HDR_SIZE];
	char   walfile_name[MAXPGPATH];
	char   walfile_path[MAXPGPATH];
	char   response[REPLICA_FEEDBACK_SIZE];
	int    walfile = -1;
	uint32 msgSize;
	uint32 sendSize;
	char*  msgBuf = pg_malloc(LIBPQ_HDR_SIZE + XLOG_HDR_SIZE + MAX_SEND_SIZE);
	char const identifySystemResponseDesc[] = {
		0x54,0x00,0x00,0x00,0x6f,0x00,0x04,0x73,
		0x79,0x73,0x74,0x65,0x6d,0x69,0x64,0x00,
		0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
		0x00,0x19,0xff,0xff,0xff,0xff,0xff,0xff,
		0x00,0x00,0x74,0x69,0x6d,0x65,0x6c,0x69,
		0x6e,0x65,0x00,0x00,0x00,0x00,0x00,0x00,
		0x00,0x00,0x00,0x00,0x17,0x00,0x04,0xff,
		0xff,0xff,0xff,0x00,0x00,0x78,0x6c,0x6f,
		0x67,0x70,0x6f,0x73,0x00,0x00,0x00,0x00,
		0x00,0x00,0x00,0x00,0x00,0x00,0x19,0xff,
		0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x64,
		0x62,0x6e,0x61,0x6d,0x65,0x00,0x00,0x00,
		0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x19,
		0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00
	};

	EnqueueWalSender(ws);

	/* As far as we are streaming wal in separate thread, then use blocking IO */
	if (!pg_set_block(ws->sock))
	{
		pg_log_error("Failed to switch socket to blocking mode");
		goto Epilogue;
	}
	/* Read and just ignore startup packet */
	startupBuf = pg_malloc(LIBPQ_DATA_SIZE(ws->startupPacketLength));
	if (!ReadSocket(ws->sock, startupBuf, LIBPQ_DATA_SIZE(ws->startupPacketLength)))
	{
		pg_log_error("Failed to read startup packet");
		goto Epilogue;
	}
	/* Send handshake response */
	msg = msgBuf;
	*msg++ = 'R';
	fe_sendint32(8, msg);
	msg += 4;
	fe_sendint32(0, msg);
	msg += 4;
	*msg++ = 'Z';
	fe_sendint32(5, msg);
	msg += 4;
	*msg++ = 'I';
	if (!WriteSocket(ws->sock, msgBuf, msg - msgBuf))
	{
		pg_log_error("Failed to write connection handshake response");
		goto Epilogue;
	}

	/* Process replication command until we found START_REPLICATION */
	while (true)
	{
		if (!ReadSocket(ws->sock, hdr, sizeof hdr))
		{
			pg_log_error("Failed to read replication message header");
			goto Epilogue;
		}
		if (hdr[0] != 'Q')
		{
			pg_log_error("Unexpected message %c", hdr[0]);
			goto Epilogue;
		}
		len = fe_recvint32(&hdr[LIBPQ_MSG_SIZE_OFFS]);
		query = pg_malloc(LIBPQ_DATA_SIZE(len));
		if (!ReadSocket(ws->sock, query, LIBPQ_DATA_SIZE(len)))
		{
			pg_log_error("Failed to read replication message body");
			goto Epilogue;
		}
		if (strcmp(query, "IDENTIFY_SYSTEM") == 0)
		{
			char lsn_buf[32];
			char timeline_buf[32];
			char sysid_buf[32];
			int lsn_len;
			int timeline_len;
			int sysid_len;

			startpos = FindEndOfWAL(&timeline, false);
			lsn_len = sprintf(lsn_buf, "%X/%X", (uint32)(startpos>>32), (uint32)startpos);
			timeline_len = sprintf(timeline_buf, "%d", timeline);
			sysid_len = sprintf(sysid_buf, INT64_FORMAT, ws->systemId);

			msg = msgBuf;
			memcpy(msg, identifySystemResponseDesc, sizeof identifySystemResponseDesc);
			msg += sizeof(identifySystemResponseDesc);

			*msg++ = 'D';
			fe_sendint32(4 + 2 + 4 + sysid_len + 4 + timeline_len + 4 + lsn_len + 4, msg);
			msg += 4;

			fe_sendint16(4, msg); /* 4 columns */
			msg += 2;

			fe_sendint32(sysid_len, msg);
			msg += 4;
			memcpy(msg, sysid_buf, sysid_len);
			msg += sysid_len;

			fe_sendint32(timeline_len, msg);
			msg += 4;
			memcpy(msg, timeline_buf, timeline_len);
			msg += timeline_len;

			fe_sendint32(lsn_len, msg);
			msg += 4;
			memcpy(msg, lsn_buf, lsn_len);
			msg += lsn_len;

			fe_sendint32(-1, msg); /* null */
			msg += 4;

			*msg++ = 'C';
			fe_sendint32(4 + sizeof("IDENTIFY_SYSTEM"), msg);
			msg += 4;
			memcpy(msg, "IDENTIFY_SYSTEM", sizeof("IDENTIFY_SYSTEM"));
			msg += sizeof("IDENTIFY_SYSTEM");

			*msg++ = 'Z';
			fe_sendint32(5, msg);
			msg += 4;
			*msg++ = 'I';

			if (!WriteSocket(ws->sock, msgBuf, msg - msgBuf))
			{
				pg_log_error("Failed to write IDENTIFY_SYSTEM response");
				goto Epilogue;
			}
			pg_free(query);
			query = NULL;
		}
		else
		{
			int rc = sscanf(query, "START_REPLICATION %X/%X TIMELINE %u TILL %X/%X",
							&w[0], &w[1], &timeline, &w[2], &w[3]);
			if (rc < 3)
			{
				pg_log_error("Unexpected command '%s': START_REPLICATION expected", query);
				goto Epilogue;
			}
			startpos = ((XLogRecPtr)w[0] << 32) | w[1];
			if (startpos == 0)
				startpos = FindEndOfWAL(&timeline, false);
			if (rc == 5)
				ws->stopLsn = ((XLogRecPtr)w[2] << 32) | w[3];
			msg = msgBuf;
			*msg++ = 'W';
			fe_sendint32(7, msg);
			msg += 4;
			*msg++ = '\0';
			*msg++ = '\0';
			*msg++ = '\0';
			if (!WriteSocket(ws->sock, msgBuf, msg - msgBuf))
			{
				pg_log_error("Failed to initiate COPY protocol");
				goto Epilogue;
			}
			break;
		}
	}
	/*
	 * Always start streaming at the beginning of a segment
	 */
	startpos -= XLogSegmentOffset(startpos, ws->walSegSize);

	while (streaming)
	{
		/* Wait until we have some data to stream */
		if (ws->stopLsn) /* recovery mode: stream up to the specified LSN (VCL) */
		{
			if (startpos >= ws->stopLsn)
			{
				/* recovery finished */
				break;
			}
			endpos = ws->stopLsn;
		}
		else /* normal mode */
		{
			pthread_mutex_lock(&mutex);
			while (startpos >= commitLsn && streaming)
			{
				pthread_cond_wait(&cond, &mutex);
			}
			endpos = commitLsn;
			pthread_mutex_unlock(&mutex);
		}
		if (!streaming)
			break;

		/* Consume replica's feedbacks if any */
		while (ReadSocketNowait(ws->sock, hdr, sizeof hdr))
		{
			int len;
			if (hdr[0] != 'd')
				pg_log_info("Unexpected replica's feedback %c", hdr[0]);
			len = fe_recvint32(&hdr[LIBPQ_MSG_SIZE_OFFS]) - 4;
			if (len > REPLICA_FEEDBACK_SIZE)
				pg_log_info("Replica's feedback too large: %d", len);
			else if (!ReadSocket(ws->sock, response, len))
				pg_log_info("Failed to read relica's response");
			else if (response[0] == 'h')
			{
				char* hs = response+1;
				Assert(len == HS_FEEDBACK_SIZE);
				ws->hsFeedback.ts = fe_recvint64(hs);
				hs += 8;
				ws->hsFeedback.xmin = FullTransactionIdFromEpochAndXid(fe_recvint32(hs+4), fe_recvint32(hs));
				hs += 8;
				ws->hsFeedback.catalog_xmin = FullTransactionIdFromEpochAndXid(fe_recvint32(hs+4), fe_recvint32(hs));
			}
		}

		/* Open file if not opened yet */
		if (walfile < 0)
		{
			XLByteToSeg(startpos, segno, ws->walSegSize);
			XLogFileName(walfile_name, timeline, segno, ws->walSegSize);

			/* First try to open partial file, because it can be concurrently renamed */
			sprintf(walfile_path, "%s/%s.partial", ws->basedir, walfile_name);
			walfile = open(walfile_path, O_RDONLY | PG_BINARY, 0);
			if (walfile < 0)
			{
				sprintf(walfile_path, "%s/%s", ws->basedir, walfile_name);
				walfile = open(walfile_path, O_RDONLY | PG_BINARY, 0);
				if (walfile < 0)
				{
					pg_log_error("Failed to open file %s: %m",
								 walfile_path);
					goto Epilogue;
				}
			}
		}

		/* Avoid sending more than MAX_SEND_SIZE bytes */
		sendSize = Min((uint32)(endpos - startpos), MAX_SEND_SIZE);
		if (read(walfile, msgBuf + LIBPQ_HDR_SIZE + XLOG_HDR_SIZE, sendSize) != sendSize)
		{
			pg_log_error("Failed to read %d bytes from file %s: %m",
						 sendSize, walfile_path);
			goto Epilogue;
		}
		msgSize =  LIBPQ_HDR_SIZE + XLOG_HDR_SIZE + sendSize;
		msg = msgBuf;
		*msg++ = 'd'; /* copy data message */
		fe_sendint32(msgSize - LIBPQ_MSG_SIZE_OFFS, msg);
		msg += 4;
		*msg++ = 'w';
		fe_sendint64(startpos, msg);	/* dataStart */
		msg += 8;
		fe_sendint64(endpos, msg); /* walEnd */
		msg += 8;
		fe_sendint64(feGetCurrentTimestamp(), msg);	/* sendtime  */
		msg += 8;
		Assert(msg - msgBuf == LIBPQ_HDR_SIZE + XLOG_HDR_SIZE);
		if (!WriteSocket(ws->sock, msgBuf, msgSize))
			goto Epilogue;

		startpos += sendSize;
		if (XLogSegmentOffset(startpos, ws->walSegSize) == 0)
		{
			close(walfile);
			walfile = -1;
		}
	}

  Epilogue:
	if (walfile >= 0)
		close(walfile);
	closesocket(ws->sock);
	DequeueWalSender(ws);
	pg_free(ws);
	pg_free(msgBuf);
	pg_free(query);
	pg_free(startupBuf);
	return NULL;
}

/*
 * Start new thread for WAL sender at given socket.
 */
void
StartWalSender(pgsocket sock, char const* basedir, int startupPacketLength, int walSegSize, uint64 systemId)
{
	WalSender* ws = (WalSender*)pg_malloc(sizeof(WalSender));
	int rc;
	ws->sock = sock;
	ws->basedir = basedir;
	ws->startupPacketLength = startupPacketLength;
	ws->walSegSize = walSegSize;
	ws->systemId = systemId;
	ws->stopLsn = 0;
	ws->hsFeedback.ts = 0;
	ws->hsFeedback.xmin.value = ~0; /* max unsigned value */
	ws->hsFeedback.catalog_xmin.value = ~0; /* max unsigned value */
	rc = pthread_create(&ws->thread, NULL, WalSenderMain, ws);
	if (rc != 0)
	{
		pg_log_error("Failed to launch thread: %m");
		pg_free(ws);
	}
}

/*
 * Wait termination of all WAL senders
 */
void
StopWalSenders(void)
{
	WalSender* ws;

	pthread_mutex_lock(&mutex);
	streaming = false;
	pthread_cond_broadcast(&cond);
	while ((ws = walSenders.next) != &walSenders)
	{
		void* status;
		pthread_t t = ws->thread;
		pthread_mutex_unlock(&mutex);
		pthread_join(t, &status);
		pthread_mutex_lock(&mutex);
	}
	pthread_mutex_unlock(&mutex);
}

/*
 * Combine hot standby feedbacks from all replicas.
 */
void
CollectHotStanbyFeedbacks(HotStandbyFeedback* hs)
{
	WalSender* ws;

	hs->ts = 0;
	hs->xmin.value = ~0; /* largest unsigned value */
	hs->catalog_xmin.value = ~0; /* largest unsigned value */

	pthread_mutex_lock(&mutex);
	while ((ws = walSenders.next) != &walSenders)
	{
		if (ws->hsFeedback.ts != 0)
		{
			if (FullTransactionIdPrecedes(ws->hsFeedback.xmin, hs->xmin))
			{
				hs->xmin = ws->hsFeedback.xmin;
				hs->ts = ws->hsFeedback.ts;
			}
			if (FullTransactionIdPrecedes(ws->hsFeedback.catalog_xmin, hs->catalog_xmin))
			{
				hs->catalog_xmin = ws->hsFeedback.catalog_xmin;
				hs->ts = ws->hsFeedback.ts;
			}
		}
	}
	pthread_mutex_unlock(&mutex);
}
