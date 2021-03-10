/*-------------------------------------------------------------------------
 *
 * walsender.c - stream WAL from safekeeper to pager
 *
 * Author: Konstantin Knizhnik (knizhnik@garret.ru)
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/safekeepr/wallsender.c
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

typedef struct WalSender
{
	struct WalSender* next; /* L2-List entry */
	struct WalSender* prev;
	pthread_t   thread;
	pgsocket    sock;
	char const* basedir;
	int         startupPacketLength;
	int         walSegSize;
} WalSender;

static WalSender walSenders = {&walSenders, &walSenders}; /* L2-List of active WAL senders */
static volatile bool streaming = true;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
static XLogRecPtr flushLsn;

/*
 * WAL sender main loop
 */
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
	pthread_mutex_lock(&mutex);
	flushLsn = lsn;
	pthread_cond_broadcast(&cond);
	pthread_mutex_unlock(&mutex);
}

static void*
WalSenderMain(void* arg)
{
	WalSender* ws = (WalSender*)arg;
	char*  msg;
	uint32 len;
	char*  startupBuf = NULL;
	char*  query = NULL;
	uint32 hi, lo;
	XLogRecPtr startpos;
	TimeLineID timeline;
	XLogSegNo segno;
	char   hdr[LIBPQ_HDR_SIZE];
	char   walfile_name[MAXPGPATH];
	char   walfile_path[MAXPGPATH];
	int    walfile = -1;
	uint32 msgSize;
	uint32 sendSize;
	char*  msgBuf = pg_malloc(LIBPQ_HDR_SIZE + XLOG_HDR_SIZE + MAX_SEND_SIZE);

	EnqueueWalSender(ws);

	/* As far as we are streaming wal in separate thread, then use blocking IO */
	if (!pg_set_block(ws->sock))
	{
		pg_log_error("Failed to switch socket to blocking mode");
		goto Epilogue;
	}
	/* Read and just ignire startup packet */
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
	if (sscanf(query, "START_REPLICATION %X/%X TIMELINE %u",
			   &hi, &lo, &timeline) != 3)
	{
		pg_log_error("Unexpected command '%s': START_REPLICATION expected", query);
		goto Epilogue;
	}
	startpos = ((XLogRecPtr)hi << 32) | lo;
	if (startpos == 0)
		startpos = FindStreamingStart(&timeline);

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

	/*
	 * Always start streaming at the beginning of a segment
	 */
	startpos -= XLogSegmentOffset(startpos, ws->walSegSize);

	while (streaming)
	{
		/* Wait until we have some data to stream */
		pthread_mutex_lock(&mutex);
		while (startpos >= flushLsn && streaming)
		{
			pthread_cond_wait(&cond, &mutex);
		}
		pthread_mutex_unlock(&mutex);
		if (!streaming)
			break;

		/* Open file if not opened yet */
		if (walfile < 0)
		{
			XLByteToSeg(startpos, segno, ws->walSegSize);
			XLogFileName(walfile_name, timeline, segno, ws->walSegSize);

			/* First try to open partial file, because it can be concurrenty renamed */
			sprintf(walfile_path, "%s/%s.partial", ws->basedir, walfile_name);
			walfile = open(walfile_path, O_RDONLY | PG_BINARY, 0);
			if (walfile < 0)
			{
				sprintf(walfile_path, "%s/%s", ws->basedir, walfile_name);
				walfile = open(walfile_path, O_RDONLY | PG_BINARY, 0);
				if (walfile < 0)
				{
					pg_log_error("Failed to open file %s: %s",
								 walfile_path, strerror(errno));
					goto Epilogue;
				}
			}
		}

		/* Avoid sending more than MAX_SEND_SIZE bytes */
		sendSize = Min((uint32)(flushLsn - startpos), MAX_SEND_SIZE);
		if (read(walfile, msgBuf + LIBPQ_HDR_SIZE + XLOG_HDR_SIZE, sendSize) != sendSize)
		{
			pg_log_error("Failed to read %d bytes from file %s: %s",
						 sendSize, walfile_path, strerror(errno));
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
		fe_sendint64(flushLsn, msg); /* walEnd */
		msg += 8;
		fe_sendint64(feGetCurrentTimestamp(), msg);	/* sendtime  */
		msg += 8;
		Assert(msg - msgBuf ==  LIBPQ_HDR_SIZE + XLOG_HDR_SIZE);
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
StartWalSender(pgsocket sock, char const* basedir, int startupPacketLength, int walSegSize)
{
	WalSender* ws = (WalSender*)pg_malloc(sizeof(WalSender));
	int rc;
	ws->sock = sock;
	ws->basedir = basedir;
	ws->startupPacketLength = startupPacketLength;
	ws->walSegSize = walSegSize;
	rc = pthread_create(&ws->thread, NULL, WalSenderMain, ws);
	if (rc != 0)
	{
		pg_log_error("Failed to lauch thread: %s", strerror(errno));
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
