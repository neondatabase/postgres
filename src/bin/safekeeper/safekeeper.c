/*-------------------------------------------------------------------------
 *
 * safekeeper.c - receive WAL stream fro safekeeper_proxy and write it
 *				  to a local file.
 *
 * Author: Konstantin Knizhnik (knizhnik@garret.ru)
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/safekeepr/safekeeper.c
 *-------------------------------------------------------------------------
 */

#include "safekeeper.h"

#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/logging.h"
#include "getopt_long.h"
#include "libpq-fe.h"
#include "streamutil.h"

/* Global options */
static int	 verbose = 0;
static bool  do_sync = true;
static char* basedir;
static char* host;
static char* port;

/* Node state */
SafeKeeperInfo myInfo;

/* Control file path */
static char	controlPath[MAXPGPATH];
static int  controlFile;

static RequestVote prop;

static pgsocket gateway = PGINVALID_SOCKET;
static pgsocket streamer = PGINVALID_SOCKET;

static bool AcceptNewConnection(void);

static void
usage(void)
{
	printf(_("%s receives PostgreSQL streaming write-ahead logs.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  -D, --directory=DIR    receive write-ahead log files into this directory\n"));
	printf(_("  -n, --no-sync          do not wait for changes to be written safely to disk\n"));
	printf(_("  -v, --verbose          output verbose messages\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nConnection options:\n"));
	printf(_("  -h, --host=HOSTNAME    safekeeper interface\n"));
	printf(_("  -p, --port=PORT        safekeeper port\n"));
	printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT);
	printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}

/*
 * Get destination directory.
 */
static DIR *
get_destination_dir(char *dest_folder)
{
	DIR		   *dir;

	Assert(dest_folder != NULL);
	dir = opendir(dest_folder);
	if (dir == NULL)
	{
		pg_log_error("could not open directory \"%s\": %m", basedir);
		exit(1);
	}

	return dir;
}


/*
 * Close existing directory.
 */
static void
close_destination_dir(DIR *dest_dir, char *dest_folder)
{
	Assert(dest_dir != NULL && dest_folder != NULL);
	if (closedir(dest_dir))
	{
		pg_log_error("could not close directory \"%s\": %m", dest_folder);
		exit(1);
	}
}

typedef struct XLogReaderContext
{
	XLogRecPtr start;
	XLogRecPtr end;
	TimeLineID tli;
} XLogReaderContext;

/* XLogReaderRoutine->segment_open callback */
static void
WALReaderOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo,
					 TimeLineID *tli_p)
{
	char  walfile_name[MAXPGPATH];
	char  walfile_path[MAXPGPATH];
	int   walSegSize = myInfo.server.walSegSize;

	XLogFileName(walfile_name, *tli_p, nextSegNo, walSegSize);
	sprintf(walfile_path, "%s/%s.partial", basedir, walfile_name);

	state->seg.ws_file = open(walfile_path, O_RDONLY | PG_BINARY, 0);
	if (state->seg.ws_file < 0)
	{
		pg_log_error("could not find file \"%s\": %m", walfile_path);
		exit(1);
	}
}

/*
 * XLogReaderRoutine->segment_close callback.
 */
static void
WALReaderCloseSegment(XLogReaderState *state)
{
	close(state->seg.ws_file);
	/* need to check errno? */
	state->seg.ws_file = -1;
}

/* XLogReaderRoutine->page_read callback */
static int
WALReaderReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				   XLogRecPtr targetPtr, char *readBuff)
{
	XLogReaderContext* ctx = (XLogReaderContext*)state->private_data;
	int			count = XLOG_BLCKSZ;
	WALReadError errinfo;

	if (ctx->end != InvalidXLogRecPtr)
	{
		if (targetPagePtr + XLOG_BLCKSZ <= ctx->end)
			count = XLOG_BLCKSZ;
		else if (targetPagePtr + reqLen <= ctx->end)
			count = ctx->end - targetPagePtr;
		else
		{
			ctx->end = targetPagePtr;
			return -1;
		}
	}

	return WALRead(state, readBuff, targetPagePtr, count, ctx->tli,  &errinfo)
		? count : -1;
}

static int
FindEndOfWALSegment(XLogSegNo segno, TimeLineID tli)
{
	XLogReaderContext ctx;
	XLogReaderState *xlogreader;
	int walSegSize = myInfo.server.walSegSize;
	char *errormsg;
	int offs = 0;

	XLogSegNoOffsetToRecPtr(segno, 0, walSegSize, ctx.start);
	ctx.end = ctx.start + walSegSize;
	ctx.tli = tli;
	xlogreader = XLogReaderAllocate(walSegSize, basedir,
									XL_ROUTINE(.page_read = WALReaderReadPage,
											   .segment_open = WALReaderOpenSegment,
											   .segment_close = WALReaderCloseSegment),
									&ctx);

	if (XLogFindNextRecord(xlogreader, ctx.start) != InvalidXLogRecPtr)
	{
		while (XLogReadRecord(xlogreader, &errormsg));
		offs = xlogreader->ReadRecPtr - ctx.start;
		Assert(offs <= walSegSize);
	}
	XLogReaderFree(xlogreader);
	return offs;
}

/*
 * Find end of WAL. If precise=true then scan last segment to locate last record.
 */
XLogRecPtr
FindEndOfWAL(TimeLineID *tli, bool precise)
{
	DIR		   *dir;
	struct dirent *dirent;
	XLogSegNo	high_segno = 0;
	TimeLineID	high_tli = 0;
	bool		high_ispartial = false;
	int         WalSegSz = myInfo.server.walSegSize;
	dir = get_destination_dir(basedir);

	while (errno = 0, (dirent = readdir(dir)) != NULL)
	{
		TimeLineID	tli;
		XLogSegNo	segno;
		bool		ispartial;

		/*
		 * Check if the filename looks like an xlog file, or a .partial file.
		 */
		if (IsXLogFileName(dirent->d_name))
		{
			ispartial = false;
		}
		else if (IsPartialXLogFileName(dirent->d_name))
		{
			ispartial = true;
		}
		else
			continue;

		/*
		 * Looks like an xlog file. Parse its position.
		 */
		XLogFromFileName(dirent->d_name, &tli, &segno, WalSegSz);

		/*
		 * Check that the segment has the right size, if it's supposed to be
		 * completed.
		 */
		if (!ispartial)
		{
			struct stat statbuf;
			char		fullpath[MAXPGPATH * 2];

			snprintf(fullpath, sizeof(fullpath), "%s/%s", basedir, dirent->d_name);
			if (stat(fullpath, &statbuf) != 0)
			{
				pg_log_error("could not stat file \"%s\": %m", fullpath);
				exit(1);
			}

			if (statbuf.st_size != WalSegSz)
			{
				pg_log_warning("segment file \"%s\" has incorrect size %lld, skipping",
							   dirent->d_name, (long long int) statbuf.st_size);
				continue;
			}
		}
		/* Looks like a valid segment. Remember that we saw it. */
		if ((segno > high_segno) ||
			(segno == high_segno && tli > high_tli) ||
			(segno == high_segno && tli == high_tli && high_ispartial && !ispartial))
		{
			high_segno = segno;
			high_tli = tli;
			high_ispartial = ispartial;
		}
	}

	if (errno)
	{
		pg_log_error("could not read directory \"%s\": %m", basedir);
		exit(1);
	}

	close_destination_dir(dir, basedir);

	if (high_segno > 0)
	{
		XLogRecPtr	high_ptr;
		uint32      high_offs = 0;
		/*
		 * Move the starting pointer to the start of the next segment, if the
		 * highest one we saw was completed.
		 */
		if (!high_ispartial)
		{
			high_segno++;
		}
		else if (precise) /* otherwise locate last record in last partial segment */
		{
			high_offs = FindEndOfWALSegment(high_segno, high_tli);
		}
		XLogSegNoOffsetToRecPtr(high_segno, high_offs, WalSegSz, high_ptr);

		*tli = high_tli;
		return high_ptr;
	}
	else
		return InvalidXLogRecPtr;
}

/*
 * Read from non-blocking socket and accept new connections.
 */
static bool
ReadStream(void* buf, size_t size)
{
	ssize_t rc;
	char*   src = (char*)buf;

	while (size != 0)
	{
		rc = (streamer == PGINVALID_SOCKET) ? -1 : recv(streamer, src, size, 0);
		if (rc < 0) {
			if (streamer == PGINVALID_SOCKET || errno == EWOULDBLOCK || errno == EAGAIN)
			{
				fd_set readSet;
				FD_ZERO(&readSet);
				FD_SET(gateway, &readSet);
				if (streamer != PGINVALID_SOCKET)
					FD_SET(streamer, &readSet);

				while ((rc = select(Max(streamer,gateway)+1, &readSet, NULL, NULL, NULL)) < 0 && errno == EINTR);
				if (rc > 0)
				{
					if (FD_ISSET(gateway, &readSet))
					{
						if (AcceptNewConnection())
							return false;
					}
					continue;
				}
				pg_log_error("Select failed: %m");
				if (streamer != PGINVALID_SOCKET)
				{
					closesocket(streamer);
					streamer = PGINVALID_SOCKET;
				}
				else
				{
					/* Bad gateway */
					exit(1);
				}
				return false;
			}
			if (errno == EINTR)
				continue;

			pg_log_error("Failed to read socket: %m");
			closesocket(streamer);
			streamer = PGINVALID_SOCKET;
			return false;
		}
		else if (rc == 0)
		{
			pg_log_error("Stream closed by peer: %m");
			closesocket(streamer);
			streamer = PGINVALID_SOCKET;
			return false;
		}
		src += rc;
		size -= rc;
	}
	return true;
}

/*
 * Accept new connection and if it is safekeeper_proxy than use Paxos-like algorithm to ensure that only one safekeeper_proxy is active.
 * Returns true if we safekeeper is switched to new streamer.
 */
static bool
AcceptNewConnection(void)
{
	pgsocket   sock;
	pgsocket   oldStreamer;
	ServerInfo serverInfo;
	NodeId     nodeId;
	uint32     len;

	while ((sock = accept(gateway, NULL, NULL)) == PGINVALID_SOCKET)
	{
		if (errno != EINTR)
		{
			pg_log_error("Failed to accept connection: %m");
			return false;
		}
	}
	/* Use non-blocking IO to make it possible to accept new connection while waiting for stream data */
	if (!pg_set_noblock(sock))
	{
		pg_log_error("Failed to switch socket to non-blocking mode: %m");
		closesocket(sock);
		return false;
	}
	/* Start handshake */
	oldStreamer = streamer;
	streamer = sock;
	/*
	 * If it is standard libpq connection (for example opened by Pager),
     * then there should be length of startup packet.
     * In case of safekeeper_proxy we pass 0 a length,
     * to distinguish it from standard replication connection.
	 */
	if (!ReadStream(&len, sizeof len))
	{
		pg_log_error("Failed to receive message length");
		closesocket(sock);
		streamer = oldStreamer;
		return false;
	}
	if (len != 0) /* Standard replication connection with startup package */
	{
		streamer = oldStreamer;
		if (myInfo.server.walSegSize == 0)
		{
			pg_log_error("Can not start replication before connecting to safekeeper_proxy");
			closesocket(sock);
		}
		else
		{
			/* Start new Wal sender thread */
			StartWalSender(sock, basedir, fe_recvint32((char*)&len), myInfo.server.walSegSize, myInfo.server.systemId);
		}
		return false;
	}
	/* Otherwise it is connection from safekeeper_proxy and we should read information about server */
	if (!ReadStream(&serverInfo, sizeof serverInfo))
	{
		pg_log_error("Failed to receive server info");
		closesocket(sock);
		streamer = oldStreamer;
		return false;
	}
	/* Check protocol compatibility */
	if (serverInfo.protocolVersion != SK_PROTOCOL_VERSION)
	{
		pg_log_error("Incompatible protocol version %d vs. %d", serverInfo.protocolVersion, SK_PROTOCOL_VERSION);
		/* Send my version of node-id so that server understand that protocol is not matched */
		WriteSocket(sock, &myInfo, sizeof myInfo);
		closesocket(sock);
		streamer = oldStreamer;
		return false;
	}
	/* Postgres upgrade is not treated as fatal error */
	if (serverInfo.pgVersion != myInfo.server.pgVersion && myInfo.server.pgVersion != UNKNOWN_SERVER_VERSION)
	{
		pg_log_info("Server version doesn't match %d vs. %d", serverInfo.pgVersion, myInfo.server.pgVersion);
		myInfo.server.pgVersion = serverInfo.pgVersion;
	}
	/* Preserve locally stored nodeId */
	nodeId = myInfo.server.nodeId;
	myInfo.server = serverInfo;
	myInfo.server.nodeId = nodeId;

	/* Calculate WAL end based on local data */
	myInfo.flushLsn = FindEndOfWAL(&myInfo.server.timeline, true);

	/* Report my identifier to proxy */
	if (!WriteSocket(sock, &myInfo, sizeof myInfo))
	{
		pg_log_info("Failed to send node info to proxy");
		closesocket(sock);
		streamer = oldStreamer;
		return false;
	}
	/* Wait for vote request */
	if (!ReadStream(&prop, sizeof prop))
	{
		pg_log_error("Failed to receive handshake response");
		closesocket(sock);
		streamer = oldStreamer;
		return false;
	}

	/* This is RAFT check which should ensure that only one master can perform commits */
	if (CompareNodeId(&prop.nodeId, &myInfo.server.nodeId) < 0)
	{
		pg_log_info("Reject connection attempt with term " INT64_FORMAT ", because my term is " INT64_FORMAT "",
					prop.nodeId.term, myInfo.server.nodeId.term);
		/* Send my node-id to inform proxy that it's candidate was rejected */
		WriteSocket(sock, &myInfo.server.nodeId, sizeof myInfo.server.nodeId);
		closesocket(sock);
		return false;
	}
	myInfo.server.nodeId = prop.nodeId;
	/* Need to persist our vote first */
	if (!SaveData(controlFile, &myInfo, sizeof myInfo, true))
	{
		pg_log_error("Failed to save safekeeper control file");
		exit(1);
	}
	/* Good by old streamer */
	closesocket(oldStreamer);

	/* Acknowledge the proposed candidate by returning it to the proxy */
	if (WriteSocket(sock, &prop.nodeId, sizeof prop.nodeId))
	{
		pg_log_info("Switch to new streamer with term " INT64_FORMAT,
					prop.nodeId.term);
	}
	else
	{
		pg_log_error("Failed to send handshake response");
		closesocket(sock);
		streamer = PGINVALID_SOCKET;
	}
	return true;
}

/*
 * Save received data in WAL files
 */
static bool
WriteWALFile(XLogRecPtr startpoint, char* rec, size_t recSize)
{
	XLogSegNo segno;
	char  walfile_name[MAXPGPATH];
	char  walfile_path[MAXPGPATH];
	char  walfile_partial_path[MAXPGPATH];
	int   walfile = -1;
	int	  xlogoff;
	int	  bytes_left = recSize;
	int	  bytes_written = 0;
	int   WalSegSz = myInfo.server.walSegSize;
	bool  partial = false;
	char* curr_file = walfile_path;
	static char zero_block[XLOG_BLCKSZ];

	/* Extract WAL location for this block */
	xlogoff = XLogSegmentOffset(startpoint, WalSegSz);

	while (bytes_left)
	{
		int			bytes_to_write;

		/*
		 * If crossing a WAL boundary, only write up until we reach wal
		 * segment size.
		 */
		if (xlogoff + bytes_left > WalSegSz)
			bytes_to_write = WalSegSz - xlogoff;
		else
			bytes_to_write = bytes_left;

		if (walfile < 0)
		{
			XLByteToSeg(startpoint, segno, WalSegSz);
			XLogFileName(walfile_name, myInfo.server.timeline, segno, WalSegSz);

			/* Try to open already completed segment */
			sprintf(walfile_path, "%s/%s", basedir, walfile_name);
			walfile = open(walfile_path, O_WRONLY | PG_BINARY, pg_file_create_mode);
			partial = false;
			if (walfile < 0)
			{
				/* Try to open existed partial file */
			    sprintf(walfile_partial_path, "%s/%s.partial", basedir, walfile_name);
				curr_file = walfile_partial_path;
				walfile = open(walfile_partial_path, O_WRONLY | PG_BINARY, pg_file_create_mode);
				if (walfile < 0)
				{
					/* Create and fill new partial file */
					walfile = open(walfile_partial_path, O_WRONLY | O_CREAT | PG_BINARY, pg_file_create_mode);
					if (walfile < 0)
					{
						pg_log_error("Failed to open WAL file %s: %m", curr_file);
						return false;
					}
					for (int i = WalSegSz/XLOG_BLCKSZ; --i >= 0;)
					{
						if (write(walfile, zero_block, XLOG_BLCKSZ) != XLOG_BLCKSZ)
						{
							pg_log_error("Failed to initialize WAL file %s: %m", curr_file);
							return false;
						}
					}
				}
				partial = true;
			}
		}

		if (pg_pwrite(walfile, rec + bytes_written, bytes_to_write, xlogoff) != bytes_to_write)
		{
			pg_log_error("could not write %u bytes to WAL file \"%s\": %m",
						 bytes_to_write, curr_file);
			return false;
		}

		if (do_sync)
		{
			if (fsync(walfile) < 0)
			{
				pg_log_error("failed to sync file \"%s\": %m", curr_file);
				return false;
			}
		}

		/* Write was successful, advance our position */
		bytes_written += bytes_to_write;
		bytes_left -= bytes_to_write;
		startpoint += bytes_to_write;
		xlogoff += bytes_to_write;

		/* Did we reach the end of a WAL segment? */
		if (XLogSegmentOffset(startpoint, WalSegSz) == 0)
		{
			if (close(walfile) != 0)
			{
				pg_log_error("failed to close file \"%s\": %m",
							 curr_file);
				return false;
			}
			if (partial)
			{
				if (durable_rename(walfile_partial_path, walfile_path) != 0)
				{
					pg_log_error("failed to rename file \"%s\" to \"%s\": %m",
								 walfile_partial_path, walfile_path);
					return false;
				}
			}
			xlogoff = 0;
			walfile = -1;
		}
	}
	if (walfile >= 0 && close(walfile) != 0)
	{
		pg_log_error("failed to close file \"%s\": %m", curr_file);
		return false;
	}
	return true;
}


/*
 * Start the log streaming
 */
static void
ReceiveWalStream(void)
{
	XLogRecPtr startPos;
	XLogRecPtr endPos;
	XLogRecPtr flushedRestartLsn = 0;
	size_t     recSize;
	char       buf[MAX_SEND_SIZE];
	SafekeeperRequest req;
	SafekeeperResponse resp;

	gateway = CreateSocket(host, port, 1);
	if (gateway == PGINVALID_SOCKET)
	{
		pg_log_error("Failed to connect to %s:%s", host, port);
		exit(1);
	}

	while (true)
	{
		bool syncControlFile = false;

		/* Receive message header */
		if (!ReadStream(&req, sizeof req))
			continue;

		if (CompareNodeId(&req.senderId, &myInfo.server.nodeId) != 0)
		{
			pg_log_info("Sender NodeId is changed");
			break;
		}
		if (req.beginLsn == END_OF_STREAM)
		{
			pg_log_info("Server stops streaming");
			break;
		}
		startPos = req.beginLsn;
		endPos = req.endLsn;
		recSize = (size_t)(endPos - startPos);
		Assert(recSize <= MAX_SEND_SIZE);

		/* Receive message body */
		if (!ReadStream(buf, recSize))
			continue;

		/* Save message in file */
		if (!WriteWALFile(startPos, buf, recSize))
			exit(1);

		myInfo.restartLsn = req.restartLsn;
		myInfo.commitLsn = req.commitLsn;

		/*
		 * Epoch switch happen when written WAL record cross the boundary.
         * The boundary is maximum of last WAL position at this node (FlushLSN) and global
		 * maximum (VCL) determined by safekeeper_proxy during handshake.
		 * Switching epoch means that node completes recovery and start writing in the WAL new data.
		 */
		if (myInfo.epoch < prop.epoch && endPos > Max(myInfo.flushLsn,prop.VCL))
		{
			if (verbose)
				pg_log_info("Switch to new epoch " INT64_FORMAT, prop.epoch);
			myInfo.epoch = prop.epoch; /* bump epoch */
			syncControlFile = true;
		}
		if (endPos > myInfo.flushLsn)
			myInfo.flushLsn = endPos;

		/*
		 * Update restart LSN in control file.
		 * To avoid negative impact on performance of extra fsync, do it only
		 * when restartLsn delta exceeds WAL segment size.
		 */
		syncControlFile |= flushedRestartLsn + myInfo.server.walSegSize < myInfo.restartLsn;
		if (!SaveData(controlFile, &myInfo, sizeof(myInfo), syncControlFile))
		{
			pg_log_error("Failed to save safekeeper control file");
			exit(1);
		}
		if (syncControlFile)
			flushedRestartLsn = myInfo.restartLsn;

		/* Report flush position */
		resp.epoch = myInfo.epoch;
		resp.flushLsn = endPos;
		CollectHotStanbyFeedbacks(&resp.hs);
		if (!WriteSocket(streamer, &resp, sizeof(resp)))
		{
			closesocket(streamer);
			streamer = PGINVALID_SOCKET;
		}
		/*
		 * Ping wal sender that new data is available.
         * FlushLSN (endPos) can be smaller than commitLSN in case we are at catching-up safekeeper.
		 */
		NotifyWalSenders(Min(req.commitLsn, endPos));
	}
	StopWalSenders();
}

int
main(int argc, char **argv)
{
	static struct option long_options[] = {
		{"help", no_argument, NULL, '?'},
		{"version", no_argument, NULL, 'V'},
		{"directory", required_argument, NULL, 'D'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"verbose", no_argument, NULL, 'v'},
		{"no-sync", no_argument, NULL, 'n'},
		{NULL, 0, NULL, 0}
	};

	int			c;
	int			option_index;
	ssize_t     rc;

	pg_logging_init(argv[0]);
	progname = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("safekeeper"));

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		else if (strcmp(argv[1], "-V") == 0 ||
				 strcmp(argv[1], "--version") == 0)
		{
			puts("safekeeper (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "D:h:p:nv",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'D':
				basedir = pg_strdup(optarg);
				break;
			case 'v':
				verbose++;
				break;
			case 'n':
				do_sync = false;
				break;
		    case 'h':
			    host = pg_strdup(optarg);
				break;
		    case 'p':
				port = pg_strdup(optarg);
				break;
			default:
				/*
				 * getopt_long already emitted a complaint
				 */
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
				exit(1);
		}
	}

	/*
	 * Any non-option arguments?
	 */
	if (optind < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/*
	 * Required arguments
	 */
	if (basedir == NULL)
	{
		pg_log_error("no target directory specified");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	sprintf(controlPath, "%s/safekeeper.control", basedir);
	controlFile = open(controlPath, O_RDWR | O_CREAT | PG_BINARY, pg_file_create_mode);
	if (controlFile < 0)
	{
		pg_log_error("Failed to create control file %s: %m", controlPath);
		exit(1);
	}
	rc = read(controlFile, &myInfo, sizeof myInfo);
	if (rc != sizeof(myInfo))
	{
		pg_log_error("Failed to load safekeeper control file %s", controlPath);
		myInfo.magic = SK_MAGIC;
		myInfo.formatVersion = SK_FORMAT_VERSION;
		myInfo.server.pgVersion = UNKNOWN_SERVER_VERSION;
	}
	else
	{
		if (myInfo.magic != SK_MAGIC)
		{
			pg_log_error("Invalid control file magic: %u", myInfo.magic);
			exit(1);
		}
		if (myInfo.formatVersion != SK_FORMAT_VERSION)
		{
			pg_log_error("Incompatible format version: %d vs. %d", myInfo.formatVersion, SK_FORMAT_VERSION);
			exit(1);
		}
	}
	ReceiveWalStream();

	return 0;
}
