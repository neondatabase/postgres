/*-------------------------------------------------------------------------
 *
 * zenith_wal_redo.c
 *	  Entry point for WAL redo helper
 *
 *
 * This file contains an alternative main() function for the 'postgres'
 * binary. In the special mode, we go into a special mode that's similar
 * to the single user mode. We don't launch postmaster or any auxiliary
 * processes. Instead, we wait for command from 'stdin', and respond to
 * 'stdout'.
 *
 * There's a TAP test for this in contrib/zenith_store/t/002_wal_redo_helper.pl
 *
 * The protocol through stdin/stdout is loosely based on the libpq protocol.
 * The process accepts messages through stdin, and each message has the format:
 *
 * char   msgtype;
 * int32  length; // length of message including 'length' but excluding
 *                // 'msgtype', in network byte order
 * <payload>
 *
 * There are three message types:
 *
 * BeginRedoForBlock ('B'): Prepare for WAL replay for given block
 * PushPage ('P'): Copy a page image (in the payload) to buffer cache
 * ApplyRecord ('A'): Apply a WAL record (in the payload)
 * GetPage ('G'): Return a page image from buffer cache.
 *
 * Currently, you only get a response to GetPage requests; the response is
 * simply a 8k page, without any headers. Errors are logged to stderr.
 *
 * FIXME:
 * - this currently requires a valid PGDATA, and creates a lock file there
 *   like a normal postmaster. There's no fundamental reason for that, though.
 * - should have EndRedoForBlock, and flush page cache, to allow using this
 *   mechanism for more than one block without restarting the process.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/tcop/zenith_wal_redo.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif
#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#if defined(HAVE_LIBSECCOMP) && defined(__GLIBC__)
#define MALLOC_NO_MMAP
#include <malloc.h>
#endif

#ifndef HAVE_GETRUSAGE
#include "rusagestub.h"
#endif

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "postmaster/seccomp.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"

static int	ReadRedoCommand(StringInfo inBuf);
static void BeginRedoForBlock(StringInfo input_message);
static void PushPage(StringInfo input_message);
static void ApplyRecord(StringInfo input_message);
static bool redo_block_filter(XLogReaderState *record, uint8 block_id);
static void GetPage(StringInfo input_message);
static ssize_t buffered_read(void *buf, size_t count);

static BufferTag target_redo_tag;

bool am_wal_redo_postgres;
static XLogReaderState* reader_state;

#define TRACE DEBUG5

#ifdef HAVE_LIBSECCOMP
static void
enter_seccomp_mode(void)
{
	PgSeccompRule syscalls[] =
	{
		/* Hard requirements */
		PG_SCMP_ALLOW(exit_group),
		PG_SCMP_ALLOW(pselect6),
		PG_SCMP_ALLOW(read),
		PG_SCMP_ALLOW(select),
		PG_SCMP_ALLOW(write),

		/* Memory allocation */
		PG_SCMP_ALLOW(brk),
#ifndef MALLOC_NO_MMAP
		/* TODO: musl doesn't have mallopt */
		PG_SCMP_ALLOW(mmap),
		PG_SCMP_ALLOW(munmap),
#endif

		/* Enable those for a proper shutdown.
		PG_SCMP_ALLOW(munmap),
		PG_SCMP_ALLOW(shmctl),
		PG_SCMP_ALLOW(shmdt),
		PG_SCMP_ALLOW(unlink), // shm_unlink
		*/
	};

#ifdef MALLOC_NO_MMAP
	/* Ask glibc not to use mmap() */
	mallopt(M_MMAP_MAX, 0);
#endif

	seccomp_load_rules(syscalls, lengthof(syscalls));
}
#endif

/* ----------------------------------------------------------------
 * FIXME comment
 * PostgresMain
 *	   postgres main loop -- all backends, interactive or otherwise start here
 *
 * argc/argv are the command line arguments to be used.  (When being forked
 * by the postmaster, these are not the original argv array of the process.)
 * dbname is the name of the database to connect to, or NULL if the database
 * name should be extracted from the command line arguments or defaulted.
 * username is the PostgreSQL user name to be used for the session.
 * ----------------------------------------------------------------
 */
void
WalRedoMain(int argc, char *argv[],
			const char *dbname,
			const char *username)
{
	int			firstchar;
	StringInfoData input_message;
#ifdef HAVE_LIBSECCOMP
	bool		enable_seccomp;
#endif

	/* Initialize startup process environment if necessary. */
	InitStandaloneProcess(argv[0]);

	SetProcessingMode(InitProcessing);
	am_wal_redo_postgres = true;

	/*
	 * Set default values for command-line options.
	 */
	InitializeGUCOptions();

	/* WAL REDO postgres do no need larger number of buffers.
	 * And speed of DropRelFileNodeAllLocalBuffers()is proportional to number of buffers.
	 * So let's keep it small enough (defaut value is 1024)
	 */
	num_temp_buffers = 4;

	/*
	 * Parse command-line options.
	 * TODO
	 */
	//process_postgres_switches(argc, argv, PGC_POSTMASTER, &dbname);

	/* Acquire configuration parameters */
	if (!SelectConfigFiles(NULL, progname))
		proc_exit(1);

	/*
	 * Set up signal handlers.  (InitPostmasterChild or InitStandaloneProcess
	 * has already set up BlockSig and made that the active signal mask.)
	 *
	 * Note that postmaster blocked all signals before forking child process,
	 * so there is no race condition whereby we might receive a signal before
	 * we have set up the handler.
	 *
	 * Also note: it's best not to use any signals that are SIG_IGNored in the
	 * postmaster.  If such a signal arrives before we are able to change the
	 * handler to non-SIG_IGN, it'll get dropped.  Instead, make a dummy
	 * handler in the postmaster to reserve the signal. (Of course, this isn't
	 * an issue for signals that are locally generated, such as SIGALRM and
	 * SIGPIPE.)
	 */
#if 0
	if (am_walsender)
		WalSndSignals();
	else
	{
		pqsignal(SIGHUP, SignalHandlerForConfigReload);
		pqsignal(SIGINT, StatementCancelHandler);	/* cancel current query */
		pqsignal(SIGTERM, die); /* cancel current query and exit */

		/*
		 * In a postmaster child backend, replace SignalHandlerForCrashExit
		 * with quickdie, so we can tell the client we're dying.
		 *
		 * In a standalone backend, SIGQUIT can be generated from the keyboard
		 * easily, while SIGTERM cannot, so we make both signals do die()
		 * rather than quickdie().
		 */
		if (IsUnderPostmaster)
			pqsignal(SIGQUIT, quickdie);	/* hard crash time */
		else
			pqsignal(SIGQUIT, die); /* cancel current query and exit */
		InitializeTimeouts();	/* establishes SIGALRM handler */

		/*
		 * Ignore failure to write to frontend. Note: if frontend closes
		 * connection, we will notice it and exit cleanly when control next
		 * returns to outer loop.  This seems safer than forcing exit in the
		 * midst of output during who-knows-what operation...
		 */
		pqsignal(SIGPIPE, SIG_IGN);
		pqsignal(SIGUSR1, procsignal_sigusr1_handler);
		pqsignal(SIGUSR2, SIG_IGN);
		pqsignal(SIGFPE, FloatExceptionHandler);

		/*
		 * Reset some signals that are accepted by postmaster but not by
		 * backend
		 */
		pqsignal(SIGCHLD, SIG_DFL); /* system() requires this on some
									 * platforms */
	}
#endif

	/*
	 * Validate we have been given a reasonable-looking DataDir and change into it.
	 */
	checkDataDir();
	ChangeToDataDir();

	/*
	 * Create lockfile for data directory.
	 */
	CreateDataDirLockFile(false);

	/* read control file (error checking and contains config ) */
	LocalProcessControlFile(false);

	process_shared_preload_libraries();

	/* Initialize MaxBackends (if under postmaster, was done already) */
	InitializeMaxBackends();

	/* Early initialization */
	BaseInit();

	/*
	 * Create a per-backend PGPROC struct in shared memory. We must do
	 * this before we can use LWLocks.
	 */
	InitAuxiliaryProcess();

	SetProcessingMode(NormalProcessing);

	/* Redo routines won't work if we're not "in recovery" */
	InRecovery = true;

	/*
	 * Create the memory context we will use in the main loop.
	 *
	 * MessageContext is reset once per iteration of the main loop, ie, upon
	 * completion of processing of each command message from the client.
	 */
	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_SIZES);

	/* we need a ResourceOwner to hold buffer pins */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "wal redo");

	/* Initialize resource managers */
	for (int rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_startup != NULL)
			RmgrTable[rmid].rm_startup();
	}
	reader_state = XLogReaderAllocate(wal_segment_size, NULL, XL_ROUTINE(), NULL);

#ifdef HAVE_LIBSECCOMP
	/* We prefer opt-out to opt-in for greater security */
	enable_seccomp = true;
	for (int i = 1; i < argc; i++)
		if (strcmp(argv[i], "--disable-seccomp") == 0)
			enable_seccomp = false;

	/*
	 * We deliberately delay the transition to the seccomp mode
	 * until it's time to enter the main processing loop;
	 * else we'd have to add a lot more syscalls to the allowlist.
	 */
	if (enable_seccomp)
		enter_seccomp_mode();
#endif

	/*
	 * Main processing loop
	 */
	MemoryContextSwitchTo(MessageContext);
	initStringInfo(&input_message);

	for (;;)
	{
		/*
		 * Release storage left over from prior query cycle, and create a new
		 * query input buffer in the cleared MessageContext.
		 */
		resetStringInfo(&input_message);

		set_ps_display("idle");

		/*
		 * (3) read a command (loop blocks here)
		 */
		firstchar = ReadRedoCommand(&input_message);
		switch (firstchar)
		{
			case 'B':			/* BeginRedoForBlock */
				BeginRedoForBlock(&input_message);
				break;

			case 'P':			/* PushPage */
				PushPage(&input_message);
				break;

			case 'A':			/* ApplyRecord */
				ApplyRecord(&input_message);
				break;

			case 'G':			/* GetPage */
				GetPage(&input_message);
				break;

				/*
				 * EOF means we're done. Perform normal shutdown.
				 */
			case EOF:
				ereport(LOG,
						(errmsg("received EOF on stdin, shutting down")));

#ifdef HAVE_LIBSECCOMP
				/*
				 * Skip the shutdown sequence, leaving some garbage behind.
				 * Hopefully, postgres will clean it up in the next run.
				 * This way we don't have to enable extra syscalls, which is nice.
				 * See enter_seccomp_mode() above.
				 */
				if (enable_seccomp)
					_exit(0);
#endif
				/*
				 * NOTE: if you are tempted to add more code here, DON'T!
				 * Whatever you had in mind to do should be set up as an
				 * on_proc_exit or on_shmem_exit callback, instead. Otherwise
				 * it will fail to be called during other backend-shutdown
				 * scenarios.
				 */
				proc_exit(0);

			default:
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid frontend message type %d",
								firstchar)));
		}
	}							/* end of input-reading loop */
}

/*
 * Some debug function that may be handy for now.
 */
pg_attribute_unused()
static char *
pprint_buffer(char *data, int len)
{
	StringInfoData s;
	initStringInfo(&s);
	appendStringInfo(&s, "\n");
	for (int i = 0; i < len; i++) {

		appendStringInfo(&s, "%02x ", (*(((char *) data) + i) & 0xff) );
		if (i % 32 == 31) {
			appendStringInfo(&s, "\n");
		}
	}
	appendStringInfo(&s, "\n");

	return s.data;
}

/* ----------------------------------------------------------------
 *		routines to obtain user input
 * ----------------------------------------------------------------
 */

/*
 * Read next command from the client.
 *
 *	the string entered by the user is placed in its parameter inBuf,
 *	and we act like a Q message was received.
 *
 *	EOF is returned if end-of-file input is seen; time to shut down.
 * ----------------
 */
static int
ReadRedoCommand(StringInfo inBuf)
{
	ssize_t		ret;
	char		hdr[1 + sizeof(int32)];
	int			qtype;
	int32		len;

	/* Read message type and message length */
	ret = buffered_read(hdr, sizeof(hdr));
	if (ret != sizeof(hdr))
	{
		if (ret == 0)
			return EOF;
		else if (ret < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not read message header: %m")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected EOF")));
	}

	qtype = hdr[0];
	memcpy(&len, &hdr[1], sizeof(int32));
	len = pg_ntoh32(len);

	if (len < 4)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message length")));

	len -= 4;					/* discount length itself */

	/* Read the message payload */
	enlargeStringInfo(inBuf, len);
	ret = buffered_read(inBuf->data, len);
	if (ret != len)
	{
		if (ret < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not read message: %m")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected EOF")));
	}
	inBuf->len = len;
	inBuf->data[len] = '\0';

	return qtype;
}

/*
 * Prepare for WAL replay on given block
 */
static void
BeginRedoForBlock(StringInfo input_message)
{
	RelFileNode rnode;
	ForkNumber forknum;
	BlockNumber blknum;
	SMgrRelation reln;

	/*
	 * message format:
	 *
	 * spcNode
	 * dbNode
	 * relNode
	 * ForkNumber
	 * BlockNumber
	 */
	forknum = pq_getmsgbyte(input_message);
	rnode.spcNode = pq_getmsgint(input_message, 4);
	rnode.dbNode = pq_getmsgint(input_message, 4);
	rnode.relNode = pq_getmsgint(input_message, 4);
	blknum = pq_getmsgint(input_message, 4);

	INIT_BUFFERTAG(target_redo_tag, rnode, forknum, blknum);

	elog(TRACE, "BeginRedoForBlock %u/%u/%u.%d blk %u",
		 target_redo_tag.rnode.spcNode,
		 target_redo_tag.rnode.dbNode,
		 target_redo_tag.rnode.relNode,
		 target_redo_tag.forkNum,
		 target_redo_tag.blockNum);

	reln = smgropen(rnode, InvalidBackendId, RELPERSISTENCE_PERMANENT);
	if (reln->smgr_cached_nblocks[forknum] == InvalidBlockNumber ||
		reln->smgr_cached_nblocks[forknum] < blknum + 1)
	{
		reln->smgr_cached_nblocks[forknum] = blknum + 1;
	}
}

/*
 * Receive a page given by the client, and put it into buffer cache.
 */
static void
PushPage(StringInfo input_message)
{
	RelFileNode rnode;
	ForkNumber forknum;
	BlockNumber blknum;
	const char *content;
	Buffer		buf;
	Page		page;

	/*
	 * message format:
	 *
	 * spcNode
	 * dbNode
	 * relNode
	 * ForkNumber
	 * BlockNumber
	 * 8k page content
	 */
	forknum = pq_getmsgbyte(input_message);
	rnode.spcNode = pq_getmsgint(input_message, 4);
	rnode.dbNode = pq_getmsgint(input_message, 4);
	rnode.relNode = pq_getmsgint(input_message, 4);
	blknum = pq_getmsgint(input_message, 4);
	content = pq_getmsgbytes(input_message, BLCKSZ);

	buf = ReadBufferWithoutRelcache(rnode, forknum, blknum, RBM_ZERO_AND_LOCK, NULL);
	page = BufferGetPage(buf);
	memcpy(page, content, BLCKSZ);
	MarkBufferDirty(buf); /* pro forma */
	UnlockReleaseBuffer(buf);
}

/*
 * Receive a WAL record, and apply it.
 *
 * All the pages should be loaded into the buffer cache by PushPage calls already.
 */
static void
ApplyRecord(StringInfo input_message)
{
	/* recovery here */
	char	   *errormsg;
	XLogRecPtr	lsn;
	XLogRecord *record;
	int			nleft;

	/*
	 * message format:
	 *
	 * LSN (the *end* of the record)
	 * record
	 */
	lsn = pq_getmsgint64(input_message);

	/* note: the input must be aligned here */
	record = (XLogRecord *) pq_getmsgbytes(input_message, sizeof(XLogRecord));

	nleft = input_message->len - input_message->cursor;
	if (record->xl_tot_len != sizeof(XLogRecord) + nleft)
		elog(ERROR, "mismatch between record (%d) and message size (%d)",
			 record->xl_tot_len, (int) sizeof(XLogRecord) + nleft);

	XLogBeginRead(reader_state, lsn);
	reader_state->decoded_record = record;
	if (!DecodeXLogRecord(reader_state, record, &errormsg))
		elog(ERROR, "failed to decode WAL record: %s", errormsg);

	/* Ignore any other blocks than the ones the caller is interested in */
	redo_read_buffer_filter = redo_block_filter;

	RmgrTable[record->xl_rmid].rm_redo(reader_state);
	redo_read_buffer_filter = NULL;

	elog(TRACE, "applied WAL record with LSN %X/%X",
		 (uint32) (lsn >> 32), (uint32) lsn);
}

static bool
redo_block_filter(XLogReaderState *record, uint8 block_id)
{
	BufferTag	target_tag;

	if (!XLogRecGetBlockTag(record, block_id,
							&target_tag.rnode, &target_tag.forkNum, &target_tag.blockNum))
	{
		/* Caller specified a bogus block_id */
		elog(PANIC, "failed to locate backup block with ID %d", block_id);
	}

	/*
	 * If this block isn't one we are currently restoring, then return 'true'
	 * so that this gets ignored
	 */
	return !BUFFERTAGS_EQUAL(target_tag, target_redo_tag);
}

/*
 * Get a page image back from buffer cache.
 *
 * After applying some records.
 */
static void
GetPage(StringInfo input_message)
{
	RelFileNode rnode;
	ForkNumber forknum;
	BlockNumber blknum;
	Buffer		buf;
	Page		page;
	int			tot_written;

	/*
	 * message format:
	 *
	 * spcNode
	 * dbNode
	 * relNode
	 * ForkNumber
	 * BlockNumber
	 */
	forknum = pq_getmsgbyte(input_message);
	rnode.spcNode = pq_getmsgint(input_message, 4);
	rnode.dbNode = pq_getmsgint(input_message, 4);
	rnode.relNode = pq_getmsgint(input_message, 4);
	blknum = pq_getmsgint(input_message, 4);

	/* FIXME: check that we got a BeginRedoForBlock message or this earlier */

	buf = ReadBufferWithoutRelcache(rnode, forknum, blknum, RBM_NORMAL, NULL);
	page = BufferGetPage(buf);
	/* single thread, so don't bother locking the page */

	/* Response: Page content */
	tot_written = 0;
	do {
		ssize_t		rc;

		rc = write(STDOUT_FILENO, &page[tot_written], BLCKSZ - tot_written);
		if (rc < 0) {
			/* If interrupted by signal, just retry */
			if (errno == EINTR)
				continue;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to stdout: %m")));
		}
		tot_written += rc;
	} while (tot_written < BLCKSZ);

	ReleaseBuffer(buf);
	DropRelFileNodeAllLocalBuffers(rnode);
	smgrinit(); //reset inmem smgr state

	elog(TRACE, "Page sent back for block %u", blknum);
}


/* Buffer used by buffered_read() */
static char stdin_buf[16 * 1024];
static size_t stdin_len = 0;	/* # of bytes in buffer */
static size_t stdin_ptr = 0;	/* # of bytes already consumed */

/*
 * Like read() on stdin, but buffered.
 *
 * We cannot use libc's buffered fread(), because it uses syscalls that we
 * have disabled with seccomp(). Depending on the platform, it can call
 * 'fstat' or 'newfstatat'. 'fstat' is probably harmless, but 'newfstatat'
 * seems problematic because it allows interrogating files by path name.
 *
 * The return value is the number of bytes read. On error, -1 is returned, and
 * errno is set appropriately. Unlike read(), this fills the buffer completely
 * unless an error happens or EOF is reached.
 */
static ssize_t
buffered_read(void *buf, size_t count)
{
	char	   *dst = buf;

	while (count > 0)
	{
		size_t		nthis;

		if (stdin_ptr == stdin_len)
		{
			ssize_t		ret;

			ret = read(STDIN_FILENO, stdin_buf, sizeof(stdin_buf));
			if (ret < 0)
			{
				/* don't do anything here that could set 'errno' */
				return ret;
			}
			if (ret == 0)
			{
				/* EOF */
				break;
			}
			stdin_len = (size_t) ret;
			stdin_ptr = 0;
		}
		nthis = Min(stdin_len - stdin_ptr, count);

		memcpy(dst, &stdin_buf[stdin_ptr], nthis);

		stdin_ptr += nthis;
		count -= nthis;
		dst += nthis;
	}

	return (dst - (char *) buf);
}
