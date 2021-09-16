/*-------------------------------------------------------------------------
 *
 * walproposer.c
 *
 * Proposer/leader part of the total order broadcast protocol between postgres
 * and WAL safekeepers.
 *
 * We have two ways of launching WalProposer:
 *
 *   1. As a background worker which will run physical WalSender with
 *      am_wal_proposer flag set to true. WalSender in turn would handle WAL
 *      reading part and call WalProposer when ready to scatter WAL.
 *
 *   2. As a standalone utility by running `postgres --sync-safekeepers`. That
 *      is needed to create LSN from which it is safe to start postgres. More
 *      specifically it addresses following problems:
 *
 *      a) Chicken-or-the-egg problem: compute postgres needs data directory
 *         with non-rel files that are downloaded from pageserver by calling
 *         basebackup@LSN. This LSN is not arbitrary, it must include all
 *         previously committed transactions and defined through consensus
 *         voting, which happens... in walproposer, a part of compute node.
 *
 *      b) Just warranting such LSN is not enough, we must also actually commit
 *         it and make sure there is a safekeeper who knows this LSN is
 *         committed so WAL before it can be streamed to pageserver -- otherwise
 *         basebackup will hang waiting for WAL. Advancing commit_lsn without
 *         playing consensus game is impossible, so speculative 'let's just poll
 *         safekeepers, learn start LSN of future epoch and run basebackup'
 *         won't work.
 *
 *      TODO: check that LSN on safekeepers after start is the same as it was
 *            after `postgres --sync-safekeepers`.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include "access/xlogdefs.h"
#include "replication/walproposer.h"
#include "storage/latch.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/xlog.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/pmsignal.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"


char	   *wal_acceptors_list;
int			wal_acceptor_reconnect_timeout;
bool		am_wal_proposer;

/* Declared in walproposer.h, defined here, initialized in libpqwalproposer.c */
WalProposerFunctionsType *WalProposerFunctions = NULL;

#define WAL_PROPOSER_SLOT_NAME "wal_proposer_slot"

static int	n_walkeepers = 0;
static int	quorum = 0;
static WalKeeper walkeeper[MAX_WALKEEPERS];
static WalMessage *msgQueueHead;
static WalMessage *msgQueueTail;
static XLogRecPtr lastSentLsn;	/* WAL has been appended to msg queue up to
								 * this point */
static XLogRecPtr lastSentCommitLsn;	/* last commitLsn broadcast to
										 * walkeepers */
static ProposerGreeting proposerGreeting;
static WaitEventSet *waitEvents;
static AppendResponse lastFeedback;
/*
 *  minimal LSN which may be needed for recovery of some safekeeper (end lsn
 *  + 1 of last chunk streamed to everyone)
 */
static XLogRecPtr truncateLsn;
static XLogRecPtr candidateTruncateLsn;
static VoteRequest voteRequest; /* Vote request for walkeeper */
static term_t propTerm;			/* term of the proposer */
static XLogRecPtr propEpochStartLsn;	/* epoch start lsn of the proposer */
static term_t donorEpoch;		/* Most advanced acceptor epoch */
static int	donor;				/* Most advanced acceptor */
static int	n_votes = 0;
static int	n_connected = 0;
static TimestampTz last_reconnect_attempt;

/* Set to true only in standalone run of `postgres --sync-safekeepers` (see comment on top) */
static bool syncSafekeepers;

/* Declarations of a few functions ahead of time, so that we can define them out of order. */
static void AdvancePollState(int i, uint32 events);
static bool AsyncRead(int i, void *value, size_t value_size);
static bool BlockingWrite(int i, void *msg, size_t msg_size, WalKeeperState success_state);
static bool AsyncWrite(int i, void *msg, size_t msg_size, WalKeeperState flush_state, WalKeeperState success_state);
static bool AsyncFlush(int i, bool socket_read_ready, WalKeeperState success_state);
static void HackyRemoveWalProposerEvent(int to_remove);
static WalMessage *CreateMessageCommitLsnOnly(XLogRecPtr lsn);
static void BroadcastMessage(WalMessage *msg);


/*
 * Combine hot standby feedbacks from all walkeepers.
 */
static void
CombineHotStanbyFeedbacks(HotStandbyFeedback * hs)
{
	hs->ts = 0;
	hs->xmin.value = ~0;		/* largest unsigned value */
	hs->catalog_xmin.value = ~0;	/* largest unsigned value */

	for (int i = 0; i < n_walkeepers; i++)
	{
		if (walkeeper[i].feedback.hs.ts != 0)
		{
			if (FullTransactionIdPrecedes(walkeeper[i].feedback.hs.xmin, hs->xmin))
			{
				hs->xmin = walkeeper[i].feedback.hs.xmin;
				hs->ts = walkeeper[i].feedback.hs.ts;
			}
			if (FullTransactionIdPrecedes(walkeeper[i].feedback.hs.catalog_xmin, hs->catalog_xmin))
			{
				hs->catalog_xmin = walkeeper[i].feedback.hs.catalog_xmin;
				hs->ts = walkeeper[i].feedback.hs.ts;
			}
		}
	}
}

/* Initializes the internal event set, provided that it is currently null */
static void
InitEventSet(void)
{
	if (waitEvents)
		elog(FATAL, "double-initialization of event set");

	waitEvents = CreateWaitEventSet(TopMemoryContext, 2 + n_walkeepers);
	AddWaitEventToSet(waitEvents, WL_LATCH_SET, PGINVALID_SOCKET,
					  MyLatch, NULL);
	AddWaitEventToSet(waitEvents, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
					  NULL, NULL);
}

/*
 * Updates the events we're already waiting on for the WAL keeper, setting it to
 * the provided `events`
 *
 * This function is called any time the WAL keeper's state switches to one where
 * it has to wait to continue. This includes the full body of AdvancePollState
 * and each call to AsyncRead/BlockingWrite/AsyncWrite/AsyncFlush.
 */
static void
UpdateEventSet(WalKeeper *wk, uint32 events)
{
	/* eventPos = -1 when we don't have an event */
	Assert(wk->eventPos != -1);

	ModifyWaitEvent(waitEvents, wk->eventPos, events, NULL);
}

/* Hack: provides a way to remove the event corresponding to an individual walproposer from the set.
 *
 * Note: Internally, this completely reconstructs the event set. It should be avoided if possible.
 */
static void
HackyRemoveWalProposerEvent(int to_remove)
{
	/* Remove the existing event set */
	if (waitEvents)
	{
		FreeWaitEventSet(waitEvents);
		waitEvents = NULL;
	}
	/* Re-initialize it without adding any walkeeper events */
	InitEventSet();

	/*
	 * loop through the existing walkeepers. If they aren't the one we're
	 * removing, and if they have a socket we can use, re-add the applicable
	 * events.
	 */
	for (int i = 0; i < n_walkeepers; i++)
	{
		uint32		desired_events = WL_NO_EVENTS;
		WalKeeper  *wk = &walkeeper[i];

		wk->eventPos = -1;

		if (i == to_remove)
			continue;

		/* If this WAL keeper isn't offline, add an event for it! */
		if ((desired_events = WalKeeperStateDesiredEvents(wk->state)))
		{
			wk->eventPos = AddWaitEventToSet(waitEvents, desired_events, walprop_socket(wk->conn), NULL, wk);
		}
	}
}

/* Shuts down and cleans up the connection for a walkeeper. Sets its state to SS_OFFLINE */
static void
ShutdownConnection(int i)
{
	if (walkeeper[i].conn)
		walprop_finish(walkeeper[i].conn);
	walkeeper[i].conn = NULL;
	walkeeper[i].state = SS_OFFLINE;
	walkeeper[i].currMsg = NULL;

	HackyRemoveWalProposerEvent(i);
}

/*
 * This function is called to establish new connection or to reestablish
 * connection in case of connection failure.
 *
 * On success, sets the state to SS_CONNECTING_WRITE.
 */
static void
ResetConnection(int i)
{
	pgsocket	sock;			/* socket of the new connection */
	WalKeeper  *wk = &walkeeper[i];

	if (wk->state != SS_OFFLINE)
	{
		ShutdownConnection(i);
	}

	/*
	 * Try to establish new connection
	 *
	 * If the connection information hasn't been filled out, we need to do
	 * that here.
	 */
	if (wk->conninfo[0] == '\0')
	{
		sprintf((char *) &wk->conninfo,
				"host=%s port=%s dbname=replication options='-c ztimelineid=%s ztenantid=%s'",
				wk->host, wk->port, zenith_timeline_walproposer, zenith_tenant_walproposer);
	}

	wk->conn = walprop_connect_start((char *) &wk->conninfo);

	/*
	 * "If the result is null, then libpq has been unable to allocate a new
	 * PGconn structure"
	 */
	if (!wk->conn)
		elog(FATAL, "failed to allocate new PGconn object");

	/*
	 * PQconnectStart won't actually start connecting until we run
	 * PQconnectPoll. Before we do that though, we need to check that it
	 * didn't immediately fail.
	 */
	if (walprop_status(wk->conn) == WP_CONNECTION_BAD)
	{
		/*---
		 * According to libpq docs:
		 *   "If the result is CONNECTION_BAD, the connection attempt has already failed,
		 *    typically because of invalid connection parameters."
		 * We should report this failure.
		 *
		 * https://www.postgresql.org/docs/devel/libpq-connect.html#LIBPQ-PQCONNECTSTARTPARAMS
		 */
		elog(WARNING, "Immediate failure to connect with node:\n\t%s\n\terror: %s",
			 wk->conninfo, walprop_error_message(wk->conn));

		/*
		 * Even though the connection failed, we still need to clean up the
		 * object
		 */
		walprop_finish(wk->conn);
		wk->conn = NULL;
		return;
	}

	/*
	 * The documentation for PQconnectStart states that we should call
	 * PQconnectPoll in a loop until it returns PGRES_POLLING_OK or
	 * PGRES_POLLING_FAILED. The other two possible returns indicate whether
	 * we should wait for reading or writing on the socket. For the first
	 * iteration of the loop, we're expected to wait until the socket becomes
	 * writable.
	 *
	 * The wording of the documentation is a little ambiguous; thankfully
	 * there's an example in the postgres source itself showing this behavior.
	 * (see libpqrcv_connect, defined in
	 * src/backend/replication/libpqwalreceiver/libpqwalreceiver.c)
	 */
	elog(LOG, "Connecting with node %s:%s", wk->host, wk->port);

	wk->state = SS_CONNECTING_WRITE;

	sock = walprop_socket(wk->conn);
	wk->eventPos = AddWaitEventToSet(waitEvents, WL_SOCKET_WRITEABLE, sock, NULL, wk);
	return;
}

/*
 * Calculate WAL position acknowledged by quorum
 */
static XLogRecPtr
GetAcknowledgedByQuorumWALPosition(void)
{
	XLogRecPtr	responses[MAX_WALKEEPERS];

	/*
	 * Sort acknowledged LSNs
	 */
	for (int i = 0; i < n_walkeepers; i++)
	{
		/*
		 * Note that while we haven't pushed WAL up to epoch start lsn to the
		 * majority we don't really know which LSN is reliably committed as
		 * reported flush_lsn is physical end of wal, which can contain
		 * diverged history (compared to donor).
		 */
		responses[i] = walkeeper[i].feedback.epoch == propTerm
			? walkeeper[i].feedback.flushLsn : 0;
	}
	qsort(responses, n_walkeepers, sizeof(XLogRecPtr), CompareLsn);

	/*
	 * Get the smallest LSN committed by quorum
	 */
	return responses[n_walkeepers - quorum];
}

static void
HandleWalKeeperResponse(void)
{
	HotStandbyFeedback hsFeedback;
	XLogRecPtr	minQuorumLsn;

	minQuorumLsn = GetAcknowledgedByQuorumWALPosition();
	if (minQuorumLsn > lastFeedback.flushLsn)
	{
		lastFeedback.flushLsn = minQuorumLsn;
		/* advance the replication slot */
		if (!syncSafekeepers)
			ProcessStandbyReply(minQuorumLsn, minQuorumLsn, InvalidXLogRecPtr, GetCurrentTimestamp(), false);
	}
	CombineHotStanbyFeedbacks(&hsFeedback);
	if (hsFeedback.ts != 0 && memcmp(&hsFeedback, &lastFeedback.hs, sizeof hsFeedback) != 0)
	{
		lastFeedback.hs = hsFeedback;
		if (!syncSafekeepers)
			ProcessStandbyHSFeedback(hsFeedback.ts,
									 XidFromFullTransactionId(hsFeedback.xmin),
									 EpochFromFullTransactionId(hsFeedback.xmin),
									 XidFromFullTransactionId(hsFeedback.catalog_xmin),
									 EpochFromFullTransactionId(hsFeedback.catalog_xmin));
	}


	/* Cleanup message queue */
	while (msgQueueHead != NULL && msgQueueHead->ackMask == ((1 << n_walkeepers) - 1))
	{
		WalMessage *msg = msgQueueHead;

		msgQueueHead = msg->next;

		/*
		 * This piece is received by everyone; try to advance truncateLsn, but
		 * hold it back to nearest commitLsn. Thus we will always start
		 * streaming from the beginning of the record, which simplifies
		 * decoding on the far end.
		 *
		 * This also prevents surprising violation of truncateLsn <= commitLsn
		 * invariant which might occur because 1) truncateLsn can be advanced
		 * immediately once chunk is broadcast to all safekeepers, and
		 * commitLsn generally can't be advanced based on feedback from
		 * safekeeper who is still in the previous epoch (similar to 'leader
		 * can't commit entries from previous term' in Raft); 2) chunks we
		 * read from WAL and send are plain sheets of bytes, but safekeepers
		 * ack only on commit boundaries.
		 */
		if (msg->req.endLsn >= minQuorumLsn && minQuorumLsn != InvalidXLogRecPtr)
		{
			truncateLsn = minQuorumLsn;
			candidateTruncateLsn = InvalidXLogRecPtr;
		}
		else if (msg->req.endLsn >= candidateTruncateLsn &&
				 candidateTruncateLsn != InvalidXLogRecPtr)
		{
			truncateLsn = candidateTruncateLsn;
			candidateTruncateLsn = InvalidXLogRecPtr;
		}
		memset(msg, 0xDF, sizeof(WalMessage) + msg->size - sizeof(AppendRequestHeader));
		free(msg);
	}
	if (!msgQueueHead)			/* queue is empty */
		msgQueueTail = NULL;

	/*
	 * Generally sync is done when majority switched the epoch so we committed
	 * epochStartLsn and made the majority aware of it, ensuring they are
	 * ready to give all WAL to pageserver. It would mean whichever majority
	 * is alive, there will be at least one safekeeper who is able to stream
	 * WAL to pageserver to make basebackup possible. However, since at the
	 * moment we don't have any good mechanism of defining the healthy and
	 * most advanced safekeeper who should push the wal into pageserver and
	 * basically the random one gets connected, to prevent hanging basebackup
	 * (due to pageserver connecting to not-synced-walkeeper) we currently
	 * wait for all seemingly alive walkeepers to get synced.
	 */
	if (syncSafekeepers)
	{
		int			n_synced;

		n_synced = 0;
		for (int i = 0; i < n_walkeepers; i++)
		{
			WalKeeper  *wk = &walkeeper[i];
			bool		synced = wk->feedback.commitLsn >= propEpochStartLsn;

			/* alive safekeeper which is not synced yet; wait for it */
			if (wk->state != SS_OFFLINE && !synced)
				return;
			if (synced)
				n_synced++;
		}
		if (n_synced >= quorum)
		{
			/* All walkeepers synced! */
			fprintf(stdout, "%X/%X\n", LSN_FORMAT_ARGS(propEpochStartLsn));
			exit(0);
		}
	}
}

char	   *zenith_timeline_walproposer = NULL;
char	   *zenith_tenant_walproposer = NULL;


static void
WalProposerInit(XLogRecPtr flushRecPtr, uint64 systemId)
{
	char	   *host;
	char	   *sep;
	char	   *port;

	/* Load the libpq-specific functions */
	load_file("libpqwalproposer", false);
	if (WalProposerFunctions == NULL)
		elog(ERROR, "libpqwalproposer didn't initialize correctly");

	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");
	load_file("zenith", false);

	for (host = wal_acceptors_list; host != NULL && *host != '\0'; host = sep)
	{
		port = strchr(host, ':');
		if (port == NULL)
		{
			elog(FATAL, "port is not specified");
		}
		*port++ = '\0';
		sep = strchr(port, ',');
		if (sep != NULL)
			*sep++ = '\0';
		if (n_walkeepers + 1 >= MAX_WALKEEPERS)
		{
			elog(FATAL, "Too many walkeepers");
		}
		walkeeper[n_walkeepers].host = host;
		walkeeper[n_walkeepers].port = port;
		walkeeper[n_walkeepers].state = SS_OFFLINE;
		walkeeper[n_walkeepers].conn = NULL;

		/*
		 * Set conninfo to empty. We'll fill it out once later, in
		 * `ResetConnection` as needed
		 */
		walkeeper[n_walkeepers].conninfo[0] = '\0';
		walkeeper[n_walkeepers].currMsg = NULL;
		walkeeper[n_walkeepers].startStreamingAt = InvalidXLogRecPtr;
		n_walkeepers += 1;
	}
	if (n_walkeepers < 1)
	{
		elog(FATAL, "WalKeepers addresses are not specified");
	}
	quorum = n_walkeepers / 2 + 1;

	/* Fill the greeting package */
	proposerGreeting.tag = 'g';
	proposerGreeting.protocolVersion = SK_PROTOCOL_VERSION;
	proposerGreeting.pgVersion = PG_VERSION_NUM;
	pg_strong_random(&proposerGreeting.proposerId, sizeof(proposerGreeting.proposerId));
	proposerGreeting.systemId = systemId;
	if (!zenith_timeline_walproposer)
		elog(FATAL, "zenith.zenith_timeline is not provided");
	if (*zenith_timeline_walproposer != '\0' &&
		!HexDecodeString(proposerGreeting.ztimelineid, zenith_timeline_walproposer, 16))
		elog(FATAL, "Could not parse zenith.zenith_timeline, %s", zenith_timeline_walproposer);
	if (!zenith_tenant_walproposer)
		elog(FATAL, "zenith.zenith_tenant is not provided");
	if (*zenith_tenant_walproposer != '\0' &&
		!HexDecodeString(proposerGreeting.ztenantid, zenith_tenant_walproposer, 16))
		elog(FATAL, "Could not parse zenith.zenith_tenant, %s", zenith_tenant_walproposer);
	proposerGreeting.timeline = ThisTimeLineID;
	proposerGreeting.walSegSize = wal_segment_size;

	InitEventSet();
}

static void
WalProposerLoop(void)
{
	while (true)
		WalProposerPoll();
}

static void
WalProposerStart(void)
{

	/* Initiate connections to all walkeeper nodes */
	for (int i = 0; i < n_walkeepers; i++)
	{
		ResetConnection(i);
	}

	WalProposerLoop();
}

/*
 * WAL proposer bgworeker entry point
 */
void
WalProposerMain(Datum main_arg)
{
	/* Establish signal handlers. */
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	GetXLogReplayRecPtr(&ThisTimeLineID);

	WalProposerInit(GetFlushRecPtr(), GetSystemIdentifier());

	last_reconnect_attempt = GetCurrentTimestamp();

	application_name = (char *) "walproposer";	/* for
												 * synchronous_standby_names */
	am_wal_proposer = true;
	am_walsender = true;
	InitWalSender();

	/* Create replication slot for WAL proposer if not exists */
	if (SearchNamedReplicationSlot(WAL_PROPOSER_SLOT_NAME, false) == NULL)
	{
		ReplicationSlotCreate(WAL_PROPOSER_SLOT_NAME, false, RS_PERSISTENT, false);
		ReplicationSlotReserveWal();
		/* Write this slot to disk */
		ReplicationSlotMarkDirty();
		ReplicationSlotSave();
		ReplicationSlotRelease();
	}

	WalProposerStart();
}

void
WalProposerSync(int argc, char *argv[])
{
	syncSafekeepers = true;

	InitStandaloneProcess(argv[0]);

	SetProcessingMode(InitProcessing);

	/*
	 * Set default values for command-line options.
	 */
	InitializeGUCOptions();

	/* Acquire configuration parameters */
	if (!SelectConfigFiles(NULL, progname))
		exit(1);

	/*
	 * Imitate we are early in bootstrap loading shared_preload_libraries;
	 * zenith extension sets PGC_POSTMASTER gucs requiring this.
	 */
	process_shared_preload_libraries_in_progress = true;

	/*
	 * Initialize postmaster_alive_fds as WaitEventSet checks them.
	 *
	 * Copied from InitPostmasterDeathWatchHandle()
	 */
	if (pipe(postmaster_alive_fds) < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg_internal("could not create pipe to monitor postmaster death: %m")));
	if (fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFL, O_NONBLOCK) == -1)
		ereport(FATAL,
				(errcode_for_socket_access(),
				 errmsg_internal("could not set postmaster death monitoring pipe to nonblocking mode: %m")));

	WalProposerInit(0, 0);

	process_shared_preload_libraries_in_progress = false;

	BackgroundWorkerUnblockSignals();

	WalProposerStart();
}

static void
WalProposerStartStreaming(XLogRecPtr startpos)
{
	StartReplicationCmd cmd;

	elog(LOG, "WAL proposer starts streaming at %X/%X",
		 LSN_FORMAT_ARGS(startpos));
	cmd.slotname = WAL_PROPOSER_SLOT_NAME;
	cmd.timeline = proposerGreeting.timeline;
	cmd.startpoint = startpos;
	StartReplication(&cmd);
}

/*
 * Send message to the particular node
 *
 * Always updates the state and event set for the WAL keeper; setting either of
 * these before calling would be redundant work.
 */
static void
SendMessageToNode(int i, WalMessage *msg)
{
	WalKeeper  *wk = &walkeeper[i];

	/* we shouldn't be already sending something */
	Assert(wk->currMsg == NULL);

	/*
	 * Skip already acknowledged messages. Used after reconnection to get to
	 * the first not yet sent message. Otherwise we always just send 'msg'.
	 */
	while (msg != NULL && (msg->ackMask & (1 << i)) != 0)
		msg = msg->next;

	wk->currMsg = msg;

	/* Only try to send the message if it's non-null */
	if (wk->currMsg)
	{
		wk->currMsg->req.commitLsn = GetAcknowledgedByQuorumWALPosition();
		wk->currMsg->req.truncateLsn = truncateLsn;

		/*
		 * Once we've selected and set up our message, actually start sending
		 * it.
		 */
		wk->state = SS_SEND_WAL;
		/* Don't ned to update the event set; that's done by AdvancePollState */

		AdvancePollState(i, WL_NO_EVENTS);
	}
	else
	{
		wk->state = SS_IDLE;
		UpdateEventSet(wk, WL_SOCKET_READABLE);
	}
}

/*
 * Broadcast new message to all caught-up walkeepers
 */
static void
BroadcastMessage(WalMessage *msg)
{
	for (int i = 0; i < n_walkeepers; i++)
	{
		if (walkeeper[i].state == SS_IDLE && walkeeper[i].currMsg == NULL)
		{
			SendMessageToNode(i, msg);
		}
	}
}

static WalMessage *
CreateMessage(XLogRecPtr startpos, char *data, int len)
{
	/* Create new message and append it to message queue */
	WalMessage *msg;
	XLogRecPtr	endpos;

	len -= XLOG_HDR_SIZE;
	endpos = startpos + len;
	if (msgQueueTail && msgQueueTail->req.endLsn >= endpos)
	{
		/* Message already queued */
		return NULL;
	}
	Assert(len >= 0);
	msg = (WalMessage *) malloc(sizeof(WalMessage) + len);
	if (msgQueueTail != NULL)
		msgQueueTail->next = msg;
	else
		msgQueueHead = msg;
	msgQueueTail = msg;

	msg->size = sizeof(AppendRequestHeader) + len;
	msg->next = NULL;
	msg->ackMask = 0;
	msg->req.tag = 'a';
	msg->req.term = propTerm;
	msg->req.epochStartLsn = propEpochStartLsn;
	msg->req.beginLsn = startpos;
	msg->req.endLsn = endpos;
	msg->req.proposerId = proposerGreeting.proposerId;
	memcpy(&msg->req + 1, data + XLOG_HDR_SIZE, len);

	Assert(msg->req.endLsn >= lastSentLsn);
	lastSentLsn = msg->req.endLsn;
	return msg;
}

void
WalProposerBroadcast(XLogRecPtr startpos, char *data, int len)
{
	WalMessage *msg = CreateMessage(startpos, data, len);

	if (msg != NULL)
		BroadcastMessage(msg);
}

/*
 * Create WAL message with no data, just to let the walkeepers
 * know that commit lsn has advanced.
 */
static WalMessage *
CreateMessageCommitLsnOnly(XLogRecPtr lsn)
{
	/* Create new message and append it to message queue */
	WalMessage *msg;

	msg = (WalMessage *) malloc(sizeof(WalMessage));
	if (msgQueueTail != NULL)
		msgQueueTail->next = msg;
	else
		msgQueueHead = msg;
	msgQueueTail = msg;

	msg->size = sizeof(AppendRequestHeader);
	msg->next = NULL;
	msg->ackMask = 0;
	msg->req.tag = 'a';
	msg->req.term = propTerm;
	msg->req.epochStartLsn = propEpochStartLsn;

	/*
	 * This serves two purposes: 1) After all msgs from previous epochs are
	 * pushed we queue empty WalMessage with lsn set to epochStartLsn which
	 * commands to switch the epoch, which allows to do the switch without
	 * creating new epoch records (we especially want to avoid such in --sync
	 * mode). Walproposer can advance commit_lsn only after the switch, so
	 * this lsn (reported back) also is the first possible advancement point.
	 * 2) Maintain common invariant of queue entries sorted by LSN.
	 */
	msg->req.beginLsn = lsn;
	msg->req.endLsn = lsn;
	msg->req.proposerId = proposerGreeting.proposerId;

	/*
	 * truncateLsn and commitLsn are set just before the message sent, in
	 * SendMessageToNode()
	 */
	return msg;
}


/*
 * Called after majority of acceptors gave votes, it calculates the most
 * advanced safekeeper (who will be the donor) and epochStartLsn -- LSN since
 * which we'll write WAL in our term.
 * Sets truncateLsn along the way (though it
 * is not of much use at this point).
 */
static void
DetermineEpochStartLsn(void)
{
	propEpochStartLsn = InvalidXLogRecPtr;
	donorEpoch = 0;
	truncateLsn = InvalidXLogRecPtr;

	for (int i = 0; i < n_walkeepers; i++)
	{
		if (walkeeper[i].state == SS_IDLE)
		{
			if (walkeeper[i].voteResponse.epoch > donorEpoch ||
				(walkeeper[i].voteResponse.epoch == donorEpoch &&
				 walkeeper[i].voteResponse.flushLsn > propEpochStartLsn))
			{
				donorEpoch = walkeeper[i].voteResponse.epoch;
				propEpochStartLsn = walkeeper[i].voteResponse.flushLsn;
				donor = i;
			}
			truncateLsn = Max(walkeeper[i].voteResponse.truncateLsn, truncateLsn);
		}
	}

	/*
	 * If propEpochStartLsn is 0 everywhere, we are bootstrapping -- nothing
	 * was committed yet. To keep the idea of always starting streaming since
	 * record boundary (which simplifies decoding on safekeeper), take start
	 * position of the slot.
	 */
	if (propEpochStartLsn == InvalidXLogRecPtr && !syncSafekeepers)
	{
		(void) ReplicationSlotAcquire(WAL_PROPOSER_SLOT_NAME, SAB_Error);
		propEpochStartLsn = truncateLsn = MyReplicationSlot->data.restart_lsn;
		ReplicationSlotRelease();
		elog(LOG, "bumped epochStartLsn to the first record %X/%X", LSN_FORMAT_ARGS(propEpochStartLsn));
	}

	/*
	 * If propEpochStartLsn is not 0, at least one msg with WAL was sent to
	 * some connected safekeeper; it must have carried truncateLsn pointing to
	 * the first record.
	 */
	Assert((truncateLsn != InvalidXLogRecPtr) ||
		   (syncSafekeepers && truncateLsn == propEpochStartLsn));

	elog(LOG, "got votes from majority (%d) of nodes, term " UINT64_FORMAT ", epochStartLsn %X/%X, donor %s:%s, truncate_lsn %X/%X",
		 quorum,
		 propTerm,
		 LSN_FORMAT_ARGS(propEpochStartLsn),
		 walkeeper[donor].host, walkeeper[donor].port,
		 LSN_FORMAT_ARGS(truncateLsn)
		);
}

/*
 * How much milliseconds left till we should attempt reconnection to
 * safekeepers? Returns 0 if it is already high time, -1 if we never reconnect
 * (do we actually need this?).
 */
static long
TimeToReconnect(TimestampTz now)
{
	TimestampTz passed;
	TimestampTz till_reconnect;

	if (wal_acceptor_reconnect_timeout <= 0)
		return -1;

	passed = now - last_reconnect_attempt;
	till_reconnect = wal_acceptor_reconnect_timeout * 1000 - passed;
	if (till_reconnect <= 0)
		return 0;
	return (long) (till_reconnect / 1000);
}

/* If the timeout has expired, attempt to reconnect to all offline walkeepers */
static void
ReconnectWalKeepers(void)
{
	TimestampTz now = GetCurrentTimestamp();

	if (TimeToReconnect(now) == 0)
	{
		last_reconnect_attempt = now;
		for (int i = 0; i < n_walkeepers; i++)
		{
			if (walkeeper[i].state == SS_OFFLINE)
				ResetConnection(i);
		}
	}
}

/*
 * Receive WAL from most advanced WAL keeper
 */
static bool
WalProposerRecovery(int donor, TimeLineID timeline, XLogRecPtr startpos, XLogRecPtr endpos)
{
	char		conninfo[MAXCONNINFO];
	char	   *err;
	WalReceiverConn *wrconn;
	WalRcvStreamOptions options;

	sprintf(conninfo, "host=%s port=%s dbname=replication options='-c ztimelineid=%s ztenantid=%s'",
			walkeeper[donor].host, walkeeper[donor].port, zenith_timeline_walproposer, zenith_tenant_walproposer);
	wrconn = walrcv_connect(conninfo, false, "wal_proposer_recovery", &err);
	if (!wrconn)
	{
		ereport(WARNING,
				(errmsg("could not connect to WAL acceptor %s:%s: %s",
						walkeeper[donor].host, walkeeper[donor].port,
						err)));
		return false;
	}
	elog(LOG,
		 "start recovery from %s:%s starting from %X/%08X till %X/%08X timeline "
		 "%d",
		 walkeeper[donor].host, walkeeper[donor].port, (uint32) (startpos >> 32),
		 (uint32) startpos, (uint32) (endpos >> 32), (uint32) endpos, timeline);

	options.logical = false;
	options.startpoint = startpos;
	options.slotname = NULL;
	options.proto.physical.startpointTLI = timeline;

	if (walrcv_startstreaming(wrconn, &options))
	{
		XLogRecPtr	rec_start_lsn;
		XLogRecPtr	rec_end_lsn = 0;
		int			len;
		char	   *buf;
		pgsocket	wait_fd = PGINVALID_SOCKET;

		while ((len = walrcv_receive(wrconn, &buf, &wait_fd)) >= 0)
		{
			if (len == 0)
			{
				(void) WaitLatchOrSocket(
										 MyLatch, WL_EXIT_ON_PM_DEATH | WL_SOCKET_READABLE, wait_fd,
										 -1, WAIT_EVENT_WAL_RECEIVER_MAIN);
			}
			else
			{
				Assert(buf[0] == 'w');
				memcpy(&rec_start_lsn, &buf[XLOG_HDR_START_POS],
					   sizeof rec_start_lsn);
				rec_start_lsn = pg_ntoh64(rec_start_lsn);
				rec_end_lsn = rec_start_lsn + len - XLOG_HDR_SIZE;
				(void) CreateMessage(rec_start_lsn, buf, len);
				elog(DEBUG1, "Recover message %X/%X length %d",
					 LSN_FORMAT_ARGS(rec_start_lsn), len);
				if (rec_end_lsn >= endpos)
					break;
			}
		}
		elog(DEBUG1, "end of replication stream at %X/%X: %m",
			 LSN_FORMAT_ARGS(rec_end_lsn));
		walrcv_disconnect(wrconn);
	}
	else
	{
		ereport(LOG,
				(errmsg("primary server contains no more WAL on requested timeline %u LSN %X/%08X",
						timeline, (uint32) (startpos >> 32), (uint32) startpos)));
		return false;
	}

	/*
	 * Start sending entries to everyone from the beginning (truncateLsn),
	 * except for those who lives in donor's epoch and thus for sure has
	 * correct WAL. We could do here even slightly better, taking into account
	 * commitLsn of the rest to avoid sending them excessive data.
	 */
	for (int i = 0; i < n_walkeepers; i++)
	{
		if (walkeeper[i].state != SS_IDLE)
			continue;

		if (walkeeper[i].voteResponse.epoch != donorEpoch)
		{
			SendMessageToNode(i, msgQueueHead);
		}
		else
		{
			for (WalMessage *msg = msgQueueHead; msg != NULL; msg = msg->next)
			{
				if (msg->req.endLsn <= walkeeper[i].voteResponse.flushLsn)
				{
					/* message is already received by this walkeeper */
					msg->ackMask |= 1 << i;
				}
				else
				{
					/*
					 * By convention we always stream since the beginning of
					 * the record, and flushLsn points to it.
					 */
					walkeeper[i].startStreamingAt = walkeeper[i].voteResponse.flushLsn;
					SendMessageToNode(i, msg);
					break;
				}
			}
		}
	}
	return true;
}

/*
 * Advance the WAL proposer state machine, waiting each time for events to occur
 */
void
WalProposerPoll(void)
{
	while (true)
	{
		WalKeeper  *wk;
		int			rc;
		int			i;
		WaitEvent	event;
		TimestampTz now = GetCurrentTimestamp();

		rc = WaitEventSetWait(waitEvents, TimeToReconnect(now),
							  &event, 1, WAIT_EVENT_WAL_SENDER_MAIN);
		wk = (WalKeeper *) event.user_data;
		i = (int) (wk - walkeeper);

		/*
		 * If the event contains something that one of our walkeeper states
		 * was waiting for, we'll advance its state.
		 */
		if (rc != 0 && (event.events & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)))
			AdvancePollState(i, event.events);

		/*
		 * If the timeout expired, attempt to reconnect to any walkeepers that
		 * we dropped
		 */
		ReconnectWalKeepers();

		/*
		 * If wait is terminated by latch set (walsenders' latch is set on
		 * each wal flush), then exit loop. (no need for pm death check due to
		 * WL_EXIT_ON_PM_DEATH)
		 */
		if (rc != 0 && (event.events & WL_LATCH_SET))
		{
			ResetLatch(MyLatch);
			break;
		}
	}
}

/* Performs the logic for advancing the state machine of the 'i'th walkeeper,
 * given that a certain set of events has occured. */
static void
AdvancePollState(int i, uint32 events)
{
	WalKeeper  *wk = &walkeeper[i];

	/*
	 * Keep advancing the state while either: (a) the event is still
	 * unprocessed (usually because it's the first iteration of the loop), or
	 * (b) the state can execute, and does not need to wait for any socket
	 * events
	 */
	while (events || StateShouldImmediatelyExecute(wk->state))
	{
		/*
		 * Sanity check. We assume further down that the operations don't
		 * block because the socket is ready.
		 */
		AssertEventsOkForState(events, wk);

		/* Execute the code corresponding to the current state */
		switch (wk->state)
		{
				/*
				 * WAL keepers are only taken out of SS_OFFLINE by calls to
				 * ResetConnection
				 */
			case SS_OFFLINE:
				elog(FATAL, "Unexpected walkeeper %s:%s state advancement: is offline",
					 wk->host, wk->port);
				break;			/* actually unreachable, but prevents
								 * -Wimplicit-fallthrough */

				/*
				 * Both connecting states run the same logic. The only
				 * difference is the events they're expecting
				 */
			case SS_CONNECTING_READ:
			case SS_CONNECTING_WRITE:
				{
					WalProposerConnectPollStatusType result = walprop_connect_poll(wk->conn);

					/* The new set of events we'll wait on, after updating */
					uint32		new_events = WL_NO_EVENTS;

					switch (result)
					{
						case WP_CONN_POLLING_OK:
							elog(LOG, "connected with node %s:%s", wk->host,
								 wk->port);

							/*
							 * Once we're fully connected, we can move to the
							 * next state
							 */
							wk->state = SS_EXEC_STARTWALPUSH;

							/*
							 * Even though SS_EXEC_STARTWALPUSH doesn't wait
							 * on anything, we do need to replace the current
							 * event, so we have to just pick something. We'll
							 * eventually need the socket to be readable, so
							 * we go with that.
							 */
							new_events = WL_SOCKET_READABLE;
							break;

							/*
							 * If we need to poll to finish connecting,
							 * continue doing that
							 */
						case WP_CONN_POLLING_READING:
							wk->state = SS_CONNECTING_READ;
							new_events = WL_SOCKET_READABLE;
							break;
						case WP_CONN_POLLING_WRITING:
							wk->state = SS_CONNECTING_WRITE;
							new_events = WL_SOCKET_WRITEABLE;
							break;

						case WP_CONN_POLLING_FAILED:
							elog(WARNING, "Failed to connect to node '%s:%s': %s",
								 wk->host, wk->port, walprop_error_message(wk->conn));

							/*
							 * If connecting failed, we don't want to restart
							 * the connection because that might run us into a
							 * loop. Instead, shut it down -- it'll naturally
							 * restart at a slower interval on calls to
							 * ReconnectWalKeepers.
							 */
							ShutdownConnection(i);
							return;
					}

					/*
					 * Because PQconnectPoll can change the socket, we have to
					 * un-register the old event and re-register an event on
					 * the new socket.
					 */
					HackyRemoveWalProposerEvent(i);
					wk->eventPos = AddWaitEventToSet(waitEvents, new_events, walprop_socket(wk->conn), NULL, wk);
					break;
				}

				/*
				 * Send "START_WAL_PUSH" command to the walkeeper. After
				 * sending, wait for response with SS_WAIT_EXEC_RESULT
				 */
			case SS_EXEC_STARTWALPUSH:
				if (!walprop_send_query(wk->conn, "START_WAL_PUSH"))
				{
					elog(WARNING, "Failed to send 'START_WAL_PUSH' query to walkeeper %s:%s: %s",
						 wk->host, wk->port, walprop_error_message(wk->conn));
					ResetConnection(i);
					return;
				}

				wk->state = SS_WAIT_EXEC_RESULT;
				UpdateEventSet(wk, WL_SOCKET_READABLE);
				break;

			case SS_WAIT_EXEC_RESULT:
				switch (walprop_get_query_result(wk->conn))
				{
						/*
						 * Successful result, move on to starting the
						 * handshake
						 */
					case WP_EXEC_SUCCESS_COPYBOTH:

						/*
						 * Because this state is immediately executable, we'll
						 * start this on the next iteration of the loop
						 */
						wk->state = SS_HANDSHAKE_SEND;
						break;

						/*
						 * Needs repeated calls to finish. Wait until the
						 * socket is readable
						 */
					case WP_EXEC_NEEDS_INPUT:

						/*
						 * SS_WAIT_EXEC_RESULT is always reached through an
						 * event, so we don't need to update the event set
						 */
						break;

					case WP_EXEC_FAILED:
						elog(WARNING, "Failed to send query to walkeeper %s:%s: %s",
							 wk->host, wk->port, walprop_error_message(wk->conn));
						ResetConnection(i);
						return;

						/*
						 * Unexpected result -- funamdentally an error, but we
						 * want to produce a custom message, rather than a
						 * generic "something went wrong"
						 */
					case WP_EXEC_UNEXPECTED_SUCCESS:
						elog(WARNING, "Received bad resonse from walkeeper %s:%s query execution",
							 wk->host, wk->port);
						ResetConnection(i);
						return;
				}
				break;

				/*
				 * Start handshake: first of all send information about the
				 * WAL keeper. After sending, we wait on SS_HANDSHAKE_RECV for
				 * a response to finish the handshake.
				 */
			case SS_HANDSHAKE_SEND:

				/*
				 * On failure, logging & resetting the connection is handled.
				 * We just need to handle the control flow.
				 */
				if (!BlockingWrite(i, &proposerGreeting, sizeof(proposerGreeting), SS_HANDSHAKE_RECV))
					return;

				break;

				/*
				 * Finish handshake comms: receive information about the WAL
				 * keeper
				 */
			case SS_HANDSHAKE_RECV:

				/*
				 * If our reading doesn't immediately succeed, any necessary
				 * error handling or state setting is taken care of. We can
				 * leave any other work until later.
				 */
				if (!AsyncRead(i, &wk->greet, sizeof(wk->greet)))
					return;

				/* Protocol is all good, move to voting. */
				wk->state = SS_VOTING;

				/*
				 * Don't need to update the event set yet. Either we update
				 * the event set to WL_SOCKET_READABLE *or* we change the
				 * state to SS_SEND_VOTE in the loop below
				 */
				UpdateEventSet(wk, WL_SOCKET_READABLE);
				wk->feedback.flushLsn = truncateLsn;
				wk->feedback.hs.ts = 0;

				/*
				 * We want our term to be highest and unique, so choose max
				 * and +1 once we have majority.
				 */
				propTerm = Max(walkeeper[i].greet.term, propTerm);

				/*
				 * Check if we have quorum. If there aren't enough walkeepers,
				 * wait and do nothing. We'll eventually get a task when the
				 * election starts.
				 *
				 * If we do have quorum, we can start an election
				 */
				if (++n_connected < quorum)
				{
					/*
					 * SS_VOTING is an idle state; read-ready indicates the
					 * connection closed.
					 */
					UpdateEventSet(wk, WL_SOCKET_READABLE);
				}
				else
				{
					if (n_connected == quorum)
					{
						propTerm++;
						/* prepare voting message */
						voteRequest = (VoteRequest)
						{
							.tag = 'v',
								.term = propTerm
						};
						memcpy(voteRequest.proposerId.data, proposerGreeting.proposerId.data, UUID_LEN);
					}

					/*
					 * Now send voting request to the cohort and wait
					 * responses
					 */
					for (int j = 0; j < n_walkeepers; j++)
					{
						/*
						 * Remember: SS_VOTING indicates that the walkeeper is
						 * participating in voting, but hasn't sent anything
						 * yet. The ones that have sent something are given
						 * SS_SEND_VOTE or SS_WAIT_VERDICT.
						 */
						if (walkeeper[j].state == SS_VOTING)
						{
							walkeeper[j].state = SS_SEND_VOTE;
							/* Immediately send info */
							AdvancePollState(j, WL_NO_EVENTS);
						}
					}
				}
				break;

				/*
				 * Voting is an idle state - we don't expect any events to
				 * trigger. Refer to the execution of SS_HANDSHAKE_RECV to see
				 * how nodes are transferred from SS_VOTING to SS_SEND_VOTE.
				 */
			case SS_VOTING:
				elog(WARNING, "EOF from node %s:%s in %s state", wk->host,
					 wk->port, FormatWalKeeperState(wk->state));
				ResetConnection(i);
				break;

				/* We have quorum for voting, send our vote request */
			case SS_SEND_VOTE:
				/* On failure, logging & resetting is handled */
				if (!BlockingWrite(i, &voteRequest, sizeof(voteRequest), SS_WAIT_VERDICT))
					return;

				/* If successful, wait for read-ready with SS_WAIT_VERDICT */
				break;

				/* Start reading the walkeeper response for our candidate */
			case SS_WAIT_VERDICT:

				/*
				 * If our reading doesn't immediately succeed, any necessary
				 * error handling or state setting is taken care of. We can
				 * leave any other work until later.
				 */
				if (!AsyncRead(i, &wk->voteResponse, sizeof(wk->voteResponse)))
					return;

				elog(LOG,
					 "got VoteResponse from acceptor %s:%s, voteGiven=" UINT64_FORMAT ", epoch=" UINT64_FORMAT ", flushLsn=%X/%X, truncateLsn=%X/%X",
					 wk->host, wk->port, wk->voteResponse.voteGiven, wk->voteResponse.epoch,
					 LSN_FORMAT_ARGS(wk->voteResponse.flushLsn),
					 LSN_FORMAT_ARGS(wk->voteResponse.truncateLsn));

				/*
				 * In case of acceptor rejecting our vote, bail out, but only
				 * if either it already lives in strictly higher term
				 * (concurrent compute spotted) or we are not elected yet and
				 * thus need the vote.
				 */
				if ((!wk->voteResponse.voteGiven) &&
					(wk->voteResponse.term > propTerm || n_votes < quorum))
				{
					elog(FATAL, "WAL acceptor %s:%s with term " INT64_FORMAT " rejects our connection request with term " INT64_FORMAT "",
						 wk->host, wk->port,
						 wk->voteResponse.term, propTerm);
				}
				Assert(wk->voteResponse.term == propTerm);

				/* Handshake completed, do we have quorum? */

				if (++n_votes != quorum)
				{
					/*
					 * We are already streaming WAL: send all pending messages
					 * to the attached walkeeper
					 */
					SendMessageToNode(i, msgQueueHead);
				}
				else
				{
					wk->state = SS_IDLE;
					UpdateEventSet(wk, WL_SOCKET_READABLE); /* Idle states wait for
															 * read-ready */

					DetermineEpochStartLsn();

					/*
					 * Check if not all safekeepers are up-to-date, we need to
					 * download WAL needed to synchronize them
					 */
					if (truncateLsn < propEpochStartLsn)
					{
						elog(LOG,
							 "start recovery because truncateLsn=%X/%X is not "
							 "equal to epochStartLsn=%X/%X",
							 LSN_FORMAT_ARGS(truncateLsn),
							 LSN_FORMAT_ARGS(propEpochStartLsn));
						/* Perform recovery */
						if (!WalProposerRecovery(donor, proposerGreeting.timeline, truncateLsn, propEpochStartLsn))
							elog(FATAL, "Failed to recover state");

						/*
						 * This message signifies epoch switch; it is needed
						 * to make the switch happen on donor, as he won't get
						 * any other messages until we start writing new WAL
						 * (and we e.g. don't in --sync mode at all)
						 */
						BroadcastMessage(CreateMessageCommitLsnOnly(propEpochStartLsn));

						if (syncSafekeepers)
						{
							/* Wait until all walkeepers are synced */
							WalProposerLoop();
						}
					}
					else if (syncSafekeepers)
					{
						/* Sync is not needed: just exit */
						fprintf(stdout, "%X/%X\n", LSN_FORMAT_ARGS(propEpochStartLsn));
						exit(0);
					}
					WalProposerStartStreaming(propEpochStartLsn);
					/* Should not return here */
				}

				break;

				/*
				 * Idle state for sending WAL. Moved out only by calls to
				 * SendMessageToNode
				 */
			case SS_IDLE:
				elog(WARNING, "EOF from node %s:%s in %s state", wk->host,
					 wk->port, FormatWalKeeperState(wk->state));
				ResetConnection(i);
				break;

				/*
				 * Start to send the message at wk->currMsg. Triggered only by
				 * calls to SendMessageToNode
				 */
			case SS_SEND_WAL:
				{
					WalMessage *msg = wk->currMsg;
					AppendRequestHeader *req = &msg->req;

					/*
					 * If we need to send this message not from the beginning,
					 * form the cut version. Only happens for the first
					 * message.
					 */
					if (wk->startStreamingAt > msg->req.beginLsn)
					{
						uint32		len;
						uint32		size;

						Assert(wk->startStreamingAt < req->endLsn);

						len = msg->req.endLsn - wk->startStreamingAt;
						size = sizeof(AppendRequestHeader) + len;
						req = malloc(size);
						*req = msg->req;
						req->beginLsn = wk->startStreamingAt;
						memcpy(req + 1,
							   (char *) (&msg->req + 1) + wk->startStreamingAt -
							   msg->req.beginLsn,
							   len);
					}

					elog(LOG,
						 "sending message len %ld beginLsn=%X/%X endLsn=%X/%X commitLsn=%X/%X truncateLsn=%X/%X to %s:%s",
						 req->endLsn - req->beginLsn,
						 LSN_FORMAT_ARGS(req->beginLsn),
						 LSN_FORMAT_ARGS(req->endLsn),
						 LSN_FORMAT_ARGS(req->commitLsn),
						 LSN_FORMAT_ARGS(truncateLsn), wk->host, wk->port);

					/*
					 * We write with msg->size here because the body of the
					 * message is stored after the end of the WalMessage
					 * struct, in the allocation for each msg
					 */
					if (!AsyncWrite(i, req,
									sizeof(AppendRequestHeader) + req->endLsn -
									req->beginLsn,
									SS_SEND_WAL_FLUSH, SS_RECV_FEEDBACK))
					{
						if (req != &msg->req)
							free(req);
						return;
					}
					if (req != &msg->req)
						free(req);

					break;
				}

				/* Flush the WAL message we're sending from SS_SEND_WAL */
			case SS_SEND_WAL_FLUSH:

				/*
				 * AsyncFlush ensures we only move on to SS_RECV_FEEDBACK once
				 * the flush completes. If we still have more to do, we'll
				 * wait until the next poll comes along.
				 */
				if (!AsyncFlush(i, (events & WL_SOCKET_READABLE) != 0, SS_RECV_FEEDBACK))
					return;

				break;

				/*
				 * Start to receive the feedback from a message sent via
				 * SS_SEND_WAL
				 */
			case SS_RECV_FEEDBACK:
				{
					WalMessage *next;
					XLogRecPtr	minQuorumLsn;

					/*
					 * If our reading doesn't immediately succeed, any
					 * necessary error handling or state setting is taken care
					 * of. We can leave any other work until later.
					 */
					if (!AsyncRead(i, &wk->feedback, sizeof(wk->feedback)))
						return;

					next = wk->currMsg->next;
					wk->currMsg->ackMask |= 1 << i; /* this walkeeper confirms
													 * receiving of this
													 * message */

					wk->currMsg = NULL;
					HandleWalKeeperResponse();
					SendMessageToNode(i, next); /* Updates state & event set */

					/*
					 * Also send the new commit lsn to all the walkeepers.
					 *
					 * FIXME: This is redundant for walkeepers that have other
					 * outbound messages pending.
					 */
					minQuorumLsn = GetAcknowledgedByQuorumWALPosition();

					if (minQuorumLsn > lastSentCommitLsn)
					{
						BroadcastMessage(CreateMessageCommitLsnOnly(lastSentLsn));

						/*
						 * commitLsn is always the record boundary; remember
						 * it so we can advance truncateLsn there. But do so
						 * only if previous value is applied, otherwise it
						 * might never catch up.
						 */
						if (candidateTruncateLsn == InvalidXLogRecPtr)
						{
							candidateTruncateLsn = minQuorumLsn;
						}
						lastSentCommitLsn = minQuorumLsn;
					}
					break;
				}
		}

		/*
		 * We've already done something for these events - don't attempt more
		 * states than we need to.
		 */
		events = WL_NO_EVENTS;
	}
}

/*
 * Reads a CopyData block from the 'i'th WAL keeper's postgres connection,
 * returning whether the read was successful.
 *
 * If the read needs more polling, we return 'false' and keep the state
 * unmodified, waiting until it becomes read-ready to try again. If it fully
 * failed, a warning is emitted and the connection is reset.
 */
static bool
AsyncRead(int i, void *value, size_t value_size)
{
	WalKeeper  *wk = &walkeeper[i];
	char	   *buf = NULL;
	int			buf_size = -1;
	uint32		events;

	switch (walprop_async_read(wk->conn, &buf, &buf_size))
	{
			/* On success, there's just a couple more things we'll check below */
		case PG_ASYNC_READ_SUCCESS:
			break;

			/*
			 * If we need more input, wait until the socket is read-ready and
			 * try again.
			 */
		case PG_ASYNC_READ_TRY_AGAIN:
			UpdateEventSet(wk, WL_SOCKET_READABLE);
			return false;

		case PG_ASYNC_READ_FAIL:
			elog(WARNING, "Failed to read from node %s:%s in %s state: %s",
				 wk->host, wk->port,
				 FormatWalKeeperState(wk->state),
				 walprop_error_message(wk->conn));
			ResetConnection(i);
			return false;
	}

	/*
	 * If we get here, the read was ok, but we still need to check it was the
	 * right amount
	 */
	if ((size_t) buf_size != value_size)
	{
		elog(FATAL,
			 "Unexpected walkeeper %s:%s read length from %s state. Expected %ld, found %d",
			 wk->host, wk->port,
			 FormatWalKeeperState(wk->state),
			 value_size, buf_size);
	}

	/* Copy the resulting info into place */
	memcpy(value, buf, buf_size);

	/* Update the events for the WalKeeper, if it's going to wait */
	events = WalKeeperStateDesiredEvents(wk->state);
	if (events)
		UpdateEventSet(wk, events);

	return true;
}

/*
 * Blocking equivalent to AsyncWrite.
 *
 * We use this everywhere messages are small enough that they should fit in a
 * single packet.
 */
static bool
BlockingWrite(int i, void *msg, size_t msg_size, WalKeeperState success_state)
{
	WalKeeper  *wk = &walkeeper[i];
	uint32		events;

	if (!walprop_blocking_write(wk->conn, msg, msg_size))
	{
		elog(WARNING, "Failed to send to node %s:%s in %s state: %s",
			 wk->host, wk->port, FormatWalKeeperState(wk->state),
			 walprop_error_message(wk->conn));
		ResetConnection(i);
		return false;
	}

	wk->state = success_state;

	/*
	 * If the new state will be waiting for events to happen, update the event
	 * set to wait for those
	 */
	events = WalKeeperStateDesiredEvents(success_state);
	if (events)
		UpdateEventSet(wk, events);

	return true;
}

/*
 * Starts a write into the 'i'th WAL keeper's postgres connection, moving to
 * success_state only when the write succeeds. If the write needs flushing,
 * moves to flush_state.
 *
 * Returns false only if the write immediately fails. Upon failure, a warning is
 * emitted and the connection is reset.
 */
static bool
AsyncWrite(int i, void *msg, size_t msg_size, WalKeeperState flush_state, WalKeeperState success_state)
{
	WalKeeper  *wk = &walkeeper[i];
	uint32		events;

	switch (walprop_async_write(wk->conn, msg, msg_size))
	{
		case PG_ASYNC_WRITE_SUCCESS:
			wk->state = success_state;
			break;
		case PG_ASYNC_WRITE_TRY_FLUSH:

			/*
			 * We still need to call PQflush some more to finish the job; go
			 * to the appropriate state. Update the event set at the bottom of
			 * this function
			 */
			wk->state = flush_state;
			break;
		case PG_ASYNC_WRITE_FAIL:
			elog(WARNING, "Failed to send to node %s:%s in %s state: %s",
				 wk->host, wk->port, FormatWalKeeperState(wk->state),
				 walprop_error_message(wk->conn));
			ResetConnection(i);
			return false;
	}

	/* If the new state will be waiting for something, update the event set */
	events = WalKeeperStateDesiredEvents(wk->state);
	if (events)
		UpdateEventSet(wk, events);

	return true;
}

/*
 * Flushes a previous call to AsyncWrite. This only needs to be called when the
 * socket becomes read or write ready *after* calling AsyncWrite.
 *
 * If flushing completes, moves to 'success_state' and returns true. If more
 * flushes are needed, does nothing and returns true.
 *
 * On failure, emits a warning, resets the connection, and returns false.
 */
static bool
AsyncFlush(int i, bool socket_read_ready, WalKeeperState success_state)
{
	WalKeeper  *wk = &walkeeper[i];
	uint32		events;

	/*---
	 * PQflush returns:
	 *   0 if successful                    [we're good to move on]
	 *   1 if unable to send everything yet [call PQflush again]
	 *  -1 if it failed                     [emit an error]
	 */
	switch (walprop_flush(wk->conn, socket_read_ready))
	{
		case 0:
			/* On success, move to the next state - that logic is further down */
			break;
		case 1:
			/* Nothing to do; try again when the socket's ready */
			return true;
		case -1:
			elog(WARNING, "Failed to flush write to node %s:%s in %s state: %s",
				 wk->host, wk->port, FormatWalKeeperState(wk->state),
				 walprop_error_message(wk->conn));
			ResetConnection(i);
			return false;
	}

	wk->state = success_state;

	/* If the new state will be waiting for something, update the event set */
	events = WalKeeperStateDesiredEvents(wk->state);
	if (events)
		UpdateEventSet(wk, events);

	return true;
}

/*
 * WalProposerRegister
 *		Register a background worker porposing WAL to wal acceptors
 */
void
WalProposerRegister(void)
{
	BackgroundWorker bgw;

	if (*wal_acceptors_list == '\0')
		return;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "WalProposerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "WAL proposer");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "WAL proposer");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}
