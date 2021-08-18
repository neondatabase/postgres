/*-------------------------------------------------------------------------
 *
 * walproposer.c
 *
 * Broadcast WAL stream to Zenith WAL acceptetors
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
#include "storage/pmsignal.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"


char* wal_acceptors_list;
int   wal_acceptor_reconnect_timeout;
bool  am_wal_proposer;


/* Declared in walproposer.h, defined here, initialized in libpqwalproposer.c */
WalProposerFunctionsType* WalProposerFunctions = NULL;

#define WAL_PROPOSER_SLOT_NAME "wal_proposer_slot"

static int          n_walkeepers = 0;
static int          quorum = 0;
static WalKeeper    walkeeper[MAX_WALKEEPERS];
static WalMessage*  msgQueueHead;
static WalMessage*  msgQueueTail;
static XLogRecPtr	lastSentLsn;	/* WAL has been appended to msg queue up to this point */
static XLogRecPtr	lastSentCommitLsn;	/* last commitLsn broadcast to walkeepers */
static ProposerGreeting   proposerGreeting;
static WaitEventSet* waitEvents;
static AppendResponse lastFeedback;
static XLogRecPtr   truncateLsn; /* Last position received by all walkeepers. */
static VoteRequest voteRequest; /* Vote request for walkeeper */
static term_t       propTerm; /* term of the proposer */
static XLogRecPtr   propEpochStartLsn;    /* epoch start lsn of the proposer */
static term_t		donorEpoch; /* Most advanced acceptor epoch */
static int          donor;     /* Most advanced acceptor */
static int          n_votes = 0;
static int          n_connected = 0;
static TimestampTz  last_reconnect_attempt;

/* Declarations of a few functions ahead of time, so that we can define them out of order. */
static void AdvancePollState(int i, uint32 events);
static bool AsyncRead(int i, void* value, size_t value_size);
static bool AsyncWrite(int i, void* msg, size_t msg_size, WalKeeperState success_state);
static void HackyRemoveWalProposerEvent(int to_remove);

/*
 * Combine hot standby feedbacks from all walkeepers.
 */
static void
CombineHotStanbyFeedbacks(HotStandbyFeedback* hs)
{
	hs->ts = 0;
	hs->xmin.value = ~0; /* largest unsigned value */
	hs->catalog_xmin.value = ~0; /* largest unsigned value */

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
 * Updates the events we're waiting on for the WAL keeper, setting it to the
 * provided `events`
 *
 * This function expects that there is already an event for the WAL keeper.
 */
static void
UpdateEventSet(WalKeeper* wk, uint32 events)
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
	if (waitEvents) {
		FreeWaitEventSet(waitEvents);
		waitEvents = NULL;
	}
	/* Re-initialize it without adding any walkeeper events */
	InitEventSet();

	/* loop through the existing walkeepers. If they aren't the one we're removing, and if they have
	 * a socket we can use, re-add the applicable events. */
	for (int i = 0; i < n_walkeepers; i++)
	{
		uint32 desired_events = WL_NO_EVENTS;
		WalKeeper* wk = &walkeeper[i];

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
	pgsocket sock; /* socket of the new connection */
	WalKeeper *wk = &walkeeper[i];

	if (wk->state != SS_OFFLINE)
	{
		elog(WARNING, "Connection with node %s:%s in %s state failed",
			wk->host, wk->port, FormatWalKeeperState(wk->state));
		ShutdownConnection(i);
	}

	/* Try to establish new connection
	 *
	 * If the connection information hasn't been filled out, we need to do
	 * that here. */
	if (wk->conninfo[0] == '\0')
	{
		sprintf((char*) &wk->conninfo,
				"host=%s port=%s dbname=replication options='-c ztimelineid=%s'",
				wk->host, wk->port, zenith_timeline_walproposer);
	}

	wk->conn = walprop_connect_start((char*) &wk->conninfo);

	/* "If the result is null, then libpq has been unable to allocate a new PGconn structure" */
	if (!wk->conn)
		elog(FATAL, "failed to allocate new PGconn object");

	/* The connection should always be non-blocking. It's easiest to just set that here. */
	walprop_set_nonblocking(wk->conn, true);

	/* PQconnectStart won't actually start connecting until we run PQconnectPoll. Before we do that
	 * though, we need to check that it didn't immediately fail. */
	if (walprop_status(wk->conn) == WP_CONNECTION_BAD)
	{
		/* According to libpq docs:
		 *   "If the result is CONNECTION_BAD, the connection attempt has already failed, typically
		 *    because of invalid connection parameters."
		 * We should report this failure.
		 *
		 * https://www.postgresql.org/docs/devel/libpq-connect.html#LIBPQ-PQCONNECTSTARTPARAMS */
		elog(WARNING, "Immediate failure to connect with node:\n\t%s\n\terror: %s",
			 wk->conninfo, walprop_error_message(wk->conn));
		/* Even though the connection failed, we still need to clean up the object */
		walprop_finish(wk->conn);
		wk->conn = NULL;
		return;
	}

	/* The documentation for PQconnectStart states that we should call PQconnectPoll in a loop until
	 * it returns PGRES_POLLING_OK or PGRES_POLLING_FAILED. The other two possible returns indicate
	 * whether we should wait for reading or writing on the socket. For the first iteration of the
	 * loop, we're expected to wait until the socket becomes writable.
	 *
	 * The wording of the documentation is a little ambiguous; thankfully there's an example in the
	 * postgres source itself showing this behavior.
	 *   (see libpqrcv_connect, defined in
	 *              src/backend/replication/libpqwalreceiver/libpqwalreceiver.c)
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
	XLogRecPtr responses[MAX_WALKEEPERS];
	/*
	 * Sort acknowledged LSNs
	 */
	for (int i = 0; i < n_walkeepers; i++)
	{
		/*
		 * Note that while we haven't pushed WAL up to epoch start lsn to the majority we
		 * don't really know which LSN is reliably committed as reported
		 * flush_lsn is physical end of wal, which can contain diverged
		 * history (compared to donor).
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
	XLogRecPtr minQuorumLsn;

	minQuorumLsn = GetAcknowledgedByQuorumWALPosition();
	if (minQuorumLsn > lastFeedback.flushLsn)
	{
		lastFeedback.flushLsn = minQuorumLsn;
		/* advance the replication slot */
		ProcessStandbyReply(minQuorumLsn, minQuorumLsn, InvalidXLogRecPtr, GetCurrentTimestamp(), false);
	}
	CombineHotStanbyFeedbacks(&hsFeedback);
	if (hsFeedback.ts != 0 && memcmp(&hsFeedback, &lastFeedback.hs, sizeof hsFeedback) != 0)
	{
		lastFeedback.hs = hsFeedback;
		ProcessStandbyHSFeedback(hsFeedback.ts,
								 XidFromFullTransactionId(hsFeedback.xmin),
								 EpochFromFullTransactionId(hsFeedback.xmin),
								 XidFromFullTransactionId(hsFeedback.catalog_xmin),
								 EpochFromFullTransactionId(hsFeedback.catalog_xmin));
	}


	/* Cleanup message queue */
	while (msgQueueHead != NULL && msgQueueHead->ackMask == ((1 << n_walkeepers) - 1))
	{
		WalMessage* msg = msgQueueHead;
		msgQueueHead = msg->next;
		if (truncateLsn < msg->req.beginLsn)
		{
			Assert(truncateLsn < msg->req.endLsn);
			truncateLsn = msg->req.endLsn;
		}
		memset(msg, 0xDF, sizeof(WalMessage) + msg->size - sizeof(AppendRequestHeader));
		free(msg);
	}
	if (!msgQueueHead) /* queue is empty */
		msgQueueTail = NULL;
}

char *zenith_timeline_walproposer = NULL;
char *zenith_tenant_walproposer = NULL;

/*
 * WAL proposer bgworeker entry point
 */
void
WalProposerMain(Datum main_arg)
{
	char* host;
	char* sep;
	char* port;

	/* Establish signal handlers. */
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	/* Load the libpq-specific functions */
	load_file("libpqwalproposer", false);
	if (WalProposerFunctions == NULL)
		elog(ERROR, "libpqwalproposer didn't initialize correctly");

	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");

	load_file("zenith", false);

	BackgroundWorkerUnblockSignals();

	for (host = wal_acceptors_list; host != NULL && *host != '\0'; host = sep)
	{
		port = strchr(host, ':');
		if (port == NULL) {
			elog(FATAL, "port is not specified");
		}
		*port++ = '\0';
		sep = strchr(port, ',');
		if (sep != NULL)
			*sep++ = '\0';
		if (n_walkeepers+1 >= MAX_WALKEEPERS)
		{
			elog(FATAL, "Too many walkeepers");
		}
		walkeeper[n_walkeepers].host = host;
		walkeeper[n_walkeepers].port = port;
		walkeeper[n_walkeepers].state = SS_OFFLINE;
		walkeeper[n_walkeepers].conn = NULL;
		/* Set conninfo to empty. We'll fill it out once later, in `ResetConnection` as needed */
		walkeeper[n_walkeepers].conninfo[0] = '\0';
		walkeeper[n_walkeepers].currMsg = NULL;
		n_walkeepers += 1;
	}
	if (n_walkeepers < 1)
	{
		elog(FATAL, "WalKeepers addresses are not specified");
	}
	quorum = n_walkeepers/2 + 1;

	GetXLogReplayRecPtr(&ThisTimeLineID);

	/* Fill the greeting package */
	proposerGreeting.tag = 'g';
	proposerGreeting.protocolVersion = SK_PROTOCOL_VERSION;
	proposerGreeting.pgVersion = PG_VERSION_NUM;
	pg_strong_random(&proposerGreeting.proposerId, sizeof(proposerGreeting.proposerId));
	proposerGreeting.systemId = GetSystemIdentifier();
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

	last_reconnect_attempt = GetCurrentTimestamp();

	application_name = (char *) "walproposer"; /* for synchronous_standby_names */
	am_wal_proposer = true;
	am_walsender = true;
	InitWalSender();
	InitEventSet();

	/* Create replication slot for WAL proposer if not exists */
	if (SearchNamedReplicationSlot(WAL_PROPOSER_SLOT_NAME, false) == NULL)
	{
		ReplicationSlotCreate(WAL_PROPOSER_SLOT_NAME, false, RS_PERSISTENT, false);
		ReplicationSlotRelease();
	}

	/* Initiate connections to all walkeeper nodes */
	for (int i = 0; i < n_walkeepers; i++)
	{
		ResetConnection(i);
	}

	while (true)
		WalProposerPoll();
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
 */
static void
SendMessageToNode(int i, WalMessage* msg)
{
	WalKeeper* wk = &walkeeper[i];

	/* we shouldn't be already sending something */
	Assert(wk->currMsg == NULL);
	/*
	 * Skip already acknowledged messages. Used during start to get to the
	 * first not yet received message. Otherwise we always just send
	 * 'msg'.
	 */
	while (msg != NULL && (msg->ackMask & (1 << i)) != 0)
		msg = msg->next;

	wk->currMsg = msg;

	/* Only try to send the message if it's non-null */
	if (wk->currMsg)
	{
		wk->currMsg->req.truncateLsn = truncateLsn;
		wk->currMsg->req.commitLsn = GetAcknowledgedByQuorumWALPosition();

		/* Once we've selected and set up our message, actually start sending it. */
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
BroadcastMessage(WalMessage* msg)
{
	for (int i = 0; i < n_walkeepers; i++)
	{
		if (walkeeper[i].state == SS_IDLE && walkeeper[i].currMsg == NULL)
		{
			SendMessageToNode(i, msg);
		}
	}
}

static WalMessage*
CreateMessage(XLogRecPtr startpos, char* data, int len)
{
	/* Create new message and append it to message queue */
	WalMessage*	msg;
	XLogRecPtr endpos;
	len -= XLOG_HDR_SIZE;
	endpos = startpos + len;
	if (msgQueueTail && msgQueueTail->req.endLsn >= endpos)
	{
		/* Message already queued */
		return NULL;
	}
	Assert(len >= 0);
	msg = (WalMessage*)malloc(sizeof(WalMessage) + len);
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
	memcpy(&msg->req+1, data + XLOG_HDR_SIZE, len);

	Assert(msg->req.endLsn >= lastSentLsn);
	lastSentLsn = msg->req.endLsn;
	return msg;
}

void
WalProposerBroadcast(XLogRecPtr startpos, char* data, int len)
{
	WalMessage* msg = CreateMessage(startpos, data, len);
	if (msg != NULL)
		BroadcastMessage(msg);
}

/*
 * Create WAL message with no data, just to let the walkeepers
 * know that commit lsn has advanced.
 */
static WalMessage*
CreateMessageCommitLsnOnly(void)
{
	/* Create new message and append it to message queue */
	WalMessage*	msg;

	if (lastSentLsn == 0)
	{
		/* FIXME: We haven't sent anything yet. Not sure what to do then.. */
		return NULL;
	}

	msg = (WalMessage*)malloc(sizeof(WalMessage));
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
	msg->req.beginLsn = lastSentLsn;
	msg->req.endLsn = lastSentLsn;
	msg->req.proposerId = proposerGreeting.proposerId;
	/* truncateLsn and commitLsn are set just before the message sent, in SendMessageToNode() */
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
	// FIXME: If the WAL acceptors have nothing, start from "the beginning of time"
	propEpochStartLsn = wal_segment_size;
	donorEpoch = 0;
	truncateLsn = wal_segment_size;

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

	elog(LOG, "got votes from majority (%d) of nodes, epochStartLsn %X/%X, donor %s:%s, restart_lsn %X/%X",
		 quorum,
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
	char conninfo[MAXCONNINFO];
	char *err;
	WalReceiverConn *wrconn;
	WalRcvStreamOptions options;

	sprintf(conninfo, "host=%s port=%s dbname=replication options='-c ztimelineid=%s'",
			walkeeper[donor].host, walkeeper[donor].port, zenith_timeline_walproposer);
	wrconn = walrcv_connect(conninfo, false, "wal_proposer_recovery", &err);
	if (!wrconn)
	{
		ereport(WARNING,
				(errmsg("could not connect to WAL acceptor %s:%s: %s",
						walkeeper[donor].host, walkeeper[donor].port,
						err)));
		return false;
	}
	elog(LOG, "Start recovery from %s:%s starting from %X/%08X till %X/%08X timeline %d",
		 walkeeper[donor].host, walkeeper[donor].port,
		 (uint32)(startpos>>32), (uint32)startpos, (uint32)(endpos >> 32), (uint32)endpos,
		 timeline);

	options.logical = false;
	options.startpoint = startpos;
	options.slotname = NULL;
	options.proto.physical.startpointTLI = timeline;

	if (walrcv_startstreaming(wrconn, &options))
	{
		XLogRecPtr rec_start_lsn;
		XLogRecPtr rec_end_lsn;
		int len;
		char *buf;
		pgsocket wait_fd = PGINVALID_SOCKET;
		while ((len = walrcv_receive(wrconn, &buf, &wait_fd)) > 0)
		{
			Assert(buf[0] == 'w');
			memcpy(&rec_start_lsn, &buf[XLOG_HDR_START_POS], sizeof rec_start_lsn);
			rec_start_lsn = pg_ntoh64(rec_start_lsn);
			rec_end_lsn = rec_start_lsn + len - XLOG_HDR_SIZE;
			(void)CreateMessage(rec_start_lsn, buf, len);
			if (rec_end_lsn >= endpos)
				break;
		}
		walrcv_disconnect(wrconn);
	}
	else
	{
		ereport(LOG,
				(errmsg("primary server contains no more WAL on requested timeline %u LSN %X/%08X",
						timeline, (uint32)(startpos >> 32), (uint32)startpos)));
		return false;
	}
	/* Setup restart point for all walkeepers */
	for (int i = 0; i < n_walkeepers; i++)
	{
		if (walkeeper[i].state == SS_IDLE)
		{
			for (WalMessage* msg = msgQueueHead; msg != NULL; msg = msg->next)
			{
				if (msg->req.endLsn <= walkeeper[i].voteResponse.flushLsn)
				{
					msg->ackMask |= 1 << i; /* message is already received by this walkeeper */
				}
				else
				{
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
		WalKeeper*  wk;
		int         rc;
		int         i;
		WaitEvent	event;
		TimestampTz now = GetCurrentTimestamp();

		rc = WaitEventSetWait(waitEvents, TimeToReconnect(now),
						&event, 1, WAIT_EVENT_WAL_SENDER_MAIN);
		wk = (WalKeeper*) event.user_data;
		i = (int)(wk - walkeeper);

		/*
		 * If the event contains something that one of our walkeeper states
		 * was waiting for, we'll advance its state.
		 */
		if (rc != 0 && (event.events & (WL_SOCKET_READABLE|WL_SOCKET_WRITEABLE)))
			AdvancePollState(i, event.events);

		/* If the timeout expired, attempt to reconnect to any walkeepers that we dropped */
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
	WalKeeper* wk = &walkeeper[i];

	/* Keep advancing the state while either:
	 *   (a) the event is still unprocessed (usually because it's the first
	 *       iteration of the loop), or
	 *   (b) the state normally waits but isn't (e.g. SS_SEND_WAL, without
	 *       SMOD_NEEDS_FLUSH)
	 */
	while (events || StateShouldImmediatelyExecute(wk->state))
	{
		/* Sanity check. We assume further down that the operations don't block
		 * because the socket is ready. */
		AssertEventsOkForState(events, wk);

		/* -------- Shared libpq functionality -------- */

		/* Shared functionality between states: if there's libpq-specific
		 * polling we need to do, based on the state modifier, we can do it
		 * here. */
		if (wk->state & SMOD_NEEDS_FLUSH)
		{
			int flush_result;

			/* If the socket is ready for reading, we have to call PQconsumeInput
			 * before attempting to flush */
			if (events & WL_SOCKET_READABLE)
			{
				if (!walprop_consume_input(wk->conn))
				{
					elog(WARNING, "Failed to pre-flush read input for node %s:%s in state [%s]: %s",
						 wk->host, wk->port, FormatWalKeeperState(wk->state),
						 walprop_error_message(wk->conn));
					ResetConnection(i);
					return;
				}
			}

			/* PQflush returns:
			 *   0 if successful                    [we're good to move on]
			 *   1 if unable to send everything yet [call PQflush again]
			 *  -1 if it failed                     [emit an error] */
			switch (flush_result = walprop_flush(wk->conn))
			{
				case 0:
					/* Our new state is just the unmodified version of this one.
					 * This is a special guarantee for SMOD_NEEDS_FLUSH */
					wk->state &= ~SMOD_ALL;
					break;
				case 1:
					/* Can't do anything yet, try again when the socket's ready */
					return;
				case -1:
					elog(WARNING, "Failed to flush write to node %s:%s in %s state: %s",
						 wk->host, wk->port, FormatWalKeeperState(wk->state),
						 walprop_error_message(wk->conn));
					ResetConnection(i);
					return;
				default:
					elog(FATAL, "invalid return %d from PQflush", flush_result);
			}

			events = WL_NO_EVENTS;
		}

		/* -------- Execute state logic -------- */

		/* Knowing which event happened is no longer necessary. Erasing it now
		 * ensures that any events we see on future loops are intentionally
		 * placed there to trick the section of code above. */
		events = WL_NO_EVENTS;

		/* Switch on the "base" state, without modifiers */
		switch (wk->state & (~SMOD_ALL))
		{
			/* WAL keepers are only taken out of SS_OFFLINE by calls to
			 * ResetConnection */
			case SS_OFFLINE:
				elog(FATAL, "Unexpected walkeeper %s:%s state advancement: is offline",
					 wk->host, wk->port);
				break; /* actually unreachable, but prevents -Wimplicit-fallthrough */

			/* Both connecting states run the same logic. The only difference is
			 * the events they're expecting */
			case SS_CONNECTING_READ:
			case SS_CONNECTING_WRITE:
				WalProposerConnectPollStatusType result = walprop_connect_poll(wk->conn);

				/* The new set of events we'll wait on, after updating */
				uint32 new_events = WL_NO_EVENTS;

				switch (result)
				{
					case WP_CONN_POLLING_OK:
						elog(LOG, "Connected with node %s:%s", wk->host, wk->port);

						/* Once we're fully connected, we can move to the next state */
						wk->state = SS_EXEC_STARTWALPUSH;

						/* We will *eventually* need the socket to be writable, but
						 * not quite yet. */
						new_events = WL_SOCKET_WRITEABLE;
						break;

					/* If we need to poll to finish connecting, continue doing that */
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
						/* If connecting failed, we don't want to restart the connection because
						 * that might run us into a loop. Instead, shut it down -- it'll naturally
						 * restart at a slower interval on calls to ReconnectWalKeepers. */
						ShutdownConnection(i);
						return;
				}

				/* Because PQconnectPoll can change the socket, we have to
				 * un-register the old event and re-register an event on the new
				 * socket. */
				HackyRemoveWalProposerEvent(i);
				wk->eventPos = AddWaitEventToSet(waitEvents, new_events, walprop_socket(wk->conn), NULL, wk);
				break;

			/* Send "START_WAL_PUSH" command to the walkeeper. After sending,
			 * wait for response with SS_WAIT_EXEC_RESULT */
			case SS_EXEC_STARTWALPUSH:
				if (!walprop_send_query(wk->conn, "START_WAL_PUSH"))
				{
					elog(WARNING, "Failed to send 'START_WAL_PUSH' query to walkeeper %s:%s: %s",
							wk->host, wk->port, walprop_error_message(wk->conn));
					ResetConnection(i);
					return;
				}

				/* The query has been put into buffers but not flushed yet. We
				 * can trigger a flush on the next iteration of this loop by
				 * adding the SMOD_NEEDS_FLUSH modifier to the state. */
				wk->state = SS_WAIT_EXEC_RESULT | SMOD_NEEDS_FLUSH;
				UpdateEventSet(wk, WL_SOCKET_READABLE|WL_SOCKET_WRITEABLE);
				/* We also need to pretend like the socket is write-ready to get
				 * the control flow to behave nicely & flush for us. */
				events = WL_SOCKET_WRITEABLE;
				break;

			case SS_WAIT_EXEC_RESULT:
				switch (walprop_get_query_result(wk->conn))
				{
					/* Successful result, move on to starting the handshake */
					case WP_EXEC_SUCCESS_COPYBOTH:
						/* Because this state is immediately executable, we'll
						 * start this on the next iteration of the loop */
						wk->state = SS_HANDSHAKE_SEND;
						break;

					/* Needs repeated calls to finish. Wait until the socket is
					 * readable */
					case WP_EXEC_NEEDS_INPUT:
						UpdateEventSet(wk, WL_SOCKET_READABLE);
						break;

					case WP_EXEC_FAILED:
						elog(WARNING, "Failed to send query to walkeeper %s:%s: %s",
								wk->host, wk->port, walprop_error_message(wk->conn));
						ResetConnection(i);
						return;

					/* Unexpected result -- funamdentally an error, but we want to produce a custom
					 * message, rather than a generic "something went wrong" */
					case WP_EXEC_UNEXPECTED_SUCCESS:
						elog(WARNING, "Received bad resonse from walkeeper %s:%s query execution",
								wk->host, wk->port);
						ResetConnection(i);
						break;
				}
				break;

			/* Start handshake: first of all send information about the WAL
			 * keeper. After sending, we wait on SS_HANDSHAKE_RECV for a
			 * response to finish the handshake. */
			case SS_HANDSHAKE_SEND:
				/* On failure, logging & resetting the connection is handled. We
				 * just need to handle the control flow. */
				if (!AsyncWrite(i, &proposerGreeting, sizeof(proposerGreeting), SS_HANDSHAKE_RECV))
					return;

				break;

			/* Finish handshake comms: receive information about the WAL keeper */
			case SS_HANDSHAKE_RECV:
				/* If our reading doesn't immediately succeed, any necessary error handling or state
				 * setting is taken care of. We can leave any other work until later. */
				if (!AsyncRead(i, &wk->greet, sizeof(wk->greet)))
					return;

				/* Protocol is all good, move to voting. */
				wk->state = SS_VOTING;
				/* voting is an idle state; wait for read-ready in case the
				 * connection closes */
				UpdateEventSet(wk, WL_SOCKET_READABLE);
				wk->feedback.flushLsn = truncateLsn;
				wk->feedback.hs.ts = 0;

				/*
				 * We want our term to be highest and unique, so choose max
				 * and +1 once we have majority.
				 */
				propTerm = Max(walkeeper[i].greet.term, propTerm);

				/* Check if we have quorum. If there aren't enough walkeepers, wait and do nothing.
				 * We'll eventually get a task when the election starts.
				 *
				 * If we do have quorum, we can start an election */
				if (++n_connected >= quorum)
				{
					if (n_connected == quorum)
					{
						propTerm++;
						/* prepare voting message */
						voteRequest = (VoteRequest) {
							.tag = 'v',
							.term = propTerm
						};
						memcpy(voteRequest.proposerId.data, proposerGreeting.proposerId.data, UUID_LEN);
					}

					/* Now send voting request to the cohort and wait responses */
					for (int j = 0; j < n_walkeepers; j++)
					{
						/* Remember: SS_VOTING indicates that the walkeeper is participating in
						 * voting, but hasn't sent anything yet. The ones that have sent something
						 * are given SS_SEND_VOTE or SS_WAIT_VERDICT. */
						if (walkeeper[j].state == SS_VOTING)
						{
							walkeeper[j].state = SS_SEND_VOTE;
							/* Immediately send info */
							AdvancePollState(j, WL_NO_EVENTS);
						}
					}
				}
				break;

			/* Voting is an idle state - we don't expect any events to trigger. Refer to the
			 * execution of SS_HANDSHAKE_RECV to see how nodes are transferred from SS_VOTING to
			 * SS_SEND_VOTE. */
			case SS_VOTING:
				elog(FATAL, "Unexpected walkeeper %s:%s state advancement: is voting",
					 wk->host, wk->port);
				break; /* actually unreachable, but prevents -Wimplicit-fallthrough */

			/* We have quorum for voting, send our vote request */
			case SS_SEND_VOTE:
				/* On failure, logging & resetting is handled */
				if (!AsyncWrite(i, &voteRequest, sizeof(voteRequest), SS_WAIT_VERDICT))
					return;

				/* If successful, continue on to SS_WAIT_VERDICT (in the next
				 * iteration of the loop) */
				break;

			/* Start reading the walkeeper response for our candidate */
			case SS_WAIT_VERDICT:
				/* If our reading doesn't immediately succeed, any necessary error handling or state
				 * setting is taken care of. We can leave any other work until later. */
				if (!AsyncRead(i, &wk->voteResponse, sizeof(wk->voteResponse)))
					return;

				/*
				 * In case of acceptor rejecting our vote, bail out, but only if
				 * either it already lives in strictly higher term (concurrent
				 * compute spotted) or we are not elected yet and thus need the
				 * vote.
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
				wk->state = SS_IDLE;
				UpdateEventSet(wk, WL_SOCKET_READABLE); /* Idle states wait for read-ready */

				if (++n_votes == quorum)
				{
					DetermineEpochStartLsn();

					/* Check if not all safekeepers are up-to-date, we need to download WAL needed to synchronize them */
					if (truncateLsn < propEpochStartLsn)
					{
						elog(LOG, "start recovery because restart LSN=%X/%X is not equal to epochStartLsn=%X/%X",
							 LSN_FORMAT_ARGS(truncateLsn), LSN_FORMAT_ARGS(propEpochStartLsn));
						/* Perform recovery */
						if (!WalProposerRecovery(donor, proposerGreeting.timeline, truncateLsn, propEpochStartLsn))
							elog(FATAL, "Failed to recover state");
					}
					WalProposerStartStreaming(propEpochStartLsn);
					/* Should not return here */
				}
				else
				{
					/* We are already streaming WAL: send all pending messages to the attached walkeeper */
					SendMessageToNode(i, msgQueueHead);
				}

				break;

			/* Idle state for sending WAL. Moved out only by calls to
			 * SendMessageToNode */
			case SS_IDLE:
				elog(FATAL, "Unexpected walkeeper %s:%s state advancement: is idle", wk->host, wk->port);
				break; /* actually unreachable, but prevents -Wimplicit-fallthrough */

			/* Start to send the message at wk->currMsg. Triggered only by calls
			 * to SendMessageToNode */
			case SS_SEND_WAL:
				WalMessage* msg = wk->currMsg;

				elog(LOG, "Sending message with len %ld commitLsn=%X/%X restart LSN=%X/%X to %s:%s",
					 msg->size - sizeof(AppendRequestHeader),
					 LSN_FORMAT_ARGS(msg->req.commitLsn),
					 LSN_FORMAT_ARGS(truncateLsn),
					 wk->host, wk->port);

				/* We write with msg->size here because the body of the message
				 * is stored after the end of the WalMessage struct, in the
				 * allocation for each msg */
				if (!AsyncWrite(i, &msg->req, msg->size, SS_RECV_FEEDBACK))
					return;

				break;

			/* Start to receive the feedback from a message sent via SS_SEND_WAL */
			case SS_RECV_FEEDBACK:
				WalMessage* next;
				XLogRecPtr  minQuorumLsn;
				WalMessage* commitLsnUpdateMsg;

				/* If our reading doesn't immediately succeed, any necessary error handling or state
				 * setting is taken care of. We can leave any other work until later. */
				if (!AsyncRead(i, &wk->feedback, sizeof(wk->feedback)))
					return;

				next = wk->currMsg->next;
				Assert(wk->feedback.flushLsn == wk->currMsg->req.endLsn);
				wk->currMsg->ackMask |= 1 << i; /* this walkeeper confirms receiving of this message */

				wk->state = SS_IDLE;
				/* Don't update the event set; that's handled by SendMessageToNode if necessary */

				wk->currMsg = NULL;
				HandleWalKeeperResponse();
				SendMessageToNode(i, next);

				/*
				 * Also send the new commit lsn to all the walkeepers.
				 *
				 * FIXME: This is redundant for walkeepers that have other outbound messages
				 * pending.
				 */
				minQuorumLsn = GetAcknowledgedByQuorumWALPosition();

				if (minQuorumLsn > lastSentCommitLsn)
				{
					commitLsnUpdateMsg = CreateMessageCommitLsnOnly();
					if (commitLsnUpdateMsg)
						BroadcastMessage(commitLsnUpdateMsg);
					lastSentCommitLsn = minQuorumLsn;
				}
				break;
		}
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
AsyncRead(int i, void* value, size_t value_size)
{
	WalKeeper* wk = &walkeeper[i];
	char *buf = NULL;
	int buf_size = -1;

	switch (walprop_async_read(wk->conn, &buf, &buf_size))
	{
		/* On success, there's just a couple more things we'll check below */
		case PG_ASYNC_READ_SUCCESS:
			break;

		/* If we need more input, wait until the socket is read-ready and try
		 * again. */
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
	 * If we get here, the read was ok, but we still need to check it was the right amount
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
	return true;
}

/*
 * Starts a write into the 'i'th WAL keeper's postgres connection, moving to the
 * success state only when the write succeeds
 *
 * Returns false only if the write immediately fails. Upon failure, a warning is
 * emitted and the connection is reset.
 */
static bool
AsyncWrite(int i, void* msg, size_t msg_size, WalKeeperState success_state)
{
	WalKeeper* wk = &walkeeper[i];
	bool result;

	switch (walprop_async_write(wk->conn, msg, msg_size))
	{
		case PG_ASYNC_WRITE_SUCCESS:
			wk->state = success_state;
			result = true;
			break;
		case PG_ASYNC_WRITE_TRY_FLUSH:
			/* We still need to call PQflush some more to finish the job, but
			 * we'll still go to success_state when we're done. */
			wk->state = success_state | SMOD_NEEDS_FLUSH;
			UpdateEventSet(wk, WL_SOCKET_WRITEABLE|WL_SOCKET_READABLE);
			result = true;
			break;
		case PG_ASYNC_WRITE_FAIL:
			elog(WARNING, "Failed to send to node %s:%s in %s state: %s",
				 wk->host, wk->port, FormatWalKeeperState(wk->state),
				 walprop_error_message(wk->conn));
			ResetConnection(i);
			result = false;
			break;
	}

	return result;
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
