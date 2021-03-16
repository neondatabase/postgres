/*-------------------------------------------------------------------------
 *
 * safekeeper_proxy.c - receive streaming WAL data and braodcast it to multiple safekeepers
 *
 * Author: Konstantin Knizhnik <knizhnik@garret.ru>
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/safekeeper/safekeeper_proxy.c
 *-------------------------------------------------------------------------
 */

#include "safekeeper.h"

#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "common/file_perm.h"
#include "common/logging.h"
#include "getopt_long.h"
#include "libpq-fe.h"
#include "receivelog.h"
#include "streamutil.h"

/* Global options */
static int	verbose = 0;
static int  quorum = 0;
static int  n_safekeepers = 0;

static char*        safekeepersList;
static Safekeeper   safekeeper[MAX_SAFEKEEPERS];
static WalMessage*  msgQueueHead;
static WalMessage*  msgQueueTail;
static ServerInfo   serverInfo;
static fd_set       readSet;
static fd_set       writeSet;
static int          maxFds;
static SafekeeperResponse lastFeedback;
static XLogRecPtr   minRestartLsn; /* Last position received by all safekeepers. */

static void
disconnect_atexit(void)
{
	if (conn != NULL)
		PQfinish(conn);
}

/*
 * Send a Standby Status Update message to server.
 */
static bool
SendFeedback(PGconn *conn, XLogRecPtr blockpos, TimestampTz now, bool replyRequested)
{
	char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int			len = 0;

	replybuf[len] = 'r';
	len += 1;
	fe_sendint64(blockpos, &replybuf[len]); /* write */
	len += 8;
	fe_sendint64(blockpos, &replybuf[len]);	/* flush */
	len += 8;
	fe_sendint64(InvalidXLogRecPtr, &replybuf[len]);	/* apply */
	len += 8;
	fe_sendint64(now, &replybuf[len]);	/* sendTime */
	len += 8;
	replybuf[len] = replyRequested ? 1 : 0; /* replyRequested */
	len += 1;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		pg_log_error("Could not send feedback packet: %s",
					 PQerrorMessage(conn));
		return false;
	}

	return true;
}

/*
 * Send a hot standby feedback to the master
 */
static bool
SendHSFeedback(PGconn *conn, HotStandbyFeedback* hs)
{
	char		replybuf[1 + 8 + 8 + 8];
	int			len = 0;

	replybuf[len] = 'h';
	len += 1;
	fe_sendint64(hs->ts, &replybuf[len]); /* write */
	len += 8;
	fe_sendint32(XidFromFullTransactionId(hs->xmin), &replybuf[len]);
	len += 4;
	fe_sendint32(EpochFromFullTransactionId(hs->xmin), &replybuf[len]);
	len += 4;
	fe_sendint32(XidFromFullTransactionId(hs->catalog_xmin), &replybuf[len]);
	len += 4;
	fe_sendint32(EpochFromFullTransactionId(hs->catalog_xmin), &replybuf[len]);
	len += 4;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		pg_log_error("Could not send hot standby feedback packet: %s",
					 PQerrorMessage(conn));
		return false;
	}

	return true;
}


/*
 * Combine hot standby feedbacks from all safekeepers.
 */
static void
CombineHotStanbyFeedbacks(HotStandbyFeedback* hs)
{
	hs->ts = 0;
	hs->xmin.value = ~0; /* largest unsigned value */
	hs->catalog_xmin.value = ~0; /* largest unsigned value */

	for (int i = 0; i < n_safekeepers; i++)
	{
		if (safekeeper[i].feedback.hs.ts != 0)
		{
			if (FullTransactionIdPrecedes(safekeeper[i].feedback.hs.xmin, hs->xmin))
			{
				hs->xmin = safekeeper[i].feedback.hs.xmin;
				hs->ts = safekeeper[i].feedback.hs.ts;
			}
			if (FullTransactionIdPrecedes(safekeeper[i].feedback.hs.catalog_xmin, hs->catalog_xmin))
			{
				hs->catalog_xmin = safekeeper[i].feedback.hs.catalog_xmin;
				hs->ts = safekeeper[i].feedback.hs.ts;
			}
		}
	}
}

/*
 * This function is called to establish new connection or to reestablish connection in case
 * of connection failure.
 * Close current connection if any and try to initiate new one
 */
static void
ResetConnection(int i)
{
	bool established;

	if (safekeeper[i].state != SS_OFFLINE)
	{
		pg_log_info("Connection with node %s:%s failed: %m",
					safekeeper[i].host, safekeeper[i].port);

		/* Close old connecton */
		closesocket(safekeeper[i].sock);
		FD_CLR(safekeeper[i].sock, &writeSet);
		FD_CLR(safekeeper[i].sock, &readSet);
		safekeeper[i].sock = PGINVALID_SOCKET;
		safekeeper[i].state = SS_OFFLINE;
	}

	/* Try to establish new connection */
	safekeeper[i].sock = ConnectSocketAsync(safekeeper[i].host, safekeeper[i].port, &established);
	if (safekeeper[i].sock != PGINVALID_SOCKET)
	{
		pg_log_info("%s with node %s:%s",
					established ? "Connected" : "Connecting", safekeeper[i].host, safekeeper[i].port);
		if (safekeeper[i].sock > maxFds)
			maxFds = safekeeper[i].sock;

		if (established)
		{
			/* Start handshake: first of all send information about server */
			if (WriteSocket(safekeeper[i].sock, &serverInfo, sizeof serverInfo))
			{
				FD_SET(safekeeper[i].sock, &readSet);
				safekeeper[i].state = SS_HANDSHAKE;
				safekeeper[i].asyncOffs = 0;
			}
			else
			{
				ResetConnection(i);
			}
		}
		else
		{
			FD_SET(safekeeper[i].sock, &writeSet);
			safekeeper[i].state = SS_CONNECTING;
		}
	}
}

/*
 * Calculate WAL position acknowledged by quorum
 */
static XLogRecPtr
GetAcknowledgedByQuorumWALPosition(void)
{
	XLogRecPtr responses[MAX_SAFEKEEPERS];
	/*
	 * Sort acknowledged LSNs
	 */
	for (int i = 0; i < n_safekeepers; i++)
	{
		responses[i] = safekeeper[i].feedback.flushLsn;
	}
	qsort(responses, n_safekeepers, sizeof(XLogRecPtr), CompareLsn);

	/*
	 * Get the smallest LSN committed by quorum
	 */
	return responses[n_safekeepers - quorum];
}


static bool
HandleSafekeeperResponse(PGconn* conn)
{
	HotStandbyFeedback hsFeedback;
	XLogRecPtr minQuorumLsn = GetAcknowledgedByQuorumWALPosition();
	if (minQuorumLsn > lastFeedback.flushLsn)
	{
		lastFeedback.flushLsn = minQuorumLsn;
		if (!SendFeedback(conn, lastFeedback.flushLsn, feGetCurrentTimestamp(), false))
			return false;
	}
	CombineHotStanbyFeedbacks(&hsFeedback);
	if (hsFeedback.ts != 0 && memcmp(&hsFeedback, &lastFeedback.hs, sizeof hsFeedback) != 0)
	{
		lastFeedback.hs = hsFeedback;
		if (!SendHSFeedback(conn, &hsFeedback))
			return false;
	}

	/* Cleanup message queue */
	while (msgQueueHead != NULL && msgQueueHead->ackMask == ((1 << n_safekeepers) - 1))
	{
		WalMessage* msg = msgQueueHead;
		msgQueueHead = msg->next;
		Assert(minRestartLsn <= msg->walPos);
		minRestartLsn = WAL_MSG_END(msg);
		pfree(msg->data);
		pfree(msg);
	}
	if (!msgQueueHead) /* queue is empty */
		msgQueueTail = NULL;

	return true;
}


static void
usage(void)
{
	printf(_("%s tee PostgreSQL streaming write-ahead logs.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  -q, --quorum           quorum for sending response to server\n"));
	printf(_("  -r, --safekeepers      comma separated list of safe keeprs in format 'host1:port1,host2:port2'\n"));
	printf(_("  -v, --verbose          output verbose messages\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nConnection options:\n"));
	printf(_("  -d, --dbname=CONNSTR   connection string\n"));
	printf(_("  -h, --host=HOSTNAME    database server host or socket directory\n"));
	printf(_("  -p, --port=PORT        database server port number\n"));
	printf(_("  -U, --username=NAME    connect as specified database user\n"));
	printf(_("  -w, --no-password      never prompt for password\n"));
	printf(_("  -W, --password         force password prompt (should happen automatically)\n"));
	printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT);
	printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}


/*
 * Send message to the particular node
 */
static void
SendMessageToNode(int i, WalMessage* msg)
{
	ssize_t rc;

	/* If there is no pending message then send new one */
	if (safekeeper[i].currMsg == NULL)
		safekeeper[i].currMsg = msg;
	else
		msg = safekeeper[i].currMsg;

	if (msg != NULL)
	{
		/* Send minRestartLsn instead of timestamp (which is not needed for safekeepers) */
		fe_sendint64(minRestartLsn, &msg->data[XLOG_HDR_TS_POS]);

		rc = WriteSocketAsync(safekeeper[i].sock, msg->data, msg->size);
		if (rc < 0)
		{
			ResetConnection(i);
		}
		else if ((size_t)rc == msg->size) /* message was completely sent */
		{
			safekeeper[i].asyncOffs = 0;
			safekeeper[i].state = SS_RECV_FEEDBACK;
		}
		else
		{
			/* wait until socket is avaialble for write */
			safekeeper[i].state = SS_SEND_WAL;
			safekeeper[i].asyncOffs = rc;
			FD_SET(safekeeper[i].sock, &writeSet);
		}
	}
}

/*
 * Broadcast new message tp all caught-up safekeepers
 */
static void
BroadcastMessage(WalMessage* msg)
{
	for (int i = 0; i < n_safekeepers; i++)
	{
		if (safekeeper[i].state == SS_IDLE && safekeeper[i].currMsg == NULL)
		{
			SendMessageToNode(i, msg);
		}
	}
}


/*
 * Send termination message to safekeepers
 */
static void
StopSafekeepers(void)
{
	char const msg[XLOG_HDR_SIZE] = {'q'}; /* quit */

	Assert(!msgQueueHead); /* there should be no pending messages */

	for (int i = 0; i < n_safekeepers; i++)
	{
		if (safekeeper[i].sock != PGINVALID_SOCKET)
		{
			WriteSocket(safekeeper[i].sock, msg, sizeof(msg));
			closesocket(safekeeper[i].sock);
			safekeeper[i].sock = PGINVALID_SOCKET;
		}
	}
}

/*
 * Create WAL message frpm received COPy data and link it into queue
 */
static WalMessage*
CreateMessage(char* data, int len)
{
	/* Create new message and append it to message queue */
	WalMessage*	msg = (WalMessage*)pg_malloc(sizeof(WalMessage));
	if (msgQueueTail != NULL)
		msgQueueTail->next = msg;
	else
		msgQueueHead = msg;
	msgQueueTail = msg;

	msg->data = data;
	msg->size = len;
	msg->next = NULL;
	msg->ackMask = 0;
	msg->walPos = fe_recvint64(&data[XLOG_HDR_START_POS]);
	/* Set walEnd to the end of the record - we will use it at safekeepr to calculate wal record size */
	fe_sendint64(WAL_MSG_END(msg), &data[XLOG_HDR_END_POS]);
	return msg;
}

/*
 * Synchronize state of safekeepers.
 * We will find most advanced safekeeper within quorum and download
 * from it WAL from max(restartLsn) till max(flushLsn).
 * Then we adjust message queue to popuate rest safekeepers with missed WAL.
 * It enforces the rule that there are no "alternative" versions of WAL in safekeepers.
 * Before any record from new epoch can reach safekeeper, we enforce that all WAL records fro prior epoches
 * are pushed here.
 */
static bool
StartRecovery(void)
{
	int i;
	XLogRecPtr restartLsn = serverInfo.walSegSize;
	XLogRecPtr flushLsn = 0;
	int mostAdvancedSafekeeper = 0;
	PGresult  *res;

	/* Determine max(restartLsn) and max(flushLsn) within quorum */
	for (i = 0; i < n_safekeepers; i++)
	{
		if (safekeeper[i].state == SS_IDLE)
		{
			/* We took maximum of restartLsn stored at safekeepers because it is most up-to-data value */
			restartLsn = Max(safekeeper[i].info.restartLsn, restartLsn);
			if (safekeeper[i].info.flushLsn > flushLsn)
			{
				flushLsn = safekeeper[i].info.flushLsn;
				mostAdvancedSafekeeper = i;
			}
		}
	}
	pg_log_info("Restart LSN=" INT64_FORMAT ", flush LSN=" INT64_FORMAT,
				restartLsn, flushLsn);

	minRestartLsn = restartLsn;

	if (restartLsn != flushLsn) /* if not all safekeepers are up-to-date, we need to download WAL needed to sycnhronize them */
	{
		WalMessage* msg;
		char query[256];
		PGconn* conn = ConnectSafekeeper(safekeeper[mostAdvancedSafekeeper].host,
										 safekeeper[mostAdvancedSafekeeper].port);
		if (!conn)
			return false;

		snprintf(query, sizeof(query), "START_REPLICATION %X/%X TIMELINE %u TILL %X/%X", /* TILL is safekeeper exension of START_REPLICATION command */
				 (uint32) (restartLsn >> 32), (uint32)restartLsn,
				 serverInfo.timeline,
				 (uint32) (flushLsn >> 32), (uint32)flushLsn);
		res = PQexec(conn, query);
		if (PQresultStatus(res) != PGRES_COPY_BOTH)
		{
			pg_log_error("could not send replication command \"%s\": %s",
						 "START_REPLICATION", PQresultErrorMessage(res));
			PQclear(res);
			PQfinish(conn);
			return false;
		}
		/*
		 * Receive WAL from most advaned safekeeper. As far as connection quorum may be different from last commit quorum,
		 * we can not conclude wether last wal record was committed or not. So we assume that it is committed and replicate it
		 * to all all safekeepers.
		 */
		do
		{
			char* copybuf;
			int rawlen = PQgetCopyData(conn, &copybuf, 0);
			if (rawlen <= 0)
			{
				if (rawlen == -2)
					pg_log_error("Could not read COPY data: %s", PQerrorMessage(conn));
				else
					pg_log_info("End of WAL stream reached");
				PQfinish(conn);
				return false;
			}
			Assert (copybuf[0] == 'w');
			msg = CreateMessage(copybuf, rawlen);
		} while (WAL_MSG_END(msg) < flushLsn); /* loop until we reach last flush position */

		/* Setup restart point for all safekeepers */
		for (i = 0; i < n_safekeepers; i++)
		{
			if (safekeeper[i].state == SS_IDLE)
			{
				for (msg = msgQueueHead; msg != NULL; msg = msg->next)
				{
					if (WAL_MSG_END(msg) <= safekeeper[i].info.flushLsn)
					{
						msg->ackMask |= 1 << i; /* message is aready received by this safekeeper */
					}
					else
					{
						safekeeper[i].currMsg = msg; /* start replication for this safekeeper from this message */
						break;
					}
				}
			}
		}
	}
	return true;
}

/*
 * Start WAL sender at master
 */
static bool
StartReplication(PGconn* conn)
{
	char	   query[128];
	PGresult  *res;
	XLogRecPtr startpos = serverInfo.walEnd;

	/*
	 * Always start streaming at the beginning of a segment
	 */
	startpos -= XLogSegmentOffset(startpos, serverInfo.walSegSize);

	/* Initiate the replication stream at specified location */
	snprintf(query, sizeof(query), "START_REPLICATION %X/%X TIMELINE %u",
			 (uint32) (startpos >> 32), (uint32)startpos,
			 serverInfo.timeline);
	if (verbose)
		pg_log_info("%s", query);
	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		pg_log_error("could not send replication command \"%s\": %s",
					 "START_REPLICATION", PQresultErrorMessage(res));
		PQclear(res);
		return false;
	}
	PQclear(res);
	return true;
}

/*
 * WAL broadcasting loop
 */
static void
BroadcastWalStream(PGconn* conn)
{
	pgsocket server = PQsocket(conn);
	bool     streaming = true;
	int      i;
	ssize_t  rc;
	int      n_votes = 0;
	int      n_connected = 0;
	NodeId   maxNodeId;

	FD_ZERO(&readSet);
	FD_ZERO(&writeSet);
	maxFds = server;

	/* Initiate connections to all safekeeper nodes */
	for (i = 0; i < n_safekeepers; i++)
	{
		ResetConnection(i);
	}

	while (streaming || msgQueueHead != NULL) /* continue until server is streaming WAL or we have some unacknowledged messages */
	{
		fd_set rs = readSet;
		fd_set ws = writeSet;
		rc = select(maxFds+1, &rs, &ws, NULL, NULL);
		if (rc < 0)
		{
			pg_log_error("Select failed: %m");
			break;
		}
		if (server != PGINVALID_SOCKET && FD_ISSET(server, &rs)) /* New message from server */
		{
			char* copybuf;
			int rawlen = PQgetCopyData(conn, &copybuf, 0);
			if (rawlen <= 0)
			{
				if (rawlen == -2)
					pg_log_error("Could not read COPY data: %s", PQerrorMessage(conn));
				else
					pg_log_info("End of WAL stream reached");
				FD_CLR(server, &readSet);
				closesocket(server);
				server = PGINVALID_SOCKET;
				streaming = false;
				continue;
			}
			if (copybuf[0] == 'w')
			{
				WalMessage* msg = CreateMessage(copybuf, rawlen);
				BroadcastMessage(msg);
			}
			else
			{
				Assert(copybuf[0] == 'k'); /* keep alive */
				if (copybuf[KEEPALIVE_RR_OFFS] /* response requested */
					&& !SendFeedback(conn, lastFeedback.flushLsn, feGetCurrentTimestamp(), true))
				{
					FD_CLR(server, &readSet);
					closesocket(server);
					server = PGINVALID_SOCKET;
					streaming = false;
				}
				pfree(copybuf);
			}
		}
		else /* communication with safekeepers */
		{
			for (int i = 0; i < n_safekeepers; i++)
			{
				if (safekeeper[i].sock == PGINVALID_SOCKET)
					continue;
				if (FD_ISSET(safekeeper[i].sock, &rs))
				{
					switch (safekeeper[i].state)
					{
					  case SS_HANDSHAKE:
					  {
						  /* Receive safekeeper node state */
						  rc = ReadSocketAsync(safekeeper[i].sock,
											   (char*)&safekeeper[i].info + safekeeper[i].asyncOffs,
											   sizeof(safekeeper[i].info) - safekeeper[i].asyncOffs);
						  if (rc < 0)
						  {
							  ResetConnection(i);
						  }
						  else if ((safekeeper[i].asyncOffs += rc) == sizeof(safekeeper[i].info))
						  {
							  /* Safekeeper resonse completely received */

							  /* Check protocol version */
							  if (safekeeper[i].info.server.protocolVersion != SK_PROTOCOL_VERSION)
							  {
								  pg_log_error("Safekeeper has incompatible protocol version %d vs. %d",
											   safekeeper[i].info.server.protocolVersion, SK_PROTOCOL_VERSION);
								  ResetConnection(i);
							  }
							  else
							  {
								  safekeeper[i].state = SS_VOTE;
								  safekeeper[i].feedback.flushLsn = safekeeper[i].info.server.walEnd;
								  safekeeper[i].feedback.hs.ts = 0;

								  /* RAFT term comparison */
								  if (CompareNodeId(&safekeeper[i].info.server.nodeId, &maxNodeId) > 0)
									  maxNodeId = safekeeper[i].info.server.nodeId;

								  /* Check if we have quorum */
								  if (++n_connected >= quorum)
								  {
									  if (n_connected == quorum)
										  maxNodeId.term += 1; /* increment term to generate new unique identifier */

									  /* Now send max-node-id to everyone participating in voting and wait their responses */
									  for (int j = 0; j < n_safekeepers; j++)
									  {
										  if (safekeeper[j].state == SS_VOTE)
										  {
											  if (!WriteSocket(safekeeper[j].sock, &maxNodeId, sizeof(maxNodeId)))
											  {
												  ResetConnection(j);
											  }
											  else
											  {
												  safekeeper[j].asyncOffs = 0;
												  safekeeper[j].state = SS_WAIT_VERDICT;
											  }
										  }
									  }
								  }
							  }
						  }
						  break;
					  }
					  case SS_WAIT_VERDICT:
					  {
						  /* Receive safekeeper response for our candidate */
						  rc = ReadSocketAsync(safekeeper[i].sock,
											   (char*)&safekeeper[i].info.server.nodeId + safekeeper[i].asyncOffs,
											   sizeof(safekeeper[i].info.server.nodeId) - safekeeper[i].asyncOffs);
						  if (rc < 0)
						  {
							  ResetConnection(i);
						  }
						  else if ((safekeeper[i].asyncOffs += rc) == sizeof(safekeeper[i].info.server.nodeId))
						  {
							  /* Response completely received */

							  /* If server accept our candidate, then it returns it in response */
							  if (CompareNodeId(&safekeeper[i].info.server.nodeId, &maxNodeId) != 0)
							  {
								  pg_log_error("SafeKeeper %s:%s with term " INT64_FORMAT " rejects our connection request with term " INT64_FORMAT "",
											   safekeeper[i].host, safekeeper[i].port,
											   safekeeper[i].info.server.nodeId.term, maxNodeId.term);
								  exit(1);
							  }
							  else
							  {
								  /* Handshake completed, do we have quorum? */
								  safekeeper[i].state = SS_IDLE;
								  if (++n_votes == quorum)
								  {
									  pg_log_info("Successfully established connection with %d nodes and start streaming",
												  quorum);
									  /* Perform recovery */
									  if (!StartRecovery())
										  exit(1);

									  /* Start replication from master */
									  if (StartReplication(conn))
									  {
										  FD_SET(server, &readSet);
									  }
									  else
									  {
										  exit(1);
									  }
								  }
								  else
								  {
									  /* We are already streaming WAL: send all pending messages to the attached safekeeper */
									  SendMessageToNode(i, msgQueueHead);
								  }
							  }
						  }
						  break;
					  }
					  case SS_RECV_FEEDBACK:
					  {
						  /* Read safekeeper response with flushed WAL position */
						  rc = ReadSocketAsync(safekeeper[i].sock,
											   (char*)&safekeeper[i].feedback + safekeeper[i].asyncOffs,
											   sizeof(safekeeper[i].feedback) - safekeeper[i].asyncOffs);
						  if (rc < 0)
						  {
							  ResetConnection(i);
						  }
						  else if ((safekeeper[i].asyncOffs += rc) == sizeof(safekeeper[i].feedback))
						  {
							  WalMessage* next = safekeeper[i].currMsg->next;
							  Assert(safekeeper[i].feedback.flushLsn == fe_recvint64(&safekeeper[i].currMsg->data[XLOG_HDR_END_POS]));
							  safekeeper[i].currMsg->ackMask |= 1 << i; /* this safekeeper confirms receiving of this message */
							  safekeeper[i].state = SS_IDLE;
							  safekeeper[i].asyncOffs = 0;
							  safekeeper[i].currMsg = NULL;
							  SendMessageToNode(i, next);
							  if (!HandleSafekeeperResponse(conn))
							  {
								  FD_CLR(server, &readSet);
								  closesocket(server);
								  server = PGINVALID_SOCKET;
								  streaming = false;
							  }
						  }
						  break;
					  }
					  case SS_IDLE:
					  {
						  pg_log_info("Safekeer %s:%s drops connection", safekeeper[i].host, safekeeper[i].port);
						  ResetConnection(i);
						  break;
					  }
					  default:
					  {
						  pg_log_error("Unexpected safekeeper %s:%s read state %d", safekeeper[i].host, safekeeper[i].port, safekeeper[i].state);
						  exit(1);
					  }
					}
				}
				else if (FD_ISSET(safekeeper[i].sock, &ws))
				{
					switch (safekeeper[i].state)
					{
					  case SS_CONNECTING:
					  {
						  int			optval;
						  ACCEPT_TYPE_ARG3 optlen = sizeof(optval);
						  if (getsockopt(safekeeper[i].sock, SOL_SOCKET, SO_ERROR, (char *) &optval, &optlen) < 0 || optval != 0)
						  {
							  pg_log_error("Failed to connect to node '%s:%s': %m",
										   safekeeper[i].host, safekeeper[i].port);
							  closesocket(safekeeper[i].sock);
							  FD_CLR(safekeeper[i].sock, &writeSet);
							  safekeeper[i].sock =  PGINVALID_SOCKET;
							  safekeeper[i].state = SS_OFFLINE;
						  }
						  else
						  {
							  uint32 len = 0;
							  FD_CLR(safekeeper[i].sock, &writeSet);
							  FD_SET(safekeeper[i].sock, &readSet);
							  /*
							   * Start handshake: send information about server.
							   * Frist of all send 0 as package size: it allows safe keeper to distinguish
							   * connection from safekeeper_proxy from standard replication connection from pagers.
							   */
							  if (WriteSocket(safekeeper[i].sock, &len, sizeof len)
								  && WriteSocket(safekeeper[i].sock, &serverInfo, sizeof serverInfo))
							  {
								  safekeeper[i].state = SS_HANDSHAKE;
								  safekeeper[i].asyncOffs = 0;
							  }
							  else
							  {
								  ResetConnection(i);
							  }
						  }
						  break;
					  }
					  case SS_SEND_WAL:
					  {
						  rc = WriteSocketAsync(safekeeper[i].sock, safekeeper[i].currMsg->data + safekeeper[i].asyncOffs, safekeeper[i].currMsg->size - safekeeper[i].asyncOffs);
						  if (rc < 0)
						  {
							  ResetConnection(i);
						  }
						  else if ((safekeeper[i].asyncOffs += rc) == safekeeper[i].currMsg->size)
						  {
							  /* WAL block completely sent */
							  safekeeper[i].state = SS_RECV_FEEDBACK;
							  safekeeper[i].asyncOffs = 0;
							  FD_CLR(safekeeper[i].sock, &writeSet);
						  }
						  break;
					  }
					  default:
						pg_log_error("Unexpected write state %d", safekeeper[i].state);
						exit(1);
					}
				}
			}
		}
	}
	StopSafekeepers();
}

int
main(int argc, char **argv)
{
	static struct option long_options[] = {
		{"help", no_argument, NULL, '?'},
		{"version", no_argument, NULL, 'V'},
		{"dbname", required_argument, NULL, 'd'},
		{"safekeepers", required_argument, NULL, 's'},
		{"username", required_argument, NULL, 'U'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	int			c;
	int			option_index;
	char	   *db_name;
	PGconn 	   *conn;
	char       *host;
	char       *port;
	char       *sep;
	char       *sysid;

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
			puts("sefekeeper (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "d:h:p:U:s:vwW",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'd':
				connection_string = pg_strdup(optarg);
				break;
			case 'h':
				dbhost = pg_strdup(optarg);
				break;
			case 'p':
				if (atoi(optarg) <= 0)
				{
					pg_log_error("invalid port number \"%s\"", optarg);
					exit(1);
				}
				dbport = pg_strdup(optarg);
				break;
			case 'U':
				dbuser = pg_strdup(optarg);
				break;
			case 's':
			    safekeepersList = pg_strdup(optarg);
				break;
			case 'w':
				dbgetpassword = -1;
				break;
			case 'W':
				dbgetpassword = 1;
				break;
			case 'v':
				verbose++;
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

	for (host = safekeepersList; host != NULL && *host != '\0'; host = sep)
	{
		port = strchr(host, ':');
		if (port == NULL) {
			pg_log_error("port is not specified");
			exit(1);
		}
		*port++ = '\0';
		sep = strchr(port, ',');
		if (sep != NULL)
			*sep++ = '\0';
		if (n_safekeepers+1 >= MAX_SAFEKEEPERS)
		{
			pg_log_error("Too many safekeepers");
			exit(1);
		}
		safekeeper[n_safekeepers].host = host;
		safekeeper[n_safekeepers].port = port;
		safekeeper[n_safekeepers].state = SS_OFFLINE;
		safekeeper[n_safekeepers].sock = PGINVALID_SOCKET;
		safekeeper[n_safekeepers].currMsg = NULL;
		n_safekeepers += 1;
	}
	if (n_safekeepers < 1)
	{
		pg_log_error("Safekeepers addresses aer not specified");
		exit(1);
	}
	if (quorum == 0)
	{
		quorum = n_safekeepers/2 + 1;
	}
	else if (quorum < n_safekeepers/2 + 1 || quorum > n_safekeepers)
	{
		pg_log_error("Invalid quorum value: %d, should be %d..%d", quorum, n_safekeepers/2 + 1, n_safekeepers);
		exit(1);
	}

	/*
	 * Obtain a connection before doing anything.
	 */
	conn = GetConnection();
	if (!conn)
		/* error message already written in GetConnection() */
		exit(1);
	atexit(disconnect_atexit);

	/*
	 * Run IDENTIFY_SYSTEM to make sure we've successfully have established a
	 * replication connection and haven't connected using a database specific
	 * connection.
	 */
	if (!RunIdentifySystem(conn, &sysid, &serverInfo.timeline, &serverInfo.walEnd, &db_name))
		exit(1);


	/* determine remote server's xlog segment size */
	if (!RetrieveWalSegSize(conn))
		exit(1);

	/* Fill information about server */
	serverInfo.walSegSize = WalSegSz;
	serverInfo.pgVersion = PG_VERSION_NUM;
	serverInfo.protocolVersion = SK_PROTOCOL_VERSION;
	pg_strong_random(&serverInfo.nodeId.uuid, sizeof(serverInfo.nodeId.uuid));
	sscanf(sysid, INT64_FORMAT, &serverInfo.systemId);

	/*
	 * Check that there is a database associated with connection, none should
	 * be defined in this context.
	 */
	if (db_name)
	{
		pg_log_error("replication connection is unexpectedly database specific");
		exit(1);
	}

	BroadcastWalStream(conn);

	PQfinish(conn);

	return 0;
}
