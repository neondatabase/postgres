#ifndef __WALKEEPER_H__
#define __WALKEEPER_H__

#include "access/xlogdefs.h"
#include "postgres.h"
#include "port.h"
#include "access/xlog_internal.h"
#include "access/transam.h"
#include "nodes/replnodes.h"
#include "utils/uuid.h"
#include "replication/walreceiver.h"

#define SK_MAGIC              0xCafeCeefu
#define SK_PROTOCOL_VERSION   1

#define MAX_WALKEEPERS        32
#define XLOG_HDR_SIZE         (1+8*3)  /* 'w' + startPos + walEnd + timestamp */
#define XLOG_HDR_START_POS    1        /* offset of start position in wal sender message header */
#define XLOG_HDR_END_POS      (1+8)    /* offset of end position in wal sender message header */

/*
 * In the spirit of WL_SOCKET_READABLE and others, this corresponds to no events having occured,
 * because all WL_* events are given flags equal to some (1 << i), starting from i = 0
 */
#ifndef WL_NO_EVENTS
#define WL_NO_EVENTS 0
#else
#error "WL_NO_EVENTS already defined"
#endif

extern char* wal_acceptors_list;
extern int   wal_acceptor_reconnect_timeout;
extern bool  am_wal_proposer;

struct WalProposerConn; /* Defined in libpqwalproposer */
typedef struct WalProposerConn WalProposerConn;

struct WalMessage;
typedef struct WalMessage WalMessage;

extern char *zenith_timeline_walproposer;
extern char *zenith_tenant_walproposer;

/* Possible return values from ReadPGAsync */
typedef enum
{
	/* The full read was successful. buf now points to the data */
	PG_ASYNC_READ_SUCCESS,
	/* The read is ongoing. Wait until the connection is read-ready, then try
	 * again. */
	PG_ASYNC_READ_CONSUME_AND_TRY_AGAIN,
	/* Reading failed. Check PQerrorMessage(conn) */
	PG_ASYNC_READ_FAIL,
} PGAsyncReadResult;

/* Possible return values from WritePGAsync */
typedef enum
{
	/* The write fully completed */
	PG_ASYNC_WRITE_SUCCESS,
	/* The write started, but you'll need to call PQflush some more times
	 * to finish it off. We just tried, so it's best to wait until the
	 * connection is read- or write-ready to try again.
	 *
	 * If it becomes read-ready, call PQconsumeInput and flush again. If it
	 * becomes write-ready, just call PQflush.
	 */
	PG_ASYNC_WRITE_TRY_FLUSH,
	/* Writing failed. Check PQerrorMessage(conn) */
	PG_ASYNC_WRITE_FAIL,
} PGAsyncWriteResult;

/*
 * WAL safekeeper state
 *
 * Some states may be paired with additional modifiers (prefixed "SMOD"), which
 * indicate the type of libpq-specific polling that may be necessary to proceed
 * to that state. Polling types that would be single-use (like PQconnectPoll)
 * are just executed from within the state itself.
 *
 * For each state that can be paired with a modifier, we list in the comment
 * above it the modifier(s) it can be paired with.
 */
typedef enum
{
	/* --- Modifiers --- */

	/* Currently, there's only one modifier :) */
	SMOD_NEEDS_FLUSH = 64, /* In order to finish moving to this state, we need to flush */

	/* Marker for all possible modifiers - used as: state = state & (~SMOD_ALL) to remove modifiers */
	SMOD_ALL = 64,

	/* --- Regular states --- */

	/*
	 * Does not have an active connection and will stay that way until
	 * further notice.
	 *
	 * Moves to SS_CONNECTING_WRITE by calls to ResetConnection.
	 */
	SS_OFFLINE = 0,

	/*
	 * Connecting states. "_READ" waits for the socket to be available for
	 * reading, "_WRITE" waits for writing. There's no difference in the code
	 * they execute when polled, but we have this distinction in order to
	 * recreate the event set in HackyRemoveWalProposerEvent.
	 *
	 * After the connection is made, moves to SS_EXEC_STARTWALPUSH.
	 */
	SS_CONNECTING_READ,
	SS_CONNECTING_WRITE,

	/*
	 * Sending the "START_WAL_PUSH" message as an empty query to the walkeeper.
	 * May be paired with:
	 *   - SMOD_NEEDS_FLUSH
	 *
	 * After the query sends, moves to SS_WAIT_EXEC_RESULT.
	 */
	SS_EXEC_STARTWALPUSH,
	/*
	 * Waiting for the result of the "START_WAL_PUSH" command.
	 *
	 * After we get a successful result, moves to SS_HANDSHAKE_SEND.
	 */
	SS_WAIT_EXEC_RESULT,

	/*
	 * Executing the sending half of the handshake. May be paired with:
	 *   - SMOD_NEEDS_FLUSH
	 *
	 * After sending, moves to SS_HANDSHAKE_RECV.
	 */
	SS_HANDSHAKE_SEND,
	/*
	 * Executing the receiving half of the handshake.
	 *
	 * After receiving, moves to SS_VOTING.
	 */
	SS_HANDSHAKE_RECV,

	/*
	 * Currently participating in voting, but a quorum hasn't yet been reached.
	 * This is an idle state - we do not expect AdvancePollState to be called.
	 *
	 * Moved externally to SS_SEND_VOTE or SS_WAIT_VERDICT by execution of
	 * SS_HANDSHAKE_RECV.
	 */
	SS_VOTING,
	/*
	 * Currently sending the assigned vote. 
	 */
	SS_SEND_VOTE,
	/*
	 * Sent voting information, waiting to receive confirmation from the node.
	 * May be paired with:
	 *   - SMOD_NEEDS_FLUSH
	 *
	 * After receiving, moves to SS_IDLE.
	 */
	SS_WAIT_VERDICT,

	/*
	 * Waiting for quorum to send WAL. Idle state. If the socket becomes
	 * read-ready, the connection has been closed.
	 *
	 * Moves to SS_SEND_WAL only by calls to SendMessageToNode.
	 */
	SS_IDLE,
	/*
	 * Currently sending the message at currMsg. This state is only ever reached
	 * through calls to SendMessageToNode. May be paired with:
	 *   - SMOD_NEEDS_FLUSH
	 *
	 * After sending, moves to SS_RECV_FEEDBACK.
	 */
	SS_SEND_WAL,
	/*
	 * Currently reading feedback from sending the WAL.
	 *
	 * After reading, moves to (SS_SEND_WAL or SS_IDLE) by calls to
	 * SendMessageToNode.
	 */
	SS_RECV_FEEDBACK,
} WalKeeperState;

/* Consensus logical timestamp. */
typedef uint64 term_t;

/*
 * Proposer -> Acceptor messaging.
 */

/* Initial Proposer -> Acceptor message */
typedef struct ProposerGreeting
{
	uint64	   tag;				  /* message tag */
	uint32	   protocolVersion;	  /* proposer-walkeeper protocol version */
	uint32	   pgVersion;
	pg_uuid_t  proposerId;
	uint64	   systemId;		  /* Postgres system identifier */
	uint8	   ztimelineid[16];	  /* Zenith timeline id */
	uint8	   ztenantid[16];
	TimeLineID timeline;
	uint32	   walSegSize;
} ProposerGreeting;

/*
 * Acceptor -> Proposer initial response: the highest term acceptor voted for.
 */
typedef struct AcceptorGreeting
{
	uint64		tag;
	term_t		term;
} AcceptorGreeting;

/*
 * Proposer -> Acceptor vote request.
 */
typedef struct VoteRequest
{
	uint64		tag;
	term_t		term;
	pg_uuid_t   proposerId; /* for monitoring/debugging */
} VoteRequest;

/* Vote itself, sent from safekeeper to proposer */
typedef struct VoteResponse {
	uint64 tag;
	term_t term; /* not really needed, just adds observability */
	uint64 voteGiven;
    /// Safekeeper's log position, to let proposer choose the most advanced one
	term_t epoch;
	XLogRecPtr flushLsn;
	XLogRecPtr truncateLsn;  /* minimal LSN which may be needed for recovery of some walkeeper */
} VoteResponse;

/*
 * Header of request with WAL message sent from proposer to walkeeper.
 */
typedef struct AppendRequestHeader
{
	uint64 tag;
	term_t term; /* term of the proposer */
	/*
	 * LSN since which current proposer appends WAL (begin_lsn of its first
	 * record); determines epoch switch point.
	 */
	XLogRecPtr epochStartLsn;
	XLogRecPtr beginLsn;    /* start position of message in WAL */
	XLogRecPtr endLsn;      /* end position of message in WAL */
	XLogRecPtr commitLsn;   /* LSN committed by quorum of walkeepers */
	/*
	 *  minimal LSN which may be needed for recovery of some safekeeper (end lsn
	 *  + 1 of last record streamed to everyone)
	 */
    XLogRecPtr truncateLsn;
    pg_uuid_t  proposerId; /* for monitoring/debugging */
} AppendRequestHeader;

/*
 * All copy data message ('w') are linked in L1 send list and asynchronously sent to receivers.
 * When message is sent to all receivers, it is removed from send list.
 */
struct WalMessage
{
	WalMessage* next;      /* L1 list of messages */
	uint32 size;           /* message size */
	uint32 ackMask;        /* mask of receivers acknowledged receiving of this message */
	AppendRequestHeader req; /* request to walkeeper (message header) */

	/* PHANTOM FIELD:
	 *
	 * All WalMessages are allocated with exactly (size - sizeof(WalKeeperRequest)) additional bytes
	 * after them, containing the body of the message. This allocation is done in `CreateMessage`
	 * (for body len > 0) and `CreateMessageVCLOnly` (for body len == 0). */
};

/*
 * Hot standby feedback received from replica
 */
typedef struct HotStandbyFeedback
{
	TimestampTz       ts;
	FullTransactionId xmin;
	FullTransactionId catalog_xmin;
} HotStandbyFeedback;

/*
 * Report walkeeper state to proposer
 */
typedef struct AppendResponse
{
	/*
	 * Current term of the safekeeper; if it is higher than proposer's, the
	 * compute is out of date.
	 */
	uint64 tag;
	term_t     term;
	term_t     epoch;
	XLogRecPtr flushLsn;
	HotStandbyFeedback hs;
} AppendResponse;


/*
 * Descriptor of walkeeper
 */
typedef struct WalKeeper
{
	char const*        host;
	char const*        port;
	char               conninfo[MAXCONNINFO]; /* connection info for connecting/reconnecting */
	WalProposerConn*   conn;          /* postgres protocol connection to the walreceiver */

	WalMessage*        currMsg;       /* message been send to the receiver */

	int                eventPos;      /* position in wait event set. Equal to -1 if no event */
	WalKeeperState     state;         /* walkeeper state machine state */
	AcceptorGreeting   greet;         /* acceptor greeting  */
	VoteResponse	   voteResponse;  /* the vote */
	AppendResponse  feedback;      /* feedback to master */
} WalKeeper;


int        CompareLsn(const void *a, const void *b);
char*      FormatWalKeeperState(WalKeeperState state);
void       AssertEventsOkForState(uint32 events, WalKeeper* wk);
uint32     WalKeeperStateDesiredEvents(WalKeeperState state);
bool       StateShouldImmediatelyExecute(WalKeeperState state);
char*      FormatEvents(uint32 events);
void       WalProposerMain(Datum main_arg);
void       WalProposerBroadcast(XLogRecPtr startpos, char* data, int len);
bool       HexDecodeString(uint8 *result, char *input, int nbytes);
void       WalProposerPoll(void);
void       WalProposerRegister(void);
void       ProcessStandbyReply(XLogRecPtr	writePtr,
							   XLogRecPtr	flushPtr,
							   XLogRecPtr	applyPtr,
							   TimestampTz replyTime,
							   bool		replyRequested);
void       ProcessStandbyHSFeedback(TimestampTz   replyTime,
									TransactionId feedbackXmin,
									uint32		feedbackEpoch,
									TransactionId feedbackCatalogXmin,
									uint32		feedbackCatalogEpoch);
void       StartReplication(StartReplicationCmd *cmd);

/* libpqwalproposer hooks & helper type */

/* Re-exported PostgresPollingStatusType */
typedef enum
{
	WP_CONN_POLLING_FAILED = 0,
	WP_CONN_POLLING_READING,
	WP_CONN_POLLING_WRITING,
	WP_CONN_POLLING_OK,
	/*
	 * 'libpq-fe.h' still has PGRES_POLLING_ACTIVE, but says it's unused.
	 * We've removed it here to avoid clutter.
	 */
} WalProposerConnectPollStatusType;

/* Re-exported and modified ExecStatusType */
typedef enum
{
	/* We received a single CopyBoth result */
	WP_EXEC_SUCCESS_COPYBOTH,
	/* Any success result other than a single CopyBoth was received. The specifics of the result
	 * were already logged, but it may be useful to provide an error message indicating which
	 * walkeeper messed up.
	 *
	 * Do not expect PQerrorMessage to be appropriately set. */
	WP_EXEC_UNEXPECTED_SUCCESS,
	/* No result available at this time. Wait until read-ready, then call again. Internally, this is
	 * returned when PQisBusy indicates that PQgetResult would block. */
	WP_EXEC_NEEDS_INPUT,
	/* Catch-all failure. Check PQerrorMessage. */
	WP_EXEC_FAILED,
} WalProposerExecStatusType;

/* Re-exported ConnStatusType */
typedef enum
{
	WP_CONNECTION_OK,
	WP_CONNECTION_BAD,

	/*
	 * The original ConnStatusType has many more tags, but requests that
	 * they not be relied upon (except for displaying to the user). We
	 * don't need that extra functionality, so we collect them into a
	 * single tag here.
	 */
	WP_CONNECTION_IN_PROGRESS,
} WalProposerConnStatusType;

/* Re-exported PQerrorMessage */
typedef char* (*walprop_error_message_fn) (WalProposerConn* conn);

/* Re-exported PQstatus */
typedef WalProposerConnStatusType (*walprop_status_fn) (WalProposerConn* conn);

/* Re-exported PQconnectStart */
typedef WalProposerConn* (*walprop_connect_start_fn) (char* conninfo);

/* Re-exported PQconectPoll */
typedef WalProposerConnectPollStatusType (*walprop_connect_poll_fn) (WalProposerConn* conn);

/* Re-exported PQsendQuery */
typedef bool (*walprop_send_query_fn) (WalProposerConn* conn, char* query);

/* Wrapper around PQconsumeInput + PQisBusy + PQgetResult */
typedef WalProposerExecStatusType (*walprop_get_query_result_fn) (WalProposerConn* conn);

/* Re-exported PQsetnonblocking */
typedef int (*walprop_set_nonblocking_fn) (WalProposerConn* conn, int arg);

/* Re-exported PQsocket */
typedef pgsocket (*walprop_socket_fn) (WalProposerConn* conn);

/* Re-exported PQflush */
typedef int (*walprop_flush_fn) (WalProposerConn* conn);

/* Re-exported PQconsumeInput */
typedef int (*walprop_consume_input_fn) (WalProposerConn* conn);

/* Re-exported PQfinish */
typedef void (*walprop_finish_fn) (WalProposerConn* conn);

/*
 * Ergonomic wrapper around PGgetCopyData
 *
 * Reads a CopyData block from a walkeeper, setting *amount to the number
 * of bytes returned.
 *
 * This function is allowed to assume certain properties specific to the
 * protocol with the walkeepers, so it should not be used as-is for any
 * other purpose.
 *
 * Note: If possible, using <AsyncRead> is generally preferred, because it
 * performs a bit of extra checking work that's always required and is normally
 * somewhat verbose.
 */
typedef PGAsyncReadResult (*walprop_async_read_fn) (WalProposerConn* conn,
													char** buf,
													int* amount);

/*
 * Ergonomic wrapper around PQputCopyData + PQflush
 *
 * Starts to write a CopyData block to a walkeeper.
 *
 * For information on the meaning of return codes, refer to PGAsyncWriteResult.
 */
typedef PGAsyncWriteResult (*walprop_async_write_fn) (WalProposerConn* conn,
													  void const* buf,
													  size_t size);

/* All libpqwalproposer exported functions collected together. */
typedef struct WalProposerFunctionsType
{
	walprop_error_message_fn	walprop_error_message;
	walprop_status_fn			walprop_status;
	walprop_connect_start_fn	walprop_connect_start;
	walprop_connect_poll_fn		walprop_connect_poll;
	walprop_send_query_fn		walprop_send_query;
	walprop_get_query_result_fn	walprop_get_query_result;
	walprop_set_nonblocking_fn  walprop_set_nonblocking;
	walprop_socket_fn			walprop_socket;
	walprop_flush_fn			walprop_flush;
	walprop_consume_input_fn	walprop_consume_input;
	walprop_finish_fn			walprop_finish;
	walprop_async_read_fn		walprop_async_read;
	walprop_async_write_fn		walprop_async_write;
} WalProposerFunctionsType;

/* Allow the above functions to be "called" with normal syntax */
#define walprop_error_message(conn) \
	WalProposerFunctions->walprop_error_message(conn)
#define walprop_status(conn) \
	WalProposerFunctions->walprop_status(conn)
#define walprop_connect_start(conninfo) \
	WalProposerFunctions->walprop_connect_start(conninfo)
#define walprop_connect_poll(conn) \
	WalProposerFunctions->walprop_connect_poll(conn)
#define walprop_send_query(conn, query) \
	WalProposerFunctions->walprop_send_query(conn, query)
#define walprop_get_query_result(conn) \
	WalProposerFunctions->walprop_get_query_result(conn)
#define walprop_set_nonblocking(conn, arg) \
	WalProposerFunctions->walprop_set_nonblocking(conn, arg)
#define walprop_socket(conn) \
	WalProposerFunctions->walprop_socket(conn)
#define walprop_flush(conn) \
	WalProposerFunctions->walprop_flush(conn)
#define walprop_consume_input(conn) \
	WalProposerFunctions->walprop_consume_input(conn)
#define walprop_finish(conn) \
	WalProposerFunctions->walprop_finish(conn)
#define walprop_async_read(conn, buf, amount) \
	WalProposerFunctions->walprop_async_read(conn, buf, amount)
#define walprop_async_write(conn, buf, size) \
	WalProposerFunctions->walprop_async_write(conn, buf, size)

/*
 * The runtime location of the libpqwalproposer functions.
 *
 * This pointer is set by the initializer in libpqwalproposer, so that we
 * can use it later.
 */
extern PGDLLIMPORT WalProposerFunctionsType *WalProposerFunctions;

#endif
