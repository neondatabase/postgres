#ifndef __WALPROPOSER_H__
#define __WALPROPOSER_H__

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

#define MAX_SAFEKEEPERS        32
#define XLOG_HDR_SIZE         (1+8*3)  /* 'w' + startPos + walEnd + timestamp */
#define XLOG_HDR_START_POS    1        /* offset of start position in wal sender message header */
#define XLOG_HDR_END_POS      (1+8)    /* offset of end position in wal sender message header */

/*
 * In the spirit of WL_SOCKET_READABLE and others, this corresponds to no events having occured,
 * because all WL_* events are given flags equal to some (1 << i), starting from i = 0
 */
#define WL_NO_EVENTS 0

extern char* wal_acceptors_list;
extern int   wal_acceptor_reconnect_timeout;
extern bool  am_wal_proposer;

struct WalProposerConn; /* Defined in libpqwalproposer */
typedef struct WalProposerConn WalProposerConn;

struct WalMessage;
typedef struct WalMessage WalMessage;

extern char *zenith_timeline_walproposer;
extern char *zenith_tenant_walproposer;
extern char	*zenith_pageserver_connstring_walproposer;

/* Possible return values from ReadPGAsync */
typedef enum
{
	/* The full read was successful. buf now points to the data */
	PG_ASYNC_READ_SUCCESS,
	/* The read is ongoing. Wait until the connection is read-ready, then try
	 * again. */
	PG_ASYNC_READ_TRY_AGAIN,
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
 * WAL safekeeper state, which is used to wait for some event.
 *
 * States are listed here in the order that they're executed.
 *
 * Most states, upon failure, will move back to SS_OFFLINE by calls to
 * ResetConnection or ShutdownConnection.
 */
typedef enum
{
	/*
	 * Does not have an active connection and will stay that way until
	 * further notice.
	 *
	 * Moves to SS_CONNECTING_WRITE by calls to ResetConnection.
	 */
	SS_OFFLINE,

	/*
	 * Connecting states. "_READ" waits for the socket to be available for
	 * reading, "_WRITE" waits for writing. There's no difference in the code
	 * they execute when polled, but we have this distinction in order to
	 * recreate the event set in HackyRemoveWalProposerEvent.
	 *
	 * After the connection is made, "START_WAL_PUSH" query is sent.
	 */
	SS_CONNECTING_WRITE,
	SS_CONNECTING_READ,

	/*
	 * Waiting for the result of the "START_WAL_PUSH" command.
	 *
	 * After we get a successful result, sends handshake to safekeeper.
	 */
	SS_WAIT_EXEC_RESULT,

	/*
	 * Executing the receiving half of the handshake. After receiving, moves to
	 * SS_VOTING.
	 */
	SS_HANDSHAKE_RECV,

	/*
	 * Waiting to participate in voting, but a quorum hasn't yet been reached.
	 * This is an idle state - we do not expect AdvancePollState to be called.
	 *
	 * Moved externally by execution of SS_HANDSHAKE_RECV, when we received a
	 * quorum of handshakes.
	 */
	SS_VOTING,

	/*
	 * Already sent voting information, waiting to receive confirmation from the
	 * node. After receiving, moves to SS_IDLE, if the quorum isn't reached yet.
	 */
	SS_WAIT_VERDICT,

	/* Need to flush ProposerElected message. */
	SS_SEND_ELECTED_FLUSH,

	/*
	 * Waiting for quorum to send WAL. Idle state. If the socket becomes
	 * read-ready, the connection has been closed.
	 *
	 * Moves to SS_ACTIVE only by call to StartStreaming.
	 */
	SS_IDLE,

	/*
	 * Active phase, when we acquired quorum and have WAL to send or feedback
	 * to read.
	 */
	SS_ACTIVE,
} SafekeeperState;

/* Consensus logical timestamp. */
typedef uint64 term_t;

/*
 * Proposer <-> Acceptor messaging.
 */

/* Initial Proposer -> Acceptor message */
typedef struct ProposerGreeting
{
	uint64	   tag;				  /* message tag */
	uint32	   protocolVersion;	  /* proposer-safekeeper protocol version */
	uint32	   pgVersion;
	pg_uuid_t  proposerId;
	uint64	   systemId;		  /* Postgres system identifier */
	uint8	   ztimelineid[16];	  /* Zenith timeline id */
	uint8	   ztenantid[16];
	TimeLineID timeline;
	uint32	   walSegSize;
} ProposerGreeting;

typedef struct AcceptorProposerMessage
{
	uint64 tag;
} AcceptorProposerMessage;

/*
 * Acceptor -> Proposer initial response: the highest term acceptor voted for.
 */
typedef struct AcceptorGreeting
{
	AcceptorProposerMessage apm;
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

/* Element of term switching chain. */
typedef struct TermSwitchEntry
{
	term_t term;
	XLogRecPtr lsn;
} TermSwitchEntry;

typedef struct TermHistory
{
	uint32 n_entries;
	TermSwitchEntry *entries;
} TermHistory;

/* Vote itself, sent from safekeeper to proposer */
typedef struct VoteResponse {
	AcceptorProposerMessage apm;
	term_t term;
	uint64 voteGiven;
	/*
	 * Safekeeper flush_lsn (end of WAL) + history of term switches allow
     * proposer to choose the most advanced one.
	 */
	XLogRecPtr flushLsn;
	XLogRecPtr truncateLsn;  /* minimal LSN which may be needed for recovery of some safekeeper */
	TermHistory termHistory;
} VoteResponse;

/*
 * Proposer -> Acceptor message announcing proposer is elected and communicating
 * epoch history to it.
 */
typedef struct ProposerElected
{
	uint64 tag;
	term_t term;
	/* proposer will send since this point */
	XLogRecPtr startStreamingAt;
	/* history of term switches up to this proposer */
	TermHistory *termHistory;
} ProposerElected;

/*
 * Header of request with WAL message sent from proposer to safekeeper.
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
	XLogRecPtr commitLsn;   /* LSN committed by quorum of safekeepers */
	/*
	 *  minimal LSN which may be needed for recovery of some safekeeper (end lsn
	 *  + 1 of last chunk streamed to everyone)
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
	AppendRequestHeader req; /* request to safekeeper (message header) */

	/* PHANTOM FIELD:
	 *
	 * All WalMessages are allocated with exactly (size - sizeof(AppendRequestHeader)) additional bytes
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


typedef	struct ZenithFeedback
{
	// current size of the timeline on pageserver
	uint64 currentInstanceSize;
	// standby_status_update fields that safekeeper received from pageserver
	XLogRecPtr ps_writelsn;
	XLogRecPtr ps_flushlsn;
	XLogRecPtr ps_applylsn;
	TimestampTz ps_replytime;
} ZenithFeedback;

/*
 * Report safekeeper state to proposer
 */
typedef struct AppendResponse
{
	AcceptorProposerMessage apm;
	/*
	 * Current term of the safekeeper; if it is higher than proposer's, the
	 * compute is out of date.
	 */
	term_t     term;
	// TODO: add comment
	XLogRecPtr flushLsn;
	// Safekeeper reports back his awareness about which WAL is committed, as
	// this is a criterion for walproposer --sync mode exit
	XLogRecPtr commitLsn;
	HotStandbyFeedback hs;
	// Feedback recieved from pageserver includes standby_status_update fields
	// and custom zenith feedback.
	// This part of the message is extensible.
	ZenithFeedback zf;
} AppendResponse;

// ZenithFeedback is extensible part of the message that is parsed separately
// Other fields are fixed part
#define APPENDRESPONSE_FIXEDPART_SIZE offsetof(AppendResponse, zf)


/*
 * Descriptor of safekeeper
 */
typedef struct Safekeeper
{
	char const*        host;
	char const*        port;
	char               conninfo[MAXCONNINFO]; /* connection info for connecting/reconnecting */

	/*
	 * postgres protocol connection to the WAL acceptor
	 *
	 * Equals NULL only when state = SS_OFFLINE. Nonblocking is set once we
	 * reach SS_ACTIVE; not before.
	 */
	WalProposerConn*   conn;
	StringInfoData outbuf;

	bool               flushWrite;    /* set to true if we need to call AsyncFlush, to flush pending messages */
	WalMessage*        currMsg;       /* message that wasn't sent yet or NULL, if we have nothing to send */

	int                eventPos;      /* position in wait event set. Equal to -1 if no event */
	SafekeeperState     state;         /* safekeeper state machine state */
	AcceptorGreeting   greetResponse;         /* acceptor greeting  */
	VoteResponse	   voteResponse;  /* the vote */
	AppendResponse appendResponse;		  /* feedback to master */
	/*
	 * Streaming will start here; must be record boundary.
	 */
	XLogRecPtr startStreamingAt;
} Safekeeper;


int        CompareLsn(const void *a, const void *b);
char*      FormatSafekeeperState(SafekeeperState state);
void       AssertEventsOkForState(uint32 events, Safekeeper* sk);
uint32     SafekeeperStateDesiredEvents(SafekeeperState state);
char*      FormatEvents(uint32 events);
void       WalProposerMain(Datum main_arg);
void       WalProposerBroadcast(XLogRecPtr startpos, char* data, int len);
bool       HexDecodeString(uint8 *result, char *input, int nbytes);
uint32     pq_getmsgint32_le(StringInfo msg);
uint64     pq_getmsgint64_le(StringInfo msg);
void	   pq_sendint32_le(StringInfo buf, uint32 i);
void	   pq_sendint64_le(StringInfo buf, uint64 i);
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
void ParseZenithFeedbackMessage(StringInfo reply_message,
								ZenithFeedback *zf);
void       StartReplication(StartReplicationCmd *cmd);
void       WalProposerSync(int argc, char *argv[]);


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
	 * safekeeper messed up.
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

/* Blocking wrapper around PQsendQuery */
typedef bool (*walprop_send_query_fn) (WalProposerConn* conn, char* query);

/* Wrapper around PQconsumeInput + PQisBusy + PQgetResult */
typedef WalProposerExecStatusType (*walprop_get_query_result_fn) (WalProposerConn* conn);

/* Re-exported PQsocket */
typedef pgsocket (*walprop_socket_fn) (WalProposerConn* conn);

/* Wrapper around PQconsumeInput (if socket's read-ready) + PQflush */
typedef int (*walprop_flush_fn) (WalProposerConn* conn);

/* Re-exported PQfinish */
typedef void (*walprop_finish_fn) (WalProposerConn* conn);

/*
 * Ergonomic wrapper around PGgetCopyData
 *
 * Reads a CopyData block from a safekeeper, setting *amount to the number
 * of bytes returned.
 *
 * This function is allowed to assume certain properties specific to the
 * protocol with the safekeepers, so it should not be used as-is for any
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
 * Starts to write a CopyData block to a safekeeper.
 *
 * For information on the meaning of return codes, refer to PGAsyncWriteResult.
 */
typedef PGAsyncWriteResult (*walprop_async_write_fn) (WalProposerConn* conn,
													  void const* buf,
													  size_t size);

/*
 * Blocking equivalent to walprop_async_write_fn
 *
 * Returns 'true' if successful, 'false' on failure.
 */
typedef bool (*walprop_blocking_write_fn) (WalProposerConn* conn, void const* buf, size_t size);

/* All libpqwalproposer exported functions collected together. */
typedef struct WalProposerFunctionsType
{
	walprop_error_message_fn	walprop_error_message;
	walprop_status_fn			walprop_status;
	walprop_connect_start_fn	walprop_connect_start;
	walprop_connect_poll_fn		walprop_connect_poll;
	walprop_send_query_fn		walprop_send_query;
	walprop_get_query_result_fn	walprop_get_query_result;
	walprop_socket_fn			walprop_socket;
	walprop_flush_fn			walprop_flush;
	walprop_finish_fn			walprop_finish;
	walprop_async_read_fn		walprop_async_read;
	walprop_async_write_fn		walprop_async_write;
	walprop_blocking_write_fn   walprop_blocking_write;
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
#define walprop_finish(conn) \
	WalProposerFunctions->walprop_finish(conn)
#define walprop_async_read(conn, buf, amount) \
	WalProposerFunctions->walprop_async_read(conn, buf, amount)
#define walprop_async_write(conn, buf, size) \
	WalProposerFunctions->walprop_async_write(conn, buf, size)
#define walprop_blocking_write(conn, buf, size) \
	WalProposerFunctions->walprop_blocking_write(conn, buf, size)

/*
 * The runtime location of the libpqwalproposer functions.
 *
 * This pointer is set by the initializer in libpqwalproposer, so that we
 * can use it later.
 */
extern PGDLLIMPORT WalProposerFunctionsType *WalProposerFunctions;

#endif
