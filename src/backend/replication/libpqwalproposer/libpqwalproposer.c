#include "postgres.h"

#include "replication/walproposer.h"
#include "libpq-fe.h"

/* Required for anything that's dynamically loaded */
PG_MODULE_MAGIC;
void _PG_init(void);

/* Header in walproposer.h -- Wrapper struct to abstract away the libpq connection */
struct WalProposerConn
{
	PGconn* pg_conn;
};

/* Prototypes for exported functions */
static char*							libpqprop_error_message(WalProposerConn* conn);
static WalProposerConnStatusType		libpqprop_status(WalProposerConn* conn);
static WalProposerConn*					libpqprop_connect_start(char* conninfo);
static WalProposerConnectPollStatusType	libpqprop_connect_poll(WalProposerConn* conn);
static bool								libpqprop_send_query(WalProposerConn* conn, char* query);
static WalProposerExecStatusType		libpqprop_get_query_result(WalProposerConn* conn);
static int								libpqprop_set_nonblocking(WalProposerConn* conn, int arg);
static pgsocket							libpqprop_socket(WalProposerConn* conn);
static int								libpqprop_flush(WalProposerConn* conn);
static int								libpqprop_consume_input(WalProposerConn* conn);
static void								libpqprop_finish(WalProposerConn* conn);
static PGAsyncReadResult				libpqprop_async_read(WalProposerConn* conn, char** buf, int* amount);
static PGAsyncWriteResult				libpqprop_async_write(WalProposerConn* conn, void const* buf, size_t size);

static WalProposerFunctionsType PQWalProposerFunctions = {
	libpqprop_error_message,
	libpqprop_status,
	libpqprop_connect_start,
	libpqprop_connect_poll,
	libpqprop_send_query,
	libpqprop_get_query_result,
	libpqprop_set_nonblocking,
	libpqprop_socket,
	libpqprop_flush,
	libpqprop_consume_input,
	libpqprop_finish,
	libpqprop_async_read,
	libpqprop_async_write,
};

/* Module initialization */
void
_PG_init(void)
{
	if (WalProposerFunctions != NULL)
		elog(ERROR, "libpqwalproposer already loaded");
	WalProposerFunctions = &PQWalProposerFunctions;
}

/* Exported function definitions */
static char*
libpqprop_error_message(WalProposerConn* conn)
{
	return PQerrorMessage(conn->pg_conn);
}

static WalProposerConnStatusType
libpqprop_status(WalProposerConn* conn)
{
	switch (PQstatus(conn->pg_conn))
	{
		case CONNECTION_OK:
			return WP_CONNECTION_OK;
		case CONNECTION_BAD:
			return WP_CONNECTION_BAD;
		default:
			return WP_CONNECTION_IN_PROGRESS;
	}
}

static WalProposerConn*
libpqprop_connect_start(char* conninfo)
{
	WalProposerConn*	conn;
	PGconn*				pg_conn;

	pg_conn = PQconnectStart(conninfo);
	/*
	 * Allocation of a PQconn can fail, and will return NULL. We want to fully replicate the
	 * behavior of PQconnectStart here.
	 */
	if (!pg_conn)
		return NULL;

	/*
	 * And in theory this allocation can fail as well, but it's incredibly unlikely if we just
	 * successfully allocated a PGconn.
	 *
	 * palloc will exit on failure though, so there's not much we could do if it *did* fail.
	 */
	conn = palloc(sizeof(WalProposerConn));
	conn->pg_conn = pg_conn;
	return conn;
}

static WalProposerConnectPollStatusType
libpqprop_connect_poll(WalProposerConn* conn)
{
	WalProposerConnectPollStatusType return_val;

	switch (PQconnectPoll(conn->pg_conn))
	{
		case PGRES_POLLING_FAILED:
			return_val = WP_CONN_POLLING_FAILED;
			break;
		case PGRES_POLLING_READING:
			return_val = WP_CONN_POLLING_READING;
			break;
		case PGRES_POLLING_WRITING:
			return_val = WP_CONN_POLLING_WRITING;
			break;
		case PGRES_POLLING_OK:
			return_val = WP_CONN_POLLING_OK;
			break;

		/* There's a comment at its source about this constant being unused. We'll expect it's never
		 * returned. */
		case PGRES_POLLING_ACTIVE:
			elog(FATAL, "Unexpected PGRES_POLLING_ACTIVE returned from PQconnectPoll");
			/* This return is never actually reached, but it's here to make the compiler happy */
			return WP_CONN_POLLING_FAILED;
	}

	return return_val;
}

static bool
libpqprop_send_query(WalProposerConn* conn, char* query)
{
	int  result;
	bool return_val;

	switch ((result = PQsendQuery(conn->pg_conn, query)))
	{
		case 0:
			return_val = false;
			break;
		case 1:
			return_val = true;
			break;
		default:
			elog(FATAL, "unexpected return %d from PQsendQuery", result);
	}

	return return_val;
}

static WalProposerExecStatusType
libpqprop_get_query_result(WalProposerConn* conn)
{
	PGresult* result;
	WalProposerExecStatusType return_val;

	/* Marker variable if we need to log an unexpected success result */
	char* unexpected_success = NULL;

	if (PQisBusy(conn->pg_conn))
		return WP_EXEC_NEEDS_INPUT;


	result = PQgetResult(conn->pg_conn);
	/* PQgetResult returns NULL only if getting the result was successful & there's no more of the
	 * result to get. */
	if (!result)
	{
		elog(WARNING, "[libpqwalproposer] Unexpected successful end of command results");
		return WP_EXEC_UNEXPECTED_SUCCESS;
	}

	/* Helper macro to reduce boilerplate */
	#define UNEXPECTED_SUCCESS(msg) \
		return_val = WP_EXEC_UNEXPECTED_SUCCESS; \
		unexpected_success = msg; \
		break;


	switch (PQresultStatus(result))
	{
		/* "true" success case */
		case PGRES_COPY_BOTH:
			return_val = WP_EXEC_SUCCESS_COPYBOTH;
			break;

		/* Unexpected success case */
		case PGRES_EMPTY_QUERY:
			UNEXPECTED_SUCCESS("empty query return");
		case PGRES_COMMAND_OK:
			UNEXPECTED_SUCCESS("data-less command end");
		case PGRES_TUPLES_OK:
			UNEXPECTED_SUCCESS("tuples return");
		case PGRES_COPY_OUT:
			UNEXPECTED_SUCCESS("'Copy Out' response");
		case PGRES_COPY_IN:
			UNEXPECTED_SUCCESS("'Copy In' response");
		case PGRES_SINGLE_TUPLE:
			UNEXPECTED_SUCCESS("single tuple return");
		case PGRES_PIPELINE_SYNC:
			UNEXPECTED_SUCCESS("pipeline sync point");

		/* Failure cases */
		case PGRES_BAD_RESPONSE:
		case PGRES_NONFATAL_ERROR:
		case PGRES_FATAL_ERROR:
		case PGRES_PIPELINE_ABORTED:
			return_val = WP_EXEC_FAILED;
			break;
	}

	if (unexpected_success)
		elog(WARNING, "[libpqwalproposer] Unexpected successful %s", unexpected_success);

	return return_val;
}

static int
libpqprop_set_nonblocking(WalProposerConn* conn, int arg)
{
	return PQsetnonblocking(conn->pg_conn, arg);
}

static pgsocket
libpqprop_socket(WalProposerConn* conn)
{
	return PQsocket(conn->pg_conn);
}

static int
libpqprop_flush(WalProposerConn* conn)
{
	return (PQflush(conn->pg_conn));
}

static int
libpqprop_consume_input(WalProposerConn* conn)
{
	return (PQconsumeInput(conn->pg_conn));
}

static void
libpqprop_finish(WalProposerConn* conn)
{
	PQfinish(conn->pg_conn);
	pfree(conn);
}

static PGAsyncReadResult
libpqprop_async_read(WalProposerConn* conn, char** buf, int* amount)
{
	int result;

	/* The docs for PQgetCopyData list the return values as:
	 *      0 if the copy is still in progress, but no "complete row" is
	 *        available
	 *     -1 if the copy is done
	 *     -2 if an error occured
	 *  (> 0) if it was successful; that value is the amount transferred.
	 *
	 * The protocol we use between walproposer and walkeeper means that we
	 * (i.e. walproposer) won't ever receive a message saying that the copy
	 * is done. */
	switch (result = PQgetCopyData(conn->pg_conn, buf, true))
	{
		case 0:
			return PG_ASYNC_READ_CONSUME_AND_TRY_AGAIN;
		case -1:
			/* As mentioned above; this shouldn't happen */
			elog(FATAL, "unexpected return -1 from PQgetCopyData");
			break;
		case -2:
			return PG_ASYNC_READ_FAIL;
		default:
			/* Positive values indicate the size of the returned result */
			*amount = result;
			return PG_ASYNC_READ_SUCCESS;
	}
}

static PGAsyncWriteResult
libpqprop_async_write(WalProposerConn* conn, void const* buf, size_t size)
{
	int result;

	/* The docs for PQputcopyData list the return values as:
	 *   1 if the data was queued,
	 *   0 if it was not queued because of full buffers, or
	 *  -1 if an error occured
	 */
	switch (result = PQputCopyData(conn->pg_conn, buf, size))
	{
		case 1:
			/* good -- continue */
			break;
		case 0:
			/* FIXME: can this ever happen? the structure of walproposer
			 * should always empty the connection's buffers before trying
			 * to send more, right? */
			return PG_ASYNC_WRITE_WOULDBLOCK;
		case -1:
			return PG_ASYNC_WRITE_FAIL;
		default:
			elog(FATAL, "invalid return %d from PQputCopyData", result);
	}

	/* After queueing the data, we still need to flush to get it to send.
	 * This might take multiple tries, but we don't want to wait around
	 * until it's done.
	 *
	 * PQflush has the following returns (directly quoting the docs):
	 *   0 if sucessful,
	 *   1 if it was unable to send all the data in the send queue yet
	 *  -1 if it failed for some reason
	 */
	switch (result = PQflush(conn->pg_conn)) {
		case 0:
			return PG_ASYNC_WRITE_SUCCESS;
		case 1:
			return PG_ASYNC_WRITE_TRY_FLUSH;
		case -1:
			return PG_ASYNC_WRITE_FAIL;
		default:
			elog(FATAL, "invalid return %d from PQflush", result);
	}
}
