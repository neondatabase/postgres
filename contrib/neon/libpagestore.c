/*-------------------------------------------------------------------------
 *
 * libpagestore.c
 *	  Handles network communications with the remote pagestore.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	 contrib/neon/libpqpagestore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pagestore_client.h"
#include "fmgr.h"
#include "access/xlog.h"

#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "utils/guc.h"

#include "neon.h"
#include "walproposer.h"
#include "walproposer_utils.h"


#define PageStoreTrace DEBUG5

#define NEON_TAG "[NEON_SMGR] "
#define neon_log(tag, fmt, ...) ereport(tag, \
		(errmsg(NEON_TAG fmt, ## __VA_ARGS__), \
		 errhidestmt(true), errhidecontext(true)))

bool		connected = false;
PGconn	   *pageserver_conn = NULL;

char	   *page_server_connstring_raw;

static ZenithResponse *pageserver_call(ZenithRequest *request);
page_server_api api = {
	.request = pageserver_call
};

static void
pageserver_connect()
{
	char	   *query;
	int			ret;

	Assert(!connected);

	pageserver_conn = PQconnectdb(page_server_connstring);

	if (PQstatus(pageserver_conn) == CONNECTION_BAD)
	{
		char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

		PQfinish(pageserver_conn);
		pageserver_conn = NULL;
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg(NEON_TAG "could not establish connection to pageserver"),
				 errdetail_internal("%s", msg)));
	}

	query = psprintf("pagestream %s %s", zenith_tenant, zenith_timeline);
	ret = PQsendQuery(pageserver_conn, query);
	if (ret != 1)
	{
		PQfinish(pageserver_conn);
		pageserver_conn = NULL;
		neon_log(ERROR, "could not send pagestream command to pageserver");
	}

	while (PQisBusy(pageserver_conn))
	{
		int			wc;

		/* Sleep until there's something to do */
		wc = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_SOCKET_READABLE |
							   WL_EXIT_ON_PM_DEATH,
							   PQsocket(pageserver_conn),
							   -1L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (wc & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(pageserver_conn))
			{
				char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

				PQfinish(pageserver_conn);
				pageserver_conn = NULL;

				neon_log(ERROR, "could not complete handshake with pageserver: %s",
						 msg);
			}
		}
	}

	neon_log(LOG, "libpagestore: connected to '%s'", page_server_connstring_raw);

	connected = true;
}

/*
 * A wrapper around PQgetCopyData that checks for interrupts while sleeping.
 */
static int
call_PQgetCopyData(PGconn *conn, char **buffer)
{
	int			ret;

retry:
	ret = PQgetCopyData(conn, buffer, 1 /* async */ );

	if (ret == 0)
	{
		int			wc;

		/* Sleep until there's something to do */
		wc = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_SOCKET_READABLE |
							   WL_EXIT_ON_PM_DEATH,
							   PQsocket(conn),
							   -1L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (wc & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(conn))
				neon_log(ERROR, "could not get response from pageserver: %s",
						 PQerrorMessage(conn));
		}

		goto retry;
	}

	return ret;
}


static ZenithResponse *
pageserver_call(ZenithRequest *request)
{
	StringInfoData req_buff;
	StringInfoData resp_buff;
	ZenithResponse *resp;

	PG_TRY();
	{
		/* If the connection was lost for some reason, reconnect */
		if (connected && PQstatus(pageserver_conn) == CONNECTION_BAD)
		{
			PQfinish(pageserver_conn);
			pageserver_conn = NULL;
			connected = false;
		}

		if (!connected)
			pageserver_connect();

		req_buff = zm_pack_request(request);

		/*
		 * Send request.
		 *
		 * In principle, this could block if the output buffer is full, and we
		 * should use async mode and check for interrupts while waiting. In
		 * practice, our requests are small enough to always fit in the output
		 * and TCP buffer.
		 */
		if (PQputCopyData(pageserver_conn, req_buff.data, req_buff.len) <= 0 || PQflush(pageserver_conn))
		{
			neon_log(ERROR, "failed to send page request: %s",
					 PQerrorMessage(pageserver_conn));
		}
		pfree(req_buff.data);

		if (message_level_is_interesting(PageStoreTrace))
		{
			char	   *msg = zm_to_string((ZenithMessage *) request);

			neon_log(PageStoreTrace, "sent request: %s", msg);
			pfree(msg);
		}

		/* read response */
		resp_buff.len = call_PQgetCopyData(pageserver_conn, &resp_buff.data);
		resp_buff.cursor = 0;

		if (resp_buff.len == -1)
			neon_log(ERROR, "end of COPY");
		else if (resp_buff.len == -2)
			neon_log(ERROR, "could not read COPY data: %s", PQerrorMessage(pageserver_conn));

		resp = zm_unpack_response(&resp_buff);
		PQfreemem(resp_buff.data);

		if (message_level_is_interesting(PageStoreTrace))
		{
			char	   *msg = zm_to_string((ZenithMessage *) resp);

			neon_log(PageStoreTrace, "got response: %s", msg);
			pfree(msg);
		}
	}
	PG_CATCH();
	{
		/*
		 * If anything goes wrong while we were sending a request, it's not
		 * clear what state the connection is in. For example, if we sent the
		 * request but didn't receive a response yet, we might receive the
		 * response some time later after we have already sent a new unrelated
		 * request. Close the connection to avoid getting confused.
		 */
		if (connected)
		{
			neon_log(LOG, "dropping connection to page server due to error");
			PQfinish(pageserver_conn);
			pageserver_conn = NULL;
			connected = false;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	return (ZenithResponse *) resp;
}


static bool
check_zenith_id(char **newval, void **extra, GucSource source)
{
	uint8		zid[16];

	return **newval == '\0' || HexDecodeString(zid, *newval, 16);
}

static char *
substitute_pageserver_password(const char *page_server_connstring_raw)
{
	char	   *host = NULL;
	char	   *port = NULL;
	char	   *user = NULL;
	char	   *auth_token = NULL;
	char	   *err = NULL;
	char	   *page_server_connstring = NULL;
	PQconninfoOption *conn_options;
	PQconninfoOption *conn_option;
	MemoryContext oldcontext;

	/*
	 * Here we substitute password in connection string with an environment
	 * variable. To simplify things we construct a connection string back with
	 * only known options. In particular: host port user and password. We do
	 * not currently use other options and constructing full connstring in an
	 * URI shape is quite messy.
	 */

	if (page_server_connstring_raw == NULL || page_server_connstring_raw[0] == '\0')
		return NULL;

	/* extract the auth token from the connection string */
	conn_options = PQconninfoParse(page_server_connstring_raw, &err);
	if (conn_options == NULL)
	{
		/* The error string is malloc'd, so we must free it explicitly */
		char	   *errcopy = err ? pstrdup(err) : "out of memory";

		PQfreemem(err);
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid connection string syntax: %s", errcopy)));
	}

	/*
	 * Trying to populate pageserver connection string with auth token from
	 * environment. We are looking for password in with placeholder value like
	 * $ENV_VAR_NAME, so if password field is present and starts with $ we try
	 * to fetch environment variable value and fail loudly if it is not set.
	 */
	for (conn_option = conn_options; conn_option->keyword != NULL; conn_option++)
	{
		if (strcmp(conn_option->keyword, "host") == 0)
		{
			if (conn_option->val != NULL && conn_option->val[0] != '\0')
				host = conn_option->val;
		}
		else if (strcmp(conn_option->keyword, "port") == 0)
		{
			if (conn_option->val != NULL && conn_option->val[0] != '\0')
				port = conn_option->val;
		}
		else if (strcmp(conn_option->keyword, "user") == 0)
		{
			if (conn_option->val != NULL && conn_option->val[0] != '\0')
				user = conn_option->val;
		}
		else if (strcmp(conn_option->keyword, "password") == 0)
		{
			if (conn_option->val != NULL && conn_option->val[0] != '\0')
			{
				/* ensure that this is a template */
				if (strncmp(conn_option->val, "$", 1) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("expected placeholder value in pageserver password starting from $ but found: %s", &conn_option->val[1])));

				neon_log(LOG, "found auth token placeholder in pageserver conn string '%s'", &conn_option->val[1]);
				auth_token = getenv(&conn_option->val[1]);
				if (!auth_token)
				{
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("cannot get auth token, environment variable %s is not set", &conn_option->val[1])));
				}
				else
				{
					neon_log(LOG, "using auth token from environment passed via env");
				}
			}
		}
	}

	/*
	 * allocate connection string in TopMemoryContext to make sure it is not
	 * freed
	 */
	oldcontext = CurrentMemoryContext;
	MemoryContextSwitchTo(TopMemoryContext);
	page_server_connstring = psprintf("postgresql://%s:%s@%s:%s", user, auth_token ? auth_token : "", host, port);
	MemoryContextSwitchTo(oldcontext);

	PQconninfoFree(conn_options);
	return page_server_connstring;
}

/*
 * Module initialization function
 */
void
pg_init_libpagestore(void)
{
	DefineCustomStringVariable("neon.pageserver_connstring",
							   "connection string to the page server",
							   NULL,
							   &page_server_connstring_raw,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   NULL, NULL, NULL);

	DefineCustomStringVariable("neon.timeline_id",
							   "Zenith timelineid the server is running on",
							   NULL,
							   &zenith_timeline,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   check_zenith_id, NULL, NULL);

	DefineCustomStringVariable("neon.tenant_id",
							   "Neon tenantid the server is running on",
							   NULL,
							   &zenith_tenant,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   check_zenith_id, NULL, NULL);

	DefineCustomBoolVariable("neon.wal_redo",
							 "start in wal-redo mode",
							 NULL,
							 &wal_redo,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("neon.max_cluster_size",
							"cluster size limit",
							NULL,
							&max_cluster_size,
							-1, -1, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MB,
							NULL, NULL, NULL);

	relsize_hash_init();
	EmitWarningsOnPlaceholders("neon");

	if (page_server != NULL)
		neon_log(ERROR, "libpagestore already loaded");

	neon_log(PageStoreTrace, "libpagestore already loaded");
	page_server = &api;

	/* substitute password in pageserver_connstring */
	page_server_connstring = substitute_pageserver_password(page_server_connstring_raw);

	/* Is there more correct way to pass CustomGUC to postgres code? */
	zenith_timeline_walproposer = zenith_timeline;
	zenith_tenant_walproposer = zenith_tenant;

	/*
	 * Walproposer instructs safekeeper which pageserver to use for
	 * replication
	 */
	zenith_pageserver_connstring_walproposer = page_server_connstring;

	if (wal_redo)
	{
		neon_log(PageStoreTrace, "set inmem_smgr hook");
		smgr_hook = smgr_inmem;
		smgr_init_hook = smgr_init_inmem;
	}
	else if (page_server_connstring && page_server_connstring[0])
	{
		neon_log(PageStoreTrace, "set neon_smgr hook");
		smgr_hook = smgr_zenith;
		smgr_init_hook = smgr_init_zenith;
		dbsize_hook = zenith_dbsize;
	}
}
