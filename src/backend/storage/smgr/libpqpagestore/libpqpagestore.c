/*-------------------------------------------------------------------------
 *
 * libpqpagestore.c
 *	  Handles network communications with the remote pagestore.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/libpqpagestore/libpqpagestore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/pagestore_client.h"
#include "fmgr.h"

#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"

#include "miscadmin.h"
#include "pgstat.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

#define ZENITH_TAG "[ZENITH_SMGR] "
#define zenith_log(tag, fmt, ...) ereport(tag, \
		(errmsg(ZENITH_TAG fmt, ## __VA_ARGS__), \
		 errhidestmt(true), errhidecontext(true)))

bool connected = false;
PGconn *pageserver_conn;

static ZenithResponse *zenith_call(ZenithRequest request);
page_server_api api = {
	.request = zenith_call
};

static void
zenith_connect()
{
	pageserver_conn = PQconnectdb(page_server_connstring);

	if (PQstatus(pageserver_conn) == CONNECTION_BAD)
	{
		char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

		PQfinish(pageserver_conn);
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
					errmsg("[ZENITH_SMGR] could not establish connection"),
					errdetail_internal("%s", msg)));
	}

	char	   *query = psprintf("select zenith_store.dispatcher_loop()");
	int ret = PQsendQuery(pageserver_conn, query);
	if (ret != 1)
		zenith_log(ERROR,
			"[ZENITH_SMGR] failed to start dispatcher_loop on pageserver");

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
				zenith_log(ERROR, "[ZENITH_SMGR] failed to get handshake from pageserver: %s",
					PQerrorMessage(pageserver_conn));
		}
	}

	connected = true;
}


static ZenithResponse*
zenith_call(ZenithRequest request)
{
	if (!connected)
		zenith_connect();

	StringInfoData req_buff = zm_pack((ZenithMessage *) &request, false);

	/* send request */
	if (PQputCopyData(pageserver_conn, req_buff.data, req_buff.len) <= 0 || PQflush(pageserver_conn))
	{
		zenith_log(ERROR, "[ZENITH_SMGR] failed to send page request: %s",
					PQerrorMessage(pageserver_conn));
	}

	zenith_log(LOG, "[ZENITH_SMGR] Sent %s request", ZenithMessageStr[request.tag]);

	/* read response */
	StringInfoData resp_buff;
	initStringInfo(&resp_buff);
	resp_buff.len = PQgetCopyData(pageserver_conn, &resp_buff.data, 0);

	if (resp_buff.len == -1)
		zenith_log(ERROR, "end of COPY");
	else if (resp_buff.len == -2)
		zenith_log(ERROR, "could not read COPY data: %s", PQerrorMessage(pageserver_conn));

	// pq_getmsgbyte(&resp_buff); /* 'd' */
	ZenithMessage *resp = zm_unpack(&resp_buff);
	Assert(messageTag(resp) == T_ZenithStatusResponse
		|| messageTag(resp) == T_ZenithNblocksResponse
		|| messageTag(resp) == T_ZenithReadResponse);

	return (ZenithResponse *) resp;
}

/*
 * Module initialization function
 */
void
_PG_init(void)
{
	if (page_server != NULL)
		elog(ERROR, "libpqpagestore already loaded");

	elog(LOG, "libpqpagestore already loaded");
	page_server = &api;
}
