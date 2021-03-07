/*-------------------------------------------------------------------------
 *
 * zenith.c
 *	  Base backup + WAL in a zenith file format
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/zenith.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/pageserver.h"
#include "fmgr.h"

#include "libpq-fe.h"
#include "libpq/pqformat.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

#define ZENITH_TAG "[ZENITH_SMGR] "
#define zenith_log(tag, fmt, ...) ereport(tag, \
		(errmsg(ZENITH_TAG fmt, ## __VA_ARGS__), \
		 errhidestmt(true), errhidecontext(true)))

bool connected = false;

PGconn *pageserver_conn;


static ZenithResponse zenith_call(ZenithRequest request);
page_server_api api = {
	.request = zenith_call
};

/*
 *	zenith_init() -- Initialize private state
 */
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
	{
		zenith_log(ERROR, "[ZENITH_SMGR] failed to start dispatcher_loop on pageserver");
	}

	while (true)
	{
		if (!PQconsumeInput(pageserver_conn))
		{
			zenith_log(ERROR, "[ZENITH_SMGR] failed to get handshake from pageserver: %s",
					PQerrorMessage(pageserver_conn));
		}
		if (!PQisBusy(pageserver_conn))
		{

			PQsetnonblocking(pageserver_conn, 1);
			zenith_log(ERROR, "[ZENITH_SMGR] failed to get handshake from pageserver: %s",
					PQerrorMessage(pageserver_conn));
			return;
		}

		pg_usleep(1000L); // XXX: poll
	}

	connected = true;
}

static ZenithResponse
zenith_call(ZenithRequest request)
{
	if (!connected)
		zenith_connect();

	/* serialize request */
	StringInfoData req_buff;
	initStringInfo(&req_buff);

	pq_sendbyte(&req_buff, request.tag);
	pq_sendint32(&req_buff, request.page_key.rnode.spcNode);
	pq_sendint32(&req_buff, request.page_key.rnode.dbNode);
	pq_sendint32(&req_buff, request.page_key.rnode.relNode);
	pq_sendbyte(&req_buff, request.page_key.forknum);
	pq_sendint32(&req_buff, request.page_key.blkno);
	pq_endmessage(&req_buff);

	if (PQputCopyData(pageserver_conn, req_buff.data, req_buff.len) < 0)
		goto err;

	if (PQflush(pageserver_conn) < 0)
		goto err;

	StringInfoData resp_buff;
	resp_buff.maxlen = -1;
	resp_buff.cursor = 0;

	int			ret;
	ret = PQgetCopyData(pageserver_conn, &resp_buff.data, 0);
	if (ret < 0)
		goto err;

	resp_buff.len = ret;
	ZenithResponse resp;

	resp.tag = pq_getmsgbyte(&resp_buff);
	resp.status = pq_getmsgbyte(&resp_buff);
	
	if (request.tag == SMGR_EXISTS || request.tag == SMGR_TRUNC
		|| request.tag == SMGR_UNLINK)
	{
		Assert(resp.tag == T_ZenithStatusResponse);
	}
	else if (request.tag == SMGR_NBLOCKS)
	{
		Assert(resp.tag == T_ZenithNblocksResponse);
		resp.n_blocks = pq_getmsgint(&resp_buff, 4);
	}
	else if (request.tag == SMGR_READ)
	{
		Assert(resp.tag == T_ZenithReadResponse);
		resp.page = pq_getmsgbytes(&resp_buff, BLCKSZ);
	}
	else
		Assert(false);

	pq_getmsgend(&resp_buff);

	return resp;

err:
	zenith_log(ERROR,
		"[ZENITH_SMGR] failed to get data from pageserver: %s",
			PQerrorMessage(pageserver_conn));
}

/*
 * Module initialization function
 */
void
_PG_init(void)
{
	if (page_server != NULL)
		elog(ERROR, "libpqpageserver already loaded");

	elog(LOG, "libpqpageserver already loaded");
	page_server = &api;
}
