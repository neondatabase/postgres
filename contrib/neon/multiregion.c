/*-------------------------------------------------------------------------
 *
 * multiregion.c
 *	  Handles network communications in a multi-region setup.
 *
 * IDENTIFICATION
 *	 contrib/zenith/multiregion.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "multiregion.h"
#include "pagestore_client.h"

#include "catalog/catalog.h"
#include "catalog/pg_remote_tablespace.h"
#include "catalog/pg_tablespace_d.h"
#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "utils/varlena.h"
#include "walproposer_utils.h"

#define ZENITH_TAG "[ZENITH_SMGR] "
#define zenith_log(tag, fmt, ...) ereport(tag,		\
		(errmsg(ZENITH_TAG fmt, ##__VA_ARGS__), 	\
		errhidestmt(true), errhidecontext(true)))

#define GLOBAL_REGION 0

/* GUCs */
int zenith_current_region;
static char *zenith_region_timelines;
static char *zenith_region_safekeeper_addrs;

static bool
check_zenith_region_timelines(char **newval, void **extra, GucSource source)
{
	char *rawstring;
	List *timelines;
	ListCell *l;
	uint8 zid[16];

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	/* Parse string into list of timeline ids */
	if (!SplitIdentifierString(rawstring, ',', &timelines))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(timelines);
		return false;
	}

	/* Check if the given timeline ids are valid */
	foreach (l, timelines)
	{
		char *tok = (char *)lfirst(l);

		/* Taken from check_zenith_id in libpagestore.c */
		if (*tok != '\0' && !HexDecodeString(zid, tok, 16))
		{
			GUC_check_errdetail("Invalid Zenith id: \"%s\".", tok);
			pfree(rawstring);
			list_free(timelines);
			return false;
		}
	}

	pfree(rawstring);
	list_free(timelines);
	return true;
}

static bool
check_zenith_region_safekeeper_addrs(char **newval, void **extra, GucSource source)
{
	char *rawstring;
	List *safekeeper_addrs;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	/* Parse string into list of safekeeper_addrs */
	if (!SplitIdentifierString(rawstring, ',', &safekeeper_addrs))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(safekeeper_addrs);
		return false;
	}

	pfree(rawstring);
	list_free(safekeeper_addrs);

	return true;
}

void DefineMultiRegionCustomVariables(void)
{
	DefineCustomStringVariable("zenith.region_timelines",
								"List of timelineids corresponding to the partitions. The first timeline is always for the global partition.",
								NULL,
								&zenith_region_timelines,
								"",
								PGC_POSTMASTER,
								0, /* no flags required */
								check_zenith_region_timelines, NULL, NULL);

	DefineCustomStringVariable("zenith.region_safekeeper_addrs",
								"List of addresses to the safekeepers in every regions. The first address is always for the global partition",
								NULL,
								&zenith_region_safekeeper_addrs,
								"",
								PGC_POSTMASTER,
								0, /* no flags required */
								check_zenith_region_safekeeper_addrs, NULL, NULL);

	DefineCustomIntVariable("zenith.current_region",
							"ID of the current region",
							NULL,
							&zenith_current_region,
							GLOBAL_REGION,
							0, INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);
}

bool zenith_multiregion_enabled(void)
{
	return zenith_region_timelines && zenith_region_timelines[0] &&
		zenith_region_safekeeper_addrs && zenith_region_safekeeper_addrs[0];
}

static bool
split_into_host_and_port(const char *addr, char** host, char** port)
{
	int 		hostlen;
	int 		portlen;
	const char 	*colon = strchr(addr, ':');

	if (colon == NULL)
		return false;

	hostlen = colon - addr;
	portlen = strlen(addr) - hostlen - 1;

	*host = (char *)palloc(hostlen + 1);
	*port = (char *)palloc(portlen + 1);
	if (*host == NULL || *port == NULL)
		return false;

	strncpy(*host, addr, hostlen);
	(*host)[hostlen] = '\0';
	strncpy(*port, colon + 1, portlen);
	(*port)[portlen] = '\0';

	return true;
}

/*
 * Similar function to zenith_connect in libpagestore.c but used for
 * multiple timelines.
 */
void zenith_multiregion_connect(PGconn **pageserver_conn, bool *connected)
{
	List *timelines;
	ListCell *lc_timeline;
	List *safekeeper_addrs;
	ListCell *lc_addr;
	char *query;
	int ret;

	Assert(!*connected);

	*pageserver_conn = PQconnectdb(page_server_connstring);

	if (PQstatus(*pageserver_conn) == CONNECTION_BAD)
	{
		char *msg = pchomp(PQerrorMessage(*pageserver_conn));

		PQfinish(*pageserver_conn);
		*pageserver_conn = NULL;
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				errmsg("[ZENITH_SMGR] could not establish connection"),
				errdetail_internal("%s", msg)));
	}

	/* These strings are already syntax-checked by GUC hooks */
	Assert(SplitIdentifierString(pstrdup(zenith_region_timelines), ',', &timelines));
	Assert(SplitIdentifierString(pstrdup(zenith_region_safekeeper_addrs), ',', &safekeeper_addrs));

	/* Make sure that they are the same length */
	Assert(list_length(timelines) == list_length(safekeeper_addrs));

	/* 
	 * Ask the Page Server to connect to other regions
	 */
	// FIXME(ctring): neon no longer uses callmemaybe
	forboth (lc_timeline, timelines, lc_addr, safekeeper_addrs)
	{
		PGresult *res;
		int i = foreach_current_index(lc_timeline);
		char *timeline = (char *)lfirst(lc_timeline);
		char *addr = (char *)lfirst(lc_addr);
		char *host, *port;

		/* Skip the global partition and current partition */
		if (i == 0 || i == zenith_current_region)
			continue;
		
		if (!split_into_host_and_port(addr, &host, &port))
		{
			PQfinish(*pageserver_conn);
			*pageserver_conn = NULL;
			zenith_log(ERROR, "[ZENITH_SMGR] invalid safekeeper address \"%s\"", addr);
		}

		/* Connection string format taken from callmemaybe.rs in walkeeper */
		query = psprintf(
			"callmemaybe %s %s host=%s port=%s options='-c ztenantid=%s ztimelineid=%s pageserver_connstr=%s'",
			zenith_tenant, timeline, host, port, zenith_tenant, timeline, page_server_connstring);

		pfree(host);
		pfree(port);

		res = PQexec(*pageserver_conn, query);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			PQfinish(*pageserver_conn);
			*pageserver_conn = NULL;
			zenith_log(ERROR, "[ZENITH_SMGR] callmemaybe command failed for timeline %s", timeline);
		}
		PQclear(res);
	}

	list_free(timelines);
	list_free(safekeeper_addrs);

	query = psprintf("multipagestream %s %s", zenith_tenant, zenith_region_timelines);
	ret = PQsendQuery(*pageserver_conn, query);
	if (ret != 1)
	{
		PQfinish(*pageserver_conn);
		*pageserver_conn = NULL;
		zenith_log(ERROR,
				   "[ZENITH_SMGR] failed to start dispatcher_loop on pageserver");
	}

	while (PQisBusy(*pageserver_conn))
	{
		int wc;

		/* Sleep until there's something to do */
		wc = WaitLatchOrSocket(MyLatch,
							   	WL_LATCH_SET | WL_SOCKET_READABLE |
								WL_EXIT_ON_PM_DEATH,
								PQsocket(*pageserver_conn),
								-1L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (wc & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(*pageserver_conn))
			{
				char *msg = pchomp(PQerrorMessage(*pageserver_conn));

				PQfinish(*pageserver_conn);
				*pageserver_conn = NULL;

				zenith_log(ERROR, "[ZENITH_SMGR] failed to get handshake from pageserver: %s",
									 msg);
			}
		}
	}

	// FIXME: when auth is enabled this ptints JWT to logs
	zenith_log(LOG, "libpqpagestore: connected to '%s'", page_server_connstring);

	*connected = true;
}

int
lookup_region(Oid spcid, Oid relid)
{
	HeapTuple tup;
	int regionid;

	if (spcid == GLOBALTABLESPACE_OID ||
		!zenith_multiregion_enabled() ||
		IsCatalogRelationOid(relid))
		return zenith_current_region;

	tup = SearchSysCache1(REMOTETABLESPACESPCID, DatumGetObjectId(spcid));
	if (!HeapTupleIsValid(tup))
		return zenith_current_region;

	regionid = ((Form_pg_remote_tablespace) GETSTRUCT(tup))->regionid;

	ReleaseSysCache(tup);
	return regionid;
}
