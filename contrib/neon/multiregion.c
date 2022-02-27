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
static char *zenith_region_connstrings;

static List *region_timelines = NULL;
static List *region_connstrings = NULL;

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

static void
assign_zenith_region_timelines(const char *newval, void *extra)
{
	char *rawstring;
	List *timelines;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(newval);

	/* Parse string into list of timelines */
	if (!SplitIdentifierString(rawstring, ',', &timelines))
	{
		pfree(rawstring);
		list_free(timelines);
		return;
	}

	/* 
	 * The check_zenith_region_timelines function already did all
	 * the checks so assign immediately 
	 */
	region_timelines = timelines;

	pfree(rawstring);
}

static bool
check_zenith_region_connstrings(char **newval, void **extra, GucSource source)
{
	char *rawstring;
	List *connstrings;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	/* Parse string into list of connstrings */
	if (!SplitIdentifierString(rawstring, ',', &connstrings))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(connstrings);
		return false;
	}

	pfree(rawstring);
	list_free(connstrings);

	return true;
}

static void
assign_zenith_region_connstrings(const char *newval, void *extra)
{
	char *rawstring;
	List *connstrings;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(newval);

	/* Parse string into list of connstrings */
	if (!SplitIdentifierString(rawstring, ',', &connstrings))
	{
		pfree(rawstring);
		list_free(connstrings);
		return;
	}

	region_connstrings = connstrings;

	pfree(rawstring);
}

void DefineMultiRegionCustomVariables(void)
{
	DefineCustomStringVariable("zenith.region_timelines",
								"List of timelineids corresponding to the partitions",
								NULL,
								&zenith_region_timelines,
								"",
								PGC_POSTMASTER,
								0, /* no flags required */
								check_zenith_region_timelines, assign_zenith_region_timelines, NULL);

	DefineCustomStringVariable("zenith.region_connstrings",
								"List of connstrings corresponding to the partitions",
								NULL,
								&zenith_region_connstrings,
								"",
								PGC_POSTMASTER,
								0, /* no flags required */
								check_zenith_region_connstrings, assign_zenith_region_connstrings, NULL);

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
		zenith_region_connstrings && zenith_region_connstrings[0];
}

/*
 * Similar function to zenith_connect in libpagestore.c but used for
 * multiple timelines.
 */
void zenith_multiregion_connect(PGconn **pageserver_conn, bool *connected)
{
	char *query;
	int ret;

	Assert(!*connected);
	Assert(region_timelines);
	Assert(region_connstrings);

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

	/* 
	 * Ask the Page Server to connect to us, and stream WAL from us. 
	 * TODO: Connect to the safekeeper in other regions
	 */
	// if (callmemaybe_connstring && callmemaybe_connstring[0] && zenith_tenant && zenith_timeline)
	// {
	// 	PGresult *res;

	// 	query = psprintf("callmemaybe %s %s %s", zenith_tenant, zenith_timeline, callmemaybe_connstring);
	// 	res = PQexec(*pageserver_conn, query);
	// 	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	// 	{
	// 		PQfinish(*pageserver_conn);
	// 		*pageserver_conn = NULL;
	// 		zenith_log(ERROR,
	// 				   "[ZENITH_SMGR] callmemaybe command failed");
	// 	}
	// 	PQclear(res);
	// }

	query = psprintf("multipagestream %s %s", zenith_tenant, zenith_region_timelines);
	zenith_log(LOG, "timelines: %s", zenith_region_timelines);

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
