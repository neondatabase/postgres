/*-------------------------------------------------------------------------
 *
 * multiregion.c
 *	  Handles network communications in a multi-region setup.
 *
 * IDENTIFICATION
 *	 contrib/neon/multiregion.c
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

/* GUCs */
char *neon_region_timelines;

static bool
check_neon_region_timelines(char **newval, void **extra, GucSource source)
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

void DefineMultiRegionCustomVariables(void)
{
	DefineCustomStringVariable("neon.region_timelines",
								"List of timelineids corresponding to the partitions",
								NULL,
								&neon_region_timelines,
								"",
								PGC_POSTMASTER,
								0, /* no flags required */
								check_neon_region_timelines, NULL, NULL);
}

bool neon_multiregion_enabled(void)
{
	return neon_region_timelines && neon_region_timelines[0];
}
