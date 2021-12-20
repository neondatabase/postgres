/*-------------------------------------------------------------------------
 *
 * zenith.c
 *	  Utility functions to expose zenith specific information to user
 *
 * IDENTIFICATION
 *	 contrib/zenith/zenith.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"

#include "access/xact.h"
#include "access/clog.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"


PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_cluster_size);

Datum
pg_cluster_size(PG_FUNCTION_ARGS)
{
	int64		size;

	size = GetZenithCurrentClusterSize();

	if (size == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}