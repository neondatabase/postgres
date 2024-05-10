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
#include "access/xlog.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "catalog/pg_type.h"
#include "replication/walsender.h"
#include "replication/walproposer.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "utils/pg_lsn.h"

PG_FUNCTION_INFO_V1(pg_cluster_size);
PG_FUNCTION_INFO_V1(backpressure_lsns);

Datum
pg_cluster_size(PG_FUNCTION_ARGS)
{
	int64		size;

	size = GetZenithCurrentClusterSize();

	if (size == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}


Datum
backpressure_lsns(PG_FUNCTION_ARGS)
{
	XLogRecPtr writePtr;
	XLogRecPtr flushPtr;
	XLogRecPtr applyPtr;
	Datum		values[3];
	bool		nulls[3];
	TupleDesc	tupdesc;

	zenith_feedback_get_lsns(&writePtr, &flushPtr, &applyPtr);

	tupdesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "received_lsn", PG_LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "disk_consistent_lsn", PG_LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "remote_consistent_lsn", PG_LSNOID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);

	MemSet(nulls, 0, sizeof(nulls));
	values[0] = LSNGetDatum(writePtr);
	values[1] = LSNGetDatum(flushPtr);
	values[2] = LSNGetDatum(applyPtr);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
