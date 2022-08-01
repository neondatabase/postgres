#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"

PG_FUNCTION_INFO_V1(print_bytes);

Datum
print_bytes(PG_FUNCTION_ARGS)
{
	bytea	   *bytes = PG_GETARG_BYTEA_P(0);
	int			size;
	int			i;
	StringInfoData out;

	initStringInfo(&out);

	size = VARSIZE(bytes) - VARHDRSZ;
	for (i = 0; i < size; i++)
	{
		appendStringInfo(&out, "%x ", VARDATA(bytes)[i]);
	}

	ereport(LOG, errmsg("size = %d, data = %s", size, out.data));

	PG_RETURN_BOOL(true);
}
