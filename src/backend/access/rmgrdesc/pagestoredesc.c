#include "postgres.h"

#include "access/pagestore_xlog.h"

void
pagestore_desc(StringInfo buf, XLogReaderState *record)
{
}

const char *
pagestore_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_PAGESTORE_EXTEND_RELATION:
			return "EXTEND_RELATION";

		default:
			return "UNKNOWN";
	}
}
