/*
 * rmgrdesc.c
 *
 * pg_waldump resource managers definition
 *
 * src/bin/pg_waldump/rmgrdesc.c
 */
#define FRONTEND 1
#include "postgres.h"

#include "access/brin_xlog.h"
#include "access/neon_xlog.h"
#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/generic_xlog.h"
#include "access/ginxlog.h"
#include "access/gistxlog.h"
#include "access/hash_xlog.h"
#include "access/heapam_xlog.h"
#include "access/multixact.h"
#include "access/nbtxlog.h"
#include "access/rmgr.h"
#include "access/spgxlog.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/rmgrdesc_utils.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands_xlog.h"
#include "commands/sequence.h"
#include "commands/tablespace.h"
#include "replication/message.h"
#include "replication/origin.h"
#include "rmgrdesc.h"
#include "storage/standbydefs.h"
#include "utils/relmapper.h"

#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup,mask,decode) \
	{ name, desc, identify},

static const RmgrDescData RmgrDescTable[RM_N_BUILTIN_IDS] = {
#include "access/rmgrlist.h"
};

#define CUSTOM_NUMERIC_NAME_LEN sizeof("custom###")

static char CustomNumericNames[RM_N_CUSTOM_IDS][CUSTOM_NUMERIC_NAME_LEN] = {{0}};
static RmgrDescData CustomRmgrDesc[RM_N_CUSTOM_IDS] = {{0}};
static bool CustomRmgrDescInitialized = false;

/*
 * NOTE: "keyname" argument cannot have trailing spaces or punctuation
 * characters
 */
static void
infobits_desc(StringInfo buf, uint8 infobits, const char *keyname)
{
	appendStringInfo(buf, "%s: [", keyname);

	Assert(buf->data[buf->len - 1] != ' ');

	if (infobits & XLHL_XMAX_IS_MULTI)
		appendStringInfoString(buf, "IS_MULTI, ");
	if (infobits & XLHL_XMAX_LOCK_ONLY)
		appendStringInfoString(buf, "LOCK_ONLY, ");
	if (infobits & XLHL_XMAX_EXCL_LOCK)
		appendStringInfoString(buf, "EXCL_LOCK, ");
	if (infobits & XLHL_XMAX_KEYSHR_LOCK)
		appendStringInfoString(buf, "KEYSHR_LOCK, ");
	if (infobits & XLHL_KEYS_UPDATED)
		appendStringInfoString(buf, "KEYS_UPDATED, ");

	if (buf->data[buf->len - 1] == ' ')
	{
		/* Truncate-away final unneeded ", "  */
		Assert(buf->data[buf->len - 2] == ',');
		buf->len -= 2;
		buf->data[buf->len] = '\0';
	}

	appendStringInfoString(buf, "]");
}

static void
neon_rm_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	info &= XLOG_NEON_OPMASK;

	if (info == XLOG_NEON_HEAP_INSERT)
	{
		xl_neon_heap_insert *xlrec = (xl_neon_heap_insert *) rec;

		appendStringInfo(buf, "off: %u, flags: 0x%02X",
						 xlrec->offnum,
						 xlrec->flags);
	}
	else if (info == XLOG_NEON_HEAP_DELETE)
	{
		xl_neon_heap_delete *xlrec = (xl_neon_heap_delete *) rec;

		appendStringInfo(buf, "xmax: %u, off: %u, ",
						 xlrec->xmax, xlrec->offnum);
		infobits_desc(buf, xlrec->infobits_set, "infobits");
		appendStringInfo(buf, ", flags: 0x%02X", xlrec->flags);
	}
	else if (info == XLOG_NEON_HEAP_UPDATE)
	{
		xl_neon_heap_update *xlrec = (xl_neon_heap_update *) rec;

		appendStringInfo(buf, "old_xmax: %u, old_off: %u, ",
						 xlrec->old_xmax, xlrec->old_offnum);
		infobits_desc(buf, xlrec->old_infobits_set, "old_infobits");
		appendStringInfo(buf, ", flags: 0x%02X, new_xmax: %u, new_off: %u",
						 xlrec->flags, xlrec->new_xmax, xlrec->new_offnum);
	}
	else if (info == XLOG_NEON_HEAP_HOT_UPDATE)
	{
		xl_neon_heap_update *xlrec = (xl_neon_heap_update *) rec;

		appendStringInfo(buf, "old_xmax: %u, old_off: %u, ",
						 xlrec->old_xmax, xlrec->old_offnum);
		infobits_desc(buf, xlrec->old_infobits_set, "old_infobits");
		appendStringInfo(buf, ", flags: 0x%02X, new_xmax: %u, new_off: %u",
						 xlrec->flags, xlrec->new_xmax, xlrec->new_offnum);
	}
	else if (info == XLOG_NEON_HEAP_LOCK)
	{
		xl_neon_heap_lock *xlrec = (xl_neon_heap_lock *) rec;

		appendStringInfo(buf, "xmax: %u, off: %u, ",
						 xlrec->xmax, xlrec->offnum);
		infobits_desc(buf, xlrec->infobits_set, "infobits");
		appendStringInfo(buf, ", flags: 0x%02X", xlrec->flags);
	}
	else if (info == XLOG_NEON_HEAP_MULTI_INSERT)
	{
		xl_neon_heap_multi_insert *xlrec = (xl_neon_heap_multi_insert *) rec;
		bool		isinit = (XLogRecGetInfo(record) & XLOG_NEON_INIT_PAGE) != 0;

		appendStringInfo(buf, "ntuples: %d, flags: 0x%02X", xlrec->ntuples,
						 xlrec->flags);

		if (XLogRecHasBlockData(record, 0) && !isinit)
		{
			appendStringInfoString(buf, ", offsets:");
			array_desc(buf, xlrec->offsets, sizeof(OffsetNumber),
					   xlrec->ntuples, &offset_elem_desc, NULL);
		}
	}
}

static const char *
neon_rm_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_NEON_HEAP_INSERT:
			id = "INSERT";
			break;
		case XLOG_NEON_HEAP_INSERT | XLOG_NEON_INIT_PAGE:
			id = "INSERT+INIT";
			break;
		case XLOG_NEON_HEAP_DELETE:
			id = "DELETE";
			break;
		case XLOG_NEON_HEAP_UPDATE:
			id = "UPDATE";
			break;
		case XLOG_NEON_HEAP_UPDATE | XLOG_NEON_INIT_PAGE:
			id = "UPDATE+INIT";
			break;
		case XLOG_NEON_HEAP_HOT_UPDATE:
			id = "HOT_UPDATE";
			break;
		case XLOG_NEON_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE:
			id = "HOT_UPDATE+INIT";
			break;
		case XLOG_NEON_HEAP_LOCK:
			id = "LOCK";
			break;
		case XLOG_NEON_HEAP_MULTI_INSERT:
			id = "MULTI_INSERT";
			break;
		case XLOG_NEON_HEAP_MULTI_INSERT | XLOG_NEON_INIT_PAGE:
			id = "MULTI_INSERT+INIT";
			break;
	}

	return id;
}

const static RmgrDescData NeonRmgr = {
	.rm_name = "neon",
	.rm_desc = neon_rm_desc,
	.rm_identify = neon_rm_identify,
};

/*
 * No information on custom resource managers; just print the ID.
 */
static void
default_desc(StringInfo buf, XLogReaderState *record)
{
	appendStringInfo(buf, "rmid: %d", XLogRecGetRmid(record));
}

/*
 * No information on custom resource managers; just return NULL and let the
 * caller handle it.
 */
static const char *
default_identify(uint8 info)
{
	return NULL;
}

/*
 * We are unable to get the real name of a custom rmgr because the module is
 * not loaded. Generate a table of rmgrs with numeric names of the form
 * "custom###", where "###" is the 3-digit resource manager ID.
 */
static void
initialize_custom_rmgrs(void)
{
	for (int i = 0; i < RM_N_CUSTOM_IDS; i++)
	{
		snprintf(CustomNumericNames[i], CUSTOM_NUMERIC_NAME_LEN,
				 "custom%03d", i + RM_MIN_CUSTOM_ID);
		CustomRmgrDesc[i].rm_name = CustomNumericNames[i];
		CustomRmgrDesc[i].rm_desc = default_desc;
		CustomRmgrDesc[i].rm_identify = default_identify;
	}
	CustomRmgrDescInitialized = true;
}

const RmgrDescData *
GetRmgrDesc(RmgrId rmid)
{
	Assert(RmgrIdIsValid(rmid));

	if (RmgrIdIsBuiltin(rmid))
		return &RmgrDescTable[rmid];
	else
	{
		if (rmid == RM_NEON_ID) {
			/* Neon RMGR is a custom RMGR, but we have its definition */
			return &NeonRmgr;
		}

		if (!CustomRmgrDescInitialized)
			initialize_custom_rmgrs();
		return &CustomRmgrDesc[rmid - RM_MIN_CUSTOM_ID];
	}
}
