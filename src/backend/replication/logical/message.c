/*-------------------------------------------------------------------------
 *
 * message.c
 *	  Generic logical messages.
 *
 * Copyright (c) 2013-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/message.c
 *
 * NOTES
 *
 * Generic logical messages allow XLOG logging of arbitrary binary blobs that
 * get passed to the logical decoding plugin. In normal XLOG processing they
 * are same as NOOP.
 *
 * These messages can be either transactional or non-transactional.
 * Transactional messages are part of current transaction and will be sent to
 * decoding plugin using in a same way as DML operations.
 * Non-transactional messages are sent to the plugin at the time when the
 * logical decoding reads them from XLOG. This also means that transactional
 * messages won't be delivered if the transaction was rolled back but the
 * non-transactional one will always be delivered.
 *
 * Every message carries prefix to avoid conflicts between different decoding
 * plugins. The plugin authors must take extra care to use unique prefix,
 * good options seems to be for example to use the name of the extension.
 *
 * ---------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "access/xact.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "replication/logical.h"
#include "replication/message.h"
#include "utils/memutils.h"

/*
 * Write logical decoding message into XLog.
 */
XLogRecPtr
LogLogicalMessage(const char *prefix, const char *message, size_t size,
				  bool transactional)
{
	xl_logical_message xlrec;

	/*
	 * Force xid to be allocated if we're emitting a transactional message.
	 */
	if (transactional)
	{
		Assert(IsTransactionState());
		GetCurrentTransactionId();
	}

	xlrec.dbId = MyDatabaseId;
	xlrec.transactional = transactional;
	/* trailing zero is critical; see logicalmsg_desc */
	xlrec.prefix_size = strlen(prefix) + 1;
	xlrec.message_size = size;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfLogicalMessage);
	XLogRegisterData(unconstify(char *, prefix), xlrec.prefix_size);
	XLogRegisterData(unconstify(char *, message), size);

	/* allow origin filtering */
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

	return XLogInsert(RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE);
}

/*
 * Redo is basically just noop for logical decoding messages.
 */
void
logicalmsg_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info != XLOG_LOGICAL_MESSAGE)
		elog(PANIC, "logicalmsg_redo: unknown op code %u", info);

	/* This is only interesting for logical decoding, see decode.c. */
}

/*
 * NEON: persist file in WAL to save it in persistent storage.
 * If fd < 0, then remote entry from page server.
 */
void
wallog_file_descriptor(char const* path, int fd, uint64_t limit)
{
	char	prefix[MAXPGPATH];
	snprintf(prefix, sizeof(prefix), "neon-file:%s", path);
	if (fd < 0)
	{
		elog(DEBUG1, "neon: deleting contents of file %s", path);
		/* unlink file */
		XLogFlush(LogLogicalMessage(prefix, NULL, 0, false));
	}
	else
	{
		off_t size = lseek(fd, 0, SEEK_END);
		char* buf;
		elog(DEBUG1, "neon: writing contents of file %s, size %ld", path, (long)size);
		if (size < 0)
			elog(ERROR, "Failed to get size of file %s: %m", path);
		if ((uint64_t)size > limit)
		{
			elog(WARNING, "Size of file %s %ld is larger than limit %ld", path, (long)size, (long)limit);
		}
		else
		{
			char* buf = palloc((size_t)size);
			lseek(fd, 0, SEEK_SET);
			if (read(fd, buf, (size_t)size) != size)
				elog(ERROR, "Failed to read file %s: %m", path);
			XLogFlush(LogLogicalMessage(prefix, buf, (size_t)size, false));
			pfree(buf);
		}
	}
}

void
wallog_file(char const* path, uint64_t limit)
{
	int fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", path)));
	}
	else
	{
		wallog_file_descriptor(path, fd, limit);
		CloseTransientFile(fd);
	}
}
