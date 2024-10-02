/*-------------------------------------------------------------------------
 *
 * message.c
 *	  Generic logical messages.
 *
 * Copyright (c) 2013-2024, PostgreSQL Global Development Group
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
#include <sys/stat.h>

#include "access/xact.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "replication/message.h"
#include "storage/fd.h"

/*
 * Write logical decoding message into XLog.
 */
XLogRecPtr
LogLogicalMessage(const char *prefix, const char *message, size_t size,
				  bool transactional, bool flush)
{
	xl_logical_message xlrec;
	XLogRecPtr	lsn;

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

	lsn = XLogInsert(RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE);

	/*
	 * Make sure that the message hits disk before leaving if emitting a
	 * non-transactional message when flush is requested.
	 */
	if (!transactional && flush)
		XLogFlush(lsn);
	return lsn;
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
 * NEON: remove AUX object
 */
void
wallog_file_removal(char const* path)
{
	char	prefix[MAXPGPATH];

	/* Do not wallog AUX file at replica */
	if (!XLogInsertAllowed())
		return;

	snprintf(prefix, sizeof(prefix), "neon-file:%s", path);
	elog(DEBUG1, "neon: deleting contents of file %s", path);

	/* unlink file */
	LogLogicalMessage(prefix, NULL, 0, false, true);
}

/*
 * NEON: persist file in WAL to save it in persistent storage.
 */
void
wallog_file_descriptor(char const* path, int fd, uint64_t limit)
{
	char	prefix[MAXPGPATH];
	off_t	size;
	struct stat stat;

	Assert(fd >= 0);

	/* Do not wallog AUX file at replica */
	if (!XLogInsertAllowed())
		return;

	if (fstat(fd, &stat))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m", path)));
	size = stat.st_size;

	if (size < 0)
		elog(ERROR, "Failed to get size of file %s: %m", path);
	elog(DEBUG1, "neon: writing contents of file %s, size %ld", path, (long)size);

	if ((uint64_t)size > limit)
	{
		elog(WARNING, "Size of file %s %ld is larger than limit %ld", path, (long)size, (long)limit);
		wallog_file_removal(path);
	}
	else
	{
		char* buf = palloc((size_t)size); /* palloc is not able to allocate more than MaxAllocSize (1Gb) but we don't expect such large AUX files */
		size_t offs = 0;
		while (offs < size) {
			ssize_t rc = pread(fd, buf + offs, (size_t)size - offs, offs);
			if (rc <= 0)
				elog(ERROR, "Failed to read file %s: %m", path);
			offs += rc;
		}
		snprintf(prefix, sizeof(prefix), "neon-file:%s", path);
		LogLogicalMessage(prefix, buf, (size_t)size, false, true);
		pfree(buf);
	}
}

void
wallog_file(char const* path, uint64_t limit)
{
	int fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}
	else
	{
		wallog_file_descriptor(path, fd, limit);
		CloseTransientFile(fd);
	}
}
