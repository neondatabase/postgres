#include "postgres.h"

#include "replication/walproposer.h"
#include "libpq/pqformat.h"
#include "common/logging.h"
#include "common/ip.h"
#include "../interfaces/libpq/libpq-fe.h"
#include <netinet/tcp.h>
#include <unistd.h>

int
CompareLsn(const void *a, const void *b)
{
	XLogRecPtr	lsn1 = *((const XLogRecPtr *) a);
	XLogRecPtr	lsn2 = *((const XLogRecPtr *) b);

	if (lsn1 < lsn2)
		return -1;
	else if (lsn1 == lsn2)
		return 0;
	else
		return 1;
}

/* Returns a human-readable string corresonding to the WalKeeperState
 *
 * The string should not be freed.
 *
 * The strings are intended to be used as a prefix to "state", e.g.:
 *
 *   elog(LOG, "currently in %s state", FormatWalKeeperState(wk->state));
 *
 * If this sort of phrasing doesn't fit the message, instead use something like:
 *
 *   elog(LOG, "currently in state [%s]", FormatWalKeeperState(wk->state));
 */
char*
FormatWalKeeperState(WalKeeperState state)
{
	char* return_val = NULL;

	switch (state)
	{
		case SS_OFFLINE:
			return_val = "offline";
			break;
		case SS_CONNECTING_READ:
		case SS_CONNECTING_WRITE:
			return_val = "connecting";
			break;
		case SS_EXEC_STARTWALPUSH:
			return_val = "sending 'START_WAL_PUSH' query";
			break;
		case SS_WAIT_EXEC_RESULT:
			return_val = "receiving query result";
			break;
		case SS_HANDSHAKE_SEND:
			return_val = "handshake (sending)";
			break;
		case SS_HANDSHAKE_RECV:
			return_val = "handshake (receiving)";
			break;
		case SS_VOTING:
			return_val = "voting";
			break;
		case SS_SEND_VOTE:
			return_val = "sending vote";
			break;
		case SS_WAIT_VERDICT:
			return_val = "wait-for-verdict";
			break;
		case SS_SEND_ELECTED_FLUSH:
			return_val = "send-announcement-flush";
			break;
		case SS_IDLE:
			return_val = "idle";
			break;
		case SS_ACTIVE_STATE:
			return_val = "WAL-active-state";
			break;
	}

	Assert(return_val != NULL);

	return return_val;
}

/* Asserts that the provided events are expected for given WAL keeper's state */
void
AssertEventsOkForState(uint32 events, WalKeeper* wk)
{
	uint32 expected = WalKeeperStateDesiredEvents(wk->state);

	/* The events are in-line with what we're expecting, under two conditions:
	 *   (a) if we aren't expecting anything, `events` has no read- or
	 *       write-ready component.
	 *   (b) if we are expecting something, there's overlap
	 *       (i.e. `events & expected != 0`)
	 */
	bool events_ok_for_state; /* long name so the `Assert` is more clear later */

	if (expected == WL_NO_EVENTS)
		events_ok_for_state = ((events & (WL_SOCKET_READABLE|WL_SOCKET_WRITEABLE)) == 0);
	else
		events_ok_for_state = ((events & expected) != 0);

	if (!events_ok_for_state)
	{
		/* To give a descriptive message in the case of failure, we use elog and
		 * then an assertion that's guaranteed to fail. */
		elog(WARNING, "events %s mismatched for walkeeper %s:%s in state [%s]",
			 FormatEvents(events), wk->host, wk->port, FormatWalKeeperState(wk->state));
		Assert(events_ok_for_state);
	}
}

/* Returns the set of events a WAL keeper in this state should be waiting on
 *
 * This will return WL_NO_EVENTS (= 0) for some events. */
uint32
WalKeeperStateDesiredEvents(WalKeeperState state)
{
	uint32 result;

	/* If the state doesn't have a modifier, we can check the base state */
	switch (state)
	{
		/* Connecting states say what they want in the name */
		case SS_CONNECTING_READ:
			result = WL_SOCKET_READABLE;
			break;
		case SS_CONNECTING_WRITE:
			result = WL_SOCKET_WRITEABLE;
			break;

		/* Reading states need the socket to be read-ready to continue */
		case SS_WAIT_EXEC_RESULT:
		case SS_HANDSHAKE_RECV:
		case SS_WAIT_VERDICT:
			result = WL_SOCKET_READABLE;
			break;

		/* Most writing states don't require any socket conditions */
		case SS_EXEC_STARTWALPUSH:
		case SS_HANDSHAKE_SEND:
		case SS_SEND_VOTE:
			result = WL_NO_EVENTS;
			break;
		/* but flushing does require read- or write-ready */
		case SS_SEND_ELECTED_FLUSH:
		case SS_ACTIVE_STATE:
			result = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
			break;

		/* Idle states use read-readiness as a sign that the connection has been
		 * disconnected. */
		case SS_VOTING:
		case SS_IDLE:
			result = WL_SOCKET_READABLE;
			break;

		/* The offline state expects no events. */
		case SS_OFFLINE:
			result = WL_NO_EVENTS;
			break;
	}

	return result;
}

/* Returns whether the WAL keeper state corresponds to something that should be
 * immediately executed -- i.e. it is not idle, and is not currently waiting. */
bool
StateShouldImmediatelyExecute(WalKeeperState state)
{
	/* This is actually pretty simple to determine. */
	return WalKeeperStateDesiredEvents(state) == WL_NO_EVENTS
		&& state != SS_OFFLINE;
}

/* Returns a human-readable string corresponding to the event set
 *
 * If the events do not correspond to something set as the `events` field of a `WaitEvent`, the
 * returned string may be meaingless.
 *
 * The string should not be freed. It should also not be expected to remain the same between
 * function calls. */
char*
FormatEvents(uint32 events)
{
	static char return_str[8];

	/* Helper variable to check if there's extra bits */
	uint32 all_flags = WL_LATCH_SET
		| WL_SOCKET_READABLE
		| WL_SOCKET_WRITEABLE
		| WL_TIMEOUT
		| WL_POSTMASTER_DEATH
		| WL_EXIT_ON_PM_DEATH
		| WL_SOCKET_CONNECTED;

	/* The formatting here isn't supposed to be *particularly* useful -- it's just to give an
	 * sense of what events have been triggered without needing to remember your powers of two. */

	return_str[0] = (events & WL_LATCH_SET       ) ? 'L' : '_';
	return_str[1] = (events & WL_SOCKET_READABLE ) ? 'R' : '_';
	return_str[2] = (events & WL_SOCKET_WRITEABLE) ? 'W' : '_';
	return_str[3] = (events & WL_TIMEOUT         ) ? 'T' : '_';
	return_str[4] = (events & WL_POSTMASTER_DEATH) ? 'D' : '_';
	return_str[5] = (events & WL_EXIT_ON_PM_DEATH) ? 'E' : '_';
	return_str[5] = (events & WL_SOCKET_CONNECTED) ? 'C' : '_';

	if (events & (~all_flags))
	{
		elog(WARNING, "Event formatting found unexpected component %d",
				events & (~all_flags));
		return_str[6] = '*';
		return_str[7] = '\0';
	}
	else
		return_str[6] = '\0';

	return (char *) &return_str;
}

/*
 * Convert a character which represents a hexadecimal digit to an integer.
 *
 * Returns -1 if the character is not a hexadecimal digit.
 */
static int
HexDecodeChar(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;

	return -1;
}

/*
 * Decode a hex string into a byte string, 2 hex chars per byte.
 *
 * Returns false if invalid characters are encountered; otherwise true.
 */
bool
HexDecodeString(uint8 *result, char *input, int nbytes)
{
	int			i;

	for (i = 0; i < nbytes; ++i)
	{
		int			n1 = HexDecodeChar(input[i * 2]);
		int			n2 = HexDecodeChar(input[i * 2 + 1]);

		if (n1 < 0 || n2 < 0)
			return false;
		result[i] = n1 * 16 + n2;
	}

	return true;
}

/* --------------------------------
 *		pq_getmsgint32_le	- get a binary 4-byte int from a message buffer in native (LE) order
 * --------------------------------
 */
uint32
pq_getmsgint32_le(StringInfo msg)
{
	uint32		n32;

	pq_copymsgbytes(msg, (char *) &n32, sizeof(n32));

	return n32;
}

/* --------------------------------
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer in native (LE) order
 * --------------------------------
 */
uint64
pq_getmsgint64_le(StringInfo msg)
{
	uint64		n64;

	pq_copymsgbytes(msg, (char *) &n64, sizeof(n64));

	return n64;
}

/* append a binary [u]int32 to a StringInfo buffer in native (LE) order */
void
pq_sendint32_le(StringInfo buf, uint32 i)
{
	enlargeStringInfo(buf, sizeof(uint32));
	memcpy(buf->data + buf->len, &i, sizeof(uint32));
	buf->len += sizeof(uint32);
}

/* append a binary [u]int64 to a StringInfo buffer in native (LE) order */
void
pq_sendint64_le(StringInfo buf, uint64 i)
{
	enlargeStringInfo(buf, sizeof(uint64));
	memcpy(buf->data + buf->len, &i, sizeof(uint64));
	buf->len += sizeof(uint64);
}