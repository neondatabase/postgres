#include "postgres.h"

#include "replication/walproposer.h"
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

/* Converts a `WKSockWaitKind` into the bit flags that would match it
 *
 * Note: For `wait_kind = WANTS_NO_WAIT`, this will return a value of zero,
 * which does not match any events. Attempting to wait on no events will
 * always timeout, so it's best to double-check the value being provided to
 * this function where necessary. */
uint32
WaitKindAsEvents(WKSockWaitKind wait_kind)
{
	uint32 return_val;

	switch (wait_kind)
	{
		case WANTS_NO_WAIT:
			return_val = WL_NO_EVENTS;
			break;
		case WANTS_SOCK_READ:
			return_val = WL_SOCKET_READABLE;
			break;
		case WANTS_SOCK_WRITE:
			return_val = WL_SOCKET_WRITEABLE;
			break;
		case WANTS_SOCK_EITHER:
			return_val = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
			break;
	}

	return return_val;
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
	char* return_val;

	switch (state)
	{
		case SS_OFFLINE:
			return_val = "offline";
			break;
		case SS_CONNECTING:
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
		case SS_IDLE:
			return_val = "idle";
			break;
		case SS_SEND_WAL:
			return_val = "WAL-sending";
			break;
		case SS_RECV_FEEDBACK:
			return_val = "WAL-feedback-receiving";
			break;
	}

	return return_val;
}

/* Returns a human-readable string corresponding to the WKSockWaitKind
 *
 * The string should not be freed. */
char*
FormatWKSockWaitKind(WKSockWaitKind wait_kind)
{
	char* return_val;

	switch (wait_kind)
	{
		case WANTS_NO_WAIT:
			return_val = "<no events>";
			break;
		case WANTS_SOCK_READ:
			return_val = "<read event>";
			break;
		case WANTS_SOCK_WRITE:
			return_val = "<write event>";
			break;
		case WANTS_SOCK_EITHER:
			return_val = "<read or write event>";
			break;
	}

	return return_val;
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
