#!/bin/sh
# Manual diff helper: set UPSTREAM_PG_TREE to a checkout of postgres REL_18_STABLE,
# then compare OSS-marked regions in buf_init.c to upstream.
#
#   UPSTREAM_PG_TREE=/path/to/postgres ./compare_to_upstream.sh
#
set -eu
if [ -z "${UPSTREAM_PG_TREE:-}" ]; then
	echo "Set UPSTREAM_PG_TREE to a PostgreSQL REL_18_STABLE source tree." >&2
	exit 1
fi
UP_BUF="$UPSTREAM_PG_TREE/src/backend/storage/buffer/buf_init.c"
if [ ! -f "$UP_BUF" ]; then
	echo "Missing $UP_BUF" >&2
	exit 1
fi
HERE=$(dirname "$0")
echo "=== diff upstream BufferManagerShmemSize vs OSS branch (manual review) ==="
diff -u "$UP_BUF" "$HERE/buf_init.c" | sed -n '/BufferManagerShmemSize/,/^}/p' | head -n 120 || true
