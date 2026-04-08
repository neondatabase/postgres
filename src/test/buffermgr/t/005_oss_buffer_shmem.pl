# Copyright (c) 2025-2025, PostgreSQL Global Development Group
#
# OSS layout: max_shared_buffers equals shared_buffers at start — resize disabled.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('oss_buf');
$node->init;

# Tie cap to current pool so buffer_pool_uses_split_segments is false.
$node->append_conf('postgresql.conf', qq{
shared_buffers = 128
max_shared_buffers = 128
huge_pages = off
});

$node->start;

$node->command_fails_like(
	[
		'psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1',
		'-d', $node->connstr('postgres'),
		'-c', 'SELECT * FROM pg_resize_shared_buffers();'
	],
	qr/pg_resize_shared_buffers\(\) is not available|must be greater than/s,
	'pg_resize_shared_buffers rejected in OSS buffer layout');

done_testing();
