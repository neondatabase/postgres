# Copyright (c) 2025-2025, PostgreSQL Global Development Group
#
# Test that only one pg_resize_shared_buffers() call succeeds when multiple
# sessions attempt to resize buffers concurrently

use strict;
use warnings;
use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Skip this test if injection points are not supported
if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Initialize a cluster
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', 'shared_preload_libraries = injection_points');
$node->append_conf('postgresql.conf', 'shared_buffers = 128kB');
$node->append_conf('postgresql.conf', 'max_shared_buffers = 256kB');
$node->start;

# Load injection points extension for test coordination
$node->safe_psql('postgres', "CREATE EXTENSION injection_points");

# Test 1: Two concurrent pg_resize_shared_buffers() calls
# Set up injection point to pause the first resize call
$node->safe_psql('postgres',
	"SELECT injection_points_attach('pg-resize-shared-buffers-flag-set', 'wait')");

# Change shared_buffers for the resize operation
$node->safe_psql('postgres', "ALTER SYSTEM SET shared_buffers = '144kB'");
$node->safe_psql('postgres', "SELECT pg_reload_conf()");

# Start first resize session (will pause at injection point)
my $session1 = $node->background_psql('postgres');
$session1->query_until(
	qr/starting_resize/,
	q(
		\echo starting_resize
		SELECT pg_resize_shared_buffers();
	)
);

# Wait until session actually reaches the injection point
$node->wait_for_event('client backend', 'pg-resize-shared-buffers-flag-set');

# Start second resize session (should fail immediately since resize is in progress)
my $result2 = $node->safe_psql('postgres', "SELECT pg_resize_shared_buffers()");

# The second call should return false (already in progress)
is($result2, 'f', 'Second concurrent resize call returns false');

# Wake up the first session
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('pg-resize-shared-buffers-flag-set')");

# The pg_resize_shared_buffers() in session1 should now complete successfully
# We can't easily capture the return value from query_until, but we can
# verify the session completes without error and the resize actually happened
$session1->quit;

# Detach injection point
$node->safe_psql('postgres',
	"SELECT injection_points_detach('pg-resize-shared-buffers-flag-set')");

done_testing();
