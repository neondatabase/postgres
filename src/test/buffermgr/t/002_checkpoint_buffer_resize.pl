# Copyright (c) 2025-2025, PostgreSQL Global Development Group
#
# Test shared_buffer resizing coordination with checkpoint using injection points

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

# Initialize cluster with injection points enabled
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', 'shared_preload_libraries = injection_points');
$node->append_conf('postgresql.conf', 'shared_buffers = 256kB');
# Disable background writer to prevent interference with dirty buffers
$node->append_conf('postgresql.conf', 'bgwriter_lru_maxpages = 0');
$node->start;

# Load the injection points extension
$node->safe_psql('postgres', "CREATE EXTENSION injection_points");

# Create some data to make checkpoint meaningful and ensure many dirty buffers
$node->safe_psql('postgres', "CREATE TABLE test_data (id int, data text)");
# Insert enough data to fill more than 16 buffers (each row ~1KB, so 20+ rows per page)
$node->safe_psql('postgres', "INSERT INTO test_data SELECT i, repeat('x', 1000) FROM generate_series(1, 5000) i");

# Create additional tables to ensure we have plenty of dirty buffers
$node->safe_psql('postgres', "CREATE TABLE test_data2 AS SELECT * FROM test_data WHERE id <= 2500");
$node->safe_psql('postgres', "CREATE TABLE test_data3 AS SELECT * FROM test_data WHERE id > 2500");

# Update data to create more dirty buffers
$node->safe_psql('postgres', "UPDATE test_data SET data = repeat('y', 1000) WHERE id % 3 = 0");
$node->safe_psql('postgres', "UPDATE test_data2 SET data = repeat('z', 1000) WHERE id % 2 = 0");

# Prepare the new shared_buffers configuration before starting checkpoint
$node->safe_psql('postgres', "ALTER SYSTEM SET shared_buffers = '128kB'");
$node->safe_psql('postgres', "SELECT pg_reload_conf()");

# Set up the injection point to make checkpoint wait
$node->safe_psql('postgres', "SELECT injection_points_attach('buffer-sync-dirty-buffer-scan', 'wait')");

# Start a checkpoint in the background that will trigger the injection point
my $checkpoint_session = $node->background_psql('postgres');
$checkpoint_session->query_until(
	qr/starting_checkpoint/,
	q(
		\echo starting_checkpoint
		CHECKPOINT;
		\q
	)
);

# Wait until checkpointer actually reaches the injection point
$node->wait_for_event('checkpointer', 'buffer-sync-dirty-buffer-scan');

# Verify checkpoint is waiting by checking if it hasn't completed
my $checkpoint_running = $node->safe_psql('postgres',
	"SELECT COUNT(*) FROM pg_stat_activity WHERE backend_type = 'checkpointer' AND wait_event = 'buffer-sync-dirty-buffer-scan'");
is($checkpoint_running, '1', 'Checkpoint is waiting at injection point');

# Start the resize operation in the background (don't wait for completion)
my $resize_session = $node->background_psql('postgres');
$resize_session->query_until(
	qr/starting_resize/,
	q(
		\echo starting_resize
		SELECT pg_resize_shared_buffers();
	)
);

# Continue the checkpoint and wait for its completion
my $log_offset = -s $node->logfile;
$node->safe_psql('postgres', "SELECT injection_points_wakeup('buffer-sync-dirty-buffer-scan')");

# Wait for both checkpoint and resize to complete
$node->wait_for_log(qr/checkpoint complete/, $log_offset);

# Wait for the resize operation to complete using the proper method
$resize_session->query(q(\echo 'resize_complete'));

pass('Checkpoint and buffer resize both completed after injection point was released');

# Verify the resize actually worked
is($node->safe_psql('postgres', "SHOW shared_buffers"), '128kB',
	'Buffer resize completed successfully after checkpoint coordination');

# Cleanup the background session
$resize_session->quit;

# Clean up the injection point
$node->safe_psql('postgres', "SELECT injection_points_detach('buffer-sync-dirty-buffer-scan')");

# Verify system remains stable after coordinated operations

# Perform a normal checkpoint to ensure everything is working
$node->safe_psql('postgres', "CHECKPOINT");

pass('System remains stable after injection point testing');

# Cleanup
$node->safe_psql('postgres', "DROP TABLE test_data, test_data2, test_data3");

done_testing();