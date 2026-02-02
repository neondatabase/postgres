# Copyright (c) 2025-2025, PostgreSQL Global Development Group
#
# Test shared_buffer resizing coordination with client connections joining using injection points
use strict;
use warnings;
use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

# Skip this test if injection points are not supported
if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Function to calculate the size of test table required to fill up maximum
# buffer pool when populating it.
sub calculate_test_sizes
{
	my ($node, $block_size) = @_;

	# Get the maximum buffer pool size from configuration
	my $max_shared_buffers = $node->safe_psql('postgres', "SHOW max_shared_buffers");
	my ($max_val, $max_unit) = ($max_shared_buffers =~ /(\d+)(\w+)/);
	my $max_size_bytes;
	if (lc($max_unit) eq 'kb') {
		$max_size_bytes = $max_val * 1024;
	} elsif (lc($max_unit) eq 'mb') {
		$max_size_bytes = $max_val * 1024 * 1024;
	} elsif (lc($max_unit) eq 'gb') {
		$max_size_bytes = $max_val * 1024 * 1024 * 1024;
	} else {
		# Default to kB if unit is not recognized
		$max_size_bytes = $max_val * 1024;
	}

	# Fill more pages than minimally required to increase the chances of pages
	# from the test table filling the buffer cache.
	$max_size_bytes = $max_size_bytes;
	my $pages_needed = int($max_size_bytes / $block_size) + 10; # Add some extra to ensure buffers are filled
	my $rows_to_insert = $pages_needed * 100; # Assuming roughly 100 rows per page for our table structure
	return ($max_size_bytes, $pages_needed, $rows_to_insert);
}

# Function to calculate expected buffer count from size string
sub calculate_buffer_count
{
	my ($size_string, $block_size) = @_;
	# Parse size and convert to bytes
	my ($size_val, $unit) = ($size_string =~ /(\d+)(\w+)/);
	my $size_bytes;
	if (lc($unit) eq 'kb') {
		$size_bytes = $size_val * 1024;
	} elsif (lc($unit) eq 'mb') {
		$size_bytes = $size_val * 1024 * 1024;
	} elsif (lc($unit) eq 'gb') {
		$size_bytes = $size_val * 1024 * 1024 * 1024;
	} else {
		# Default to kB if unit is not recognized
		$size_bytes = $size_val * 1024;
	}
	return int($size_bytes / $block_size);
}

# Initialize cluster with very small buffer sizes for testing
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;

# Configure for buffer resizing with very small buffer pool sizes for faster tests.
# TODO: for some reason parallel workers try to load default number of shared_buffers which doesn't work with lower max_shared_buffers. We need to fix that - somewhere it's picking default value of shared buffers. For now disable parallelism
$node->append_conf('postgresql.conf', 'shared_preload_libraries = injection_points');
$node->append_conf('postgresql.conf', qq{
max_shared_buffers = 512kB
shared_buffers = 320kB
max_parallel_workers_per_gather = 0
});
$node->start;

# Enable injection points
$node->safe_psql('postgres', "CREATE EXTENSION injection_points");

# Get the block size (this is fixed for the binary)
my $block_size = $node->safe_psql('postgres', "SHOW block_size");

# Try to create pg_buffercache extension for buffer analysis
eval {
	$node->safe_psql('postgres', "CREATE EXTENSION pg_buffercache");
};
if ($@) {
	$node->stop;
	plan skip_all => 'pg_buffercache extension not available - cannot verify buffer usage';
}

# Create a small test table, and fetch its properties for later reference if required.
$node->safe_psql('postgres', qq{
	CREATE TABLE client_test (c1 int, data char(50));
});
my $table_oid = $node->safe_psql('postgres', "SELECT oid FROM pg_class WHERE relname = 'client_test'");
my $table_relfilenode = $node->safe_psql('postgres', "SELECT relfilenode FROM pg_class WHERE relname = 'client_test'");
note("Test table client_test: OID = $table_oid, relfilenode = $table_relfilenode");
my ($max_size_bytes, $pages_needed, $rows_to_insert) = calculate_test_sizes($node, $block_size);

# Create dedicated sessions for injection point handling and test queries,
# so that we don't create new backends for test operations after starting
# resize operation. Only one backend, which tests new backend synchronization
# with resizing operation, should start after resizing has commenced.
my $injection_session = $node->background_psql('postgres');
my $query_session = $node->background_psql('postgres');
my $resize_session = $node->background_psql('postgres');

# Function to run a single injection point test
sub run_injection_point_test
{
	my ($test_name, $injection_point, $target_size, $operation_type) = @_;

	# Silence the logging of the statements we run to avoid
	# unnecessarily bloating the test logs.  This runs before the
	# upgrade we're testing, so the details should not be very
	# interesting for debugging.  But if needed, you can make it more
	# verbose by setting this.
	my $verbose = 0;

	note("Test with $test_name ($operation_type)");

	# Calculate test parameters before starting resize
	my ($max_size_bytes, $pages_needed, $rows_to_insert) = calculate_test_sizes($node, $target_size, $block_size);

	# Update buffer pool size and wait for it to reflect pending state
	$resize_session->query_safe("ALTER SYSTEM SET shared_buffers = '$target_size'", verbose => $verbose);
	$resize_session->query_safe("SELECT pg_reload_conf()", verbose => $verbose);
	my $pending_size_str = "pending: $target_size";
	$resize_session->poll_query_until("SELECT substring(current_setting('shared_buffers'), '$pending_size_str')", $pending_size_str, verbose => $verbose);

	# Set up injection point in injection session
	$injection_session->query_safe("SELECT injection_points_attach('$injection_point', 'wait')", verbose => $verbose);

	# Trigger resize
	$resize_session->query_until(
		qr/starting_resize/,
		q(
			\echo starting_resize
			SELECT pg_resize_shared_buffers();
		)
	);

	# Wait until resize actually reaches the injection point using the query session
	$query_session->wait_for_event('client backend', $injection_point, verbose => $verbose);

	# Start a client while resize is paused
	my $client = $node->background_psql('postgres');
	note("Background client backend PID: " . $client->query_safe("SELECT pg_backend_pid()", verbose => $verbose));

	# Wake up the injection point from injection session
	$injection_session->query_safe("SELECT injection_points_wakeup('$injection_point')", verbose => $verbose);

	# Test buffer functionality immediately after waking up injection point
	# Insert data to test buffer pool functionality during/after resize
	$client->query_safe("INSERT INTO client_test SELECT i, 'test_data_' || i FROM generate_series(1, $rows_to_insert) i", verbose => $verbose);
	# Verify the data was inserted correctly and can be read back
	is($client->query_safe("SELECT COUNT(*) FROM client_test", verbose => $verbose), $rows_to_insert, "inserted $rows_to_insert during $test_name ($operation_type) successful");

	# Verify table size is reasonable (should be substantial for testing)
	ok($query_session->query_safe("SELECT pg_total_relation_size('client_test')", verbose => $verbose) >=  $max_size_bytes,"table size is large enough to overflow buffer pool in test $test_name ($operation_type)");

	# Wait for the resize operation to complete. There is no direct way to do so
	# in background_psql. Hence fire a psql command and wait for it to finish
	$resize_session->query(q(\echo 'done'), verbose => $verbose);

	# Detach injection point from injection session
	$injection_session->query_safe("SELECT injection_points_detach('$injection_point')", verbose => $verbose);

	# Verify resize completed successfully
	is($query_session->query_safe("SELECT current_setting('shared_buffers')", verbose => $verbose), $target_size,
		"resize completed successfully to $target_size");

	# Check buffer pool size using pg_buffercache after resize completion
	is($query_session->query_safe("SELECT COUNT(*) FROM pg_buffercache", verbose => $verbose), calculate_buffer_count($target_size, $block_size), "all buffers in the buffer pool used in $test_name ($operation_type)");

	# Wait for client to complete
	ok($client->quit, "client succeeded during $test_name ($operation_type)");

	# Clean up for next test
	$query_session->query_safe("DELETE FROM client_test", verbose => $verbose);
}

# Test injection points during buffer resize with client connections
my @common_injection_tests = (
	{
		name => 'flag setting phase',
		injection_point => 'pg-resize-shared-buffers-flag-set',
	},
	{
		name => 'memory remap phase',
		injection_point => 'pgrsb-after-shmem-resize',
	},
	{
		name => 'resize map barrier complete',
		injection_point => 'pgrsb-resize-barrier-sent',
	},
);

# Test common injection points for both shrinking and expanding
foreach my $test (@common_injection_tests)
{
	# Test shrinking scenario
	run_injection_point_test($test->{name}, $test->{injection_point}, '272kB', 'shrinking');

	# Test expanding scenario
	run_injection_point_test($test->{name}, $test->{injection_point}, '400kB', 'expanding');
}

my @shrink_only_tests = (
	{
		name => 'shrink barrier complete',
		injection_point => 'pgrsb-shrink-barrier-sent',
		size => '200kB',
	}
);
foreach my $test (@shrink_only_tests)
{
	run_injection_point_test($test->{name}, $test->{injection_point}, $test->{size}, 'shrinking only');
}

my @expand_only_tests = (
	{
		name => 'expand barrier complete',
		injection_point => 'pgrsb-expand-barrier-sent',
		size => '416kB',
	}
);

foreach my $test (@expand_only_tests)
{
	run_injection_point_test($test->{name}, $test->{injection_point}, $test->{size}, 'expanding only');
}

$injection_session->quit;
$query_session->quit;
$resize_session->quit;

done_testing();
