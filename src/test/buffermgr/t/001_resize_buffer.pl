# Copyright (c) 2025-2025, PostgreSQL Global Development Group
#
# Minimal test testing shared_buffer resizing under load

use strict;
use warnings;
use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Function to resize buffer pool and verify the change.
sub apply_and_verify_buffer_change
{
	my ($node, $new_size) = @_;

	# Use the new pg_resize_shared_buffers() interface which handles everything synchronously
	$node->safe_psql('postgres', "ALTER SYSTEM SET shared_buffers = '$new_size'");
	$node->safe_psql('postgres', "SELECT pg_reload_conf()");

	# If resize function fails, try a few times before giving up
	my $max_retries = 5;
	my $retry_delay = 1; # seconds
	my $success = 0;
	for my $attempt (1..$max_retries) {
		my $result = $node->safe_psql('postgres', "SELECT pg_resize_shared_buffers()");
		if ($result eq 't') {
			$success = 1;
			last;
		}

		# If not the last attempt, wait before retrying
		if ($attempt < $max_retries) {
			note "Resizing buffer pool to $new_size, attempt $attempt failed, retrying after $retry_delay seconds...";
			sleep($retry_delay);
		}
	}

	is($success, 1, 'resizing to ' . $new_size . ' succeeded after retries');
	is($node->safe_psql('postgres', "SHOW shared_buffers"), $new_size,
		'SHOW after resizing to '. $new_size . ' succeeded');
}

# Initialize a cluster and start pgbench in the background for concurrent load.
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;

# Permit resizing up to 1GB for this test and let the server start with 128MB.
$node->append_conf('postgresql.conf', qq{
max_shared_buffers = 160
shared_buffers = 16
log_statement = none
});

$node->start;
$node->safe_psql('postgres', "CREATE EXTENSION pg_buffercache");
my $pgb_scale = 1;
my $pgb_duration = 120;
my $pgb_num_clients = 3;
$node->pgbench(
	"--initialize --init-steps=dtpvg --scale=$pgb_scale --quiet",
	0,
	[qr{^$}],
	[   # stderr patterns to verify initialization stages
		qr{dropping old tables},
		qr{creating tables},
		qr{done in \d+\.\d\d s }
	],
	"pgbench initialization (scale=$pgb_scale)"
);
my ($pgbench_stdin, $pgbench_stdout, $pgbench_stderr) = ('', '', '');
# Use --exit-on-abort so that the test stops on the first server crash or error,
# thus making it easy to debug the failure. Use -C to increase the chances of a
# new backend being created while resizing the buffer pool.
my $pgbench_process = IPC::Run::start(
	[
		'pgbench',
		'-p', $node->port,
		'-T', $pgb_duration,
		'-c', $pgb_num_clients,
		'-C',
		'--exit-on-abort',
		'postgres'
	],
	'<'  => \$pgbench_stdin,
	'>'  => \$pgbench_stdout,
	'2>' => \$pgbench_stderr
);

ok($pgbench_process, "pgbench started successfully");

# Allow pgbench to establish connections and start generating load.
#
# TODO: When creating new backends is known to work well with buffer pool
# resizing, this wait should be removed.
sleep(1);

# Resize buffer pool to various sizes while pgbench is running in the
# background. We use smaller sizes to induce frequent buffer eviction and
# allocation.  Also smaller buffer pool means frequent wraparound in background
# writer, default buffer allocation strategy and checkpointer.
#
# TODO: These are pseudo-randomly picked sizes, but we can do better.
my $tests_completed = 0;
my @buffer_sizes = (32, 24, 29, 40, 29, 20, 16, 24);
for my $target_size (@buffer_sizes)
{
	# Convert number of buffers to a string that will be reported by SHOW
	# shared_buffers. This simple calculation works for sizes smaller than 128
	# beyond which the unit changes to MB.
	$target_size = $target_size * 8;
	$target_size = $target_size . 'kB';

	# Verify workload generator is still running
	if (!$pgbench_process->pumpable) {
		ok(0, "pgbench is still running");
		last;
	}

	apply_and_verify_buffer_change($node, $target_size);
	$tests_completed++;

	# Wait for the resized buffer pool to stabilize. If the resized buffer pool
	# is utilized fully, it might hit any wrongly initialized areas of shared
	# memory.
	sleep(2);
}
is($tests_completed, scalar(@buffer_sizes), "All buffer sizes were tested");

# Make sure that pgbench can end normally.
$pgbench_process->signal('TERM');
IPC::Run::finish $pgbench_process;
ok(grep { $pgbench_process->result == $_ } (0, 15),  "pgbench exited gracefully");

# Log any error output from pgbench for debugging
diag("pgbench stderr:\n$pgbench_stderr");
diag("pgbench stdout:\n$pgbench_stdout");

# Ensure database is still functional after all the buffer changes
$node->connect_ok("dbname=postgres",
	"Database remains accessible after $tests_completed buffer resize operations");

done_testing();
