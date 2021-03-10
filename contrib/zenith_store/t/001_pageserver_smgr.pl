use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 1;

$TestLib::use_unix_sockets = 0;

$PostgresNode::last_port_assigned = 15431;

my $node_primary = get_new_node('primary');
my $primary_connstr = $node_primary->connstr('postgres');
$primary_connstr =~ s/\'//g;

my $page_server = get_new_node('pageserver');
my $pager_connstr = $page_server->connstr('postgres');
$pager_connstr =~ s/\'//g;

#
# Initialize primary node
#
$node_primary->init(
	allows_streaming => 1
);
$node_primary->append_conf('postgresql.conf', qq{
	log_line_prefix = '%m [%p] [xid%x] %i '
	log_statement = all
	page_server_connstring = '$pager_connstr'
	shared_buffers = 1MB
});
$node_primary->start;
$node_primary->safe_psql("postgres", "SELECT pg_create_physical_replication_slot('zenith_store', true)");

#
# Initialize page store
#
$page_server->init;
$page_server->append_conf('postgresql.conf', qq{
	log_line_prefix = '%m [%p] [xid%x] %i '
	log_statement = all
	shared_preload_libraries = 'zenith_store'
	zenith_store.connstr = '$primary_connstr'
});
$page_server->start;
$page_server->safe_psql("postgres", "CREATE EXTENSION zenith_store");

#
# Create some data
#
my $payload = 'X' x 100;
$node_primary->safe_psql("postgres", "CREATE TABLE t(key int primary key, value text)");
$node_primary->safe_psql("postgres", "INSERT INTO t SELECT generate_series(1,1000000), '$payload'");

note("insert done");

sleep(5); # XXX: wait for replication; change this to some sexplicit await_lsn() call

my $count = $node_primary->safe_psql("postgres", "select count(*) from t");

note("rows count = $count");

sleep(3600);
# is($page1, $page2);
