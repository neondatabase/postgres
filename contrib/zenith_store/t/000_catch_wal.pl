use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 1;

$TestLib::use_unix_sockets = 0;

# Initialize primary node
my $node_primary = get_new_node('primary');
$node_primary->init(
	allows_streaming => 1
);
$node_primary->start;
$node_primary->safe_psql("postgres", "SELECT pg_create_physical_replication_slot('zenith_store', true)");

# Initialize page store
my $connstr = ($node_primary->connstr('postgres'));
$connstr =~ s/\'//g;
my $page_server = get_new_node('pageserver');
$page_server->init;
$page_server->append_conf('postgresql.conf', qq{
	log_line_prefix = '%m [%p] [xid%x] %i '
	log_statement = all
	shared_preload_libraries = 'zenith_store'
	zenith_store.connstr = '$connstr'
});
$page_server->start;
$page_server->safe_psql("postgres", "CREATE EXTENSION zenith_store");

# Create some data
$node_primary->safe_psql("postgres", "CREATE TABLE t(key int primary key, value text)");
foreach(1..10){
	$node_primary->safe_psql("postgres", "INSERT INTO t VALUES($_, 'payload')");
}

sleep(5); # XXX: wait for replication; change this to some sexplicit await_lsn() call

$page_server->safe_psql("postgres", "select zenith_store.get_page(42, 1663, 13231, 16384, 0, 0)");

printf("---");
sleep(36000);
is(1, 1);
