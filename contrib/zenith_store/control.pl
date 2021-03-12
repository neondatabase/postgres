#!/usr/bin/env perl

use strict;
use warnings;
use PostgresNode;
use File::Basename;
use Getopt::Long;
BEGIN { unshift @INC, '.'; unshift @INC, '../../src/test/perl' }

my $action = 'start';
GetOptions ("action=s"  => \$action);

if ($action eq "start")
{
	$PostgresNode::last_port_assigned = 65431;

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
		max_connections = 100
	});
	$node_primary->start;
	$node_primary->safe_psql("postgres", "SELECT pg_create_physical_replication_slot('zenith_store', true)");
	$node_primary->safe_psql('postgres', "CREATE DATABASE regression");

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

	@PostgresNode::all_nodes = ();
}
elsif ($action eq "stop")
{
	foreach my $pg (<$TestLib::tmp_check/*data>) {
		TestLib::system_log('pg_ctl', '-D', "$pg/pgdata",
							'-m', 'fast', 'stop');
	}
}
else
{
	die("Unknown option\n");
}
