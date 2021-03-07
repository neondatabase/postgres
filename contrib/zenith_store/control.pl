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
	$node_primary->init(allows_streaming => 1);
	$node_primary->append_conf('postgresql.conf', qq{
		max_connections = 50
	});
	$node_primary->start;
	$node_primary->safe_psql("postgres", "SELECT pg_create_physical_replication_slot('zenith_store', true)");
	$node_primary->safe_psql('postgres', "CREATE DATABASE regression");

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

	@PostgresNode::all_nodes = ();
}
elsif ($action eq "stop")
{
	# TODO
}
else
{
	die("Unknown option\n");
}
