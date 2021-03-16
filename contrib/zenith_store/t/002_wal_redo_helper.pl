#
# Test the WAL redo helper process.
#
# Launches "postgres --wal-redo", and issues some PushPage, ApplyRecord and
# GetPage requests.
#
#

use strict;
use warnings;
#use TestLib;
use Test::More tests => 1;
use IPC::Run;

my $in;
my $out;
my $err;

sub send_msg
{
	my ($msgtype, $payload) = @_;

	return pack("C N", ord($msgtype), length($payload) + 4) . $payload;
}
  
my $h = IPC::Run::start(['postgres', '--wal-redo'],
						'<',
						\$in,
						'>',
						\$out,
						'2>',
						\$err,
						IPC::Run::timeout( 10 ));

# Send PushPage request

# Construct a page header
my $phdr = pack("LL S S SSS S L x[8168]",
			 1234, 5678,    # pd_lsn
			 0,             # pd_checksum
			 0,             # pd_flags
			 20,            # pd_lower
			 100,           # pd_upper
			 8192,          # pd_special
			 0,             # pd_pagesize_version (invalid)
			 0);            # pd_prune_xid

$in .= send_msg('P', pack ("L> L> L> L> L>",
						   12, # spcNode
						   34, # dbNode
						   45, # relNode
						   0,  # forkNum
						   67) # blknum,
				. $phdr);
$h->pump() while length $in;

# Construct a "generic" WAL record that writes "helloworld" to the page we just
# pushed with PushPage
my $payload = "helloworld";

my $gpayload = pack("S S",
					8000, length($payload)) # offset and length
  . $payload;

# XLogRecordBlockHeader + RelFileNode + blknum
my $blkdata = pack("C C S LLLL",
				   0,		# id
				   0x20,	# fork_flags
				   length($gpayload),	# data_length
				   12,		# spcNode
				   34,		# dbNode
				   45,		# relNode
				   67)	    # blknum
  . $gpayload;

# XLogRecord
my $walrecord = pack("L L LL C C xx L",
					 24 + length($blkdata),		# xl_tot_len
					 0,			# xl_xid
					 0, 0,		# xl_prev
					 0,			# xl_info
					 20,		# xl_rmid
					 0)			# xl_crc
  . $blkdata;

$in .= send_msg('A', pack ('LL', 1234, 5679) . $walrecord);

#
# Finally, send a GetPage request to get the page back
#
$in .= send_msg('G', pack ("L> L> L> L> L>",
						   12, # spcNode
						   34, # dbNode
						   45, # relNode
						   0,  # forkNum
						   67)); # blknum,

while(1)
{
	if (length($err) > 0)
	{
		diag("stderr: " . $err);
		$err = '';
	}
	if (length($out) > 0)
	{
		last if $out =~ /world/;
	}
	$h->pump();
}
diag("stdout (" . length($out) . "): " . $out);

# If the WAL record was applied, the page image we got back should
# contain the string "helloworld", 
ok($out =~ /helloworld/);
