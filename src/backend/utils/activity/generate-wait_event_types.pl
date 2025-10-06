#!/usr/bin/perl
#----------------------------------------------------------------------
#
# Generate wait events support files from wait_event_names.txt:
# - wait_event_types.h (if --code is passed)
# - pgstat_wait_event.c (if --code is passed)
# - wait_event_funcs_data.c (if --code is passed)
# - wait_event_types.sgml (if --docs is passed)
#
# Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/backend/utils/activity/generate-wait_event_types.pl
#
#----------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';
use Getopt::Long;

my $output_path = '.';
my $gen_docs = 0;
my $gen_code = 0;
my $nb_waitclass_table_entries = 0;
my $nb_wait_events_with_null = 0;
my $nb_wait_events_per_class = 0;
my %waitclass_values;
my $wait_event_class_mask = 0xFF000000;
my $wait_event_id_mask = 0x0000FFFF;

my $continue = "\n";
my %hashwe;

GetOptions(
	'outdir:s' => \$output_path,
	'docs' => \$gen_docs,
	'code' => \$gen_code) || usage();

die "Needs to specify --docs or --code"
  if (!$gen_docs && !$gen_code);

die "Not possible to specify --docs and --code simultaneously"
  if ($gen_docs && $gen_code);

open my $wait_event_names, '<', $ARGV[0] or die;

# When generating code, we need lwlocklist.h as the second argument
my $lwlocklist_file = $ARGV[1] if $gen_code;

# When generating code, we need wait_classes.h as the third argument
my $wait_classes_file = $ARGV[2] if $gen_code;

my @abi_compatibility_lines;
my @lines;
my $abi_compatibility = 0;
my $section_name;

# Function to parse wait_classes.h and extract wait class definitions
sub parse_wait_classes_header
{

	open my $wait_classes_header, '<', $wait_classes_file
	  or die "Could not open $wait_classes_file: $!";

	while (<$wait_classes_header>)
	{
		chomp;
		if (/^\s*#define\s+(PG_WAIT_\w+)\s+(0x[0-9A-Fa-f]+)U?\s*$/)
		{
			my ($macro_name, $value) = ($1, $2);

			$waitclass_values{$macro_name} = $value;
		}
	}

	close $wait_classes_header;
}

# Function to get the macro from the wait class name
sub waitclass_to_macro
{

	my $waitclass = shift;
	my $last = $waitclass;
	$last =~ s/^WaitEvent//;
	my $lastuc = uc $last;

	return "PG_WAIT_" . $lastuc;
}

# Remove comments and empty lines and add waitclassname based on the section
while (<$wait_event_names>)
{
	chomp;

	# Skip comments
	next if /^#/;

	# Skip empty lines
	next if /^\s*$/;

	# Get waitclassname based on the section
	if (/^Section: ClassName(.*)/)
	{
		$section_name = $_;
		$section_name =~ s/^.*- //;
		$abi_compatibility = 0;
		next;
	}

	# ABI_compatibility region, preserving ABI compatibility of the code
	# generated.  Any wait events listed in this part of the file will
	# not be sorted during the code generation.
	if (/^ABI_compatibility:$/)
	{
		$abi_compatibility = 1;
		next;
	}

	if ($gen_code && $abi_compatibility)
	{
		push(@abi_compatibility_lines, $section_name . "\t" . $_);
	}
	else
	{
		push(@lines, $section_name . "\t" . $_);
	}
}

# Sort the lines based on the second column.
# uc() is being used to force the comparison to be case-insensitive.

my @lines_sorted;
if ($gen_code)
{
	my @lwlock_lines;

	# Separate LWLock lines from others
	foreach my $line (@lines)
	{
		if ($line =~ /^WaitEventLWLock\t/)
		{
			push(@lwlock_lines, $line);
		}
		else
		{
			push(@lines_sorted, $line);
		}
	}

	# Sort only non-LWLock lines
	@lines_sorted =
	  sort { uc((split(/\t/, $a))[1]) cmp uc((split(/\t/, $b))[1]) }
	  @lines_sorted;

	# Add LWLock lines back in their original order
	push(@lines_sorted, @lwlock_lines);
}
else
{
	# For docs, use original alphabetical sorting for all
	@lines_sorted =
	  sort { uc((split(/\t/, $a))[1]) cmp uc((split(/\t/, $b))[1]) } @lines;
}

# If we are generating code, concat @lines_sorted and then
# @abi_compatibility_lines.
if ($gen_code)
{
	push(@lines_sorted, @abi_compatibility_lines);
}

# Read the sorted lines and populate the hash table
foreach my $line (@lines_sorted)
{
	die "unable to parse wait_event_names.txt for line $line\n"
	  unless $line =~ /^(\w+)\t+(\w+)\t+("\w.*\.")$/;

	(my $waitclassname, my $waiteventname, my $waitevendocsentence) =
	  split(/\t/, $line);

	# Generate the element name for the enums based on the
	# description.  The C symbols are prefixed with "WAIT_EVENT_".
	my $waiteventenumname = "WAIT_EVENT_$waiteventname";

	# Build the descriptions.  These are in camel-case.
	# LWLock and Lock classes do not need any modifications.
	my $waiteventdescription = '';
	if (   $waitclassname eq 'WaitEventLWLock'
		|| $waitclassname eq 'WaitEventLock')
	{
		$waiteventdescription = $waiteventname;
	}
	else
	{
		my @waiteventparts = split("_", $waiteventname);
		foreach my $waiteventpart (@waiteventparts)
		{
			$waiteventdescription .= substr($waiteventpart, 0, 1)
			  . lc(substr($waiteventpart, 1, length($waiteventpart)));
		}
	}

	# Store the event into the list for each class.
	my @waiteventlist =
	  [ $waiteventenumname, $waiteventdescription, $waitevendocsentence ];
	push(@{ $hashwe{$waitclassname} }, @waiteventlist);
}


# Generate the .c and .h files.
if ($gen_code)
{
	# Include PID in suffix in case parallel make runs this script
	# multiple times.
	my $htmp = "$output_path/wait_event_types.h.tmp$$";
	my $ctmp = "$output_path/pgstat_wait_event.c.tmp$$";
	my $wctmp = "$output_path/wait_event_funcs_data.c.tmp$$";
	open my $h, '>', $htmp or die "Could not open $htmp: $!";
	open my $c, '>', $ctmp or die "Could not open $ctmp: $!";
	open my $wc, '>', $wctmp or die "Could not open $wctmp: $!";

	my $header_comment =
	  '/*-------------------------------------------------------------------------
 *
 * %s
 *    Generated wait events infrastructure code
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *  ******************************
 *  *** DO NOT EDIT THIS FILE! ***
 *  ******************************
 *
 *  It has been GENERATED by src/backend/utils/activity/generate-wait_event_types.pl
 *
 *-------------------------------------------------------------------------
 */

';

	my $wait_event_class_shift = 0;
	my $temp_mask = $wait_event_class_mask;
	while (($temp_mask & 1) == 0 && $temp_mask != 0)
	{
		$wait_event_class_shift++;
		$temp_mask >>= 1;
	}

	printf $h $header_comment, 'wait_event_types.h';
	printf $h "#ifndef WAIT_EVENT_TYPES_H\n";
	printf $h "#define WAIT_EVENT_TYPES_H\n\n";
	printf $h "#define WAIT_EVENT_CLASS_MASK   0x%08X\n",
	  $wait_event_class_mask;
	printf $h "#define WAIT_EVENT_ID_MASK      0x%08X\n", $wait_event_id_mask;
	printf $h "#define WAIT_EVENT_CLASS_SHIFT  %d\n\n",
	  $wait_event_class_shift;
	printf $h "#include \"utils/wait_classes.h\"\n\n";

	printf $c $header_comment, 'pgstat_wait_event.c';

	printf $wc $header_comment, 'wait_event_funcs_data.c';

	# Generate the pgstat_wait_event.c and wait_event_types.h files
	# uc() is being used to force the comparison to be case-insensitive.
	foreach my $waitclass (sort { uc($a) cmp uc($b) } keys %hashwe)
	{
		# Don't generate the pgstat_wait_event.c and wait_event_types.h files
		# for types handled independently.
		next
		  if ( $waitclass eq 'WaitEventExtension'
			|| $waitclass eq 'WaitEventInjectionPoint'
			|| $waitclass eq 'WaitEventLWLock'
			|| $waitclass eq 'WaitEventLock');

		my $last = $waitclass;
		$last =~ s/^WaitEvent//;
		my $lastuc = uc $last;
		my $lastlc = lc $last;
		my $firstpass = 1;
		my $pg_wait_class;

		printf $c
		  "static const char *\npgstat_get_wait_$lastlc($waitclass w)\n{\n";
		printf $c "\tconst char *event_name = \"unknown wait event\";\n\n";
		printf $c "\tswitch (w)\n\t{\n";

		foreach my $wev (@{ $hashwe{$waitclass} })
		{
			if ($firstpass)
			{
				printf $h "typedef enum\n{\n";
				$pg_wait_class = "PG_WAIT_" . $lastuc;
				printf $h "\t%s = %s", $wev->[0], $pg_wait_class;
				$continue = ",\n";
			}
			else
			{
				printf $h "%s\t%s", $continue, $wev->[0];
				$continue = ",\n";
			}
			$firstpass = 0;

			printf $c "\t\t case %s:\n", $wev->[0];
			# Apply quotes to the wait event name string.
			printf $c "\t\t\t event_name = \"%s\";\n\t\t\t break;\n",
			  $wev->[1];
		}

		printf $h "\n} $waitclass;\n\n";

		printf $c
		  "\t\t\t /* no default case, so that compiler will warn */\n";
		printf $c "\t}\n\n";
		printf $c "\treturn event_name;\n";
		printf $c "}\n\n";
	}

	# Generate wait_event_funcs_data.c, building the contents of a static
	# C structure holding all the information about the wait events.
	# uc() is being used to force the comparison to be case-insensitive,
	# even though it is not required here.
	foreach my $waitclass (sort { uc($a) cmp uc($b) } keys %hashwe)
	{
		my $last = $waitclass;
		$last =~ s/^WaitEvent//;

		foreach my $wev (@{ $hashwe{$waitclass} })
		{
			my $new_desc = substr $wev->[2], 1, -2;
			# Escape single quotes.
			$new_desc =~ s/'/\\'/g;

			# Replace the "quote" markups by real ones.
			$new_desc =~ s/<quote>(.*?)<\/quote>/\\"$1\\"/g;

			# Remove SGML markups.
			$new_desc =~ s/<.*?>(.*?)<.*?>/$1/g;

			# Tweak contents about links <xref linkend="text"/>
			# on GUCs,
			while (my ($capture) =
				$new_desc =~ m/<xref linkend="guc-(.*?)"\/>/g)
			{
				$capture =~ s/-/_/g;
				$new_desc =~ s/<xref linkend="guc-.*?"\/>/$capture/g;
			}
			# Then remove any reference to
			# "see <xref linkend="text"/>".
			$new_desc =~ s/; see.*$//;

			# Build one element of the C structure holding the
			# wait event info, as of (type, name, description).
			printf $wc "\t{\"%s\", \"%s\", \"%s\"},\n", $last, $wev->[1],
			  $new_desc;
		}
	}

	printf $h "

/* To represent wait_event_info as integers */
typedef struct DecodedWaitInfo
{
        int classId;
        int eventId;
} DecodedWaitInfo;

/* To extract classId and eventId as integers from wait_event_info */
#define WAIT_EVENT_INFO_DECODE(d, i) \\
    d.classId = ((i) & WAIT_EVENT_CLASS_MASK) / (WAIT_EVENT_CLASS_MASK & (-WAIT_EVENT_CLASS_MASK)), \\
    d.eventId = (i) & WAIT_EVENT_ID_MASK

/* To encode wait_event_info from classId and eventId as integers */
#define ENCODE_WAIT_EVENT_INFO(classId, eventId) \\
	(((classId) << WAIT_EVENT_CLASS_SHIFT) | ((eventId) & WAIT_EVENT_ID_MASK))

/* To map wait event classes into the WaitClassTable */
typedef struct
{
	const int classId;
	const int numberOfEvents;
	const int offSet;
	const char *className;
	const char *const *eventNames;
} WaitClassTableEntry;

extern WaitClassTableEntry WaitClassTable[];\n\n";

	printf $c "
/*
 * Lookup table that is used by the wait events statistics.
 * Indexed by classId (derived from the PG_WAIT_* constants), handle gaps
 * in the class ID numbering and provide metadata for wait events.
 */
WaitClassTableEntry WaitClassTable[] = {\n";

	parse_wait_classes_header();
	my $next_index = 0;
	my $class_divisor = $wait_event_class_mask & (-$wait_event_class_mask);

	foreach my $waitclass (
		sort {
			my $macro_a = waitclass_to_macro($a);
			my $macro_b = waitclass_to_macro($b);
			hex($waitclass_values{$macro_a}) <=>
			  hex($waitclass_values{$macro_b})
		} keys %hashwe)
	{
		my $event_names_array;
		my $array_size;
		my $last = $waitclass;
		$last =~ s/^WaitEvent//;

		$nb_waitclass_table_entries++;

		# The LWLocks need to be handled differently than the other classes when
		# building the WaitClassTable. We need to take care of the prefedined
		# LWLocks as well as the additional ones.
		if ($waitclass eq 'WaitEventLWLock')
		{
			# Parse lwlocklist.h to get LWLock definitions
			open my $lwlocklist, '<', $lwlocklist_file
			  or die "Could not open $lwlocklist_file: $!";

			my %predefined_lwlock_indices;
			my $max_lwlock_index = -1;

			while (<$lwlocklist>)
			{
				if (/^PG_LWLOCK\((\d+),\s+(\w+)\)$/)
				{
					my ($lockidx, $lockname) = ($1, $2);
					$predefined_lwlock_indices{$lockname} = $lockidx;
					$max_lwlock_index = $lockidx
					  if $lockidx > $max_lwlock_index;
				}
			}

			close $lwlocklist;

			# Iterates through wait_event_names.txt order
			my @event_names_sparse;
			my $next_additional_index = $max_lwlock_index + 1;

			foreach my $wev (@{ $hashwe{$waitclass} })
			{
				my $lockname = $wev->[1];

				if (exists $predefined_lwlock_indices{$lockname})
				{
					# This is a predefined one, place it at its specific index
					my $index = $predefined_lwlock_indices{$lockname};
					$event_names_sparse[$index] = "\"$lockname\"";
				}
				else
				{
					# This is an additional one, append it after predefined ones
					$event_names_sparse[$next_additional_index] =
					  "\"$lockname\"";
					$next_additional_index++;
				}
			}

			# Fill gaps with NULL for missing predefined locks
			for my $i (0 .. $max_lwlock_index)
			{
				$event_names_sparse[$i] = "NULL"
				  unless defined $event_names_sparse[$i];
			}

			# Build the array literal
			$event_names_array = "(const char *const []){"
			  . join(", ", @event_names_sparse) . "}";
			$array_size = scalar(@event_names_sparse);
		}
		else
		{
			# Construct a simple string array literal for this class
			$event_names_array = "(const char *const []){";

			# For each wait event in this class, add its name to the array
			foreach my $wev (@{ $hashwe{$waitclass} })
			{
				$event_names_array .= "\"$wev->[1]\", ";
			}

			$event_names_array .= "}";
			$array_size = scalar(@{ $hashwe{$waitclass} });
		}

		my $lastuc = uc $last;
		my $pg_wait_class = "PG_WAIT_" . $lastuc;

		my $index = hex($waitclass_values{$pg_wait_class}) / $class_divisor;

		# Fill any holes with {0, 0, 0, NULL, NULL}
		while ($next_index < $index)
		{
			printf $c "{0, 0, 0, NULL, NULL},\n";
			$next_index++;
			$nb_waitclass_table_entries++;
		}

		my $offset = $nb_wait_events_with_null;
		$nb_wait_events_with_null += $array_size;

		# Generate the entry
		printf $c "{$pg_wait_class, $array_size, $offset, \"%s\", %s},\n",
		  $last, $event_names_array;

		$next_index = $index + 1;
	}

	printf $c "};\n\n";

	printf $h "#define NB_WAITCLASSTABLE_SIZE $nb_wait_events_with_null\n";
	printf $h
	  "#define NB_WAITCLASSTABLE_ENTRIES $nb_waitclass_table_entries\n\n";
	printf $h
	  "StaticAssertDecl(NB_WAITCLASSTABLE_SIZE > 0, \"Wait class table must have entries\");\n";
	printf $h
	  "StaticAssertDecl(NB_WAITCLASSTABLE_ENTRIES > 0, \"Must have at least one wait class\");\n";
	printf $h "#endif                          /* WAIT_EVENT_TYPES_H */\n";
	close $h;
	close $c;
	close $wc;

	rename($htmp, "$output_path/wait_event_types.h")
	  || die "rename: $htmp to $output_path/wait_event_types.h: $!";
	rename($ctmp, "$output_path/pgstat_wait_event.c")
	  || die "rename: $ctmp to $output_path/pgstat_wait_event.c: $!";
	rename($wctmp, "$output_path/wait_event_funcs_data.c")
	  || die "rename: $wctmp to $output_path/wait_event_funcs_data.c: $!";
}
# Generate the .sgml file.
elsif ($gen_docs)
{
	# Include PID in suffix in case parallel make runs this multiple times.
	my $stmp = "$output_path/wait_event_names.s.tmp$$";
	open my $s, '>', $stmp or die "Could not open $stmp: $!";

	# uc() is being used to force the comparison to be case-insensitive.
	foreach my $waitclass (sort { uc($a) cmp uc($b) } keys %hashwe)
	{
		my $last = $waitclass;
		$last =~ s/^WaitEvent//;
		my $lastlc = lc $last;

		printf $s "  <table id=\"wait-event-%s-table\">\n", $lastlc;
		printf $s
		  "   <title>Wait Events of Type <literal>%s</literal></title>\n",
		  ucfirst($lastlc);
		printf $s "   <tgroup cols=\"2\">\n";
		printf $s "    <thead>\n";
		printf $s "     <row>\n";
		printf $s
		  "      <entry><literal>$last</literal> Wait Event</entry>\n";
		printf $s "      <entry>Description</entry>\n";
		printf $s "     </row>\n";
		printf $s "    </thead>\n\n";
		printf $s "    <tbody>\n";

		foreach my $wev (@{ $hashwe{$waitclass} })
		{
			printf $s "     <row>\n";
			printf $s "      <entry><literal>%s</literal></entry>\n",
			  $wev->[1];
			printf $s "      <entry>%s</entry>\n", substr $wev->[2], 1, -1;
			printf $s "     </row>\n";
		}

		printf $s "    </tbody>\n";
		printf $s "   </tgroup>\n";
		printf $s "  </table>\n\n";
	}

	close $s;

	rename($stmp, "$output_path/wait_event_types.sgml")
	  || die "rename: $stmp to $output_path/wait_event_types.sgml: $!";
}

close $wait_event_names;

sub usage
{
	die <<EOM;
Usage: perl  [--output <path>] [--code ] [ --sgml ] input_file

Options:
    --outdir         Output directory (default '.')
    --code           Generate C and header files.
    --sgml           Generate wait_event_types.sgml.

generate-wait_event_types.pl generates the SGML documentation and code
related to wait events.  This should use wait_event_names.txt in input, or
an input file with a compatible format.

Report bugs to <pgsql-bugs\@lists.postgresql.org>.
EOM
}
