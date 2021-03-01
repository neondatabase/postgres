/*-------------------------------------------------------------------------
 *
 * zenith_slicedice_main.c - decode and redistribute WAL per datafile
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/zenith_slicedice/zenith_slicedice_main.c
 *-------------------------------------------------------------------------
 */

#define FRONTEND 1
#include "postgres.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/transam.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "common/fe_memutils.h"
#include "common/logging.h"
#include "getopt_long.h"
#include "zenith_slicedice.h"

#include "zenith_s3/s3_ops.h"

// FIXME: read this from control file
static const int wal_segment_size = DEFAULT_XLOG_SEG_SIZE;

static const char *progname;

static TimeLineID timeline = 1; /* FIXME */
static XLogRecPtr slice_startptr;
static XLogRecPtr slice_endptr;
static bool	endptr_reached = false;

#define fatal_error(...) do { pg_log_fatal(__VA_ARGS__); exit(EXIT_FAILURE); } while(0)

static void find_slice_wal_points(void);

static XLogSegNo current_segno;
static StringInfo current_segment;

CURL *curl_handle;

/* pg_waldump's XLogReaderRoutine->page_read callback */
static int
WALDumpReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				XLogRecPtr targetPtr, char *readBuff)
{
	int			count = XLOG_BLCKSZ;
	uint32		startoff;

	if (slice_endptr != InvalidXLogRecPtr)
	{
		if (targetPagePtr + XLOG_BLCKSZ <= slice_endptr)
			count = XLOG_BLCKSZ;
		else if (targetPagePtr + reqLen <= slice_endptr)
			count = slice_endptr - targetPagePtr;
		else
		{
			endptr_reached = true;
			return -1;
		}
	}

	if (current_segment == NULL ||
		!XLByteInSeg(targetPagePtr, current_segno, state->segcxt.ws_segsize))
	{
		char		fname[MAXPGPATH];
		char		s3path[MAXPGPATH];
		XLogSegNo	nextSegNo;

		if (current_segment)
		{
			pfree(current_segment->data);
			pfree(current_segment);
			current_segment = NULL;
		}

		XLByteToSeg(targetPagePtr, nextSegNo, state->segcxt.ws_segsize);

		XLogFileName(fname, timeline, nextSegNo, state->segcxt.ws_segsize);

		snprintf(s3path, sizeof(s3path), "walarchive/%s", fname);

		current_segment = fetch_s3_file_memory(curl_handle, s3path);
		current_segno = nextSegNo;
	}

	startoff = XLogSegmentOffset(targetPagePtr, state->segcxt.ws_segsize);

	memcpy(readBuff, current_segment->data + startoff, count);

	return count;
}

static void
usage(void)
{
	printf(_("%s decodes and slices and dices PostgreSQL write-ahead logs per datafile.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]... [STARTSEG [ENDSEG]]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  -q, --quiet            do not print any output, except for errors\n"));
	printf(_("  -t, --timeline=TLI     timeline from which to read log records\n"
			 "                         (default: 1 or the value used in STARTSEG)\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -z, --stats[=record]   show statistics instead of records\n"
			 "                         (optionally, show per-record statistics)\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	/* printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT); */
	/* printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL); */
}

int
main(int argc, char **argv)
{
	XLogReaderState *xlogreader_state;
	XLogRecord *record;
	XLogRecPtr	first_record;
	char	   *waldir = NULL;
	char	   *errormsg;
	char		tmpdir_template[] = "zenith_slicedice_tmpXXXXXX";
	char	   *tmpdir;

	static struct option long_options[] = {
		{"help", no_argument, NULL, '?'},
		{"timeline", required_argument, NULL, 't'},
		{"version", no_argument, NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	int			option;
	int			optindex = 0;

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("zenith_slicedice"));
	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("zenith_slicedice (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((option = getopt_long(argc, argv, "p:t:",
								 long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case 'p':
				waldir = pg_strdup(optarg);
				break;
			case 't':
				if (sscanf(optarg, "%d", &timeline) != 1)
				{
					pg_log_error("could not parse timeline \"%s\"", optarg);
					goto bad_argument;
				}
				break;
			default:
				goto bad_argument;
		}
	}

	if ((optind + 2) < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind + 2]);
		goto bad_argument;
	}

	/* done with argument parsing, do the actual work */

	curl_handle = curl_easy_init();
	if (!curl_handle)
		pg_fatal("could not init libcurl");

	/*
	 * 1. Establish beginning and end point of the operation.
	 *
	 * 2. Start reading WAL, starting from the Point.
	 *
	 * 3. Write each WAL record to per-relation WAL files, plus base WAL for non-rel records
	 *
	 * 4. Upload everything to S3. Base WAL last.
	 */

	/* Switch to a temporary dir first */
	/* FIXME: the temp directory is left behind */
	tmpdir = mkdtemp(tmpdir_template);
	chdir(tmpdir);

	find_slice_wal_points();

	if (slice_startptr == slice_endptr)
		pg_fatal("nothing to do, exiting");

	/* we have everything we need, start reading */
	xlogreader_state =
		XLogReaderAllocate(wal_segment_size, waldir,
						   XL_ROUTINE(.page_read = WALDumpReadPage,
									  .segment_open = NULL,
									  .segment_close = NULL),
						   NULL);
	if (!xlogreader_state)
		fatal_error("out of memory");

	/* first find a valid recptr to start from */
	first_record = XLogFindNextRecord(xlogreader_state, slice_startptr);

	if (first_record == InvalidXLogRecPtr)
		fatal_error("could not find a valid record after %X/%X",
					(uint32) (slice_startptr >> 32),
					(uint32) slice_startptr);

	/*
	 * Display a message that we're skipping data if `from` wasn't a pointer
	 * to the start of a record and also wasn't a pointer to the beginning of
	 * a segment (e.g. we were used in file mode).
	 */
	if (first_record != slice_startptr &&
		XLogSegmentOffset(slice_startptr, wal_segment_size) != 0)
		printf(ngettext("first record is after %X/%X, at %X/%X, skipping over %u byte\n",
						"first record is after %X/%X, at %X/%X, skipping over %u bytes\n",
						(first_record - slice_startptr)),
			   (uint32) (slice_startptr >> 32), (uint32) slice_startptr,
			   (uint32) (first_record >> 32), (uint32) first_record,
			   (uint32) (first_record - slice_startptr));

	redist_init(slice_startptr, slice_endptr);

	for (;;)
	{
		/* try to read the next record */
		record = XLogReadRecord(xlogreader_state, &errormsg);
		if (!record)
		{
			if (endptr_reached)
				break;
			else
			{
				fatal_error("error in WAL record at %X/%X: %s",
							(uint32) (xlogreader_state->ReadRecPtr >> 32),
							(uint32) xlogreader_state->ReadRecPtr,
							errormsg);
			}
		}

		/* perform any per-record work */
		handle_record(xlogreader_state);
	}

	XLogReaderFree(xlogreader_state);

	if (current_segment)
	{
		pfree(current_segment->data);
		pfree(current_segment);
		current_segment = NULL;
	}

	/* Finally, upload the generated sliced WAL files */
	upload_slice_wal_files();

	curl_easy_cleanup(curl_handle);

	return EXIT_SUCCESS;

bad_argument:
	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
	return EXIT_FAILURE;
}

static bool
parse_nonreldata_filename(const char *path, XLogRecPtr *startptr)
{
	static const char pattern[] = "nonreldata/nonrel_XXXXXXXXXXXXXXXX.tar";
	uint32		walpos_hi;
	uint32		walpos_lo;

	if (strlen(path) != strlen(pattern))
		return false;

	if (sscanf(path, "nonreldata/nonrel_%08X%08X.tar", &walpos_hi, &walpos_lo) != 2)
		return false;

	*startptr = (uint64) walpos_hi << 32 | walpos_lo;
	return true;
}

static bool
parse_nonrelwal_filename(const char *path, XLogRecPtr *startptr, XLogRecPtr *endptr)
{
	static const char pattern[] = "nonreldata/nonrel_XXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX";
	uint32		startptr_hi;
	uint32		startptr_lo;
	uint32		endptr_hi;
	uint32		endptr_lo;

	if (strlen(path) != strlen(pattern))
		return false;

	if (sscanf(path, "nonreldata/nonrel_%08X%08X-%08X%08X",
			   &startptr_hi, &startptr_lo,
			   &endptr_hi, &endptr_lo) != 4)
		return false;

	*startptr = (uint64) startptr_hi << 32 | startptr_lo;
	*endptr = (uint64) endptr_hi << 32 | endptr_lo;
	return true;
}

/*
 * Parse a sequential WAL filename.
 *
 * Same file naming as in upstream.
 */
static bool
parse_wal_filename(const char *path, XLogRecPtr *startptr, XLogRecPtr *endptr)
{
	TimeLineID	tli;
	uint64		logsegno;

	if (strlen(path) < strlen("walarchive/"))
		return false;
	if (!IsXLogFileName(path + strlen("walarchive/")))
		return false;

	// FIXME: TLI is ignored
	XLogFromFileName(path + strlen("walarchive/"), &tli, &logsegno, wal_segment_size);

	XLogSegNoOffsetToRecPtr(logsegno, 0, wal_segment_size, *startptr);
	XLogSegNoOffsetToRecPtr(logsegno + 1, 0, wal_segment_size, *endptr);
	return true;
}

/*
 * Establish beginning and end point of the operation.
 */
static void
find_slice_wal_points(void)
{
	ListObjectsResult *files;
	XLogRecPtr		latest_tarball_ptr = InvalidXLogRecPtr;
	XLogRecPtr		startptr = InvalidXLogRecPtr;
	XLogRecPtr		endptr = InvalidXLogRecPtr;

	files = s3_ListObjects(curl_handle, "");
	fprintf(stderr, "number of files in bucket: %d\n", files->numfiles);

	/*
	 * List non-rel WAL files in bucket/nonreldata/
	 *
	 * Find the end of WAL. That's our ending point.
	 */
	for (int i = 0; i < files->numfiles; i++)
	{
		XLogRecPtr		ptr;
		XLogRecPtr		this_startptr;
		XLogRecPtr		this_endptr;

		if (parse_nonreldata_filename(files->filenames[i], &ptr))
		{
			if (ptr > latest_tarball_ptr)
				latest_tarball_ptr = ptr;
		}
		else if (parse_nonrelwal_filename(files->filenames[i], &this_startptr, &this_endptr))
		{
			fprintf(stderr, "non-rel WAL: %s from %X/%X to %X/%X\n", files->filenames[i],
					(uint32) (this_startptr >> 32), (uint32) this_startptr,
					(uint32) (this_endptr >> 32), (uint32) this_endptr);

			/* FIXME: check that we have all the WAL in between low and high point */
			if (this_endptr > startptr)
				startptr = this_endptr;
		}
	}

	if (startptr == InvalidXLogRecPtr && latest_tarball_ptr != InvalidXLogRecPtr)
	{
		fprintf(stderr, "No sliced WAL. Starting from last base tarball\n");
		startptr = latest_tarball_ptr;
	}

	/*
	 * List files in bucket/walarchive/
	 *
	 * Find the end of WAL. That's our ending point.
	 */
	endptr = startptr;
	for (int i = 0; i < files->numfiles; i++)
	{
		XLogRecPtr		this_startptr;
		XLogRecPtr		this_endptr;

		if (parse_wal_filename(files->filenames[i], &this_startptr, &this_endptr))
		{
			fprintf(stderr, "WAL: %s from %X/%X to %X/%X\n", files->filenames[i],
					(uint32) (this_startptr >> 32), (uint32) this_startptr,
					(uint32) (this_endptr >> 32), (uint32) this_endptr);

			/* FIXME: check that we have all the WAL in between low and high point */
			if (this_endptr > endptr)
				endptr = this_endptr;
		}
	}

	printf("Slicing WAL between %X/%X and %X/%X\n",
		   (uint32) (startptr >> 32), (uint32) startptr,
		   (uint32) (endptr >> 32), (uint32) endptr);

	slice_startptr = startptr;
	slice_endptr = endptr;
}
