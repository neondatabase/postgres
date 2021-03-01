/*-------------------------------------------------------------------------
 *
 * zenith_restore.c - decode and redistribute WAL per datafile
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/zenith_restore/zenith_restore.c
 *-------------------------------------------------------------------------
 */

#define FRONTEND 1
#include "postgres.h"

#include <dirent.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "common/controldata_utils.h"
#include "common/fe_memutils.h"
#include "common/file_perm.h"
#include "common/logging.h"
#include "getopt_long.h"
#include "zenith_restore.h"
#include "zenith_s3/s3_ops.h"

static const char *progname;

typedef struct XLogDumpPrivate
{
	TimeLineID	timeline;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
} XLogDumpPrivate;

typedef struct zenith_restore_config
{
	/* display options */
	bool		quiet;
	int			stop_after_records;
	int			already_displayed_records;

} zenith_restore_config;

#define fatal_error(...) do { pg_log_fatal(__VA_ARGS__); exit(EXIT_FAILURE); } while(0)

static CURL *curl;

static bool parse_nonreldata_filename(const char *path, XLogRecPtr *startptr);
static bool parse_nonrelwal_filename(const char *path, XLogRecPtr *startptr, XLogRecPtr *endptr);
static bool parse_reldata_filename(const char *path, char **basefname);
static void split_path(const char *path, char **dir, char **fname);

static void
usage(void)
{
	printf(_("%s restores a (lazy) backup from cloud storage.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  --archive-wal-path=PATH   restore a single WAL file\n"));
	printf(_("  --archive-wal-fname=PATH  restore a single WAL file\n"));
	printf(_("  -D, --pgdata=DIRECTORY receive base backup into directory\n"));
	printf(_("  -e, --end=RECPTR       stop reading at WAL location RECPTR\n"));
	printf(_("  -q, --quiet            do not print any output, except for errors\n"));
	printf(_("  -t, --timeline=TLI     timeline from which to read log records\n"
			 "                         (default: 1 or the value used in STARTSEG)\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	/* printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT); */
	/* printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL); */
}

static void
warn_on_mount_point(int error)
{
	if (error == 2)
		fprintf(stderr,
				_("It contains a dot-prefixed/invisible file, perhaps due to it being a mount point.\n"));
	else if (error == 3)
		fprintf(stderr,
				_("It contains a lost+found directory, perhaps due to it being a mount point.\n"));

	fprintf(stderr,
			_("Using a mount point directly as the data directory is not recommended.\n"
			  "Create a subdirectory under the mount point.\n"));
}

static void
create_data_directory(char *pg_data)
{
	int			ret;

	switch ((ret = pg_check_dir(pg_data)))
	{
		case 0:
			/* PGDATA not there, must create it */
			printf(_("creating directory %s ... "),
				   pg_data);
			fflush(stdout);

			if (pg_mkdir_p(pg_data, pg_dir_create_mode) != 0)
			{
				pg_log_error("could not create directory \"%s\": %m", pg_data);
				exit(1);
			}
			/* all seems well */
			break;

		case 1:
			/* Present but empty, fix permissions and use it */
			printf(_("fixing permissions on existing directory %s ... "),
				   pg_data);
			fflush(stdout);

			if (chmod(pg_data, pg_dir_create_mode) != 0)
			{
				pg_log_error("could not change permissions of directory \"%s\": %m",
							 pg_data);
				exit(1);
			}
			break;

		case 2:
		case 3:
		case 4:
			/* Present and not empty */
			pg_log_error("directory \"%s\" exists but is not empty", pg_data);
			if (ret != 4)
				warn_on_mount_point(ret);
			else
				fprintf(stderr,
						_("If you want to create a new database system, either remove or empty\n"
						  "the directory \"%s\" or run %s\n"
						  "with an argument other than \"%s\".\n"),
						pg_data, progname, pg_data);
			exit(1);			/* no further message needed */

		default:
			/* Trouble accessing directory */
			pg_log_error("could not access directory \"%s\": %m", pg_data);
			exit(1);
	}
	printf(_("ok\n"));
}

static void
create_lazy_file(const char *fname)
{
	FILE		*fp;

	fp = fopen(psprintf("%s_lazy", fname), "wb");

	/* TODO: write some contents. The checksum from the manifest, perhaps? */

	fclose(fp);
}

int
main(int argc, char **argv)
{
	uint32		xlogid;
	uint32		xrecoff;
	XLogDumpPrivate private;
	zenith_restore_config config;
	char	   *pg_data;
	char	   *archive_wal_path = NULL;
	char	   *archive_wal_fname = NULL;
	ListObjectsResult *files;
	XLogRecPtr	latest_tarball_ptr = InvalidXLogRecPtr;
	char	   *latest_tarball_name;
	int			numlazyfiles;
	XLogRecPtr	end_of_nonrelwal = InvalidXLogRecPtr;
	int			WalSegSz;

	static struct option long_options[] = {
		{"pgdata", required_argument, NULL, 'D'},
		{"end", required_argument, NULL, 'e'},
		{"help", no_argument, NULL, '?'},
		{"quiet", no_argument, NULL, 'q'},
		{"timeline", required_argument, NULL, 't'},
		{"version", no_argument, NULL, 'V'},
		{"archive-wal-path", required_argument, NULL, 1},
		{"archive-wal-fname", required_argument, NULL, 2},
		{NULL, 0, NULL, 0}
	};

	int			option;
	int			optindex = 0;

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("zenith_restore"));
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
			puts("zenith_restore (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	memset(&private, 0, sizeof(XLogDumpPrivate));
	memset(&config, 0, sizeof(zenith_restore_config));

	private.timeline = 1;
	private.startptr = InvalidXLogRecPtr;
	private.endptr = InvalidXLogRecPtr;

	config.quiet = false;
	config.stop_after_records = -1;
	config.already_displayed_records = 0;

	if (argc <= 1)
	{
		pg_log_error("no arguments specified");
		goto bad_argument;
	}

	while ((option = getopt_long(argc, argv, "D:e:qt:",
								 long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case 'D':
				pg_data = pg_strdup(optarg);
				break;
			case 'e':
				if (sscanf(optarg, "%X/%X", &xlogid, &xrecoff) != 2)
				{
					pg_log_error("could not parse end WAL location \"%s\"",
								 optarg);
					goto bad_argument;
				}
				private.endptr = (uint64) xlogid << 32 | xrecoff;
				break;
			case 'q':
				config.quiet = true;
				break;
			case 't':
				if (sscanf(optarg, "%d", &private.timeline) != 1)
				{
					pg_log_error("could not parse timeline \"%s\"", optarg);
					goto bad_argument;
				}
				break;
			case 1:
				archive_wal_path = pg_strdup(optarg);
				break;
			case 2:
				archive_wal_fname = pg_strdup(optarg);
				break;
			default:
				goto bad_argument;
		}
	}

	/*
	 * Required arguments
	 */
	if (pg_data == NULL && archive_wal_path == NULL)
	{
		pg_log_error("no target directory (-D) or --arhive-wal-path specified");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if ((archive_wal_path && !archive_wal_fname) ||
		(!archive_wal_path && archive_wal_fname))
	{
		pg_log_error("--archive-wal-path and --archive-wal-fname must be used together");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}
	
	if (pg_data && archive_wal_path)
	{
		pg_log_error("-D and --archive-wal-path are mutually exclusive");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (optind < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind]);
		goto bad_argument;
	}

#if 0
	if (XLogRecPtrIsInvalid(private.endptr))
	{
		pg_log_error("no end WAL location given");
		goto bad_argument;
	}
#endif
	
	/* done with argument parsing, do the actual work */

	curl = curl_easy_init();
	if (!curl)
		pg_fatal("could not init libcurl");

	/* In this mode, used as an restore_command */
	if (archive_wal_path)
	{
		char		s3pathbuf[MAXPGPATH];

		snprintf(s3pathbuf, sizeof(s3pathbuf), "walarchive/%s",
				 archive_wal_fname);

		fetch_s3_file(curl, s3pathbuf, archive_wal_path);

		fprintf(stderr, "restored %s from WAL archive\n", archive_wal_fname);

		curl_easy_cleanup(curl);
		return EXIT_SUCCESS;
	}

	
	/* TODO: check existence of the directory first, before fetching and creating.
	 * nicer that way
	 */
	create_data_directory(pg_data);
	if (chdir(pg_data) < 0)
		fatal_error("could not chdir into \"%s\": %m", pg_data);

	/* List files in bucket/base. Pick the last base backup before chosen WALPOS */
	/* Fetch all "base WAL" after the base backup position */

	/* List files in bucket/relationdata. For each relation, fetch the last base image before WALPOS,
	 * and all WAL between base image position and WALPOS.
	 */

	/* Fetch list of files */
	files = s3_ListObjects(curl, "");
	fprintf(stderr, "number of files in bucket: %d\n", files->numfiles);

	/* Find the latest base tarball */
	latest_tarball_ptr = InvalidXLogRecPtr;
	latest_tarball_name = NULL;
	for (int i = 0; i < files->numfiles; i++)
	{
		XLogRecPtr ptr;

		if (parse_nonreldata_filename(files->filenames[i], &ptr))
		{
			fprintf(stderr, "tarball: %s at %X/%X\n", files->filenames[i],
					(uint32) (ptr >> 32), (uint32) ptr);

			if ((ptr < private.endptr || private.endptr == InvalidXLogRecPtr) &&
				latest_tarball_ptr < ptr)
			{
				latest_tarball_ptr = ptr;
				latest_tarball_name = files->filenames[i];
			}
		}		
	}

	if (latest_tarball_ptr == InvalidXLogRecPtr)
		pg_fatal("could not find suitable base tarball");
	
	/* Fetch and unpack the tarball */
	fetch_s3_file(curl, psprintf("%s", latest_tarball_name), "latest_tarball.tar");
	fprintf(stderr, "extracting base tarball...");
	fflush(stderr);
	system("tar xf latest_tarball.tar");
	fprintf(stderr, "done\n");

	/* Fetch all nonrel WAL files needed */
	if (pg_mkdir_p(pstrdup("pg_wal/nonrelwal"), pg_dir_create_mode) != 0)
		pg_fatal("could not create directory \"%s\": %m", "pg_wal/nonrelwal");

	for (int i = 0; i < files->numfiles; i++)
	{
		const char *this_path = files->filenames[i];
		XLogRecPtr this_startptr;
		XLogRecPtr this_endptr;
		char *this_dir;
		char *this_fname;

		if (parse_nonrelwal_filename(this_path, &this_startptr, &this_endptr))
		{
			if ((this_startptr <= private.endptr || private.endptr == InvalidXLogRecPtr) &&
				this_endptr > latest_tarball_ptr)
			{
				fprintf(stderr, "non-rel WAL: %s from %X/%X to %X/%X\n", this_path,
						(uint32) (this_startptr >> 32), (uint32) this_startptr,
						(uint32) (this_endptr >> 32), (uint32) this_endptr);

				split_path(this_path, &this_dir, &this_fname);

				fetch_s3_file(curl, this_path, psprintf("pg_wal/nonrelwal/%s", this_fname));
			}

			if (this_endptr > end_of_nonrelwal)
				end_of_nonrelwal = this_endptr;
		}
	}

	if (private.endptr != InvalidXLogRecPtr && end_of_nonrelwal > private.endptr)
		end_of_nonrelwal = private.endptr;

	/* FIXME: check that we have all the WAL in between low and high point */

	/* FIXME: Set minRecoveryPoint to 'endptr' in the control file */

	/* Create "lazy" files for all relfiles */
	numlazyfiles = 0;
	for (int i = 0; i < files->numfiles; i++)
	{
		char	   *basefname;

		if (parse_reldata_filename(files->filenames[i], &basefname))
		{
			create_lazy_file(basefname);
			numlazyfiles++;
		}
	}
	fprintf(stderr, "created lazy files as placeholders for %d relation files\n", numlazyfiles);


	/*
	 * Create a backup label file
	 */
	{
		ControlFileData *controlfile;
		bool		crc_ok;
		time_t		stamp_time;
		char		strfbuf[128];
		StringInfo	labelfile;
		XLogRecPtr	startpoint;
		TimeLineID	starttli;
		XLogRecPtr checkpointloc;
		char		xlogfilename[MAXFNAMELEN];
		XLogSegNo	segno;
		FILE	   *fp;

		controlfile = get_controlfile(".", &crc_ok);
		if (!crc_ok)
		{
			fprintf(stderr, _("Calculated pg_control file checksum does not match value stored in file.\n"));
			return EXIT_FAILURE;
		}

		if (controlfile->state != DB_SHUTDOWNED)
		{
			fprintf(stderr, _("cluster must be cleanly shut down.\n"));
			return EXIT_FAILURE;
		}

		WalSegSz = controlfile->xlog_seg_size;
		startpoint = controlfile->checkPointCopy.redo;
		starttli = controlfile->checkPointCopy.PrevTimeLineID;
		checkpointloc = controlfile->checkPoint;

		/*
		 * Calculate name of the WAL file containing the latest checkpoint's REDO
		 * start point.
		 */
		XLByteToSeg(controlfile->checkPointCopy.redo, segno, WalSegSz);
		XLogFileName(xlogfilename, controlfile->checkPointCopy.ThisTimeLineID,
					 segno, WalSegSz);

		/* Use the log timezone here, not the session timezone */
		stamp_time = (time_t) time(NULL);
		strftime(strfbuf, sizeof(strfbuf),
					"%Y-%m-%d %H:%M:%S %Z",
				 localtime(&stamp_time));

		labelfile = makeStringInfo();
		appendStringInfo(labelfile, "START WAL LOCATION: %X/%X (file %s)\n",
						 (uint32) (startpoint >> 32), (uint32) startpoint, xlogfilename);
		appendStringInfo(labelfile, "CHECKPOINT LOCATION: %X/%X\n",
						 (uint32) (checkpointloc >> 32), (uint32) checkpointloc);
		appendStringInfo(labelfile, "BACKUP METHOD: zenith\n");
		/* FIXME: the backend doesn't recognize 'zenith', but it does what we want:
		 * it's not 'standby', as that would make the startup process complain when
		 * the control file is in SHUTDOWNED state. And it's not 'streamed', which
		 * would enforce waiting for end-of-backup WAL record, which we haven't
		 * generated.
		 */
		appendStringInfo(labelfile, "BACKUP FROM: zenith\n");
		appendStringInfo(labelfile, "START TIME: %s\n", strfbuf);
		appendStringInfo(labelfile, "LABEL: zenith\n");
		appendStringInfo(labelfile, "START TIMELINE: %u\n", starttli);

		if (private.endptr == InvalidXLogRecPtr)
		{
			fp = fopen(BACKUP_LABEL_FILE, "w");

			if (!fp)
				pg_fatal("could not create file \"%s\": %m",
						 BACKUP_LABEL_FILE);
			if (fwrite(labelfile->data, labelfile->len, 1, fp) != 1 ||
				fflush(fp) != 0 ||
#if 0
				pg_fsync(fileno(fp)) != 0 ||
#endif
				ferror(fp) ||
				fclose(fp))
				pg_fatal("could not write file \"%s\": %m",
						 BACKUP_LABEL_FILE);
		}

		pfree(labelfile->data);
		pfree(labelfile);
	}

	/* Fetch all raw WAL segments after the point of slicing */

	for (int i = 0; i < files->numfiles; i++)
	{
		const char *path = files->filenames[i];
		uint32		tli;
		uint32		log;
		uint32		seg;

		if (strlen(path) == strlen("walarchive/000000001111111122222222") &&
			sscanf(path, "walarchive/%08X%08X%08X",
				   &tli, &log, &seg) == 3)
		{
			uint64		logSegNo;
			XLogRecPtr	this_endptr;

			logSegNo = (uint64) log * XLogSegmentsPerXLogId(WalSegSz) + seg;
			this_endptr = (logSegNo + 1) * WalSegSz;

			if (this_endptr >= end_of_nonrelwal)
			{
				char		localpath[MAXPGPATH];

				snprintf(localpath, sizeof(localpath),
						 "pg_wal/%s",
						 path + strlen("walarchive/"));

				fetch_s3_file(curl, path, localpath);
			}
		}
	}

	/* create standby.signal to turn this into a standby server */
	/* FIXME: the end-of-recovery checkpoint fails with the special non-rel
	 * WAL format, so the server won't start up except as a standby */
	if (private.endptr != InvalidXLogRecPtr)
	{
		system("touch standby.signal");
		system("echo \"hot_standby=on\" >> postgresql.conf");
		system(psprintf("echo \"recovery_target_lsn='%X/%X'\" >> postgresql.conf",
						(uint32) (private.endptr >> 32), (uint32) private.endptr));
	}

	curl_easy_cleanup(curl);
	return EXIT_SUCCESS;

bad_argument:
	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
	return EXIT_FAILURE;
}

/*
 * Split a pathname as dirname(1) and basename(1) would.
 *
 * XXX this probably doesn't do very well on Windows.  We probably need to
 * apply canonicalize_path(), at the very least.
 */
static void
split_path(const char *path, char **dir, char **fname)
{
	char	   *sep;

	/* split filepath into directory & filename */
	sep = strrchr(path, '/');

	/* directory path */
	if (sep != NULL)
	{
		*dir = pnstrdup(path, sep - path);
		*fname = pg_strdup(sep + 1);
	}
	/* local directory */
	else
	{
		*dir = NULL;
		*fname = pg_strdup(path);
	}
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

static bool
parse_reldata_filename(const char *path, char **basefname)
{
	const char *fname;
	const char *suffix;
	int			basefnamelen;

	if (strncmp(path, "relationdata/", strlen("relationdata/")) != 0)
		return false;
	fname = path + strlen("relationdata/");
	if (strlen(fname) < 1 + 16)
		return false;

	suffix = fname + strlen(fname) - 1 - 16;
	if (suffix[0] != '_')
		return false;
	if (strspn(suffix + 1, "01234567890ABCDEF") != 16)
		return false;

	basefnamelen = suffix - fname;
	*basefname = palloc(basefnamelen + 1);
	memcpy(*basefname, fname, basefnamelen);
	(*basefname)[basefnamelen] = '\0';

	return true;
}
