/*-------------------------------------------------------------------------
 *
 * zenith_push.c - decode and redistribute WAL per datafile
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/zenith_zenith_push/zenith_push.c
 *-------------------------------------------------------------------------
 */

#define FRONTEND 1
#include "postgres.h"

#include <dirent.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "access/transam.h"
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
#include "zenith_push.h"
#include "put_s3.h"
#include "pgtar.h"

/*
 * Size of each block sent into the tar stream for larger files.
 */
#define TAR_SEND_SIZE 32768

XLogRecPtr	walpos;
int			wal_segment_size;

static const char *progname;

typedef struct s3restore_config
{
	/* display options */
	bool		quiet;

} s3restore_config;

#define fatal_error(...) do { pg_log_fatal(__VA_ARGS__); exit(EXIT_FAILURE); } while(0)

static bool isRelDataFile(const char *path);
static char *datasegpath(RelFileNode rnode, ForkNumber forknum,
						 BlockNumber segno);

static void recurse_dir(const char *datadir, const char *parentpath);
static void handlefile(const char *datadir, const char *path, struct stat *statbuf);
static void _tarWriteHeader(const char *filename, const char *linktarget,
							struct stat *statbuf, bool sizeonly);

static FILE *tarfile;

static void
usage(void)
{
	printf(_("%s push an existing PostgreSQL cluster to S3 bucket in Zenith file format.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  --archive-wal-path=PATH   push a single WAL file\n"));
	printf(_("  --archive-wal-fname=PATH  push a single WAL file\n"));
	printf(_("  -D, --pgdata=DIRECTORY push cluster from this data directory\n"));
	printf(_("  -q, --quiet            do not print any output, except for errors\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	/* printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT); */
	/* printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL); */
}

int
main(int argc, char **argv)
{
	s3restore_config config;
	char	   *pg_data = NULL;
	char	   *archive_wal_path = NULL;
	char	   *archive_wal_fname = NULL;
	ControlFileData *controlfile;
	bool		crc_ok;

	static struct option long_options[] = {
		{"pgdata", required_argument, NULL, 'D'},
		{"help", no_argument, NULL, '?'},
		{"quiet", no_argument, NULL, 'q'},
		{"version", no_argument, NULL, 'V'},
		{"archive-wal-path", required_argument, NULL, 1},
		{"archive-wal-fname", required_argument, NULL, 2},
		{NULL, 0, NULL, 0}
	};

	int			option;
	int			optindex = 0;

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("zenith_push"));
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
			puts("zenith_push (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	memset(&config, 0, sizeof(s3restore_config));

	config.quiet = false;

	if (argc <= 1)
	{
		pg_log_error("no arguments specified");
		goto bad_argument;
	}

	while ((option = getopt_long(argc, argv, "D:q",
								 long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case 'D':
				pg_data = pg_strdup(optarg);
				break;
			case 'q':
				config.quiet = true;
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
		pg_log_error("no data directory (-D) or --arhive-wal-path specified");
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

	/* done with argument parsing, do the actual work */
	
	/* In this mode, used as an archive_command */
	if (archive_wal_path)
	{
		struct stat fst;
		char		s3pathbuf[MAXPGPATH];

		if (lstat(archive_wal_path, &fst) < 0)
		{
			pg_fatal("could not stat file \"%s\": %m", archive_wal_path);
		}

		snprintf(s3pathbuf, sizeof(s3pathbuf), "walarchive/%s",
				 archive_wal_fname);

		put_s3_file(archive_wal_path, s3pathbuf, fst.st_size);

		return EXIT_SUCCESS;
	}

	/* Read the last checkpoint LSN */
	controlfile = get_controlfile(pg_data, &crc_ok);
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


	/* Otherwise, push a new base backup */

	walpos = controlfile->checkPoint;
	wal_segment_size = controlfile->xlog_seg_size;

	/* TODO: check other conditions. No tablespaces, right? */

	/* TODO: Scan the directory. Create a tarball, upload it */
	tarfile = fopen("/tmp/zenith_push_base.tar", "wb");
	if (!tarfile)
		pg_fatal("could not open file \"%s\" for writing", "/tmp/zenith_push_base.tar");

	chdir(pg_data);

	recurse_dir(".", NULL);

	fclose(tarfile);

	/* upload tar file */
	{
		struct stat fst;
		char		s3pathbuf[MAXPGPATH];

		if (lstat("/tmp/zenith_push_base.tar", &fst) < 0)
		{
			pg_fatal("could not stat file \"%s\": %m",
					 "/tmp/zenith_push_base.tar");
		}

		snprintf(s3pathbuf, sizeof(s3pathbuf),
				 "nonreldata/nonrel_%08X%08X.tar", 
				 (uint32) (walpos >> 32), (uint32) walpos);

		put_s3_file("/tmp/zenith_push_base.tar", s3pathbuf, fst.st_size);
	}

	return EXIT_SUCCESS;

bad_argument:
	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
	return EXIT_FAILURE;
}


/*
 * recursive part of traverse_datadir
 *
 * parentpath is the current subdirectory's path relative to datadir,
 * or NULL at the top level.
 */
static void
recurse_dir(const char *datadir, const char *parentpath)
{
	DIR		   *xldir;
	struct dirent *xlde;
	char		fullparentpath[MAXPGPATH];

	if (parentpath)
		snprintf(fullparentpath, MAXPGPATH, "%s/%s", datadir, parentpath);
	else
		snprintf(fullparentpath, MAXPGPATH, "%s", datadir);

	xldir = opendir(fullparentpath);
	if (xldir == NULL)
		pg_fatal("could not open directory \"%s\": %m",
				 fullparentpath);

	while (errno = 0, (xlde = readdir(xldir)) != NULL)
	{
		struct stat fst;
		char		fullpath[MAXPGPATH * 2];
		char		path[MAXPGPATH * 2];

		if (strcmp(xlde->d_name, ".") == 0 ||
			strcmp(xlde->d_name, "..") == 0)
			continue;

		snprintf(fullpath, sizeof(fullpath), "%s/%s", fullparentpath, xlde->d_name);

		if (lstat(fullpath, &fst) < 0)
		{
			if (errno == ENOENT)
			{
				/*
				 * File doesn't exist anymore. This is ok, if the new primary
				 * is running and the file was just removed. If it was a data
				 * file, there should be a WAL record of the removal. If it
				 * was something else, it couldn't have been anyway.
				 *
				 * TODO: But complain if we're processing the target dir!
				 */
			}
			else
				pg_fatal("could not stat file \"%s\": %m",
						 fullpath);
		}

		if (parentpath)
			snprintf(path, sizeof(path), "%s/%s", parentpath, xlde->d_name);
		else
			snprintf(path, sizeof(path), "%s", xlde->d_name);

		if (S_ISREG(fst.st_mode))
			handlefile(datadir, path, &fst);
		else if (S_ISDIR(fst.st_mode))
		{
			/* recurse to handle subdirectories */

			/*
			 * Store a directory entry in the tar file so we can get the
			 * permissions right.
			 */
			_tarWriteHeader(path, NULL, &fst, false);

			recurse_dir(datadir, path);
		}
#ifndef WIN32
		else if (S_ISLNK(fst.st_mode))
#else
		else if (pgwin32_is_junction(fullpath))
#endif
		{
			pg_fatal("symbolic links not currently supported");
		}
	}

	if (errno)
		pg_fatal("could not read directory \"%s\": %m",
				 fullparentpath);

	if (closedir(xldir))
		pg_fatal("could not close directory \"%s\": %m",
				 fullparentpath);
}


static void
_tarWriteHeader(const char *filename, const char *linktarget,
				struct stat *statbuf, bool sizeonly)
{
	char		h[TAR_BLOCK_SIZE];
	enum tarError rc;

	rc = tarCreateHeader(h, filename, linktarget, statbuf->st_size,
						 statbuf->st_mode, statbuf->st_uid, statbuf->st_gid,
						 statbuf->st_mtime);

	switch (rc)
	{
		case TAR_OK:
			break;
		case TAR_NAME_TOO_LONG:
			pg_fatal("file name too long for tar format: \"%s\"",
					 filename);
			break;
		case TAR_SYMLINK_TOO_LONG:
			pg_fatal("symbolic link target too long for tar format: "
					 "file name \"%s\", target \"%s\"",
					 filename, linktarget);
			break;
		default:
			pg_fatal("unrecognized tar error: %d", rc);
	}

	fwrite(h, sizeof(h), 1, tarfile); /* FIXME: check result */
}

static void
handlefile(const char *datadir, const char *path, struct stat *statbuf)
{
	if (isRelDataFile(path))
	{
		char		s3pathbuf[MAXPGPATH];

		snprintf(s3pathbuf, sizeof(s3pathbuf),
				 "relationdata/%s_%08X%08X",
				 path, (uint32) (walpos >> 32), (uint32) walpos);
		put_s3_file(path, s3pathbuf, statbuf->st_size);
	}
	else if (strlen(path) > strlen("pg_wal/") &&
			 IsXLogFileName(path + strlen("pg_wal/")))
	{
		/* FIXME: rely on archive_command for WAL archiving */
#if 0
		/* Is it a full segment? */
		const char *walfname = path + strlen("pg_wal/");
		TimeLineID	tli;
		uint64		logsegno;
		XLogRecPtr	walfstartptr;
		XLogRecPtr	walfendptr;

		XLogFromFileName(walfname, &tli, &logsegno, wal_segment_size);

		XLogSegNoOffsetToRecPtr(logsegno, 0, wal_segment_size, walfstartptr);
		XLogSegNoOffsetToRecPtr(logsegno + 1, 0, wal_segment_size, walfendptr);

		if (walfstartptr >= walpos)
		{
			/* This is a preallocated future segment. It's garbage */
		}
		else
		{
			/* This is a full or partial segment */
			char		s3pathbuf[MAXPGPATH];

			if (walfendptr > walpos)
				walfendptr = walpos;

			snprintf(s3pathbuf, sizeof(s3pathbuf),
					 "walarchive/wal_%08X%08X-%08X%08X",
					 (uint32) (walfstartptr >> 32), (uint32) walfstartptr,
					 (uint32) (walfendptr >> 32), (uint32) walfendptr);
			put_s3_file(path, s3pathbuf, walfendptr - walfstartptr);
		}
#endif
	}
	else
	{
		size_t		pad;
		FILE	   *fp;
		char		buf[TAR_SEND_SIZE];

		_tarWriteHeader(path, NULL, statbuf, false);

		/* write content */
		fp = fopen(path, PG_BINARY_R);
		if (!fp)
			pg_fatal("could not open file \"%s\" for reading", path);
		for (;;)
		{
			int			nread;

			nread = fread(buf, 1, TAR_SEND_SIZE, fp);
			if (nread == 0)
			{
				if (feof(fp))
					break;
				else
					pg_fatal("error reading file \"%s\"", path);
			}

			fwrite(buf, nread, 1, tarfile); /* FIXME: check result */
		}
		fclose(fp);

		/*
		 * Pad to a block boundary, per tar format requirements. (This small piece
		 * of data is probably not worth throttling, and is not checksummed
		 * because it's not actually part of the file.)
		 */
		pad = tarPaddingBytesRequired(statbuf->st_size);
		if (pad > 0)
		{
			MemSet(buf, 0, pad);

			fwrite(buf, pad, 1, tarfile); /* FIXME: check result */
		}
	}
}



/*
 * Does it look like a relation data file?
 *
 * For our purposes, only files belonging to the main fork are considered
 * relation files. Other forks are always copied in toto, because we cannot
 * reliably track changes to them, because WAL only contains block references
 * for the main fork.
 */
static bool
isRelDataFile(const char *path)
{
	RelFileNode rnode;
	unsigned int segNo;
	int			nmatch;
	bool		matched;

	/*----
	 * Relation data files can be in one of the following directories:
	 *
	 * global/
	 *		shared relations
	 *
	 * base/<db oid>/
	 *		regular relations, default tablespace
	 *
	 * pg_tblspc/<tblspc oid>/<tblspc version>/
	 *		within a non-default tablespace (the name of the directory
	 *		depends on version)
	 *
	 * And the relation data files themselves have a filename like:
	 *
	 * <oid>.<segment number>
	 *
	 *----
	 */
	rnode.spcNode = InvalidOid;
	rnode.dbNode = InvalidOid;
	rnode.relNode = InvalidOid;
	segNo = 0;
	matched = false;

	nmatch = sscanf(path, "global/%u.%u", &rnode.relNode, &segNo);
	if (nmatch == 1 || nmatch == 2)
	{
		rnode.spcNode = GLOBALTABLESPACE_OID;
		rnode.dbNode = 0;
		matched = true;
	}
	else
	{
		nmatch = sscanf(path, "base/%u/%u.%u",
						&rnode.dbNode, &rnode.relNode, &segNo);
		if (nmatch == 2 || nmatch == 3)
		{
			rnode.spcNode = DEFAULTTABLESPACE_OID;
			matched = true;
		}
		else
		{
			nmatch = sscanf(path, "pg_tblspc/%u/" TABLESPACE_VERSION_DIRECTORY "/%u/%u.%u",
							&rnode.spcNode, &rnode.dbNode, &rnode.relNode,
							&segNo);
			if (nmatch == 3 || nmatch == 4)
				matched = true;
		}
	}

	/*
	 * The sscanf tests above can match files that have extra characters at
	 * the end. To eliminate such cases, cross-check that GetRelationPath
	 * creates the exact same filename, when passed the RelFileNode
	 * information we extracted from the filename.
	 */
	if (matched)
	{
		char	   *check_path = datasegpath(rnode, MAIN_FORKNUM, segNo);

		if (strcmp(check_path, path) != 0)
			matched = false;

		pfree(check_path);
	}

	return matched;
}


/*
 * A helper function to create the path of a relation file and segment.
 *
 * The returned path is palloc'd
 */
static char *
datasegpath(RelFileNode rnode, ForkNumber forknum, BlockNumber segno)
{
	char	   *path;
	char	   *segpath;

	path = relpathperm(rnode, forknum);
	if (segno > 0)
	{
		segpath = psprintf("%s.%u", path, segno);
		pfree(path);
		return segpath;
	}
	else
		return path;
}
