/*-------------------------------------------------------------------------
 *
 * s3offload.c - offloat completed WAL segments to S3 and remove too old segments from local storage
 *
 * Author: Konstantin Knizhnik (knizhnik@garret.ru)
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/safekeepr/s3offload.c
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include <dirent.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#include "access/xlog_internal.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/logging.h"
#include "getopt_long.h"
#include "lib/stringinfo.h"
#include "zenith_s3/s3_ops.h"

#define pg_fatal(...) do { pg_log_fatal(__VA_ARGS__); exit(1); } while(0)

static char* basedir;
static int   expiration_period = 1;  /* days */
static int   poll_interval = 10;     /* seconds */
static int   verbose = 0;
static CURL *curl;

#define SECONDS_PER_DAY (3600*24)

static void
usage(void)
{
	printf(_("s3ofload push WAL files from safekeeper local directry to S3.\n\n"));
	printf(_("Usage:\n"));
	printf(_("  s3offload [OPTION]...\n"));
	printf(_("\nOptions:\n"));
	printf(_("  -D, --directory=DIR    receive write-ahead log files into this directory\n"));
	printf(_("  -x, --expiration       expiration period (days)\n"));
	printf(_("  -p, --poll             poll interval (seconds)\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT);
	printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}


static int
compare_file_names(void const* p, void const* q)
{
	return strcmp(*(char**)p, *(char**)q);
}

int
main(int argc, char **argv)
{
	static struct option long_options[] = {
		{"directory", required_argument, NULL, 'D'},
		{"expiration", required_argument, NULL, 'x'},
		{"poll", required_argument, NULL, 'p'},
		{NULL, 0, NULL, 0}
	};
	int			c;
	int			option_index;
	char		s3_path[MAXPGPATH];
	char		local_path[MAXPGPATH];

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("s3offload"));

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		else if (strcmp(argv[1], "-V") == 0 ||
				 strcmp(argv[1], "--version") == 0)
		{
			puts("safekeeper (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "D:i:x:v",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
		  case 'D':
			basedir = pg_strdup(optarg);
			break;
		  case 'x':
			expiration_period = atoi(optarg);
			break;
		  case 'p':
			poll_interval = atoi(optarg);
			break;
		  case 'v':
			verbose++;
			break;
		  default:
			/*
			 * getopt_long already emitted a complaint
			 */
			fprintf(stderr, _("Try \"s3offload --help\" for more information.\n"));
			exit(1);
		}
	}
	curl = curl_easy_init();
	if (!curl)
		pg_fatal("could not init libcurl");

	while (true)
	{
		ListObjectsResult *s3_wals = s3_ListObjects(curl, "");
		DIR* dir = opendir(basedir);
		struct dirent *dirent;
		time_t now = time(NULL);

		qsort(s3_wals->filenames, s3_wals->numfiles, sizeof(char*), compare_file_names);
		while ((dirent = readdir(dir)) != NULL)
		{
			char* file_name = dirent->d_name;
			if (IsXLogFileName(file_name))
			{
				struct stat fst;

				sprintf(local_path, "%s/%s", basedir, file_name);
				sprintf(s3_path, "walarchive/%s", file_name);

				if (stat(local_path, &fst) < 0)
					pg_fatal("could not stat file \"%s\": %m", local_path);

				if (!bsearch(&s3_path, s3_wals->filenames, s3_wals->numfiles, sizeof(char*), compare_file_names))
				{
					/* File is not present at S3 */

					if (verbose)
						pg_log_info("Offload WAL segment %s", file_name);

					put_s3_file(curl, local_path, s3_path, fst.st_size);
				}
				else
				{
					if (expiration_period != 0
						&& now - fst.st_mtime > (time_t)expiration_period*SECONDS_PER_DAY)
					{
						if (verbose)
							pg_log_info("Remove WAL segment %s", file_name);
						unlink(local_path);
					}
				}
			}
		}
		closedir(dir);
		pfree(s3_wals->filenames);
		pfree(s3_wals);

		sleep(poll_interval);
	}
	curl_easy_cleanup(curl);
}
