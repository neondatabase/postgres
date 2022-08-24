#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "replication/walpropshim.h"

bool syncSafekeepers = false;
void (*WalProposerInit) (XLogRecPtr flushRecPtr, uint64 systemId) = NULL;
void (*WalProposerStart) (void) = NULL;

/*
 * Entry point for `postgres --sync-safekeepers`.
 */
void
WalProposerSync(int argc, char *argv[])
{
	struct stat stat_buf;

	syncSafekeepers = true;

	InitStandaloneProcess(argv[0]);

	SetProcessingMode(InitProcessing);

	/*
	 * Set default values for command-line options.
	 */
	InitializeGUCOptions();

	/* Acquire configuration parameters */
	if (!SelectConfigFiles(NULL, progname))
		exit(1);

	/*
	 * Imitate we are early in bootstrap loading shared_preload_libraries;
	 * zenith extension sets PGC_POSTMASTER gucs requiring this.
	 */
	process_shared_preload_libraries_in_progress = true;

	/*
	 * Initialize postmaster_alive_fds as WaitEventSet checks them.
	 *
	 * Copied from InitPostmasterDeathWatchHandle()
	 */
	if (pipe(postmaster_alive_fds) < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
					errmsg_internal("could not create pipe to monitor postmaster death: %m")));
	if (fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFL, O_NONBLOCK) == -1)
		ereport(FATAL,
				(errcode_for_socket_access(),
					errmsg_internal("could not set postmaster death monitoring pipe to nonblocking mode: %m")));

	ChangeToDataDir();

	/* Create pg_wal directory, if it doesn't exist */
	if (stat(XLOGDIR, &stat_buf) != 0)
	{
		ereport(LOG, (errmsg("creating missing WAL directory \"%s\"", XLOGDIR)));
		if (MakePGDirectory(XLOGDIR) < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
						errmsg("could not create directory \"%s\": %m",
							   XLOGDIR)));
			exit(1);
		}
	}

	load_file("neon", false);

	if (NULL == WalProposerInit)
		elog(ERROR, "Neon failed to register WalProposerInit");

	if (NULL == WalProposerStart)
		elog(ERROR, "Neon failed to register WalProposerStart");

	WalProposerInit(0, 0);

	process_shared_preload_libraries_in_progress = false;

	BackgroundWorkerUnblockSignals();

	WalProposerStart();
}
