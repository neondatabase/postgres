/*-------------------------------------------------------------------------
 *
 * zenith_store.c - experiment with remote page storage
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/zenith/zenith_store.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/guc.h"
#include "postmaster/bgworker.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "storage/ipc.h"

#include "memstore.h"
#include "receiver_worker.h"

char		   *conn_string;

void		_PG_init(void);

PG_MODULE_MAGIC;

static shmem_startup_hook_type prev_shmem_startup_hook;

static void zenith_store_shmem_startup(void);

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable(
		"zenith_store.connstr",
		"Processing node connection string",
		NULL,
		&conn_string,
		"",
		PGC_POSTMASTER,
		0,
		NULL,
		NULL,
		NULL
	);

	BackgroundWorker worker;

	/* Set up common data for worker */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 5; /* restart after 5 seconds */
	sprintf(worker.bgw_library_name, "zenith_store");
	sprintf(worker.bgw_function_name, "receiver_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "zenith_store_receiver");
	snprintf(worker.bgw_type, BGW_MAXLEN, "zenith_store_receiver");
	RegisterBackgroundWorker(&worker);

	memstore_init();

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = zenith_store_shmem_startup;
}

static void
zenith_store_shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	memstore_init_shmem();
}
