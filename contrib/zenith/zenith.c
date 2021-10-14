/*-------------------------------------------------------------------------
 *
 * zenith.c
 *	  Initializes the Zenith storage backend
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	 contrib/zenith/zenith.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/elog.h"
#include "miscadmin.h"
#include "walproposer.h"
#include "pagestore_client.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

#define ZENITH_TAG "[ZENITH] "
#define zenith_log(tag, fmt, ...) ereport(tag, \
		(errmsg(ZENITH_TAG fmt, ## __VA_ARGS__), \
		 errhidestmt(true), errhidecontext(true)))

void
_PG_init(void)
{
	/*
	 * If we're loaded in an initializing process, but not the postmaster,
	 * nor a BGWorker, nor spawned by the postmaster, then we are in the
	 * --sync-safekeepers process.
	 *
	 * In that case, we don't initialize our pagestore / walproposer
	 * infrastructure here; as that will all later be handled by
	 * WalProposerSync, the function that's currently being loaded.
	 *
	 * We can't really prevent _PG_init being called before we get to
	 * WalProposerSync, so we prevent failing in unexpected ways by
	 * bailing out here if we detect that situation.
	 */
	if (!process_shared_preload_libraries_in_progress)
	{
		return;
	}

	zenith_log(DEBUG1, "Configuring Zenith components");
	libpagestore_init(); /* must be initialized before WAL is proposed */
	libwalproposer_init();
}
