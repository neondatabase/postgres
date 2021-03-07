/*-------------------------------------------------------------------------
 *
 * pageserver.c
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/pageserver.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/pageserver.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "fmgr.h"

bool loaded = false;

page_server_api *page_server;

char *page_server_connstring;


static void
zenith_load(void)
{
	Assert(page_server_connstring && page_server_connstring[0]);

	load_file("libpqpageserver", false);
	if (page_server == NULL)
		elog(ERROR, "libpqpageserver didn't initialize correctly");

	loaded = true;
}

/*
 *	zenith_init() -- Initialize private state
 */
void
zenith_init(void)
{
	/* noop */
}


/*
 *	zenith_exists() -- Does the physical file exist?
 */
bool
zenith_exists(SMgrRelation reln, ForkNumber forkNum)
{
	if (!loaded)
		zenith_load();

	ZenithResponse resp = page_server->request((ZenithRequest) {
		.tag = SMGR_EXISTS,
		.page_key = {
			.rnode = reln->smgr_rnode.node,
			.forknum = forkNum
		}
	});
	return resp.status;
}

/*
 *	zenith_create() -- Create a new relation on zenithd storage
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void
zenith_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	/* noop */
}

/*
 *	zenith_unlink() -- Unlink a relation.
 *
 * Note that we're passed a RelFileNodeBackend --- by the time this is called,
 * there won't be an SMgrRelation hashtable entry anymore.
 *
 * forkNum can be a fork number to delete a specific fork, or InvalidForkNumber
 * to delete all forks.
 *
 *
 * If isRedo is true, it's unsurprising for the relation to be already gone.
 * Also, we should remove the file immediately instead of queuing a request
 * for later, since during redo there's no possibility of creating a
 * conflicting relation.
 *
 * Note: any failure should be reported as WARNING not ERROR, because
 * we are usually not in a transaction anymore when this is called.
 */
void
zenith_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	if (!loaded)
		zenith_load();

	ZenithResponse resp = page_server->request((ZenithRequest) {
		.tag = SMGR_UNLINK,
		.page_key = {
			.rnode = rnode.node,
			.forknum = forkNum
		}
	});
}

/*
 *	zenith_extend() -- Add a block to the specified relation.
 *
 *		The semantics are nearly the same as mdwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
zenith_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				char *buffer, bool skipFsync)
{
	/* noop */
}

/*
 *  zenith_open() -- Initialize newly-opened relation.
 */
void
zenith_open(SMgrRelation reln)
{
	/* no work */
}

/*
 *	zenith_close() -- Close the specified relation, if it isn't closed already.
 */
void
zenith_close(SMgrRelation reln, ForkNumber forknum)
{
	/* no work */
}

/*
 *	zenith_prefetch() -- Initiate asynchronous read of the specified block of a relation
 */
bool
zenith_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	/* not implemented */
	return true;
}

/*
 * zenith_writeback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
void
zenith_writeback(SMgrRelation reln, ForkNumber forknum,
					  BlockNumber blocknum, BlockNumber nblocks)
{
	/* not implemented */
}

/*
 *	zenith_read() -- Read the specified block from a relation.
 */
void
zenith_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno,
				 char *buffer)
{
	if (!loaded)
		zenith_load();

	ZenithResponse resp = page_server->request((ZenithRequest) {
		.tag = SMGR_READ,
		.page_key = {
			.rnode = reln->smgr_rnode.node,
			.forknum = forkNum,
			.blkno = blkno
		}
	});

	memcpy(buffer, resp.page, BLCKSZ);
	// XXX: free pagebuffer somewhere

	elog(LOG, "[ZENITH_SMGR] got page");
}

/*
 *	zenith_write() -- Write the supplied block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
void
zenith_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		char *buffer, bool skipFsync)
{
	/* noop */
}

/*
 *	zenith_nblocks() -- Get the number of blocks stored in a relation.
 */
BlockNumber
zenith_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	if (!loaded)
		zenith_load();

	ZenithResponse resp = page_server->request((ZenithRequest) {
		.tag = SMGR_READ,
		.page_key = {
			.rnode = reln->smgr_rnode.node,
			.forknum = forknum,
		}
	});
	return resp.n_blocks;
}

/*
 *	zenith_truncate() -- Truncate relation to specified number of blocks.
 */
void
zenith_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	if (!loaded)
		zenith_load();

	ZenithResponse resp = page_server->request((ZenithRequest) {
		.tag = SMGR_TRUNC,
		.page_key = {
			.rnode = reln->smgr_rnode.node,
			.forknum = forknum,
			.blkno = nblocks // XXX: change that to different message type
		}
	});
}

/*
 *	zenith_immedsync() -- Immediately sync a relation to stable storage.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.  We
 * sync active and inactive segments; smgrDoPendingSyncs() relies on this.
 * Consider a relation skipping WAL.  Suppose a checkpoint syncs blocks of
 * some segment, then mdtruncate() renders that segment inactive.  If we
 * crash before the next checkpoint syncs the newly-inactive segment, that
 * segment may survive recovery, reintroducing unwanted data into the table.
 */
void
zenith_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	/* FIXME: do nothing, we rely on WAL */
}
