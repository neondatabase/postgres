/*-------------------------------------------------------------------------
 *
 * lazyrestore.h
 *	  public interface declarations for restoring data files lazily.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/lazyrestore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LAZYRESTORE_H
#define LAZYRESTORE_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"

/* lazyrestore storage manager functionality */
extern void lazyrestore_init(void);
extern void lazyrestore_open(SMgrRelation reln);
extern void lazyrestore_close(SMgrRelation reln, ForkNumber forknum);
extern void lazyrestore_create(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool lazyrestore_exists(SMgrRelation reln, ForkNumber forknum);
extern void lazyrestore_unlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void lazyrestore_extend(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, char *buffer, bool skipFsync);
extern bool lazyrestore_prefetch(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum);
extern void lazyrestore_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				   char *buffer);
extern void lazyrestore_write(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, char *buffer, bool skipFsync);
extern void lazyrestore_writeback(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber lazyrestore_nblocks(SMgrRelation reln, ForkNumber forknum);
extern void lazyrestore_truncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber nblocks);
extern void lazyrestore_immedsync(SMgrRelation reln, ForkNumber forknum);

extern bool reln_is_lazy(SMgrRelation reln, ForkNumber forknum, bool ok_missing);
extern void restore_if_lazy(SMgrRelation reln, ForkNumber forknum);

/* lazyrestore sync callbacks */
extern int	lazyrestore_syncfiletag(const FileTag *ftag, char *path);
extern int	lazyrestore_unlinkfiletag(const FileTag *ftag, char *path);
extern bool lazyrestore_filetagmatches(const FileTag *ftag, const FileTag *candidate);


extern bool lazyrestore_startup_redo(RelFileNode rnode, ForkNumber forknum, BlockNumber blkno, XLogReaderState *record);

#endif							/* LAZYRESTORE_H */
