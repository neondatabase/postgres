/*-------------------------------------------------------------------------
 *
 * smgr.h
 *	  storage manager switch public interface declarations.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/smgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SMGR_H
#define SMGR_H

#include "lib/ilist.h"
#include "catalog/pg_class.h"
#include "storage/aio_types.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"

/*
 * Neon: extended SMGR API.
 * This define can be used by extensions to determine that them are built for Neon.
 */
#define NEON_SMGR 1

typedef int SmgrId;

#define SMGRID_MD ((SmgrId) 0)

/*
 * smgr.c maintains a table of SMgrRelation objects, which are essentially
 * cached file handles.  An SMgrRelation is created (if not already present)
 * by smgropen(), and destroyed by smgrdestroy().  Note that neither of these
 * operations imply I/O, they just create or destroy a hashtable entry.  (But
 * smgrdestroy() may release associated resources, such as OS-level file
 * descriptors.)
 *
 * An SMgrRelation may be "pinned", to prevent it from being destroyed while
 * it's in use.  We use this to prevent pointers in relcache to smgr from being
 * invalidated.  SMgrRelations that are not pinned are deleted at end of
 * transaction.
 */
typedef struct SMgrRelationData
{
	/* rlocator is the hashtable lookup key, so it must be first! */
	RelFileLocatorBackend smgr_rlocator;	/* relation physical identifier */

	/*
	 * The following fields are reset to InvalidBlockNumber upon a cache flush
	 * event, and hold the last known size for each fork.  This information is
	 * currently only reliable during recovery, since there is no cache
	 * invalidation for fork extension.
	 */
	BlockNumber smgr_targblock; /* current insertion target block */
	BlockNumber smgr_cached_nblocks[MAX_FORKNUM + 1];	/* last known size */

	/* additional public fields may someday exist here */

	/*
	 * Fields below here are intended to be private to smgr.c and its
	 * submodules.  Do not touch them from elsewhere.
	 */
	SmgrId		smgr_which;		/* storage manager selector */

	/*
	 * for md.c; per-fork arrays of the number of open segments
	 * (md_num_open_segs) and the segments themselves (md_seg_fds).
	 */
	int			md_num_open_segs[MAX_FORKNUM + 1];
	struct _MdfdVec *md_seg_fds[MAX_FORKNUM + 1];

	/*
	 * Pinning support.  If unpinned (ie. pincount == 0), 'node' is a list
	 * link in list of all unpinned SMgrRelations.
	 */
	int			pincount;

	/*
	 * Persistence of the relation
	 */
	char		smgr_relpersistence;
	dlist_node	node;
} SMgrRelationData;

typedef SMgrRelationData *SMgrRelation;

#define SmgrIsTemp(smgr) \
	RelFileLocatorBackendIsTemp((smgr)->smgr_rlocator)

/*
 * This struct of function pointers defines the API between smgr.c and
 * any individual storage manager module.  Note that smgr subfunctions are
 * generally expected to report problems via elog(ERROR).  An exception is
 * that smgr_unlink should use elog(WARNING), rather than erroring out,
 * because we normally unlink relations during post-commit/abort cleanup,
 * and so it's too late to raise an error.  Also, various conditions that
 * would normally be errors should be allowed during bootstrap and/or WAL
 * recovery --- see comments in md.c for details.
 */
typedef struct f_smgr
{
	const char	*smgr_name;
	void		(*smgr_init) (void);	/* may be NULL */
	void		(*smgr_shutdown) (void);	/* may be NULL */
	void		(*smgr_open) (SMgrRelation reln);
	void		(*smgr_close) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_create) (SMgrRelation reln, ForkNumber forknum,
								bool isRedo);
	bool		(*smgr_exists) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_unlink) (RelFileLocatorBackend rlocator, ForkNumber forknum,
								bool isRedo);
	void		(*smgr_extend) (SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum, const void *buffer, bool skipFsync);
	void		(*smgr_zeroextend) (SMgrRelation reln, ForkNumber forknum,
									BlockNumber blocknum, int nblocks, bool skipFsync);
	bool		(*smgr_prefetch) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber blocknum, int nblocks);
	uint32		(*smgr_maxcombine) (SMgrRelation reln, ForkNumber forknum,
									BlockNumber blocknum);
	void		(*smgr_readv) (SMgrRelation reln, ForkNumber forknum,
							   BlockNumber blocknum,
							   void **buffers, BlockNumber nblocks);
	void		(*smgr_startreadv) (PgAioHandle *ioh,
									SMgrRelation reln, ForkNumber forknum,
									BlockNumber blocknum,
									void **buffers, BlockNumber nblocks);
	void		(*smgr_writev) (SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum,
								const void **buffers, BlockNumber nblocks,
								bool skipFsync);
	void		(*smgr_writeback) (SMgrRelation reln, ForkNumber forknum,
								   BlockNumber blocknum, BlockNumber nblocks);
	BlockNumber (*smgr_nblocks) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_truncate) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber old_blocks, BlockNumber nblocks);
	void		(*smgr_immedsync) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_registersync) (SMgrRelation reln, ForkNumber forknum);
	int			(*smgr_fd) (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off);

	/* NEON: test if this smgr is responsible for this relation */
	bool		(*smgr_owns) (RelFileLocator rlocator, ProcNumber backend,
							  char relpersistence);

	/* NEON: Hooks to smgr for unlogged build phases */
	void		(*smgr_start_unlogged_build) (SMgrRelation reln);
	void		(*smgr_finish_unlogged_build_phase_1) (SMgrRelation reln);
	void		(*smgr_end_unlogged_build) (SMgrRelation reln);
	int			(*smgr_read_slru_segment) (const char *path, int segno, void* buffer);
} f_smgr;

/* NEON: Alternative implementation of calculate_database_size(), to make it O(1) */
typedef int64 (*dbsize_hook_type) (Oid dbOid);
extern PGDLLIMPORT dbsize_hook_type dbsize_hook;

extern PGDLLIMPORT const PgAioTargetInfo aio_smgr_target_info;

extern SmgrId smgrregister(const f_smgr *smgr);

extern void smgrinit(void);
extern SMgrRelation smgropen(RelFileLocator rlocator, ProcNumber backend,
							 char relpersistence);
extern bool smgrexists(SMgrRelation reln, ForkNumber forknum);
extern void smgrpin(SMgrRelation reln);
extern void smgrunpin(SMgrRelation reln);
extern void smgrclose(SMgrRelation reln);
extern void smgrdestroyall(void);
extern void smgrrelease(SMgrRelation reln);
extern void smgrreleaseall(void);
extern void smgrreleaserellocator(RelFileLocatorBackend rlocator);
extern void smgrcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void smgrdosyncall(SMgrRelation *rels, int nrels);
extern void smgrdounlinkall(SMgrRelation *rels, int nrels, bool isRedo);
extern void smgrextend(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, const void *buffer, bool skipFsync);
extern void smgrzeroextend(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber blocknum, int nblocks, bool skipFsync);
extern bool smgrprefetch(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum, int nblocks);
extern uint32 smgrmaxcombine(SMgrRelation reln, ForkNumber forknum,
							 BlockNumber blocknum);
extern void smgrreadv(SMgrRelation reln, ForkNumber forknum,
					  BlockNumber blocknum,
					  void **buffers, BlockNumber nblocks);
extern void smgrstartreadv(PgAioHandle *ioh,
						   SMgrRelation reln, ForkNumber forknum,
						   BlockNumber blocknum,
						   void **buffers, BlockNumber nblocks);
extern void smgrwritev(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum,
					   const void **buffers, BlockNumber nblocks,
					   bool skipFsync);
extern void smgrwriteback(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber smgrnblocks(SMgrRelation reln, ForkNumber forknum);
extern BlockNumber smgrnblocks_cached(SMgrRelation reln, ForkNumber forknum);
extern void smgrtruncate(SMgrRelation reln, ForkNumber *forknum, int nforks,
						 BlockNumber *old_nblocks,
						 BlockNumber *nblocks);
extern void smgrimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void smgrregistersync(SMgrRelation reln, ForkNumber forknum);
extern void AtEOXact_SMgr(void);
extern bool ProcessBarrierSmgrRelease(void);

/* Neon: Change relpersistence for unlogged index builds */
extern void smgr_start_unlogged_build(SMgrRelation reln);
extern void	smgr_finish_unlogged_build_phase_1(SMgrRelation reln);
extern void smgr_end_unlogged_build(SMgrRelation reln);

/* Neon: Allow on-demand download of SLRU segment data */
extern int  smgr_read_slru_segment(const char *path, int segno, void* buffer);

static inline void
smgrread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 void *buffer)
{
	smgrreadv(reln, forknum, blocknum, &buffer, 1);
}

static inline void
smgrwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		  const void *buffer, bool skipFsync)
{
	smgrwritev(reln, forknum, blocknum, &buffer, 1, skipFsync);
}

extern void pgaio_io_set_target_smgr(PgAioHandle *ioh,
									 SMgrRelationData *smgr,
									 ForkNumber forknum,
									 BlockNumber blocknum,
									 int nblocks,
									 bool skip_fsync);

#endif							/* SMGR_H */
