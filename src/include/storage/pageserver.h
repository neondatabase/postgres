/*-------------------------------------------------------------------------
 *
 * pageserver.h
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pageserver.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef pageserver_h
#define pageserver_h

#include "storage/relfilenode.h"
#include "storage/block.h"
#include "storage/smgr.h"

/*
 * Requests.
 */

typedef enum
{
	SMGR_EXISTS,
	SMGR_TRUNC,
	SMGR_UNLINK,

	SMGR_NBLOCKS,

	SMGR_READ,
} ZenithRequestTag;

typedef struct
{
	RelFileNode		rnode;
	ForkNumber		forknum;
	BlockNumber		blkno;
} PageKey;

typedef struct
{
	ZenithRequestTag tag;
	PageKey		page_key;
} ZenithRequest;

/*
 * Responses
 */

typedef enum
{
	T_ZenithStatusResponse,
	T_ZenithReadResponse,
	T_ZenithNblocksResponse,
} ZenithResponseTag;

typedef struct
{
	ZenithResponseTag tag;
	bool	status;
	const char   *page;
	int		n_blocks;
} ZenithResponse;

/*
 * API
 */

typedef struct
{
	ZenithResponse	(*request) (ZenithRequest request);
} page_server_api;

extern page_server_api *page_server;

extern char *page_server_connstring;


/* zenith storage manager functionality */

extern void zenith_init(void);
extern void zenith_open(SMgrRelation reln);
extern void zenith_close(SMgrRelation reln, ForkNumber forknum);
extern void zenith_create(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool zenith_exists(SMgrRelation reln, ForkNumber forknum);
extern void zenith_unlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void zenith_extend(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, char *buffer, bool skipFsync);
extern bool zenith_prefetch(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum);
extern void zenith_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				   char *buffer);
extern void zenith_write(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, char *buffer, bool skipFsync);
extern void zenith_writeback(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber zenith_nblocks(SMgrRelation reln, ForkNumber forknum);
extern void zenith_truncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber nblocks);
extern void zenith_immedsync(SMgrRelation reln, ForkNumber forknum);

#endif
