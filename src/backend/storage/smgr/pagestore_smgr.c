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

#include "storage/pagestore_client.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "fmgr.h"

const int SmgrTrace = LOG;

bool loaded = false;

page_server_api *page_server;

char *page_server_connstring;

char const *const ZenithMessageStr[] =
{
	"ZenithExistsRequest",
	"ZenithTruncRequest",
	"ZenithUnlinkRequest",
	"ZenithNblocksRequest",
	"ZenithReadRequest",
	"ZenithCreateRequest",
	"ZenithStatusResponse",
	"ZenithReadResponse",
	"ZenithNblocksResponse",
};

StringInfoData
zm_pack(ZenithMessage *msg, bool include_libpq_type)
{
	StringInfoData	s;

	initStringInfo(&s);
	if (include_libpq_type)
		pq_sendbyte(&s, 'd');
	pq_sendbyte(&s, msg->tag);

	switch (messageTag(msg))
	{
		/* pagestore_client -> pagestore */
		case T_ZenithExistsRequest:
		case T_ZenithTruncRequest:
		case T_ZenithUnlinkRequest:
		case T_ZenithNblocksRequest:
		case T_ZenithReadRequest:
		case T_ZenithCreateRequest:
		{
			ZenithRequest *msg_req = (ZenithRequest *) msg;

			pq_sendint32(&s, msg_req->page_key.rnode.spcNode);
			pq_sendint32(&s, msg_req->page_key.rnode.dbNode);
			pq_sendint32(&s, msg_req->page_key.rnode.relNode);
			pq_sendbyte(&s, msg_req->page_key.forknum);
			pq_sendint32(&s, msg_req->page_key.blkno);

			break;
		}

		/* pagestore -> pagestore_client */
		case T_ZenithStatusResponse:
		case T_ZenithNblocksResponse:
		{
			ZenithResponse *msg_resp = (ZenithResponse *) msg;
			pq_sendbyte(&s, msg_resp->ok);
			pq_sendint32(&s, msg_resp->n_blocks);
			break;
		}
		case T_ZenithReadResponse:
		{
			ZenithResponse *msg_resp = (ZenithResponse *) msg;
			pq_sendbyte(&s, msg_resp->ok);
			pq_sendint32(&s, msg_resp->n_blocks);
			pq_sendbytes(&s, msg_resp->page, BLCKSZ); // XXX: should be varlena
			break;
		}
	}
	return s;
}

ZenithMessage *
zm_unpack(StringInfo s)
{
	ZenithMessageTag tag = pq_getmsgbyte(s);
	ZenithMessage *msg;

	switch (tag)
	{
		/* pagestore_client -> pagestore */
		case T_ZenithExistsRequest:
		case T_ZenithTruncRequest:
		case T_ZenithUnlinkRequest:
		case T_ZenithNblocksRequest:
		case T_ZenithReadRequest:
		case T_ZenithCreateRequest:
		{
			ZenithRequest *msg_req = palloc0(sizeof(ZenithRequest));

			msg_req->tag = tag;
			msg_req->system_id = 42;
			msg_req->page_key.rnode.spcNode = pq_getmsgint(s, 4);
			msg_req->page_key.rnode.dbNode = pq_getmsgint(s, 4);
			msg_req->page_key.rnode.relNode = pq_getmsgint(s, 4);
			msg_req->page_key.forknum = pq_getmsgbyte(s);
			msg_req->page_key.blkno = pq_getmsgint(s, 4);
			pq_getmsgend(s);

			msg = (ZenithMessage *) msg_req;
			break;
		}

		/* pagestore -> pagestore_client */
		case T_ZenithStatusResponse:
		case T_ZenithNblocksResponse:
		{
			ZenithResponse *msg_resp = palloc0(sizeof(ZenithResponse));

			msg_resp->tag = tag;
			msg_resp->ok = pq_getmsgbyte(s);
			msg_resp->n_blocks = pq_getmsgint(s, 4);
			pq_getmsgend(s);

			msg = (ZenithMessage *) msg_resp;
			break;
		}

		case T_ZenithReadResponse:
		{
			ZenithResponse *msg_resp = palloc0(sizeof(ZenithResponse));

			msg_resp->tag = tag;
			msg_resp->ok = pq_getmsgbyte(s);
			msg_resp->n_blocks = pq_getmsgint(s, 4);
			msg_resp->page = pq_getmsgbytes(s, BLCKSZ); // XXX: should be varlena
			pq_getmsgend(s);

			msg = (ZenithMessage *) msg_resp;
			break;
		}
	}

	return msg;
}

/* dump to json for debugging / error reporting purposes */
char *
zm_to_string(ZenithMessage *msg)
{
	StringInfoData s;
	return s.data;
}

static void
zenith_load(void)
{
	Assert(page_server_connstring && page_server_connstring[0]);

	load_file("libpqpagestore", false);
	if (page_server == NULL)
		elog(ERROR, "libpqpagestore didn't initialize correctly");

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

	bool ok;

	ZenithResponse *resp = page_server->request((ZenithRequest) {
		.tag = T_ZenithExistsRequest,
		.page_key = {
			.rnode = reln->smgr_rnode.node,
			.forknum = forkNum
		}
	});
	ok = resp->ok;
	pfree(resp);
	return ok;
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
	if (!loaded)
		zenith_load();

	ZenithResponse *resp = page_server->request((ZenithRequest) {
		.tag = T_ZenithCreateRequest,
		.page_key = {
			.rnode = reln->smgr_rnode.node,
			.forknum = forkNum
		}
	});
	pfree(resp);
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

	ZenithResponse *resp = page_server->request((ZenithRequest) {
		.tag = T_ZenithUnlinkRequest,
		.page_key = {
			.rnode = rnode.node,
			.forknum = forkNum
		}
	});
	pfree(resp);
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
	elog(SmgrTrace, "[ZENITH_SMGR] extend noop");
}

/*
 *  zenith_open() -- Initialize newly-opened relation.
 */
void
zenith_open(SMgrRelation reln)
{
	/* no work */
	elog(SmgrTrace, "[ZENITH_SMGR] open noop");
}

/*
 *	zenith_close() -- Close the specified relation, if it isn't closed already.
 */
void
zenith_close(SMgrRelation reln, ForkNumber forknum)
{
	/* no work */
	elog(SmgrTrace, "[ZENITH_SMGR] close noop");
}

/*
 *	zenith_prefetch() -- Initiate asynchronous read of the specified block of a relation
 */
bool
zenith_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	/* not implemented */
	elog(SmgrTrace, "[ZENITH_SMGR] prefetch noop");
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
	elog(SmgrTrace, "[ZENITH_SMGR] writeback noop");
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

	ZenithResponse *resp = page_server->request((ZenithRequest) {
		.tag = T_ZenithReadRequest,
		.page_key = {
			.rnode = reln->smgr_rnode.node,
			.forknum = forkNum,
			.blkno = blkno
		}
	});

	memcpy(buffer, resp->page, BLCKSZ);
	pfree(resp);
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
	elog(SmgrTrace, "[ZENITH_SMGR] write noop");
}

/*
 *	zenith_nblocks() -- Get the number of blocks stored in a relation.
 */
BlockNumber
zenith_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	if (!loaded)
		zenith_load();

	int n_blocks;

	ZenithResponse *resp = page_server->request((ZenithRequest) {
		.tag = T_ZenithNblocksRequest,
		.page_key = {
			.rnode = reln->smgr_rnode.node,
			.forknum = forknum,
		}
	});
	n_blocks = resp->n_blocks;
	pfree(resp);
	return n_blocks;
}

/*
 *	zenith_truncate() -- Truncate relation to specified number of blocks.
 */
void
zenith_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	if (!loaded)
		zenith_load();

	ZenithResponse *resp = page_server->request((ZenithRequest) {
		.tag = T_ZenithTruncRequest,
		.page_key = {
			.rnode = reln->smgr_rnode.node,
			.forknum = forknum,
			.blkno = nblocks // XXX: change that to the different message type
		}
	});
	pfree(resp);
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
	elog(SmgrTrace, "[ZENITH_SMGR] immedsync noop");
}
