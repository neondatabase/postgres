/*-------------------------------------------------------------------------
 *
 * zenithtest.c
 *	  Helpers for zenith testing and debugging
 *
 * IDENTIFICATION
 *	 contrib/zenith_test_utils/zenithtest.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "access/relation.h"
#include "catalog/namespace.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/varlena.h"


PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_consume_xids);
PG_FUNCTION_INFO_V1(clear_buffer_cache);
PG_FUNCTION_INFO_V1(get_raw_page_at_lsn);


extern void zenith_read_at_lsn(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno,
			XLogRecPtr request_lsn, bool request_latest, char *buffer);

/*
 * test_consume_xids(int4), for rapidly consuming XIDs, to test wraparound.
 */
Datum
test_consume_xids(PG_FUNCTION_ARGS)
{
	int32		nxids = PG_GETARG_INT32(0);
	TransactionId topxid;
	FullTransactionId fullxid;
	TransactionId xid;
	TransactionId targetxid;

	/* make sure we have a top-XID first */
	topxid = GetTopTransactionId();

	xid = ReadNextTransactionId();

	targetxid = xid + nxids;
	while (targetxid < FirstNormalTransactionId)
		targetxid++;

	while (TransactionIdPrecedes(xid, targetxid))
	{
		fullxid = GetNewTransactionId(true);
		xid = XidFromFullTransactionId(fullxid);
		elog(DEBUG1, "topxid: %u xid: %u", topxid, xid);
	}

	PG_RETURN_VOID();
}

/*
 * Flush the buffer cache, evicting all pages that are not currently pinned.
 */
Datum
clear_buffer_cache(PG_FUNCTION_ARGS)
{
	bool		save_zenith_test_evict;

	/*
	 * Temporarily set the zenith_test_evict GUC, so that when we pin and
	 * unpin a buffer, the buffer is evicted. We use that hack to evict all
	 * buffers, as there is no explicit "evict this buffer" function in the
	 * buffer manager.
	 */
	save_zenith_test_evict = zenith_test_evict;
	zenith_test_evict = true;
	PG_TRY();
	{
		/* Scan through all the buffers */
		for (int i = 0; i < NBuffers; i++)
		{
			BufferDesc *bufHdr;
			uint32		buf_state;
			Buffer		bufferid;
			bool		isvalid;
			RelFileNode rnode;
			ForkNumber	forknum;
			BlockNumber blocknum;

			/* Peek into the buffer header to see what page it holds. */
			bufHdr = GetBufferDescriptor(i);
			buf_state = LockBufHdr(bufHdr);

			if ((buf_state & BM_VALID) && (buf_state & BM_TAG_VALID))
				isvalid = true;
			else
				isvalid = false;
			bufferid = BufferDescriptorGetBuffer(bufHdr);
			rnode = bufHdr->tag.rnode;
			forknum = bufHdr->tag.forkNum;
			blocknum = bufHdr->tag.blockNum;

			UnlockBufHdr(bufHdr, buf_state);

			/*
			 * Pin the buffer, and release it again. Because we have
			 * zenith_test_evict==true, this will evict the page from
			 * the buffer cache if no one else is holding a pin on it.
			 */
			if (isvalid)
			{
				if (ReadRecentBuffer(rnode, forknum, blocknum, bufferid))
					ReleaseBuffer(bufferid);
			}
		}
	}
	PG_FINALLY();
	{
		/* restore the GUC */
		zenith_test_evict = save_zenith_test_evict;
	}
	PG_END_TRY();

	PG_RETURN_VOID();
}


/*
 * A version to read page for at a specific LSN
 */
Datum
get_raw_page_at_lsn(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	text	   *forkname = PG_GETARG_TEXT_PP(1);
	uint32		blkno = PG_GETARG_UINT32(2);
	uint64 read_lsn = PG_GETARG_INT64(3);

	bytea	   *raw_page;
	ForkNumber	forknum;
	RangeVar   *relrv;
	Relation	rel;
	char	   *raw_page_data;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	/* Check that this relation has storage */
	if (rel->rd_rel->relkind == RELKIND_VIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from view \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from composite type \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from foreign table \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from partitioned table \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get raw page from partitioned index \"%s\"",
						RelationGetRelationName(rel))));

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));


	forknum = forkname_to_number(text_to_cstring(forkname));

	if (blkno >= RelationGetNumberOfBlocksInFork(rel, forknum))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block number %u is out of range for relation \"%s\"",
						blkno, RelationGetRelationName(rel))));

	/* Initialize buffer to copy to */
	raw_page = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(raw_page, BLCKSZ + VARHDRSZ);
	raw_page_data = VARDATA(raw_page);

	zenith_read_at_lsn(rel->rd_smgr, forknum, blkno, read_lsn, false /* request_latest */, raw_page_data);

	relation_close(rel, AccessShareLock);

	PG_RETURN_BYTEA_P(raw_page);
}