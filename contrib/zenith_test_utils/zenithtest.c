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

#include "access/relation.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/varlena.h"
#include "zenith/pagestore_client.h"

PG_MODULE_MAGIC;

extern void _PG_init(void);

PG_FUNCTION_INFO_V1(test_consume_xids);
PG_FUNCTION_INFO_V1(clear_buffer_cache);
PG_FUNCTION_INFO_V1(get_raw_page_at_lsn);
PG_FUNCTION_INFO_V1(get_raw_page_at_lsn_ex);

/*
 * Linkage to functions in zenith module.
 * The signature here would need to be updated whenever function parameters change in pagestore_smgr.c
 */
typedef void (*zenith_read_at_lsn_type)(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno,
			XLogRecPtr request_lsn, bool request_latest, char *buffer);

static zenith_read_at_lsn_type zenith_read_at_lsn_ptr;

/*
 * Module initialize function: fetch function pointers for cross-module calls.
 */
void
_PG_init(void)
{
	/* Asserts verify that typedefs above match original declarations */
	AssertVariableIsOfType(&zenith_read_at_lsn, zenith_read_at_lsn_type);
	zenith_read_at_lsn_ptr = (zenith_read_at_lsn_type)
		load_external_function("$libdir/zenith", "zenith_read_at_lsn",
							   true, NULL);
}

#define zenith_read_at_lsn zenith_read_at_lsn_ptr

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
 * Reads the page from page server without buffer cache
 * usage mimics get_raw_page() in pageinspect, but offers reading versions at specific LSN
 * NULL read lsn will result in reading the latest version.
 *
 * Note: reading latest version will result in waiting for latest changes to reach the page server,
 *       if this is undesirable, use pageinspect' get_raw_page that uses buffered access to the latest page
 */
Datum
get_raw_page_at_lsn(PG_FUNCTION_ARGS)
{
	bytea	   *raw_page;
	ForkNumber	forknum;
	RangeVar   *relrv;
	Relation	rel;
	char	   *raw_page_data;
	text	   *relname;
	text	   *forkname;
	uint32		blkno;

	bool request_latest = PG_ARGISNULL(3);
	uint64 read_lsn = request_latest ? GetXLogInsertRecPtr() : PG_GETARG_INT64(3);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	relname = PG_GETARG_TEXT_PP(0);
	forkname = PG_GETARG_TEXT_PP(1);
	blkno = PG_GETARG_UINT32(2);

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

	/* Initialize buffer to copy to */
	raw_page = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(raw_page, BLCKSZ + VARHDRSZ);
	raw_page_data = VARDATA(raw_page);

	zenith_read_at_lsn(rel->rd_node, forknum, blkno, read_lsn, request_latest, raw_page_data);

	relation_close(rel, AccessShareLock);

	PG_RETURN_BYTEA_P(raw_page);
}

/*
 * Another option to read a relation page from page server without cache
 * this version doesn't validate input and allows reading blocks of dropped relations
 *
 * Note: reading latest version will result in waiting for latest changes to reach the page server,
 *  if this is undesirable, use pageinspect' get_raw_page that uses buffered access to the latest page
 */
Datum
get_raw_page_at_lsn_ex(PG_FUNCTION_ARGS)
{
	char	   *raw_page_data;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				errmsg("must be superuser to use raw page functions")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) ||
		PG_ARGISNULL(3) || PG_ARGISNULL(4))
		PG_RETURN_NULL();

	{
		RelFileNode rnode = {
			.spcNode = PG_GETARG_OID(0),
			.dbNode  = PG_GETARG_OID(1),
			.relNode = PG_GETARG_OID(2)
		};

		ForkNumber forknum = PG_GETARG_UINT32(3);

		uint32 blkno = PG_GETARG_UINT32(4);
		bool request_latest = PG_ARGISNULL(5);
		uint64 read_lsn = request_latest ? GetXLogInsertRecPtr() : PG_GETARG_INT64(5);


		/* Initialize buffer to copy to */
		bytea *raw_page = (bytea *) palloc(BLCKSZ + VARHDRSZ);
		SET_VARSIZE(raw_page, BLCKSZ + VARHDRSZ);
		raw_page_data = VARDATA(raw_page);

		zenith_read_at_lsn(rnode, forknum, blkno, read_lsn, request_latest, raw_page_data);
		PG_RETURN_BYTEA_P(raw_page);
	}
}
