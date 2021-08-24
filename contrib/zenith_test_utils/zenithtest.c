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
#include "fmgr.h"

#include "access/xact.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"


PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_consume_xids);
PG_FUNCTION_INFO_V1(clear_buffer_cache);

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
