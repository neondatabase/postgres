/*-------------------------------------------------------------------------
 *
 * pagestore_smgr.c
 *
 *
 *
 * Temporary and unlogged rels
 * ---------------------------
 *
 * Temporary and unlogged tables are stored locally, by md.c. The functions
 * here just pass the calls through to corresponding md.c functions.
 *
 * Index build operations that use the buffer cache are also handled locally,
 * just like unlogged tables. Such operations must be marked by calling
 * smgr_start_unlogged_build() and friends.
 *
 * In order to know what relations are permanent and which ones are not, we
 * have added a 'smgr_relpersistence' field to SmgrRelationData, and it is set
 * by smgropen() callers, when they have the relcache entry at hand.  However,
 * sometimes we need to open an SmgrRelation for a relation without the
 * relcache. That is needed when we evict a buffer; we might not have the
 * SmgrRelation for that relation open yet. To deal with that, the
 * 'relpersistence' can be left to zero, meaning we don't know if it's
 * permanent or not. Most operations are not allowed with relpersistence==0,
 * but smgrwrite() does work, which is what we need for buffer eviction.  and
 * smgrunlink() so that a backend doesn't need to have the relcache entry at
 * transaction commit, where relations that were dropped in the transaction
 * are unlinked.
 *
 * If smgrwrite() is called and smgr_relpersistence == 0, we check if the
 * relation file exists locally or not. If it does exist, we assume it's an
 * unlogged relation and write the page there. Otherwise it must be a
 * permanent relation, WAL-logged and stored on the page server, and we ignore
 * the write like we do for permanent relations.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/neon/pagestore_smgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlog_internal.h"
#include "catalog/pg_class.h"
#include "pagestore_client.h"
#include "pagestore_client.h"
#include "storage/smgr.h"
#include "access/xlogdefs.h"
#include "postmaster/interrupt.h"
#include "replication/walsender.h"
#include "storage/bufmgr.h"
#include "storage/md.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "catalog/pg_tablespace_d.h"
#include "postmaster/autovacuum.h"

/*
 * If DEBUG_COMPARE_LOCAL is defined, we pass through all the SMGR API
 * calls to md.c, and *also* do the calls to the Page Server. On every
 * read, compare the versions we read from local disk and Page Server,
 * and Assert that they are identical.
 */
/* #define DEBUG_COMPARE_LOCAL */

#ifdef DEBUG_COMPARE_LOCAL
#include "access/nbtree.h"
#include "storage/bufpage.h"
#include "access/xlog_internal.h"

static char *hexdump_page(char *page);
#endif

#define IS_LOCAL_REL(reln) (reln->smgr_rnode.node.dbNode != 0 && reln->smgr_rnode.node.relNode > FirstNormalObjectId)

const int	SmgrTrace = DEBUG5;

page_server_api *page_server;

/* GUCs */
char	   *page_server_connstring; // with substituted password
char	   *callmemaybe_connstring;
char	   *zenith_timeline;
char	   *zenith_tenant;
bool		wal_redo = false;
int32		max_cluster_size;

/* unlogged relation build states */
typedef enum
{
	UNLOGGED_BUILD_NOT_IN_PROGRESS = 0,
	UNLOGGED_BUILD_PHASE_1,
	UNLOGGED_BUILD_PHASE_2,
	UNLOGGED_BUILD_NOT_PERMANENT
} UnloggedBuildPhase;

static SMgrRelation unlogged_build_rel = NULL;
static UnloggedBuildPhase unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;

StringInfoData
zm_pack_request(ZenithRequest *msg)
{
	StringInfoData s;

	initStringInfo(&s);
	pq_sendbyte(&s, msg->tag);

	switch (messageTag(msg))
	{
			/* pagestore_client -> pagestore */
		case T_ZenithExistsRequest:
			{
				ZenithExistsRequest *msg_req = (ZenithExistsRequest *) msg;

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
				pq_sendint32(&s, msg_req->rnode.spcNode);
				pq_sendint32(&s, msg_req->rnode.dbNode);
				pq_sendint32(&s, msg_req->rnode.relNode);
				pq_sendbyte(&s, msg_req->forknum);

				break;
			}
		case T_ZenithNblocksRequest:
			{
				ZenithNblocksRequest *msg_req = (ZenithNblocksRequest *) msg;

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
				pq_sendint32(&s, msg_req->rnode.spcNode);
				pq_sendint32(&s, msg_req->rnode.dbNode);
				pq_sendint32(&s, msg_req->rnode.relNode);
				pq_sendbyte(&s, msg_req->forknum);

				break;
			}
		case T_ZenithDbSizeRequest:
			{
				ZenithDbSizeRequest *msg_req = (ZenithDbSizeRequest *) msg;

					pq_sendbyte(&s, msg_req->req.latest);
					pq_sendint64(&s, msg_req->req.lsn);
					pq_sendint32(&s, msg_req->dbNode);

					break;
			}
		case T_ZenithGetPageRequest:
			{
				ZenithGetPageRequest *msg_req = (ZenithGetPageRequest *) msg;

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
				pq_sendint32(&s, msg_req->rnode.spcNode);
				pq_sendint32(&s, msg_req->rnode.dbNode);
				pq_sendint32(&s, msg_req->rnode.relNode);
				pq_sendbyte(&s, msg_req->forknum);
				pq_sendint32(&s, msg_req->blkno);

				break;
			}

			/* pagestore -> pagestore_client. We never need to create these. */
		case T_ZenithExistsResponse:
		case T_ZenithNblocksResponse:
		case T_ZenithGetPageResponse:
		case T_ZenithErrorResponse:
		case T_ZenithDbSizeResponse:
		default:
			elog(ERROR, "unexpected zenith message tag 0x%02x", msg->tag);
			break;
	}
	return s;
}

ZenithResponse *
zm_unpack_response(StringInfo s)
{
	ZenithMessageTag tag = pq_getmsgbyte(s);
	ZenithResponse *resp = NULL;

	switch (tag)
	{
			/* pagestore -> pagestore_client */
		case T_ZenithExistsResponse:
			{
				ZenithExistsResponse *msg_resp = palloc0(sizeof(ZenithExistsResponse));

				msg_resp->tag = tag;
				msg_resp->exists = pq_getmsgbyte(s);
				pq_getmsgend(s);

				resp = (ZenithResponse *) msg_resp;
				break;
			}

		case T_ZenithNblocksResponse:
			{
				ZenithNblocksResponse *msg_resp = palloc0(sizeof(ZenithNblocksResponse));

				msg_resp->tag = tag;
				msg_resp->n_blocks = pq_getmsgint(s, 4);
				pq_getmsgend(s);

				resp = (ZenithResponse *) msg_resp;
				break;
			}

		case T_ZenithGetPageResponse:
			{
				ZenithGetPageResponse *msg_resp = palloc0(offsetof(ZenithGetPageResponse, page) + BLCKSZ);

				msg_resp->tag = tag;
				/* XXX:	should be varlena */
				memcpy(msg_resp->page, pq_getmsgbytes(s, BLCKSZ), BLCKSZ);
				pq_getmsgend(s);

				resp = (ZenithResponse *) msg_resp;
				break;
			}

		case T_ZenithDbSizeResponse:
			{
				ZenithDbSizeResponse *msg_resp = palloc0(sizeof(ZenithDbSizeResponse));

				msg_resp->tag = tag;
				msg_resp->db_size = pq_getmsgint64(s);
				pq_getmsgend(s);

				resp = (ZenithResponse *) msg_resp;
				break;
			}

		case T_ZenithErrorResponse:
			{
				ZenithErrorResponse *msg_resp;
				size_t		msglen;
				const char *msgtext;

				msgtext = pq_getmsgrawstring(s);
				msglen = strlen(msgtext);

				msg_resp = palloc0(sizeof(ZenithErrorResponse) + msglen + 1);
				msg_resp->tag = tag;
				memcpy(msg_resp->message, msgtext, msglen + 1);
				pq_getmsgend(s);

				resp = (ZenithResponse *) msg_resp;
				break;
			}

			/*
			 * pagestore_client -> pagestore
			 *
			 * We create these ourselves, and don't need to decode them.
			 */
		case T_ZenithExistsRequest:
		case T_ZenithNblocksRequest:
		case T_ZenithGetPageRequest:
		case T_ZenithDbSizeRequest:
		default:
			elog(ERROR, "unexpected zenith message tag 0x%02x", tag);
			break;
	}

	return resp;
}

/* dump to json for debugging / error reporting purposes */
char *
zm_to_string(ZenithMessage *msg)
{
	StringInfoData s;

	initStringInfo(&s);

	switch (messageTag(msg))
	{
			/* pagestore_client -> pagestore */
		case T_ZenithExistsRequest:
			{
				ZenithExistsRequest *msg_req = (ZenithExistsRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"ZenithExistsRequest\"");
				appendStringInfo(&s, ", \"rnode\": \"%u/%u/%u\"",
								 msg_req->rnode.spcNode,
								 msg_req->rnode.dbNode,
								 msg_req->rnode.relNode);
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
				appendStringInfoChar(&s, '}');
				break;
			}

		case T_ZenithNblocksRequest:
			{
				ZenithNblocksRequest *msg_req = (ZenithNblocksRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"ZenithNblocksRequest\"");
				appendStringInfo(&s, ", \"rnode\": \"%u/%u/%u\"",
								 msg_req->rnode.spcNode,
								 msg_req->rnode.dbNode,
								 msg_req->rnode.relNode);
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
				appendStringInfoChar(&s, '}');
				break;
			}

		case T_ZenithGetPageRequest:
			{
				ZenithGetPageRequest *msg_req = (ZenithGetPageRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"ZenithGetPageRequest\"");
				appendStringInfo(&s, ", \"rnode\": \"%u/%u/%u\"",
								 msg_req->rnode.spcNode,
								 msg_req->rnode.dbNode,
								 msg_req->rnode.relNode);
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"blkno\": %u", msg_req->blkno);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_ZenithDbSizeRequest:
			{
				ZenithDbSizeRequest *msg_req = (ZenithDbSizeRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"ZenithDbSizeRequest\"");
				appendStringInfo(&s, ", \"dbnode\": \"%u\"", msg_req->dbNode);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
				appendStringInfoChar(&s, '}');
				break;
			}


			/* pagestore -> pagestore_client */
		case T_ZenithExistsResponse:
			{
				ZenithExistsResponse *msg_resp = (ZenithExistsResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"ZenithExistsResponse\"");
				appendStringInfo(&s, ", \"exists\": %d}",
								 msg_resp->exists
					);
				appendStringInfoChar(&s, '}');

				break;
			}
		case T_ZenithNblocksResponse:
			{
				ZenithNblocksResponse *msg_resp = (ZenithNblocksResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"ZenithNblocksResponse\"");
				appendStringInfo(&s, ", \"n_blocks\": %u}",
								 msg_resp->n_blocks
					);
				appendStringInfoChar(&s, '}');

				break;
			}
		case T_ZenithGetPageResponse:
			{
#if 0
				ZenithGetPageResponse *msg_resp = (ZenithGetPageResponse *) msg;
#endif

				appendStringInfoString(&s, "{\"type\": \"ZenithGetPageResponse\"");
				appendStringInfo(&s, ", \"page\": \"XXX\"}");
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_ZenithErrorResponse:
			{
				ZenithErrorResponse *msg_resp = (ZenithErrorResponse *) msg;

				/* FIXME: escape double-quotes in the message */
				appendStringInfoString(&s, "{\"type\": \"ZenithErrorResponse\"");
				appendStringInfo(&s, ", \"message\": \"%s\"}", msg_resp->message);
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_ZenithDbSizeResponse:
			{
				ZenithDbSizeResponse *msg_resp = (ZenithDbSizeResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"ZenithDbSizeResponse\"");
				appendStringInfo(&s, ", \"db_size\": %ld}",
								 msg_resp->db_size
					);
				appendStringInfoChar(&s, '}');

				break;
			}

		default:
			appendStringInfo(&s, "{\"type\": \"unknown 0x%02x\"", msg->tag);
	}
	return s.data;
}

/*
 * Wrapper around log_newpage() that makes a temporary copy of the block and
 * WAL-logs that. This makes it safe to use while holding only a shared lock
 * on the page, see XLogSaveBufferForHint. We don't use XLogSaveBufferForHint
 * directly because it skips the logging if the LSN is new enough.
 */
static XLogRecPtr
log_newpage_copy(RelFileNode *rnode, ForkNumber forkNum, BlockNumber blkno,
				 Page page, bool page_std)
{
	PGAlignedBlock copied_buffer;

	memcpy(copied_buffer.data, page, BLCKSZ);
	return log_newpage(rnode, forkNum, blkno, copied_buffer.data, page_std);
}

/*
 * Is 'buffer' identical to a freshly initialized empty heap page?
 */
static bool
PageIsEmptyHeapPage(char *buffer)
{
	PGAlignedBlock empty_page;

	PageInit((Page) empty_page.data, BLCKSZ, 0);

	return memcmp(buffer, empty_page.data, BLCKSZ) == 0;
}

static void
zenith_wallog_page(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer)
{
	XLogRecPtr	lsn = PageGetLSN(buffer);

	if (ShutdownRequestPending)
		return;

	/*
	 * Whenever a VM or FSM page is evicted, WAL-log it. FSM and (some) VM
	 * changes are not WAL-logged when the changes are made, so this is our
	 * last chance to log them, otherwise they're lost. That's OK for
	 * correctness, the non-logged updates are not critical. But we want to
	 * have a reasonably up-to-date VM and FSM in the page server.
	 */
	if (forknum == FSM_FORKNUM && !RecoveryInProgress())
	{
		/* FSM is never WAL-logged and we don't care. */
		XLogRecPtr	recptr;

		recptr = log_newpage_copy(&reln->smgr_rnode.node, forknum, blocknum, buffer, false);
		XLogFlush(recptr);
		lsn = recptr;
		ereport(SmgrTrace,
				(errmsg("FSM page %u of relation %u/%u/%u.%u was force logged. Evicted at lsn=%X/%X",
						blocknum,
						reln->smgr_rnode.node.spcNode,
						reln->smgr_rnode.node.dbNode,
						reln->smgr_rnode.node.relNode,
						forknum, LSN_FORMAT_ARGS(lsn))));
	}
	else if (forknum == VISIBILITYMAP_FORKNUM && !RecoveryInProgress())
	{
		/*
		 * Always WAL-log vm. We should never miss clearing visibility map
		 * bits.
		 *
		 * TODO Is it too bad for performance? Hopefully we do not evict
		 * actively used vm too often.
		 */
		XLogRecPtr	recptr;

		recptr = log_newpage_copy(&reln->smgr_rnode.node, forknum, blocknum, buffer, false);
		XLogFlush(recptr);
		lsn = recptr;

		ereport(SmgrTrace,
				(errmsg("Visibilitymap page %u of relation %u/%u/%u.%u was force logged at lsn=%X/%X",
						blocknum,
						reln->smgr_rnode.node.spcNode,
						reln->smgr_rnode.node.dbNode,
						reln->smgr_rnode.node.relNode,
						forknum, LSN_FORMAT_ARGS(lsn))));
	}
	else if (lsn == InvalidXLogRecPtr)
	{
		/*
		 * When PostgreSQL extends a relation, it calls smgrextend() with an all-zeros pages,
		 * and we can just ignore that in Zenith. We do need to remember the new size,
		 * though, so that smgrnblocks() returns the right answer after the rel has
		 * been extended. We rely on the relsize cache for that.
		 *
		 * A completely empty heap page doesn't need to be WAL-logged, either. The
		 * heapam can leave such a page behind, if e.g. an insert errors out after
		 * initializing the page, but before it has inserted the tuple and WAL-logged
		 * the change. When we read the page from the page server, it will come back
		 * as all-zeros. That's OK, the heapam will initialize an all-zeros page on
		 * first use.
		 *
		 * In other scenarios, evicting a dirty page with no LSN is a bad sign: it implies
		 * that the page was not WAL-logged, and its contents will be lost when it's
		 * evicted.
		 */
		if (PageIsNew(buffer))
		{
			ereport(SmgrTrace,
					(errmsg("Page %u of relation %u/%u/%u.%u is all-zeros",
							blocknum,
							reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							reln->smgr_rnode.node.relNode,
							forknum)));
		}
		else if (PageIsEmptyHeapPage(buffer))
		{
			ereport(SmgrTrace,
					(errmsg("Page %u of relation %u/%u/%u.%u is an empty heap page with no LSN",
							blocknum,
							reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							reln->smgr_rnode.node.relNode,
							forknum)));
		}
		else
		{
			ereport(PANIC,
					(errmsg("Page %u of relation %u/%u/%u.%u is evicted with zero LSN",
							blocknum,
							reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							reln->smgr_rnode.node.relNode,
							forknum)));
		}
	}
	else
	{
		ereport(SmgrTrace,
				(errmsg("Page %u of relation %u/%u/%u.%u is already wal logged at lsn=%X/%X",
						blocknum,
						reln->smgr_rnode.node.spcNode,
						reln->smgr_rnode.node.dbNode,
						reln->smgr_rnode.node.relNode,
						forknum, LSN_FORMAT_ARGS(lsn))));
	}

	/*
	 * Remember the LSN on this page. When we read the page again, we must
	 * read the same or newer version of it.
	 */
	SetLastWrittenPageLSN(lsn);
}


/*
 *	zenith_init() -- Initialize private state
 */
void
zenith_init(void)
{
	/* noop */
#ifdef DEBUG_COMPARE_LOCAL
	mdinit();
#endif
}

/*
 * GetXLogInsertRecPtr uses XLogBytePosToRecPtr to convert logical insert (reserved) position
 * to physical position in WAL. It always adds SizeOfXLogShortPHD:
 *		seg_offset += fullpages * XLOG_BLCKSZ + bytesleft + SizeOfXLogShortPHD;
 * so even if there are no records on the page, offset will be SizeOfXLogShortPHD.
 * It may cause problems with XLogFlush. So return pointer backward to the origin of the page.
 */
static XLogRecPtr
zm_adjust_lsn(XLogRecPtr lsn)
{
	/*
	 * If lsn points to the beging of first record on page or segment, then
	 * "return" it back to the page origin
	 */
	if ((lsn & (XLOG_BLCKSZ - 1)) == SizeOfXLogShortPHD)
	{
		lsn -= SizeOfXLogShortPHD;
	}
	else if ((lsn & (wal_segment_size - 1)) == SizeOfXLogLongPHD)
	{
		lsn -= SizeOfXLogLongPHD;
	}
	return lsn;
}

/*
 * Return LSN for requesting pages and number of blocks from page server
 */
static XLogRecPtr
zenith_get_request_lsn(bool *latest)
{
	XLogRecPtr	lsn;

	if (RecoveryInProgress())
	{
		*latest = false;
		lsn = GetXLogReplayRecPtr(NULL);
		elog(DEBUG1, "zenith_get_request_lsn GetXLogReplayRecPtr %X/%X request lsn 0 ",
			 (uint32) ((lsn) >> 32), (uint32) (lsn));
	}
	else if (am_walsender)
	{
		*latest = true;
		lsn = InvalidXLogRecPtr;
		elog(DEBUG1, "am walsender zenith_get_request_lsn lsn 0 ");
	}
	else
	{
		XLogRecPtr	flushlsn;

		/*
		 * Use the latest LSN that was evicted from the buffer cache. Any
		 * pages modified by later WAL records must still in the buffer cache,
		 * so our request cannot concern those.
		 */
		*latest = true;
		lsn = GetLastWrittenPageLSN();
		Assert(lsn != InvalidXLogRecPtr);
		elog(DEBUG1, "zenith_get_request_lsn GetLastWrittenPageLSN lsn %X/%X ",
			 (uint32) ((lsn) >> 32), (uint32) (lsn));

		lsn = zm_adjust_lsn(lsn);

		/*
		 * Is it possible that the last-written LSN is ahead of last flush
		 * LSN? Generally not, we shouldn't evict a page from the buffer cache
		 * before all its modifications have been safely flushed. That's the
		 * "WAL before data" rule. However, such case does exist at index building,
		 * _bt_blwritepage logs the full page without flushing WAL before
		 * smgrextend (files are fsynced before build ends).
		 */
		flushlsn = GetFlushRecPtr();
		if (lsn > flushlsn)
		{
			elog(DEBUG5, "last-written LSN %X/%X is ahead of last flushed LSN %X/%X",
				 (uint32) (lsn >> 32), (uint32) lsn,
				 (uint32) (flushlsn >> 32), (uint32) flushlsn);
			XLogFlush(lsn);
		}
	}

	return lsn;
}


/*
 *	zenith_exists() -- Does the physical file exist?
 */
bool
zenith_exists(SMgrRelation reln, ForkNumber forkNum)
{
	bool		exists;
	ZenithResponse *resp;
	BlockNumber n_blocks;
	bool		latest;
	XLogRecPtr	request_lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			/*
			 * We don't know if it's an unlogged rel stored locally, or permanent
			 * rel stored in the page server. First check if it exists locally.
			 * If it does, great. Otherwise check if it exists in the page server.
			 */
			if (mdexists(reln, forkNum))
				return true;
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdexists(reln, forkNum);

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (get_cached_relsize(reln->smgr_rnode.node, forkNum, &n_blocks))
	{
		return true;
	}

	/*
	 * \d+ on a view calls smgrexists with 0/0/0 relfilenode. The page server
	 * will error out if you check that, because the whole dbdir for tablespace
	 * 0, db 0 doesn't exists. We possibly should change the page server to
	 * accept that and return 'false', to be consistent with mdexists(). But
	 * we probably also should fix pg_table_size() to not call smgrexists()
	 * with bogus relfilenode.
	 *
	 * For now, handle that special case here.
	 */
	if (reln->smgr_rnode.node.spcNode == 0 &&
		reln->smgr_rnode.node.dbNode == 0 &&
		reln->smgr_rnode.node.relNode == 0)
	{
		return false;
	}

	request_lsn = zenith_get_request_lsn(&latest);
	{
		ZenithExistsRequest request = {
			.req.tag = T_ZenithExistsRequest,
			.req.latest = latest,
			.req.lsn = request_lsn,
			.rnode = reln->smgr_rnode.node,
			.forknum = forkNum
		};

		resp = page_server->request((ZenithRequest *) &request);
	}

	switch (resp->tag)
	{
		case T_ZenithExistsResponse:
			exists = ((ZenithExistsResponse *) resp)->exists;
			break;

		case T_ZenithErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not read relation existence of rel %u/%u/%u.%u from page server at lsn %X/%08X",
							reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							reln->smgr_rnode.node.relNode,
							forkNum,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((ZenithErrorResponse *) resp)->message)));
			break;

		default:
			elog(ERROR, "unexpected response from page server with tag 0x%02x", resp->tag);
	}
	pfree(resp);
	return exists;
}

/*
 *	zenith_create() -- Create a new relation on zenithd storage
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void
zenith_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgrcreate() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdcreate(reln, forkNum, isRedo);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	elog(SmgrTrace, "Create relation %u/%u/%u.%u",
		 reln->smgr_rnode.node.spcNode,
		 reln->smgr_rnode.node.dbNode,
		 reln->smgr_rnode.node.relNode,
		 forkNum);

	/*
	 * Newly created relation is empty, remember that in the relsize cache.
	 *
	 * FIXME: This is currently not just an optimization, but required for
	 * correctness. Postgres can call smgrnblocks() on the newly-created
	 * relation. Currently, we don't call SetLastWrittenPageLSN() when a new
	 * relation created, so if we didn't remember the size in the relsize
	 * cache, we might call smgrnblocks() on the newly-created relation before
	 * the creation WAL record hass been received by the page server.
	 */
	set_cached_relsize(reln->smgr_rnode.node, forkNum, 0);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdcreate(reln, forkNum, isRedo);
#endif
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
	/*
	 * Might or might not exist locally, depending on whether it's
	 * an unlogged or permanent relation (or if DEBUG_COMPARE_LOCAL is
	 * set). Try to unlink, it won't do any harm if the file doesn't
	 * exist.
	 */
	mdunlink(rnode, forkNum, isRedo);
	if (!RelFileNodeBackendIsTemp(rnode)) {
		forget_cached_relsize(rnode.node, forkNum);
	}
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
zenith_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno,
			  char *buffer, bool skipFsync)
{
	XLogRecPtr	lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgrextend() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdextend(reln, forkNum, blkno, buffer, skipFsync);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	/*
	 * Check that the cluster size limit has not been exceeded.
	 *
	 * Temporary and unlogged relations are not included in the cluster size measured
	 * by the page server, so ignore those. Autovacuum processes are also exempt.
	 */
	if (max_cluster_size > 0 &&
		reln->smgr_relpersistence == RELPERSISTENCE_PERMANENT &&
		!IsAutoVacuumWorkerProcess())
	{
		uint64		current_size = GetZenithCurrentClusterSize();

		if (current_size >= ((uint64) max_cluster_size) * 1024 * 1024)
			ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
					errmsg("could not extend file because cluster size limit (%d MB) has been exceeded",
						   max_cluster_size),
					errhint("This limit is defined by neon.max_cluster_size GUC")));
	}

	zenith_wallog_page(reln, forkNum, blkno, buffer);
	set_cached_relsize(reln->smgr_rnode.node, forkNum, blkno + 1);

	lsn = PageGetLSN(buffer);
	elog(SmgrTrace, "smgrextend called for %u/%u/%u.%u blk %u, page LSN: %X/%08X",
		 reln->smgr_rnode.node.spcNode,
		 reln->smgr_rnode.node.dbNode,
		 reln->smgr_rnode.node.relNode,
		 forkNum, blkno,
		 (uint32) (lsn >> 32), (uint32) lsn);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdextend(reln, forkNum, blkno, buffer, skipFsync);
#endif
}

/*
 *  zenith_open() -- Initialize newly-opened relation.
 */
void
zenith_open(SMgrRelation reln)
{
	/*
	 * We don't have anything special to do here. Call mdopen() to let md.c
	 * initialize itself. That's only needed for temporary or unlogged
	 * relations, but it's dirt cheap so do it always to make sure the md
	 * fields are initialized, for debugging purposes if nothing else.
	 */
	mdopen(reln);

	/* no work */
	elog(SmgrTrace, "[ZENITH_SMGR] open noop");
}

/*
 *	zenith_close() -- Close the specified relation, if it isn't closed already.
 */
void
zenith_close(SMgrRelation reln, ForkNumber forknum)
{
	/*
	 * Let md.c close it, if it had it open. Doesn't hurt to do this
	 * even for permanent relations that have no local storage.
	 */
	mdclose(reln, forknum);
}

/*
 *	zenith_prefetch() -- Initiate asynchronous read of the specified block of a relation
 */
bool
zenith_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			/* probably shouldn't happen, but ignore it */
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdprefetch(reln, forknum, blocknum);

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

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
	switch (reln->smgr_relpersistence)
	{
		case 0:
			/* mdwriteback() does nothing if the file doesn't exist */
			mdwriteback(reln, forknum, blocknum, nblocks);
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdwriteback(reln, forknum, blocknum, nblocks);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	/* not implemented */
	elog(SmgrTrace, "[ZENITH_SMGR] writeback noop");

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdwriteback(reln, forknum, blocknum, nblocks);
#endif
}

/*
 * While function is defined in the zenith extension it's used within neon_test_utils directly.
 * To avoid breaking tests in the runtime please keep function signature in sync.
 */
void zenith_read_at_lsn(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno,
			XLogRecPtr request_lsn, bool request_latest, char *buffer)
{
	ZenithResponse *resp;

	{
		ZenithGetPageRequest request = {
			.req.tag = T_ZenithGetPageRequest,
			.req.latest = request_latest,
			.req.lsn = request_lsn,
			.rnode = rnode,
			.forknum = forkNum,
			.blkno = blkno
		};

		resp = page_server->request((ZenithRequest *) &request);
	}

	switch (resp->tag)
	{
		case T_ZenithGetPageResponse:
			memcpy(buffer, ((ZenithGetPageResponse *) resp)->page, BLCKSZ);
			break;

		case T_ZenithErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not read block %u in rel %u/%u/%u.%u from page server at lsn %X/%08X",
							blkno,
							rnode.spcNode,
							rnode.dbNode,
							rnode.relNode,
							forkNum,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((ZenithErrorResponse *) resp)->message)));
			break;

		default:
			elog(ERROR, "unexpected response from page server with tag 0x%02x", resp->tag);
	}

	pfree(resp);
}

/*
 *	zenith_read() -- Read the specified block from a relation.
 */
void
zenith_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno,
			char *buffer)
{
	bool		latest;
	XLogRecPtr	request_lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgrread() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdread(reln, forkNum, blkno, buffer);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	request_lsn = zenith_get_request_lsn(&latest);
	zenith_read_at_lsn(reln->smgr_rnode.node, forkNum, blkno, request_lsn, latest, buffer);

#ifdef DEBUG_COMPARE_LOCAL
	if (forkNum == MAIN_FORKNUM && IS_LOCAL_REL(reln))
	{
		char		pageserver_masked[BLCKSZ];
		char		mdbuf[BLCKSZ];
		char		mdbuf_masked[BLCKSZ];

		mdread(reln, forkNum, blkno, mdbuf);

		memcpy(pageserver_masked, buffer, BLCKSZ);
		memcpy(mdbuf_masked, mdbuf, BLCKSZ);

		if (PageIsNew(mdbuf))
		{
			if (!PageIsNew(pageserver_masked))
			{
				elog(PANIC, "page is new in MD but not in Page Server at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n%s\n",
					 blkno,
					 reln->smgr_rnode.node.spcNode,
					 reln->smgr_rnode.node.dbNode,
					 reln->smgr_rnode.node.relNode,
					 forkNum,
					 (uint32) (request_lsn >> 32), (uint32) request_lsn,
					 hexdump_page(buffer));
			}
		}
		else if (PageIsNew(buffer))
		{
			elog(PANIC, "page is new in Page Server but not in MD at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n%s\n",
				 blkno,
				 reln->smgr_rnode.node.spcNode,
				 reln->smgr_rnode.node.dbNode,
				 reln->smgr_rnode.node.relNode,
				 forkNum,
				 (uint32) (request_lsn >> 32), (uint32) request_lsn,
				 hexdump_page(mdbuf));
		}
		else if (PageGetSpecialSize(mdbuf) == 0)
		{
			/* assume heap */
			RmgrTable[RM_HEAP_ID].rm_mask(mdbuf_masked, blkno);
			RmgrTable[RM_HEAP_ID].rm_mask(pageserver_masked, blkno);

			if (memcmp(mdbuf_masked, pageserver_masked, BLCKSZ) != 0)
			{
				elog(PANIC, "heap buffers differ at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n------ MD ------\n%s\n------ Page Server ------\n%s\n",
					 blkno,
					 reln->smgr_rnode.node.spcNode,
					 reln->smgr_rnode.node.dbNode,
					 reln->smgr_rnode.node.relNode,
					 forkNum,
					 (uint32) (request_lsn >> 32), (uint32) request_lsn,
					 hexdump_page(mdbuf_masked),
					 hexdump_page(pageserver_masked));
			}
		}
		else if (PageGetSpecialSize(mdbuf) == MAXALIGN(sizeof(BTPageOpaqueData)))
		{
			if (((BTPageOpaqueData *) PageGetSpecialPointer(mdbuf))->btpo_cycleid < MAX_BT_CYCLE_ID)
			{
				/* assume btree */
				RmgrTable[RM_BTREE_ID].rm_mask(mdbuf_masked, blkno);
				RmgrTable[RM_BTREE_ID].rm_mask(pageserver_masked, blkno);

				if (memcmp(mdbuf_masked, pageserver_masked, BLCKSZ) != 0)
				{
					elog(PANIC, "btree buffers differ at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n------ MD ------\n%s\n------ Page Server ------\n%s\n",
						 blkno,
						 reln->smgr_rnode.node.spcNode,
						 reln->smgr_rnode.node.dbNode,
						 reln->smgr_rnode.node.relNode,
						 forkNum,
						 (uint32) (request_lsn >> 32), (uint32) request_lsn,
						 hexdump_page(mdbuf_masked),
						 hexdump_page(pageserver_masked));
				}
			}
		}
	}
#endif
}

#ifdef DEBUG_COMPARE_LOCAL
static char *
hexdump_page(char *page)
{
	StringInfoData result;

	initStringInfo(&result);

	for (int i = 0; i < BLCKSZ; i++)
	{
		if (i % 8 == 0)
			appendStringInfo(&result, " ");
		if (i % 40 == 0)
			appendStringInfo(&result, "\n");
		appendStringInfo(&result, "%02x", (unsigned char) (page[i]));
	}

	return result.data;
}
#endif

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
	XLogRecPtr	lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			/* This is a bit tricky. Check if the relation exists locally */
			if (mdexists(reln, forknum))
			{
				/* It exists locally. Guess it's unlogged then. */
				mdwrite(reln, forknum, blocknum, buffer, skipFsync);

				/*
				 * We could set relpersistence now that we have determined
				 * that it's local. But we don't dare to do it, because that
				 * would immediately allow reads as well, which shouldn't
				 * happen. We could cache it with a different 'relpersistence'
				 * value, but this isn't performance critical.
				 */
				return;
			}
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdwrite(reln, forknum, blocknum, buffer, skipFsync);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	zenith_wallog_page(reln, forknum, blocknum, buffer);

	lsn = PageGetLSN(buffer);
	elog(SmgrTrace, "smgrwrite called for %u/%u/%u.%u blk %u, page LSN: %X/%08X",
		 reln->smgr_rnode.node.spcNode,
		 reln->smgr_rnode.node.dbNode,
		 reln->smgr_rnode.node.relNode,
		 forknum, blocknum,
		 (uint32) (lsn >> 32), (uint32) lsn);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdwrite(reln, forknum, blocknum, buffer, skipFsync);
#endif
}

/*
 *	zenith_nblocks() -- Get the number of blocks stored in a relation.
 */
BlockNumber
zenith_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	ZenithResponse *resp;
	BlockNumber n_blocks;
	bool		latest;
	XLogRecPtr	request_lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgrnblocks() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdnblocks(reln, forknum);

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (get_cached_relsize(reln->smgr_rnode.node, forknum, &n_blocks))
	{
		elog(SmgrTrace, "cached nblocks for %u/%u/%u.%u: %u blocks",
			 reln->smgr_rnode.node.spcNode,
			 reln->smgr_rnode.node.dbNode,
			 reln->smgr_rnode.node.relNode,
			 forknum, n_blocks);
		return n_blocks;
	}

	request_lsn = zenith_get_request_lsn(&latest);
	{
		ZenithNblocksRequest request = {
			.req.tag = T_ZenithNblocksRequest,
			.req.latest = latest,
			.req.lsn = request_lsn,
			.rnode = reln->smgr_rnode.node,
			.forknum = forknum,
		};

		resp = page_server->request((ZenithRequest *) &request);
	}

	switch (resp->tag)
	{
		case T_ZenithNblocksResponse:
			n_blocks = ((ZenithNblocksResponse *) resp)->n_blocks;
			break;

		case T_ZenithErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not read relation size of rel %u/%u/%u.%u from page server at lsn %X/%08X",
							reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							reln->smgr_rnode.node.relNode,
							forknum,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((ZenithErrorResponse *) resp)->message)));
			break;

		default:
			elog(ERROR, "unexpected response from page server with tag 0x%02x", resp->tag);
	}
	update_cached_relsize(reln->smgr_rnode.node, forknum, n_blocks);

	elog(SmgrTrace, "zenith_nblocks: rel %u/%u/%u fork %u (request LSN %X/%08X): %u blocks",
		 reln->smgr_rnode.node.spcNode,
		 reln->smgr_rnode.node.dbNode,
		 reln->smgr_rnode.node.relNode,
		 forknum,
		 (uint32) (request_lsn >> 32), (uint32) request_lsn,
		 n_blocks);

	pfree(resp);
	return n_blocks;
}

/*
 *	zenith_db_size() -- Get the size of the database in bytes.
 */
int64
zenith_dbsize(Oid dbNode)
{
	ZenithResponse *resp;
	int64 db_size;
	XLogRecPtr request_lsn;
	bool		latest;

	request_lsn = zenith_get_request_lsn(&latest);
	{
		ZenithDbSizeRequest request = {
			.req.tag = T_ZenithDbSizeRequest,
			.req.latest = latest,
			.req.lsn = request_lsn,
			.dbNode = dbNode,
		};

		resp = page_server->request((ZenithRequest *) &request);
	}

	switch (resp->tag)
	{
		case T_ZenithDbSizeResponse:
			db_size = ((ZenithDbSizeResponse *) resp)->db_size;
			break;

		case T_ZenithErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not read db size of db %u from page server at lsn %X/%08X",
							dbNode,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((ZenithErrorResponse *) resp)->message)));
			break;

		default:
			elog(ERROR, "unexpected response from page server with tag 0x%02x", resp->tag);
	}

	elog(SmgrTrace, "zenith_dbsize: db %u (request LSN %X/%08X): %ld bytes",
		 dbNode,
		 (uint32) (request_lsn >> 32), (uint32) request_lsn,
		 db_size);

	pfree(resp);
	return db_size;
}

/*
 *	zenith_truncate() -- Truncate relation to specified number of blocks.
 */
void
zenith_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	XLogRecPtr	lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgrtruncate() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdtruncate(reln, forknum, nblocks);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	set_cached_relsize(reln->smgr_rnode.node, forknum, nblocks);

	/*
	 * Truncating a relation drops all its buffers from the buffer cache
	 * without calling smgrwrite() on them. But we must account for that in
	 * our tracking of last-written-LSN all the same: any future smgrnblocks()
	 * request must return the new size after the truncation. We don't know
	 * what the LSN of the truncation record was, so be conservative and use
	 * the most recently inserted WAL record's LSN.
	 */
	lsn = GetXLogInsertRecPtr();

	lsn = zm_adjust_lsn(lsn);

	/*
	 * Flush it, too. We don't actually care about it here, but let's uphold
	 * the invariant that last-written LSN <= flush LSN.
	 */
	XLogFlush(lsn);

	SetLastWrittenPageLSN(lsn);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdtruncate(reln, forknum, nblocks);
#endif
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
	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgrimmedsync() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdimmedsync(reln, forknum);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	elog(SmgrTrace, "[ZENITH_SMGR] immedsync noop");

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdimmedsync(reln, forknum);
#endif
}

/*
 * zenith_start_unlogged_build() -- Starting build operation on a rel.
 *
 * Some indexes are built in two phases, by first populating the table with
 * regular inserts, using the shared buffer cache but skipping WAL-logging,
 * and WAL-logging the whole relation after it's done. Zenith relies on the
 * WAL to reconstruct pages, so we cannot use the page server in the
 * first phase when the changes are not logged.
 */
static void
zenith_start_unlogged_build(SMgrRelation reln)
{
	/*
	 * Currently, there can be only one unlogged relation build operation in
	 * progress at a time. That's enough for the current usage.
	 */
	if (unlogged_build_phase != UNLOGGED_BUILD_NOT_IN_PROGRESS)
		elog(ERROR, "unlogged relation build is already in progress");
	Assert(unlogged_build_rel == NULL);

	ereport(SmgrTrace,
			(errmsg("starting unlogged build of relation %u/%u/%u",
					reln->smgr_rnode.node.spcNode,
					reln->smgr_rnode.node.dbNode,
					reln->smgr_rnode.node.relNode)));

	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgr_start_unlogged_build() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			unlogged_build_rel = reln;
			unlogged_build_phase = UNLOGGED_BUILD_NOT_PERMANENT;
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (smgrnblocks(reln, MAIN_FORKNUM) != 0)
		elog(ERROR, "cannot perform unlogged index build, index is not empty ");

	unlogged_build_rel = reln;
	unlogged_build_phase = UNLOGGED_BUILD_PHASE_1;

	/* Make the relation look like it's unlogged */
	reln->smgr_relpersistence = RELPERSISTENCE_UNLOGGED;

	/*
	 * FIXME: should we pass isRedo true to create the tablespace dir if it
	 * doesn't exist? Is it needed?
	 */
	mdcreate(reln, MAIN_FORKNUM, false);
}

/*
 * zenith_finish_unlogged_build_phase_1()
 *
 * Call this after you have finished populating a relation in unlogged mode,
 * before you start WAL-logging it.
 */
static void
zenith_finish_unlogged_build_phase_1(SMgrRelation reln)
{
	Assert(unlogged_build_rel == reln);

	ereport(SmgrTrace,
			(errmsg("finishing phase 1 of unlogged build of relation %u/%u/%u",
					reln->smgr_rnode.node.spcNode,
					reln->smgr_rnode.node.dbNode,
					reln->smgr_rnode.node.relNode)));

	if (unlogged_build_phase == UNLOGGED_BUILD_NOT_PERMANENT)
		return;

	Assert(unlogged_build_phase == UNLOGGED_BUILD_PHASE_1);
	Assert(reln->smgr_relpersistence == RELPERSISTENCE_UNLOGGED);

	unlogged_build_phase = UNLOGGED_BUILD_PHASE_2;
}

/*
 * zenith_end_unlogged_build() -- Finish an unlogged rel build.
 *
 * Call this after you have finished WAL-logging an relation that was
 * first populated without WAL-logging.
 *
 * This removes the local copy of the rel, since it's now been fully
 * WAL-logged and is present in the page server.
 */
static void
zenith_end_unlogged_build(SMgrRelation reln)
{
	Assert(unlogged_build_rel == reln);

	ereport(SmgrTrace,
			(errmsg("ending unlogged build of relation %u/%u/%u",
					reln->smgr_rnode.node.spcNode,
					reln->smgr_rnode.node.dbNode,
					reln->smgr_rnode.node.relNode)));

	if (unlogged_build_phase != UNLOGGED_BUILD_NOT_PERMANENT)
	{
		RelFileNodeBackend rnode;

		Assert(unlogged_build_phase == UNLOGGED_BUILD_PHASE_2);
		Assert(reln->smgr_relpersistence == RELPERSISTENCE_UNLOGGED);

		/* Make the relation look permanent again */
		reln->smgr_relpersistence = RELPERSISTENCE_PERMANENT;

		/* Remove local copy */
		rnode = reln->smgr_rnode;
		for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		{
			elog(SmgrTrace, "forgetting cached relsize for %u/%u/%u.%u",
				 rnode.node.spcNode,
				 rnode.node.dbNode,
				 rnode.node.relNode,
				 forknum);

			forget_cached_relsize(rnode.node, forknum);
			mdclose(reln, forknum);
			/* use isRedo == true, so that we drop it immediately */
			mdunlink(rnode, forknum, true);
		}
	}

	unlogged_build_rel = NULL;
	unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
}

static void
AtEOXact_zenith(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:

			/*
			 * Forget about any build we might have had in progress. The local
			 * file will be unlinked by smgrDoPendingDeletes()
			 */
			unlogged_build_rel = NULL;
			unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			if (unlogged_build_phase != UNLOGGED_BUILD_NOT_IN_PROGRESS)
			{
				unlogged_build_rel = NULL;
				unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 (errmsg("unlogged index build was not properly finished"))));
			}
			break;
	}
}

static const struct f_smgr zenith_smgr =
{
	.smgr_init = zenith_init,
	.smgr_shutdown = NULL,
	.smgr_open = zenith_open,
	.smgr_close = zenith_close,
	.smgr_create = zenith_create,
	.smgr_exists = zenith_exists,
	.smgr_unlink = zenith_unlink,
	.smgr_extend = zenith_extend,
	.smgr_prefetch = zenith_prefetch,
	.smgr_read = zenith_read,
	.smgr_write = zenith_write,
	.smgr_writeback = zenith_writeback,
	.smgr_nblocks = zenith_nblocks,
	.smgr_truncate = zenith_truncate,
	.smgr_immedsync = zenith_immedsync,

	.smgr_start_unlogged_build = zenith_start_unlogged_build,
	.smgr_finish_unlogged_build_phase_1 = zenith_finish_unlogged_build_phase_1,
	.smgr_end_unlogged_build = zenith_end_unlogged_build,
};


const f_smgr *
smgr_zenith(BackendId backend, RelFileNode rnode)
{

	/* Don't use page server for temp relations */
	if (backend != InvalidBackendId)
		return smgr_standard(backend, rnode);
	else
		return &zenith_smgr;
}

void
smgr_init_zenith(void)
{
	RegisterXactCallback(AtEOXact_zenith, NULL);

	smgr_init_standard();
	zenith_init();
}
