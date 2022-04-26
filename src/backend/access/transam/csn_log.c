/*-----------------------------------------------------------------------------
 *
 * csn_log.c
 *		Track commit sequence numbers of finished transactions
 *
 * This module provides SLRU to store CSN for each transaction.  This
 * mapping need to be kept only for xid's greater then oldestXid, but
 * that can require arbitrary large amounts of memory in case of long-lived
 * transactions.  Because of same lifetime and persistancy requirements
 * this module is quite similar to subtrans.c
 *
 * If we switch database from CSN-base snapshot to xid-base snapshot then,
 * nothing wrong. But if we switch xid-base snapshot to CSN-base snapshot
 * it should decide a new xid which begin csn-base check. It can not be
 * oldestActiveXID because of prepared transaction.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/csn_log.c
 *
 *-----------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/csn_log.h"
#include "access/slru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/sync.h"
#include "utils/snapmgr.h"

bool enable_csn_snapshot;

/*
 * We use csnSnapshotActive to judge if csn snapshot enabled instead of by
 * enable_csn_snapshot, this design is similar to 'track_commit_timestamp'.
 *
 * Because in process of replication if master change 'enable_csn_snapshot'
 * in a database restart, standby should apply wal record for GUC changed,
 * then it's difficult to notice all backends about that. So they can get
 * the message by 'csnSnapshotActive' which in share buffer. It will not
 * acquire a lock, so without performance issue.
 *
 */
typedef struct CSNshapshotSharedData
{
	bool		csnSnapshotActive;
} CSNshapshotSharedData;

typedef CSNshapshotSharedData *CSNshapshotShared;
CSNshapshotShared csnShared = NULL;

/*
 * Defines for CSNLog page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CSNLog page numbering also wraps around at
 * 0xFFFFFFFF/CSN_LOG_XACTS_PER_PAGE, and CSNLog segment numbering at
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCSNLog (see CSNLogPagePrecedes).
 */

/* We store the commit CSN for each xid */
#define CSN_LOG_XACTS_PER_PAGE (BLCKSZ / sizeof(XidCSN))

#define TransactionIdToPage(xid)	((xid) / (TransactionId) CSN_LOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId) CSN_LOG_XACTS_PER_PAGE)

/*
 * Link to shared-memory data structures for CLOG control
 */
static SlruCtlData CSNLogCtlData;
#define CsnlogCtl (&CSNLogCtlData)

static int	ZeroCSNLogPage(int pageno, bool write_xlog);
static void ZeroTruncateCSNLogPage(int pageno, bool write_xlog);
static bool CSNLogPagePrecedes(int page1, int page2);
static void CSNLogSetPageStatus(TransactionId xid, int nsubxids,
									  TransactionId *subxids,
									  XidCSN csn, int pageno);
static void CSNLogSetCSNInSlot(TransactionId xid, XidCSN csn,
									  int slotno);

static void WriteXidCsnXlogRec(TransactionId xid, int nsubxids,
					 TransactionId *subxids, XidCSN csn);
static void WriteZeroCSNPageXlogRec(int pageno);
static void WriteTruncateCSNXlogRec(int pageno);

/*
 * CSNLogSetCSN
 *
 * Record XidCSN of transaction and its subtransaction tree.
 *
 * xid is a single xid to set status for. This will typically be the top level
 * transactionid for a top level commit or abort. It can also be a
 * subtransaction when we record transaction aborts.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 *
 * csn is the commit sequence number of the transaction. It should be
 * AbortedCSN for abort cases.
 */
void
CSNLogSetCSN(TransactionId xid, int nsubxids,
					 TransactionId *subxids, XidCSN csn, bool write_xlog)
{
	int			pageno;
	int			i = 0;
	int			offset = 0;

	Assert(TransactionIdIsValid(xid));

	pageno = TransactionIdToPage(xid);		/* get page of parent */

	// TODO(pooja): Remove this part entirely?
	// if(write_xlog)
	// 	WriteXidCsnXlogRec(xid, nsubxids, subxids, csn);

	for (;;)
	{
		int			num_on_page = 0;

		while (i < nsubxids && TransactionIdToPage(subxids[i]) == pageno)
		{
			num_on_page++;
			i++;
		}

		CSNLogSetPageStatus(xid,
							num_on_page, subxids + offset,
							csn, pageno);
		if (i >= nsubxids)
			break;

		offset = i;
		pageno = TransactionIdToPage(subxids[offset]);
		xid = InvalidTransactionId;
	}
}

/*
 * Record the final state of transaction entries in the csn log for
 * all entries on a single page.  Atomic only on this page.
 *
 * Otherwise API is same as TransactionIdSetTreeStatus()
 */
static void
CSNLogSetPageStatus(TransactionId xid, int nsubxids,
						   TransactionId *subxids,
						   XidCSN csn, int pageno)
{
	int			slotno;
	int			i;

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(CsnlogCtl, pageno, true, xid);

	/* Subtransactions first, if needed ... */
	for (i = 0; i < nsubxids; i++)
	{
		Assert(CsnlogCtl->shared->page_number[slotno] == TransactionIdToPage(subxids[i]));
		CSNLogSetCSNInSlot(subxids[i],	csn, slotno);
	}

	/* ... then the main transaction */
	if (TransactionIdIsValid(xid))
		CSNLogSetCSNInSlot(xid, csn, slotno);

	CsnlogCtl->shared->page_dirty[slotno] = true;

	LWLockRelease(CSNLogControlLock);
}

/*
 * Sets the commit status of a single transaction.
 */
static void
CSNLogSetCSNInSlot(TransactionId xid, XidCSN csn, int slotno)
{
	int			entryno = TransactionIdToPgIndex(xid);
	XidCSN 		*ptr;

	Assert(LWLockHeldByMe(CSNLogControlLock));

	ptr = (XidCSN *) (CsnlogCtl->shared->page_buffer[slotno] + entryno * sizeof(XidCSN));

	*ptr = csn;
}

/*
 * Interrogate the state of a transaction in the log.
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; TransactionIdGetXidCSN() in csn_snapshot.c is the
 * intended caller.
 */
XidCSN
CSNLogGetCSNByXid(TransactionId xid)
{
	int			pageno = TransactionIdToPage(xid);
	int			entryno = TransactionIdToPgIndex(xid);
	int			slotno;
	XidCSN *ptr;
	XidCSN	xid_csn;

	/* lock is acquired by SimpleLruReadPage_ReadOnly */
	slotno = SimpleLruReadPage_ReadOnly(CsnlogCtl, pageno, xid);

	ptr = (XidCSN *) (CsnlogCtl->shared->page_buffer[slotno] + entryno * sizeof(XidCSN));
	xid_csn = *ptr;

	LWLockRelease(CSNLogControlLock);

	return xid_csn;
}

/*
 * Number of shared CSNLog buffers.
 */
static Size
CSNLogShmemBuffers(void)
{
	return Min(32, Max(4, NBuffers / 512));
}

/*
 * Reserve shared memory for CsnlogCtl.
 */
Size
CSNLogShmemSize(void)
{
	return SimpleLruShmemSize(CSNLogShmemBuffers(), 0);
}

/*
 * Initialization of shared memory for CSNLog.
 */
void
CSNLogShmemInit(void)
{
	bool		found;


	CsnlogCtl->PagePrecedes = CSNLogPagePrecedes;
	SimpleLruInit(CsnlogCtl, "CSNLog Ctl", CSNLogShmemBuffers(), 0,
				  CSNLogControlLock, "pg_csn", LWTRANCHE_CSN_LOG_BUFFERS,
				  SYNC_HANDLER_CSN);

	csnShared = (CSNshapshotShared) ShmemInitStruct("CSNlog shared",
									 sizeof(CSNshapshotSharedData),
									 &found);
}

/*
 * See ActivateCSNlog
 */
void
BootStrapCSNLog(void)
{
	return;
}

/*
 * Initialize (or reinitialize) a page of CSNLog to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
ZeroCSNLogPage(int pageno, bool write_xlog)
{
	Assert(LWLockHeldByMe(CSNLogControlLock));
	if(write_xlog)
		WriteZeroCSNPageXlogRec(pageno);
	return SimpleLruZeroPage(CsnlogCtl, pageno);
}

static void
ZeroTruncateCSNLogPage(int pageno, bool write_xlog)
{
	if(write_xlog)
		WriteTruncateCSNXlogRec(pageno);
	SimpleLruTruncate(CsnlogCtl, pageno);
}


void 
ActivateCSNlog(void)
{
	int				startPage;
	TransactionId	nextXid = InvalidTransactionId;

	if (csnShared->csnSnapshotActive)
		return;


	nextXid = XidFromFullTransactionId(ShmemVariableCache->nextXid);
	startPage = TransactionIdToPage(nextXid);

	/* Create the current segment file, if necessary */
	if (!SimpleLruDoesPhysicalPageExist(CsnlogCtl, startPage))
	{
		int			slotno;
		LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);
		slotno = ZeroCSNLogPage(startPage, false);
		SimpleLruWritePage(CsnlogCtl, slotno);
		LWLockRelease(CSNLogControlLock);
	}
	csnShared->csnSnapshotActive = true;
}

bool
get_csnlog_status(void)
{
	if(!csnShared)
	{
		/* Should not arrived */
		elog(ERROR, "We do not have csnShared point");
	}
	return csnShared->csnSnapshotActive;
}

void
DeactivateCSNlog(void)
{
	csnShared->csnSnapshotActive = false;
	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);
	(void) SlruScanDirectory(CsnlogCtl, SlruScanDirCbDeleteAll, NULL);
	LWLockRelease(CSNLogControlLock);
}

void
StartupCSN(void)
{
	ActivateCSNlog();
}

void
CompleteCSNInitialization(void)
{
	/*
	 * If the feature is not enabled, turn it off for good.  This also removes
	 * any leftover data.
	 *
	 * Conversely, we activate the module if the feature is enabled.  This is
	 * necessary for primary and standby as the activation depends on the
	 * control file contents at the beginning of recovery or when a
	 * XLOG_PARAMETER_CHANGE is replayed.
	 */
	if (!get_csnlog_status())
		DeactivateCSNlog();
	else
		ActivateCSNlog();
}

void
CSNlogParameterChange(bool newvalue, bool oldvalue)
{
	if (newvalue)
	{
		if (!csnShared->csnSnapshotActive)
			ActivateCSNlog();
	}
	else if (csnShared->csnSnapshotActive)
		DeactivateCSNlog();
}


/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointCSNLog(void)
{
	if (!get_csnlog_status())
		return;

	/*
	 * Flush dirty CSNLog pages to disk.
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely to improve the odds that writing of dirty pages is done by
	 * the checkpoint process and not by backends.
	 */
	TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_START(true);
	SimpleLruWriteAll(CsnlogCtl, true);
	TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_DONE(true);
}

/*
 * Make sure that CSNLog has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty clog or xlog page to make room
 * in shared memory.
 */
void
ExtendCSNLog(TransactionId newestXact)
{
	int			pageno;

	if (!get_csnlog_status())
		return;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToPgIndex(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToPage(newestXact);

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	/* Zero the page and make an XLOG entry about it */
	ZeroCSNLogPage(pageno, !InRecovery);

	LWLockRelease(CSNLogControlLock);
}

/*
 * Remove all CSNLog segments before the one holding the passed
 * transaction ID.
 *
 * This is normally called during checkpoint, with oldestXact being the
 * oldest TransactionXmin of any running transaction.
 */
void
TruncateCSNLog(TransactionId oldestXact)
{
	int				cutoffPage;

	if (!get_csnlog_status())
		return;

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate. We step
	 * back one transaction to avoid passing a cutoff page that hasn't been
	 * created yet in the rare case that oldestXact would be the first item on
	 * a page and oldestXact == next XID.  In that case, if we didn't subtract
	 * one, we'd trigger SimpleLruTruncate's wraparound detection.
	 */
	TransactionIdRetreat(oldestXact);
	cutoffPage = TransactionIdToPage(oldestXact);
	ZeroTruncateCSNLogPage(cutoffPage, true);
}

/*
 * Decide which of two CSNLog page numbers is "older" for truncation
 * purposes.
 *
 * We need to use comparison of TransactionIds here in order to do the right
 * thing with wraparound XID arithmetic.  However, if we are asked about
 * page number zero, we don't want to hand InvalidTransactionId to
 * TransactionIdPrecedes: it'll get weird about permanent xact IDs.  So,
 * offset both xids by FirstNormalTransactionId to avoid that.
 */
static bool
CSNLogPagePrecedes(int page1, int page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * CSN_LOG_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId;
	xid2 = ((TransactionId) page2) * CSN_LOG_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId;

	return TransactionIdPrecedes(xid1, xid2);
}

void
WriteAssignCSNXlogRec(XidCSN xidcsn)
{
	XidCSN log_csn = 0;

	if(xidcsn <= get_last_log_wal_csn())
	{
		return;
	}
	log_csn = xidcsn;

	XLogBeginInsert();
	XLogRegisterData((char *) (&log_csn), sizeof(XidCSN));
	XLogInsert(RM_CSNLOG_ID, XLOG_CSN_ASSIGNMENT);
}

static void
WriteXidCsnXlogRec(TransactionId xid, int nsubxids,
					 TransactionId *subxids, XidCSN csn)
{
	xl_xidcsn_set 	xlrec;

	xlrec.xtop = xid;
	xlrec.nsubxacts = nsubxids;
	xlrec.xidcsn = csn;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, MinSizeOfXidCSNSet);
	XLogRegisterData((char *) subxids, nsubxids * sizeof(TransactionId));
	XLogInsert(RM_CSNLOG_ID, XLOG_CSN_SETXIDCSN);
}

/*
 * Write a ZEROPAGE xlog record
 */
static void
WriteZeroCSNPageXlogRec(int pageno)
{
	XLogBeginInsert();
	XLogRegisterData((char *) (&pageno), sizeof(int));
	(void) XLogInsert(RM_CSNLOG_ID, XLOG_CSN_ZEROPAGE);
}

/*
 * Write a TRUNCATE xlog record
 */
static void
WriteTruncateCSNXlogRec(int pageno)
{
	XLogBeginInsert();
	XLogRegisterData((char *) (&pageno), sizeof(int));
	XLogInsert(RM_CSNLOG_ID, XLOG_CSN_TRUNCATE);
}


void
csnlog_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* Backup blocks are not used in csnlog records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	if (info == XLOG_CSN_ASSIGNMENT)
	{
		XidCSN csn;

		memcpy(&csn, XLogRecGetData(record), sizeof(XidCSN));
		LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);
		set_last_max_csn(csn);
		LWLockRelease(CSNLogControlLock);

	}
	else if (info == XLOG_CSN_SETXIDCSN)
	{
		xl_xidcsn_set *xlrec = (xl_xidcsn_set *) XLogRecGetData(record);
		CSNLogSetCSN(xlrec->xtop, xlrec->nsubxacts, xlrec->xsub, xlrec->xidcsn, false);
	}
	else if (info == XLOG_CSN_ZEROPAGE)
	{
		int			pageno;
		int			slotno;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));
		LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);
		slotno = ZeroCSNLogPage(pageno, false);
		SimpleLruWritePage(CsnlogCtl, slotno);
		LWLockRelease(CSNLogControlLock);
		Assert(!CsnlogCtl->shared->page_dirty[slotno]);

	}
	else if (info == XLOG_CSN_TRUNCATE)
	{
		int			pageno;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));
		CsnlogCtl->shared->latest_page_number = pageno;
		ZeroTruncateCSNLogPage(pageno, false);
	}
	else
		elog(PANIC, "csnlog_redo: unknown op code %u", info);
}

/*
 * Entrypoint for sync.c to sync csn files.
 */
int
csnsyncfiletag(const FileTag *ftag, char *path)
{
	return SlruSyncFileTag(CsnlogCtl, ftag, path);
}
