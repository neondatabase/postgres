/*-------------------------------------------------------------------------
 *
 * lazyrestore.c
 *	  Base backup + WAL in a lazyrestore file format
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/lazyrestore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>

#include "access/xlog.h"
#include "access/xlogutils.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/fetch_s3.h"
#include "storage/lazyrestore.h"
#include "storage/md.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "access/xlog_internal.h"

static MemoryContext LazyRestoreCxt;

static void restore_reln(SMgrRelation reln, ForkNumber forknum);
static void redo_lazy_wal(SMgrRelation reln, ForkNumber forknum, const char *path, XLogRecPtr startptr, XLogRecPtr endptr);
static bool parse_reldata_filename(const char *path, char **basefname, XLogRecPtr *recptr);
static bool parse_relwal_filename(const char *path, char **basefname, XLogRecPtr *startptr, XLogRecPtr *endptr);


static bool currently_restoring = false;

/*
 *	lazyrestore_init() -- Initialize private state
 */
void
lazyrestore_init(void)
{
	LazyRestoreCxt = AllocSetContextCreate(TopMemoryContext,
										   "LazyRestoreSmgr",
										   ALLOCSET_DEFAULT_SIZES);
}

static char *
lazyrelpath(RelFileNodeBackend rnode, ForkNumber forknum)
{
	char	   *path;
	char	   *lazypath;

	path = relpath(rnode, forknum);
	lazypath = psprintf("%s_lazy", path);
	pfree(path);

	return lazypath;
}

/*
 *	lazyrestore_exists() -- Does the physical file exist?
 */
bool
lazyrestore_exists(SMgrRelation reln, ForkNumber forkNum)
{
	char	   *path;
	int			fd;
	int			result;

	if (mdexists(reln, forkNum))
		return true;

	/* check if file_lazy exists */
	path = lazyrelpath(reln->smgr_rnode, forkNum);

	fd = PathNameOpenFile(path, O_RDONLY | PG_BINARY);

	if (fd < 0)
	{
		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));
		result = false;
	}
	else
	{
		FileClose(fd);
		result = true;
	}

	pfree(path);

	return result;
}

/*
 *	lazyrestore_create() -- Create a new relation on lazyrestored storage
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void
lazyrestore_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	mdcreate(reln, forkNum, isRedo);
}

/*
 *	lazyrestore_unlink() -- Unlink a relation.
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
lazyrestore_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	char	   *path;
	int			ret;

	path = lazyrelpath(rnode, forkNum);
	unlink(path);
	ret = unlink(path);
	if (ret < 0 && errno != ENOENT)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not remove file \"%s\": %m", path)));

	mdunlink(rnode, forkNum, isRedo);
}

static bool
reln_is_lazy(SMgrRelation reln, ForkNumber forknum)
{
	char	   *path;
	int			fd;
	bool		islazy;

	/* Is it lazy? */
	path = lazyrelpath(reln->smgr_rnode, forknum);

	fd = PathNameOpenFile(path, O_RDONLY | PG_BINARY);

	if (fd < 0)
	{
		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));
		islazy = false;
	}
	else
	{
		FileClose(fd);
		islazy = true;
	}

	return islazy;
}

void
restore_if_lazy(SMgrRelation reln, ForkNumber forknum)
{
	if (currently_restoring)
	{
		/* If a WAL redo routine accesses a relation while we're restoring it, don't recurse. */
		/* FIXME: What if redoing records on main fork try to update the FSM or VM? */
		return;
	}

	/* Is it lazy? */
	if (reln_is_lazy(reln, forknum))
	{
		PG_TRY();
		{
			currently_restoring = true;
			InRecovery = true;
			restore_reln(reln, forknum);
			currently_restoring = false;
			InRecovery = false;
		}
		PG_CATCH();
		{
			currently_restoring = false;
			InRecovery = false;
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
}

typedef struct
{
	char	   *path;
	XLogRecPtr 	startptr;
	XLogRecPtr 	endptr;
} walfile_info;


static int
walfile_info_cmp(const ListCell *a, const ListCell *b)
{
	walfile_info *aentry = (walfile_info *) lfirst(a);
	walfile_info *bentry = (walfile_info *) lfirst(b);

	if (aentry->startptr > bentry->startptr)
		return 1;
	else if (aentry->startptr < bentry->startptr)
		return -1;
	else
		return 0;
}


static void
restore_reln(SMgrRelation reln, ForkNumber forknum)
{
	ListObjectsResult *files;
	char	   *lazypath;
	int			ret;
	char	   *basepath;
	XLogRecPtr	walpos;
	TimeLineID	tli;
	XLogRecPtr	latest_image_ptr;
	char	   *latest_image_path;
	List	   *walfiles;
	ListCell   *lc;
	XLogRecPtr	last_replayed_recptr;
	int			rmid;

	/*
	 * Get current WAL position. We need to reconstruct the file "as of" this position.
	 */
	if (!RecoveryInProgress())
	{
		/* FIXME: this lags behind the real insert position by at most 1 WAL page.
		 * Is that good enough here?
		 */
		walpos = GetInsertRecPtr();
	}
	else
		walpos = GetXLogReplayRecPtr(&tli);

	files = s3_ListObjects("");
	elog(LOG, "number of files in bucket: %d\n", files->numfiles);

	basepath = relpath(reln->smgr_rnode, forknum);

	/* Find the latest usable base image of this file */
	latest_image_ptr = InvalidXLogRecPtr;
	for (int i = 0; i < files->numfiles; i++)
	{
		char	   *path = files->filenames[i];
		char	   *basefname;
		XLogRecPtr	recptr;

		/* Fetch all files related to this rel */
		/* FIXME: only fetch files that are needed to reach the target WAL position */

		if (!parse_reldata_filename(path, &basefname, &recptr))
			continue;
		if (strcmp(basefname, basepath) != 0)
			continue;	/* not for this rel */

		if (recptr <= walpos &&	recptr > latest_image_ptr)
		{
			elog(LOG, "found usable base image for rel %s at %X/%X", path,
				 (uint32) (recptr >> 32), (uint32) recptr);
			latest_image_ptr = recptr;
			latest_image_path = path;
		}
	}

	if (latest_image_ptr == InvalidXLogRecPtr)
		elog(ERROR, "could not find suitable base image for rel \"%s\"", basepath);

	/* Find all usable WAL for this file */
	walfiles = NIL;
	for (int i = 0; i < files->numfiles; i++)
	{
		char	   *path = files->filenames[i];
		char	   *basefname;
		XLogRecPtr	startptr;
		XLogRecPtr	endptr;

		if (!parse_relwal_filename(path, &basefname, &startptr, &endptr))
			continue;
		if (strcmp(basefname, basepath) != 0)
			continue;	/* not for this rel */

		if (endptr >= latest_image_ptr && startptr < walpos)
		{
			walfile_info *e;

			elog(LOG, "found usable WAL file: %s", path);

			e = palloc(sizeof(walfile_info));			
			e->path = pstrdup(path);
			e->startptr = startptr;
			e->endptr = endptr;

			walfiles = lappend(walfiles, e);
		}
		else
			elog(LOG, "found usable WAL file: %s", path);
			
	}
	list_sort(walfiles, walfile_info_cmp);

	/* Fetch and restore the base file */
	fetch_s3_file_restore(latest_image_path, basepath);

	/* Initialize resource managers */
	for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_startup != NULL)
			RmgrTable[rmid].rm_startup();
	}

	last_replayed_recptr = latest_image_ptr;
	foreach(lc, walfiles)
	{
		walfile_info *e = (walfile_info *) lfirst(lc);
		XLogRecPtr from;
		XLogRecPtr upto;

		fetch_s3_file_restore(e->path, "tmpwal");

		from = Max(last_replayed_recptr, e->startptr);
		upto = Min(walpos, e->endptr);

		elog(LOG, "replaying WAL from %s between %X/%X and %X/%X",
			 e->path,
			 (uint32) (from >> 32), (uint32) from,
			 (uint32) (upto >> 32), (uint32) upto);

		redo_lazy_wal(reln, forknum, "tmpwal", from, upto);
		last_replayed_recptr = upto;

		ret = unlink("tmpwal");
		if (ret < 0 && errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m", "tmpwal")));
	}

	/* Allow resource managers to do any required cleanup. */
	for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_cleanup != NULL)
			RmgrTable[rmid].rm_cleanup();
	}

	/* remove the _lazy file so that we don't try to restore it again */
	lazypath = lazyrelpath(reln->smgr_rnode, forknum);
	unlink(lazypath);
	ret = unlink(lazypath);
	if (ret < 0 && errno != ENOENT)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not remove file \"%s\": %m", lazypath)));

	/* clean up (FIXME: consider using a temp memory context to avoid leaks) */
	pfree(lazypath);
}

static bool
parse_reldata_filename(const char *path, char **basefname, XLogRecPtr *recptr)
{
	const char *fname;
	const char *suffix;
	int			basefnamelen;
	uint32		hi;
	uint32		lo;

	if (strncmp(path, "relationdata/", strlen("relationdata/")) != 0)
		return false;
	fname = path + strlen("relationdata/");
	if (strlen(fname) < 1 + 16)
		return false;

	suffix = fname + strlen(fname) - 1 - 16;
	if (suffix[0] != '_')
		return false;
	if (strspn(suffix + 1, "01234567890ABCDEF") != 16)
		return false;

	if (sscanf(suffix + 1, "%08X%08X", &hi, &lo) != 2)
		return false;
	*recptr = (uint64) hi << 32 | lo;

	basefnamelen = suffix - fname;
	*basefname = palloc(basefnamelen + 1);
	memcpy(*basefname, fname, basefnamelen);
	(*basefname)[basefnamelen] = '\0';

	return true;
}

static bool
parse_relwal_filename(const char *path, char **basefname, XLogRecPtr *startptr, XLogRecPtr *endptr)
{
	const char *fname;
	const char *suffix;
	int			basefnamelen;
	uint32		start_hi;
	uint32		start_lo;
	uint32		end_hi;
	uint32		end_lo;

#define SUFFIXPATTERN "_wal_1234567812345678-1234567812345678"

	if (strncmp(path, "relationdata/", strlen("relationdata/")) != 0)
		return false;
	fname = path + strlen("relationdata/");
	if (strlen(fname) < 1 + strlen(SUFFIXPATTERN))
		return false;

	suffix = fname + strlen(fname) - strlen(SUFFIXPATTERN);

	if (sscanf(suffix + 1, "wal_%08X%08X-%08X%08X", &start_hi, &start_lo, &end_hi, &end_lo) != 4)
		return false;
	*startptr = (uint64) start_hi << 32 | start_lo;
	*endptr = (uint64) end_hi << 32 | end_lo;

	basefnamelen = suffix - fname;
	*basefname = palloc(basefnamelen + 1);
	memcpy(*basefname, fname, basefnamelen);
	(*basefname)[basefnamelen] = '\0';

	return true;
}


/*
 *	lazyrestore_extend() -- Add a block to the specified relation.
 *
 *		The semantics are nearly the same as mdwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
lazyrestore_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				   char *buffer, bool skipFsync)
{
	restore_if_lazy(reln, forknum);

	mdextend(reln, forknum, blocknum, buffer, skipFsync);
}

/*
 *  lazyrestore_open() -- Initialize newly-opened relation.
 */
void
lazyrestore_open(SMgrRelation reln)
{
	/* no work */
	mdopen(reln);
}

/*
 *	lazyrestore_close() -- Close the specified relation, if it isn't closed already.
 */
void
lazyrestore_close(SMgrRelation reln, ForkNumber forknum)
{
	mdclose(reln, forknum);
}

/*
 *	lazyrestore_prefetch() -- Initiate asynchronous read of the specified block of a relation
 */
bool
lazyrestore_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	/* not implemented */
	return true;
}

/*
 * lazyrestore_writeback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
void
lazyrestore_writeback(SMgrRelation reln, ForkNumber forknum,
					  BlockNumber blocknum, BlockNumber nblocks)
{
	/* not implemented */
}

/*
 *	lazyrestore_read() -- Read the specified block from a relation.
 */
void
lazyrestore_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				 char *buffer)
{
	restore_if_lazy(reln, forknum);

	mdread(reln, forknum, blocknum, buffer);
}

/*
 *	lazyrestore_write() -- Write the supplied block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
void
lazyrestore_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		char *buffer, bool skipFsync)
{
	/* Normally there cannot be attempts to write to a file that's lazy;
	 * how could the page be in the buffer cache if it wasn't read first,
	 * and reading causes it to be restored. However, it can happen if we -
	 * or another process - is in the process of restoring the file, and we
	 * want to evict a page from buffer cache. In that case, writing it out
	 * to local disk is correct.
	 */
	//restore_if_lazy(reln, forknum);

	mdwrite(reln, forknum, blocknum, buffer, skipFsync);

	/*
	 * Remember the LSN of the flushed page. Make sure we process
	 * the WAL up to that point.
	 */
}

/*
 *	lazyrestore_nblocks() -- Get the number of blocks stored in a relation.
 */
BlockNumber
lazyrestore_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	/* todo: open the file, check the header */
	restore_if_lazy(reln, forknum);

	return mdnblocks(reln, forknum);
}

/*
 *	lazyrestore_truncate() -- Truncate relation to specified number of blocks.
 */
void
lazyrestore_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	/* FIXME: Do nothing. It's WAL-logged */
	mdtruncate(reln, forknum, nblocks);
	
}

/*
 *	lazyrestore_immedsync() -- Immediately sync a relation to stable storage.
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
lazyrestore_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	/* FIXME: do nothing, we rely on WAL */
	mdimmedsync(reln, forknum);
}

/*
 * Unlink a file, given a file tag.  Write the path into an output
 * buffer so the caller can use it in error messages.
 *
 * Return 0 on success, -1 on failure, with errno set.
 */
int
lazyrestore_unlinkfiletag(const FileTag *ftag, char *path)
{
	char	   *p;

	/* Compute the path. */
	p = relpathperm(ftag->rnode, MAIN_FORKNUM);
	strlcpy(path, p, MAXPGPATH);
	pfree(p);

	/* Try to unlink the file. */
	return unlink(path);
}

/*
 * Check if a given candidate request matches a given tag, when processing
 * a SYNC_FILTER_REQUEST request.  This will be called for all pending
 * requests to find out whether to forget them.
 */
bool
lazyrestore_filetagmatches(const FileTag *ftag, const FileTag *candidate)
{
	/*
	 * For now we only use filter requests as a way to drop all scheduled
	 * callbacks relating to a given database, when dropping the database.
	 * We'll return true for all candidates that have the same database OID as
	 * the ftag from the SYNC_FILTER_REQUEST request, so they're forgotten.
	 */
	return ftag->rnode.dbNode == candidate->rnode.dbNode;
}

static void
fread_noerr(void *buf, uint32 len, FILE *fp, const char *path)
{
	ssize_t		n;

	n = fread(buf, 1, len, fp);
	if (n == 0 && feof(fp))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("unexpected end of file in file \"%s\"", path)));

	if (n < len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\"", path)));
}

static bool
fread_noerr_eof_ok(void *buf, uint32 len, FILE *fp, const char *path)
{
	ssize_t		n;

	n = fread(buf, 1, len, fp);
	if (n == 0 && feof(fp))
		return false;

	if (n < len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\"", path)));

	return true;
}


static void
redo_lazy_wal(SMgrRelation reln, ForkNumber forknum, const char *path, XLogRecPtr startptr, XLogRecPtr endptr)
{
	FILE	   *fp;
	XLogRecPtr	ReadRecPtr;
	XLogRecPtr	EndRecPtr;
	XLogReaderState *xlogreader;
	XLogReaderRoutine dummy_routine = { NULL, NULL, NULL };

	xlogreader = XLogReaderAllocate(wal_segment_size, NULL, &dummy_routine, NULL);
	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	fp = AllocateFile(path, PG_BINARY_R);
	if (!fp)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" for reading: %m",
						path)));

	for (;;)
	{
		uint32		tot_len;
		char	   *errormsg;
		XLogRecord *record;

		if (!fread_noerr_eof_ok(&ReadRecPtr, sizeof(XLogRecPtr), fp, path))
			break;
		fread_noerr(&EndRecPtr, sizeof(XLogRecPtr), fp, path);
		fread_noerr(&tot_len, sizeof(uint32), fp, path);

		if (!AllocSizeIsValid(tot_len))
			elog(ERROR, "record length %u at %X/%X too long",
				 tot_len, (uint32) (ReadRecPtr >> 32), (uint32) ReadRecPtr);

		if (xlogreader->readRecordBufSize < tot_len)
		{
			xlogreader->readRecordBuf = repalloc(xlogreader->readRecordBuf, tot_len);
			xlogreader->readRecordBufSize = tot_len;
		}

		memcpy(xlogreader->readRecordBuf, &tot_len, sizeof(uint32));
		fread_noerr(xlogreader->readRecordBuf + sizeof(uint32), tot_len - sizeof(uint32), fp, path);
		record = (XLogRecord *) xlogreader->readRecordBuf;

		xlogreader->ReadRecPtr = ReadRecPtr;
		xlogreader->EndRecPtr = EndRecPtr;

		if (!DecodeXLogRecord(xlogreader, record, &errormsg))
		{
			elog(ERROR, "error decoding record at %X/%X: %s",
				 (uint32) (ReadRecPtr >> 32), (uint32) ReadRecPtr,
				 errormsg ? errormsg : "no error message");
		}

		/* Now apply the WAL record itself. This recurse back to smgrread() to read it. */
		if (reln->smgr_rnode.node.relNode == 16397)
			elog(LOG, "replaying record %X/%X", 
				 (uint32) (ReadRecPtr >> 32), (uint32) ReadRecPtr);
		RmgrTable[record->xl_rmid].rm_redo(xlogreader);
	}
	FreeFile(fp);

	XLogReaderFree(xlogreader);
}
