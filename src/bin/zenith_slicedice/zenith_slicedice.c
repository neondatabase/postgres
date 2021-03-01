/*-------------------------------------------------------------------------
 *
 * zenith_slicedice.c - decode and redistribute WAL per datafile
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/zenith_slicedice/zenith_slicedice.c
 *-------------------------------------------------------------------------
 */

#define FRONTEND 1
#include "postgres.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands_xlog.h"
#include "common/fe_memutils.h"
#include "common/file_perm.h"
#include "common/hashfn.h"
#include "common/logging.h"
#include "getopt_long.h"
#include "zenith_slicedice.h"
#include "zenith_s3/s3_ops.h"

/*
 * RmgrNames is an array of resource manager names, to make error messages
 * a bit nicer.
 */
#define PG_RMGR(symname,name,redo,desc,identify,startup,cleanup,mask) \
  name,

static const char *RmgrNames[RM_MAX_ID + 1] = {
#include "access/rmgrlist.h"
};

/*
 * We have a hash table that tracks the data files. 
 *
 * note: these are compared with memcmp, make sure there is no uninitialized padding
 */
typedef struct
{
	RelFileNode rnode;
	ForkNumber forknum;
	int			segno;		/* 1 GB segment (RELSEG_SIZE) */
} file_tag_t;

/*
 *
 */
typedef struct
{
	file_tag_t tag;
	int			status;

	char	   *relpath;
	char	   *walpath;
	int			wal_fd;

	size_t		size;

	XLogRecPtr	latest_lsn;
	XLogRecPtr	create_lsn;
	XLogRecPtr	delete_lsn;

} file_entry_t;

#define SH_PREFIX		filehash
#define SH_ELEMENT_TYPE	file_entry_t
#define SH_KEY_TYPE		file_tag_t
#define	SH_KEY			tag
#define SH_HASH_KEY(tb, key)	hash_bytes((unsigned char *) &(key), sizeof(file_tag_t))
#define SH_EQUAL(tb, a, b)		(memcmp(&a, &b, sizeof(file_tag_t)) == 0)
#define	SH_SCOPE		static inline
#define SH_RAW_ALLOCATOR	pg_malloc0
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

#define FILEHASH_INITIAL_SIZE	1000

static filehash_hash *filehash;

static XLogRecPtr start_lsn;
static XLogRecPtr end_lsn;

static file_entry_t *nonrel_entry;

/* prototypes for local functions */
static void append_record_to_file(XLogReaderState *record, file_entry_t *entry);


/* Look up entry for 'path', creating a new one if it doesn't exist */
static file_entry_t *
insert_filehash_entry(RelFileNode rnode, ForkNumber forknum, BlockNumber blkno)
{
	file_entry_t *entry;
	bool		found;
	file_tag_t	tag = { rnode, forknum, blkno / RELSEG_SIZE };

	entry = filehash_insert(filehash, tag, &found);
	if (!found)
	{
		entry->tag = tag;

		entry->relpath = NULL;
		entry->walpath = NULL;
		entry->wal_fd = -1;
		entry->latest_lsn = InvalidXLogRecPtr;
	}

	return entry;
}


void
redist_init(XLogRecPtr start_lsn_arg, XLogRecPtr end_lsn_arg)
{
	/* create hash table */
	filehash = filehash_create(FILEHASH_INITIAL_SIZE, NULL);
	start_lsn = start_lsn_arg;
	end_lsn = end_lsn_arg;

	nonrel_entry = palloc0(sizeof(file_entry_t));
	nonrel_entry->walpath = "/tmp/nonrelwal";
	nonrel_entry->wal_fd = open(nonrel_entry->walpath, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY, pg_file_create_mode);
	if (nonrel_entry->wal_fd < 0)
		pg_fatal("could not open target file \"%s\": %m", nonrel_entry->walpath);
}

/*
 * Parse the record. For each data file it touches, look up the file_entry_t and
 * write out a copy of the record
 */
void
handle_record(XLogReaderState *record)
{
	/*
	 * Extract information on which blocks the current record modifies.
	 */
	int			block_id;
	RmgrId		rmid = XLogRecGetRmid(record);
	uint8		info = XLogRecGetInfo(record);
	uint8		rminfo = info & ~XLR_INFO_MASK;
	enum {
		SKIP = 0,
		APPEND_REL_WAL,
		APPEND_NONREL_WAL
	} action;
	
	/* Is this a special record type that I recognize? */

	if (rmid == RM_DBASE_ID && rminfo == XLOG_DBASE_CREATE)
	{
		/* TODO */
		action = APPEND_NONREL_WAL;
	}
	else if (rmid == RM_DBASE_ID && rminfo == XLOG_DBASE_DROP)
	{
		/* TODO */
		action = APPEND_NONREL_WAL;
	}
	else if (rmid == RM_SMGR_ID && rminfo == XLOG_SMGR_CREATE)
	{
		/* These records can include "dropped rels". */
		xl_smgr_create *xlrec = (xl_smgr_create *) XLogRecGetData(record);
		file_entry_t *entry;

		/* look up or create the file_entry */
		entry = insert_filehash_entry(xlrec->rnode, xlrec->forkNum, 0);

		if (entry->latest_lsn < record->EndRecPtr)
		{
			append_record_to_file(record, entry);
			entry->latest_lsn = record->EndRecPtr;
			entry->create_lsn = record->EndRecPtr;
		}
		action = SKIP;
	}
	else if (rmid == RM_SMGR_ID && rminfo == XLOG_SMGR_TRUNCATE)
	{
		/* FIXME: This should probably go to per-rel WAL? */
		action = APPEND_NONREL_WAL;
	}
	else if (rmid == RM_XACT_ID &&
			 ((rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT ||
			  (rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT_PREPARED))
	{
		/* These records can include "dropped rels". */
		xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(record);
		xl_xact_parsed_commit parsed;

		ParseCommitRecord(info, xlrec, &parsed);
		for (int i = 0; i < parsed.nrels; i++)
		{
			/* look up or create the file_entry */
			file_entry_t *entry;

			entry = insert_filehash_entry(parsed.xnodes[i], MAIN_FORKNUM, 0);

			if (entry->latest_lsn >= record->EndRecPtr)
				continue;

			append_record_to_file(record, entry);
			entry->latest_lsn = record->EndRecPtr;
			entry->delete_lsn = record->EndRecPtr;

		}
		action = APPEND_NONREL_WAL;
	}
	else if (rmid == RM_XACT_ID &&
			 ((rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT ||
			  (rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT_PREPARED))
	{
		/* These records can include "dropped rels". */
		xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(record);
		xl_xact_parsed_abort parsed;

		ParseAbortRecord(info, xlrec, &parsed);
		for (int i = 0; i < parsed.nrels; i++)
		{
			/* look up or create the file_entry */
			file_entry_t *entry;

			entry = insert_filehash_entry(parsed.xnodes[i], MAIN_FORKNUM, 0);

			if (entry->latest_lsn >= record->EndRecPtr)
				continue;

			append_record_to_file(record, entry);
			entry->latest_lsn = record->EndRecPtr;
			entry->delete_lsn = record->EndRecPtr;

		}
		action = APPEND_NONREL_WAL;
	}
	else if (info & XLR_SPECIAL_REL_UPDATE)
	{
		/*
		 * This record type modifies a relation file in some special way, but
		 * we don't recognize the type. That's bad - we don't know how to
		 * track that change.
		 */
		pg_fatal("WAL record modifies a relation, but record type is not recognized: "
				 "lsn: %X/%X, rmgr: %s, info: %02X",
				 (uint32) (record->ReadRecPtr >> 32), (uint32) (record->ReadRecPtr),
				 RmgrNames[rmid], info);
	}
	else if (rmid == RM_XLOG_ID)
	{
		if (rminfo == XLOG_FPI || rminfo == XLOG_FPI_FOR_HINT)
			action = APPEND_REL_WAL;
		else
			action = APPEND_NONREL_WAL;
	}
	else
	{
		switch (rmid)
		{
			case RM_CLOG_ID:
			case RM_DBASE_ID:
			case RM_TBLSPC_ID:
			case RM_MULTIXACT_ID:
			case RM_RELMAP_ID:
			case RM_STANDBY_ID:
			case RM_SEQ_ID:
			case RM_COMMIT_TS_ID:
			case RM_REPLORIGIN_ID:
			case RM_LOGICALMSG_ID:
				/* These go to the "non-relation" WAL */
				action = APPEND_NONREL_WAL;
				break;

			case RM_SMGR_ID:
			case RM_HEAP2_ID:
			case RM_HEAP_ID:
			case RM_BTREE_ID:
			case RM_HASH_ID:
			case RM_GIN_ID:
			case RM_GIST_ID:
			case RM_SPGIST_ID:
			case RM_BRIN_ID:
			case RM_GENERIC_ID:
				/* These go to the per-relation WAL files */
				action = APPEND_REL_WAL;
				break;

			default:
				pg_fatal("unknown resource manager");
		}
	}

	if (action == APPEND_NONREL_WAL)
	{
		if (nonrel_entry->latest_lsn >= record->EndRecPtr)
			return;

		append_record_to_file(record, nonrel_entry);
		nonrel_entry->latest_lsn = record->EndRecPtr;
	}
	else if (action == APPEND_REL_WAL)
	{
		/* Append to per-rel WAL file(s) */
		for (block_id = 0; block_id <= record->max_block_id; block_id++)
		{
			RelFileNode rnode;
			ForkNumber	forknum;
			BlockNumber blkno;
			file_entry_t *entry;

			if (!XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blkno))
				continue;

			/* look up or create the file_entry */
			entry = insert_filehash_entry(rnode, forknum, blkno);

			if (entry->latest_lsn >= record->EndRecPtr)
				continue;

			append_record_to_file(record, entry);
			entry->latest_lsn = record->EndRecPtr;
		}
	}
	else
		Assert(action == SKIP);
}


/*
 * A helper function to create the path of per-datafile WAL file.
 *
 * The returned path is palloc'd
 */
static char *
datasegwalpath(RelFileNode rnode, ForkNumber forknum, BlockNumber segno)
{
	char	   *path;
	char	   *segpath;

	path = relpathperm(rnode, forknum);
	if (segno > 0)
	{
		segpath = psprintf("%s.%u_wal_%08X%08X-%08X%08X", path, segno,
						   (uint32) (start_lsn >> 32), (uint32) start_lsn,
						   (uint32) (end_lsn >> 32), (uint32) end_lsn);
		pfree(path);
		return segpath;
	}
	else
		return psprintf("%s_wal_%08X%08X-%08X%08X", path,
						(uint32) (start_lsn >> 32), (uint32) start_lsn,
						(uint32) (end_lsn >> 32), (uint32) end_lsn);
}

static void
write_noerr(int fd, char *buf, size_t size, const char *path)
{
	size_t		writeleft;
	char	   *p;

	p = buf;
	writeleft = size;
	while (writeleft > 0)
	{
		ssize_t		writelen;

		errno = 0;
		writelen = write(fd, p, writeleft);
		if (writelen < 0)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			pg_fatal("could not write file \"%s\": %m", path);
		}
		p += writelen;
		writeleft -= writelen;
	}
}


/*
 * Split a pathname as dirname(1) and basename(1) would.
 *
 * XXX this probably doesn't do very well on Windows.  We probably need to
 * apply canonicalize_path(), at the very least.
 */
static void
split_path(const char *path, char **dir, char **fname)
{
	char	   *sep;

	/* split filepath into directory & filename */
	sep = strrchr(path, '/');

	/* directory path */
	if (sep != NULL)
	{
		*dir = pnstrdup(path, sep - path);
		*fname = pg_strdup(sep + 1);
	}
	/* local directory */
	else
	{
		*dir = NULL;
		*fname = pg_strdup(path);
	}
}

static void
append_record_to_file(XLogReaderState *record, file_entry_t *entry)
{
	int			wal_fd;

	/* Open the file if necessary */
	if (entry->wal_fd == -1)
	{
		char	   *relpath;
		char	   *walpath;
		char	   *dir;
		char	   *fname;

		walpath = datasegwalpath(entry->tag.rnode, entry->tag.forknum, entry->tag.segno);

		split_path(walpath, &dir, &fname);
		if (dir)
			pg_mkdir_p(dir, pg_dir_create_mode);

		wal_fd = open(walpath, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY, pg_file_create_mode);
		if (wal_fd < 0)
			pg_fatal("could not open target file \"%s\": %m",
					 walpath);

		relpath = relpathperm(entry->tag.rnode, entry->tag.forknum);
		if (entry->tag.segno > 0)
			entry->relpath = psprintf("%s.%u", relpath, entry->tag.segno);
		else
			entry->relpath = relpath;
		entry->walpath = walpath;
		entry->wal_fd = wal_fd;
	}
	else
		wal_fd = entry->wal_fd;

	/* Append to it */
	write_noerr(wal_fd, (char *) &record->ReadRecPtr, sizeof(XLogRecPtr), entry->walpath);
	entry->size += sizeof(XLogRecPtr);
	write_noerr(wal_fd, (char *) &record->EndRecPtr, sizeof(XLogRecPtr), entry->walpath);
	entry->size += sizeof(XLogRecPtr);
	write_noerr(wal_fd, (char *) record->decoded_record, record->decoded_record->xl_tot_len, entry->walpath);
	entry->size += record->decoded_record->xl_tot_len;
}

void
upload_slice_wal_files(void)
{
	filehash_iterator iter;
	file_entry_t *entry;
	char		s3path[MAXPGPATH];

	filehash_start_iterate(filehash, &iter);
	while ((entry = filehash_iterate(filehash, &iter)) != NULL)
	{
		close(entry->wal_fd); /* FIXME: check return code */

		/* If this is a new relation, create an empty base image for it */
		if (entry->create_lsn != InvalidXLogRecPtr)
		{
			snprintf(s3path, sizeof(s3path), "relationdata/%s_%08X%08X",
					 entry->relpath,
					 (uint32) (entry->create_lsn >> 32),
					 (uint32) (entry->create_lsn));
			put_s3_file("/dev/null", s3path, 0);
		}

		/* Put the per-rel WAL file */
		snprintf(s3path, sizeof(s3path), "relationdata/%s", entry->walpath);
		put_s3_file(entry->walpath, s3path, entry->size);
	}

	/* finally, upload the non-rel WAL */
	close(nonrel_entry->wal_fd); /* FIXME: check return code */

	snprintf(s3path, sizeof(s3path), "nonreldata/nonrel_%08X%08X-%08X%08X",
			 (uint32) (start_lsn >> 32), (uint32) start_lsn,
			 (uint32) (end_lsn >> 32), (uint32) end_lsn);
	put_s3_file(nonrel_entry->walpath, s3path, nonrel_entry->size);
}
