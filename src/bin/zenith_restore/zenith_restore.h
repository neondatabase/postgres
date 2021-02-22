/*-------------------------------------------------------------------------
 *
 * zenith_restore.h
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef ZENITH_RESTORE_H
#define ZENITH_RESTORE_H

#include "access/xlogdefs.h"
#include "common/checksum_helper.h"
#include "common/logging.h"

/* logging support */
#define pg_fatal(...) do { pg_log_fatal(__VA_ARGS__); exit(1); } while(0)



/*
 * Each file described by the manifest file is parsed to produce an object
 * like this.
 */
typedef struct manifest_file
{
	uint32		status;			/* hash status */
	char	   *pathname;
	size_t		size;
	pg_checksum_type checksum_type;
	int			checksum_length;
	uint8	   *checksum_payload;
	bool		matched;
	bool		bad;

	struct manifest_file *next;
} manifest_file;


/*
 * Each WAL range described by the manifest file is parsed to produce an
 * object like this.
 */
typedef struct manifest_wal_range
{
	TimeLineID	tli;
	XLogRecPtr	start_lsn;
	XLogRecPtr	end_lsn;
	struct manifest_wal_range *next;
	struct manifest_wal_range *prev;
} manifest_wal_range;

extern void parse_manifest_file(char *manifest_path,
								manifest_file **manifest_files_p,
								manifest_wal_range **first_wal_range_p);

/* in rehydrate.c */
extern void rehydrate_wal(XLogRecPtr startptr, XLogRecPtr endptr);


#endif							/* ZENITH_RESTORE_H */
