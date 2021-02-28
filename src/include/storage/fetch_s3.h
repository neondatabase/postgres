/*-------------------------------------------------------------------------
 *
 * fetch_s3.h
 *	  routines TODO description
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/fetch_s3.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FETCH_S3_H
#define FETCH_S3_H

#include "nodes/pg_list.h"
#include "lib/stringinfo.h"

typedef struct
{
	int			numfiles;

	char	  **filenames;

} ListObjectsResult;


extern void fetch_s3_file_restore(const char *s3path, const char *dstpath);
extern ListObjectsResult *s3_ListObjects(const char *s3path);

/* in s3_sign.c */
extern List *s3_get_authorization_hdrs(const char *host,
									   const char *region,
									   const char *method,
									   const char *path,
									   const char *accesskeyid,
									   const char *secret);

#endif							/* FETCH_S3_H */
