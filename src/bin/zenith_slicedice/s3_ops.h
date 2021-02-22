/*-------------------------------------------------------------------------
 *
 * s3_ops.h
 *	  S3 ops
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/zenith_slicedice/s3_ops.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef S3_OPS_H
#define S3_OPS_H

#include "lib/stringinfo.h"

typedef struct
{
	int			numfiles;

	char	  **filenames;

} ListObjectsResult;


extern ListObjectsResult *s3_ListObjects(const char *s3path);

extern StringInfo fetch_s3_file_memory(const char *s3path);

extern void put_s3_file(const char *localpath, const char *s3path, size_t filesize);

#endif							/* S3_OPS_H */
