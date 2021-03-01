/*-------------------------------------------------------------------------
 *
 * s3_ops.h
 *	  S3 ops
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/zenith_s3/s3_ops.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef S3_OPS_H
#define S3_OPS_H

typedef struct
{
	int			numfiles;
	char	  **filenames;
	int			szfilenames;	/* allocated size of filenames */
} ListObjectsResult;

extern ListObjectsResult *s3_ListObjects(const char *s3path);

extern StringInfo fetch_s3_file_memory(const char *s3path);
extern void fetch_s3_file(const char *s3path, const char *dstpath);

extern void put_s3_file(const char *localpath, const char *s3path, size_t filesize);

#endif							/* S3_OPS_H */
