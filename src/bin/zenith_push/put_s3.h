/*-------------------------------------------------------------------------
 *
 * put_s3.h
 *	  
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/zenith_push/put_s3.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PUT_S3_H
#define PUT_S3_H

#include "fe_utils/simple_list.h"

extern void put_s3_file(const char *s3path, const char *dstpath, size_t filesize);

#endif							/* PUT_S3_H */
