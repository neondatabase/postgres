/*-------------------------------------------------------------------------
 *
 * s3_sign.h
 *	  S3 compatible V4 signature generation routines
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/zenith_s3/s3_sign.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef S3_SIGN_H
#define S3_SIGN_H

#include <curl/curl.h>		/* for curl_slist */

#ifdef FRONTEND
#include "fe_utils/simple_list.h"
#else
#include "nodes/pg_list.h"
#endif


extern struct curl_slist *s3_get_authorization_hdrs(const char *host,
													const char *region,
													const char *method,
													const char *path,
													const char *bodyhash,
													const char *accesskeyid,
													const char *secret);

#endif							/* S3_SIGN_H */
