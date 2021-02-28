/*-------------------------------------------------------------------------
 *
 * zenith_s3_sign.h
 *	  S3 compatible V4 signature generation routines
 *
 * FIXME: backend has its own copy of this.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/fe_utils/zenith_s3_sign.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ZENITH_S3_SIGN_H
#define ZENITH_S3_SIGN_H

#include "fe_utils/simple_list.h"

extern SimpleStringList *s3_get_authorization_hdrs(const char *host,
												   const char *region,
												   const char *method,
												   const char *path,
												   const char *bodyhash,
												   const char *accesskeyid,
												   const char *secret);

#endif							/* ZENITH_S3_SIGN_H */
