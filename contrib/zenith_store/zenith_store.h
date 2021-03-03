/*-------------------------------------------------------------------------
 *
 * zenith_store.h - experiment with remote page storage
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/zenith/zenith_store.h
 *-------------------------------------------------------------------------
 */
#ifndef _ZENITH_STORE_H
#define _ZENITH_STORE_H

char		   *conn_string;

typedef enum ZenithLogTag
{
	ReceiverLog = LOG,
	ReceiverTrace = LOG,
	RequestTrace = LOG,
} ZenithLogTag;

#define ZENITH_TAG "[ZENITH] "
#define zenith_log(tag, fmt, ...) ereport(tag, \
		(errmsg(ZENITH_TAG fmt, ## __VA_ARGS__), \
		 errhidestmt(true), errhidecontext(true)))

#endif							/* _ZENITH_STORE_H */
