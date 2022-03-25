/*-------------------------------------------------------------------------
 *
 * pg_remote_tablespace.c
 *	  routines to support manipulation of the pg_remote_tablespace relation
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_remote_tablespace.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_remote_tablespace.h"

/* GUC variable */
int current_region;
