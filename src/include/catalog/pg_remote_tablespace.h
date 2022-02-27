/*-------------------------------------------------------------------------
 *
 * pg_remote_tablespace.h
 *	  definition of the "remote_tablespace" system catalog (pg_remote_tablespace)
 *
 * src/include/catalog/pg_remote_tablespace.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_REMOTE_TABLESPACE
#define PG_REMOTE_TABLESPACE

#include "catalog/genbki.h"
#include "catalog/pg_remote_tablespace_d.h"

/* ----------------
 *		pg_remote_tablespace definition.  cpp turns this into
 *		typedef struct FormData_pg_remote_tablespace
 * ----------------
 */
CATALOG(pg_remote_tablespace,8000,RemoteTablespaceRelationId) BKI_SHARED_RELATION
{
	/* Oid of the tablespace */
	Oid			spcid BKI_LOOKUP(pg_tablespace);
	/* Id of region owning the tablespace */
	int32		regionid BKI_FORCE_NOT_NULL;
} FormData_pg_remote_tablespace;

/* ----------------
 *		Form_pg_remote_tablespace corresponds to a pointer to a tuple with
 *		the format of pg_remote_tablespace relation.
 * ----------------
 */
typedef FormData_pg_remote_tablespace *Form_pg_remote_tablespace;

DECLARE_UNIQUE_INDEX_PKEY(pg_remote_tablespace_spcid_index, 8001, on pg_remote_tablespace using btree(spcid oid_ops));
#define RemoteTablespaceSpcIdIndexId  8001

#endif							/* PG_REMOTE_TABLESPACE */
