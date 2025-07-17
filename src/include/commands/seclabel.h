/*
 * seclabel.h
 *
 * Prototypes for functions in commands/seclabel.c
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */
#ifndef SECLABEL_H
#define SECLABEL_H

#include "catalog/objectaddress.h"

/*
 * Internal APIs
 */
extern char *GetSecurityLabel(const ObjectAddress *object,
							  const char *provider);
extern void SetSecurityLabel(const ObjectAddress *object,
							 const char *provider, const char *label);
extern void DeleteSecurityLabel(const ObjectAddress *object);
extern void DeleteSharedSecurityLabel(Oid objectId, Oid classId);

/*
 * Statement and ESP hook support
 */
extern ObjectAddress ExecSecLabelStmt(SecLabelStmt *stmt);

typedef void (*check_object_relabel_type) (const ObjectAddress *object,
										   const char *seclabel);
extern void register_label_provider(const char *provider_name,
									check_object_relabel_type hook);

/* BEGIN_PG_NEON */

/*
 * OnlineTableSecurityLabel_hook is used to allow customers to perform DDL on
 * an online table. By default, only databricks_superuser can.
 * OnlineTableSecurityLabel_hook checks if the session user is one of the
 * roles attached to the online table. If it is, it allows updating the label
 * to add/remove roles. Otherwise, a permission error is thrown.
 */
typedef void (*OnlineTableSecurityLabel_hook_type) (SecLabelStmt *stmt,
													const ObjectAddress *object,
													Relation relation);

extern PGDLLIMPORT OnlineTableSecurityLabel_hook_type OnlineTableSecurityLabel_hook;
/* END_PG_NEON */

#endif							/* SECLABEL_H */
