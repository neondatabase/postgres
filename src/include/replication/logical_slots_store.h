/*-------------------------------------------------------------------------
 *
 * logical_slots_store.h
 *	  Logical replication slot state persistence
 *
 * IDENTIFICATION
 *	  src/include/replication/logical_slots_store.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICAL_SLOTS_STORE_H
#define LOGICAL_SLOTS_STORE_H

#include "replication/slot.h"

/* Hook for storing logical slot state */
typedef void (*logical_slots_store_hook_type) (const char *dir, ReplicationSlotPersistentData *saved_data);
extern PGDLLIMPORT logical_slots_store_hook_type logical_slots_store_hook;

/* Extension function - called by the hook */
extern void store_logical_slots_state(const char *dir, ReplicationSlotPersistentData *saved_data);

#endif							/* LOGICAL_SLOTS_STORE_H */

