/*-------------------------------------------------------------------------
 *
 * multiregion.h
 * 
 * contrib/neon/multiregion.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MULTIREGION_H
#define MULTIREGION_H

#include "postgres.h"

#include "access/xlogdefs.h"
#include "pagestore_client.h"

extern char *neon_region_timelines;

void DefineMultiRegionCustomVariables(void);
bool neon_multiregion_enabled(void);
void set_region_lsn(int region, ZenithResponse *msg);
XLogRecPtr get_region_lsn(int region);
void clear_region_lsns(void);

#endif