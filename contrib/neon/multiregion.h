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

extern char *neon_region_timelines;

void DefineMultiRegionCustomVariables(void);
bool neon_multiregion_enabled(void);

#endif