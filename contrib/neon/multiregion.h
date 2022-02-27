/*-------------------------------------------------------------------------
 *
 * multiregion.h
 * 
 * contrib/zenith/multiregion.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MULTIREGION_H
#define MULTIREGION_H

#include "postgres.h"

#include "libpq-fe.h"

extern int zenith_current_region;

void DefineMultiRegionCustomVariables(void);

bool zenith_multiregion_enabled(void);
void zenith_multiregion_connect(PGconn **pageserver_conn, bool *connected);

int lookup_region(Oid spcid, Oid relid);

#endif