/*-------------------------------------------------------------------------
 *
 * receiver_worker.h
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/zenith/receiver_worker.h
 *-------------------------------------------------------------------------
 */
#ifndef _RECEIVER_WORKER_H
#define _RECEIVER_WORKER_H

#include "postgres.h"

void receiver_main(Datum main_arg);

#endif							/* _RECEIVER_WORKER_H */
