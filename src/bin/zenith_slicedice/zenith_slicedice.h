/*-------------------------------------------------------------------------
 *
 * zenith_slicedice.h
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef ZENITH_SLICEDICE_H
#define ZENITH_SLICEDICE_H


/* logging support */
#define pg_fatal(...) do { pg_log_fatal(__VA_ARGS__); exit(1); } while(0)

extern void redist_init(XLogRecPtr start_lsn_arg, XLogRecPtr end_lsn_arg);
extern void handle_record(XLogReaderState *record);
extern void upload_slice_wal_files(void);


#endif							/* ZENITH_SLICEDICE_H */
