#ifndef NEON_XLOG_H
#define NEON_XLOG_H

#include "storage/off.h"

/*
 * The RMGR id of the Neon RMGR
 *
 * Reserved at https://wiki.postgresql.org/wiki/CustomWALResourceManagers
 */
#define RM_NEON_ID 134

#define XLOG_NEON_INIT_PAGE			0x80
#define XLOG_NEON_OPMASK			((~XLOG_NEON_INIT_PAGE) & XLR_RMGR_INFO_MASK)

/* from XLOG_HEAP_* */
#define XLOG_NEON_HEAP_INSERT		0x00
#define XLOG_NEON_HEAP_DELETE		0x10
#define XLOG_NEON_HEAP_UPDATE		0x20
#define XLOG_NEON_HEAP_HOT_UPDATE	0x30
#define XLOG_NEON_HEAP_LOCK			0x40
/* from XLOG_HEAP2_* */
#define XLOG_NEON_HEAP_MULTI_INSERT	0x50
#define XLOG_NEON_FILE				0x60
/* 2 variants available */

/* */
typedef struct xl_neon_heap_header {
	uint16		t_infomask2;
	uint16		t_infomask;
	uint32		t_cid;
	uint8		t_hoff;
} xl_neon_heap_header;

#define SizeOfNeonHeapHeader	(offsetof(xl_neon_heap_header, t_hoff) + sizeof(uint8))

/* This is what we need to know about insert */
typedef struct xl_neon_heap_insert
{
	OffsetNumber offnum;		/* inserted tuple's offset */
	uint8		flags;

	/* xl_neon_heap_header & TUPLE DATA in backup block 0 */
} xl_neon_heap_insert;

#define SizeOfNeonHeapInsert	(offsetof(xl_neon_heap_insert, flags) + sizeof(uint8))

/* This is what we need to know about delete */
typedef struct xl_neon_heap_delete
{
	TransactionId xmax;			/* xmax of the deleted tuple */
	OffsetNumber offnum;		/* deleted tuple's offset */
	uint8		infobits_set;	/* infomask bits */
	uint8		flags;
	uint32		t_cid;
} xl_neon_heap_delete;

#define SizeOfNeonHeapDelete	(offsetof(xl_neon_heap_delete, t_cid) + sizeof(uint32))

/*
 * This is what we need to know about update|hot_update
 *
 * Backup blk 0: new page
 *
 * If XLH_UPDATE_PREFIX_FROM_OLD or XLH_UPDATE_SUFFIX_FROM_OLD flags are set,
 * the prefix and/or suffix come first, as one or two uint16s.
 *
 * After that, xl_neon_heap_header and new tuple data follow.  The new tuple
 * data doesn't include the prefix and suffix, which are copied from the
 * old tuple on replay.
 *
 * If XLH_UPDATE_CONTAINS_NEW_TUPLE flag is given, the tuple data is
 * included even if a full-page image was taken.
 *
 * Backup blk 1: old page, if different. (no data, just a reference to the blk)
 */
typedef struct xl_neon_heap_update
{
	TransactionId old_xmax;		/* xmax of the old tuple */
	OffsetNumber old_offnum;	/* old tuple's offset */
	uint8		old_infobits_set;	/* infomask bits to set on old tuple */
	uint8		flags;
	uint32		t_cid;
	TransactionId new_xmax;		/* xmax of the new tuple */
	OffsetNumber new_offnum;	/* new tuple's offset */

	/*
	 * If XLH_UPDATE_CONTAINS_OLD_TUPLE or XLH_UPDATE_CONTAINS_OLD_KEY flags
	 * are set, xl_neon_heap_header and tuple data for the old tuple follow.
	 */
} xl_neon_heap_update;
#define SizeOfNeonHeapUpdate	(offsetof(xl_neon_heap_update, new_offnum) + sizeof(OffsetNumber))

typedef struct xl_neon_heap_lock
{
	TransactionId xmax;			/* might be a MultiXactId */
	uint32		t_cid;
	OffsetNumber offnum;		/* locked tuple's offset on page */
	uint8		infobits_set;	/* infomask and infomask2 bits to set */
	uint8		flags;			/* XLH_LOCK_* flag bits */
} xl_neon_heap_lock;
#define SizeOfNeonHeapLock	(offsetof(xl_neon_heap_lock, flags) + sizeof(uint8))

/*
 * This is what we need to know about a multi-insert.
 *
 * The main data of the record consists of this xl_neon_heap_multi_insert header.
 * 'offsets' array is omitted if the whole page is reinitialized
 * (XLOG_HEAP_INIT_PAGE).
 *
 * In block 0's data portion, there is an xl_neon_multi_insert_tuple struct,
 * followed by the tuple data for each tuple. There is padding to align
 * each xl_neon_multi_insert_tuple struct.
 */
typedef struct xl_neon_heap_multi_insert
{
	uint8		flags;
	uint16		ntuples;
	uint32		t_cid;
	OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
} xl_neon_heap_multi_insert;

#define SizeOfNeonHeapMultiInsert	offsetof(xl_neon_heap_multi_insert, offsets)

typedef struct xl_neon_multi_insert_tuple
{
	uint16		datalen;		/* size of tuple data that follows */
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		t_hoff;
	/* TUPLE DATA FOLLOWS AT END OF STRUCT */
} xl_neon_multi_insert_tuple;
#define SizeOfNeonMultiInsertTuple	(offsetof(xl_neon_multi_insert_tuple, t_hoff) + sizeof(uint8))

/* The type of file being logged to WAL. */
typedef enum xl_neon_file_filetype {
	XL_NEON_FILE_UPGRADE_TARBALL,
} xl_neon_file_filetype;

/* Necessary information for logging a file to WAL. */
typedef struct xl_neon_file
{
	uint32		size;
	uint8		filetype;
	uint8		data[FLEXIBLE_ARRAY_MEMBER];
} xl_neon_file;
#define SizeOfNeonFile	offsetof(xl_neon_file, data)

#endif //NEON_XLOG_H
