/*-------------------------------------------------------------------------
 *
 * buf_table_flat.h
 *	  Index-chained shared buffer mapping table (alternative to dynahash).
 *
 * src/include/storage/buf_table_flat.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUF_TABLE_FLAT_H
#define BUF_TABLE_FLAT_H

#include "funcapi.h"
#include "storage/buf_internals.h"

extern Size BufTableFlat_ShmemSize(int size);
extern void BufTableFlat_Init(int size);
extern uint32 BufTableFlat_HashCode(BufferTag *tagPtr);
extern int BufTableFlat_Lookup(BufferTag *tagPtr, uint32 hashcode);
extern int BufTableFlat_Insert(BufferTag *tagPtr, uint32 hashcode, int buf_id);
extern void BufTableFlat_Delete(BufferTag *tagPtr, uint32 hashcode);
extern void BufTableFlat_GetContents(Tuplestorestate *tupstore, TupleDesc tupdesc);

#endif
