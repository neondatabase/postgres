/* contrib/remotexact/rwset.c */
#include "postgres.h"

#include "access/transam.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "rwset.h"
#include "utils/memutils.h"

static void decode_header(RWSet *rwset, StringInfo msg);
static RWSetRelation *decode_relation(RWSet *rwset, StringInfo msg);
static RWSetRelation *alloc_relation(RWSet *rwset);
static RWSetPage *decode_page(RWSet *rwset, StringInfo msg);
static RWSetPage *alloc_page(RWSet *rwset);
static RWSetTuple *decode_tuple(RWSet *rwset, StringInfo msg);
static RWSetTuple *alloc_tuple(RWSet *rwset);

RWSet *
RWSetAllocate(void)
{
	RWSet	   *rwset;
	MemoryContext new_ctx;

	new_ctx = AllocSetContextCreate(CurrentMemoryContext,
									"Read/write set",
									ALLOCSET_DEFAULT_SIZES);
	rwset = (RWSet *) MemoryContextAlloc(new_ctx, sizeof(RWSet));

	rwset->context = new_ctx;

	rwset->header.dbid = 0;
	rwset->header.xid = InvalidTransactionId;

	dlist_init(&rwset->relations);

	return rwset;
}

void
RWSetFree(RWSet *rwset)
{
	MemoryContextDelete(rwset->context);
}

void
RWSetDecode(RWSet *rwset, StringInfo msg)
{
	int			read_set_len;
	int			consumed = 0;
	int			prev_cursor;

	decode_header(rwset, msg);

	read_set_len = pq_getmsgint(msg, 4);

	if (read_set_len > msg->len)
	{
		ereport(ERROR,
				errmsg("length of read set (%d) too large", read_set_len),
				errdetail("remaining message length: %d", msg->len));
	}

	while (consumed < read_set_len)
	{
		RWSetRelation *rel;

		prev_cursor = msg->cursor;

		rel = decode_relation(rwset, msg);
		dlist_push_tail(&rwset->relations, &rel->node);

		consumed += msg->cursor - prev_cursor;
	}

	if (consumed > read_set_len)
	{
		ereport(ERROR,
				errmsg("length of read set (%d) is corrupted", read_set_len),
				errdetail("length of decoded read set: %d", consumed));
	}
}

void
decode_header(RWSet *rwset, StringInfo msg)
{
	rwset->header.dbid = pq_getmsgint(msg, 4);
	rwset->header.xid = pq_getmsgint(msg, 4);
}

RWSetRelation *
decode_relation(RWSet *rwset, StringInfo msg)
{
	RWSetRelation *rel;
	unsigned char reltype;
	int			nitems;
	int			i;

	rel = alloc_relation(rwset);

	reltype = pq_getmsgbyte(msg);

	if (reltype == 'I')
		rel->is_index = true;
	else if (reltype == 'T')
		rel->is_index = false;
	else
		ereport(ERROR, errmsg("invalid relation type: %c", reltype));

	rel->relid = pq_getmsgint(msg, 4);
	nitems = pq_getmsgint(msg, 4);

	if (rel->is_index)
	{
		for (i = 0; i < nitems; i++)
		{
			RWSetPage  *page = decode_page(rwset, msg);

			dlist_push_tail(&rel->pages, &page->node);
		}
	}
	else
	{
		rel->csn = pq_getmsgint(msg, 4);

		for (i = 0; i < nitems; i++)
		{
			RWSetTuple *tup = decode_tuple(rwset, msg);

			dlist_push_tail(&rel->tuples, &tup->node);
		}
	}

	return rel;
}

RWSetRelation *
alloc_relation(RWSet *rwset)
{
	MemoryContext ctx;
	RWSetRelation *rel;

	ctx = rwset->context;
	rel = (RWSetRelation *) MemoryContextAlloc(ctx, sizeof(RWSetRelation));

	memset(rel, 0, sizeof(RWSetRelation));

	dlist_init(&rel->pages);
	dlist_init(&rel->tuples);

	return rel;
}

RWSetPage *
decode_page(RWSet *rwset, StringInfo msg)
{
	RWSetPage  *page;

	page = alloc_page(rwset);

	page->blkno = pq_getmsgint(msg, 4);
	page->csn = pq_getmsgint(msg, 4);

	return page;
}


RWSetPage *
alloc_page(RWSet *rwset)
{
	MemoryContext ctx;
	RWSetPage  *page;

	ctx = rwset->context;
	page = (RWSetPage *) MemoryContextAlloc(ctx, sizeof(RWSetPage));

	memset(page, 0, sizeof(RWSetPage));

	return page;
}

RWSetTuple *
decode_tuple(RWSet *rwset, StringInfo msg)
{
	RWSetTuple *tup;
	int			blkno;
	int			offset;

	blkno = pq_getmsgint(msg, 4);
	offset = pq_getmsgint(msg, 2);

	tup = alloc_tuple(rwset);
	ItemPointerSet(&tup->tid, blkno, offset);

	return tup;
}

RWSetTuple *
alloc_tuple(RWSet *rwset)
{
	MemoryContext ctx;
	RWSetTuple *tup;

	ctx = rwset->context;
	tup = (RWSetTuple *) MemoryContextAlloc(ctx, sizeof(RWSetTuple));

	memset(tup, 0, sizeof(RWSetTuple));

	return tup;
}

char *
RWSetToString(RWSet *rwset)
{
	StringInfoData s;
	RWSetHeader *header;
	dlist_iter	rel_iter;
	bool		first_rel = true;

	initStringInfo(&s);

	/* Header */
	header = &rwset->header;
	appendStringInfoString(&s, "{\"header\": ");
	appendStringInfo(&s, "{\"dbid\": %d, \"xid\": %d}, ", header->dbid, header->xid);

	/* Relations */
	appendStringInfoString(&s, "\"relations\": [");
	dlist_foreach(rel_iter, &rwset->relations)
	{
		RWSetRelation *rel = dlist_container(RWSetRelation, node, rel_iter.cur);
		dlist_iter	item_iter;
		bool		first_item;

		if (!first_rel)
			appendStringInfoString(&s, ", ");
		first_rel = false;

		appendStringInfoString(&s, "{");
		appendStringInfo(&s, "\"relid\": %d, ", rel->relid);
		appendStringInfo(&s, "\"is_index\": %d, ", rel->is_index);
		appendStringInfo(&s, "\"csn\": %d, ", rel->csn);

		/* Pages */
		appendStringInfoString(&s, "\"pages\": [");
		first_item = true;
		dlist_foreach(item_iter, &rel->pages)
		{
			RWSetPage  *page = dlist_container(RWSetPage, node, item_iter.cur);

			if (!first_item)
				appendStringInfoString(&s, ", ");
			first_item = false;

			appendStringInfoString(&s, "{");
			appendStringInfo(&s, "\"blkno\": %d, ", page->blkno);
			appendStringInfo(&s, "\"csn\": %d", page->csn);
			appendStringInfoString(&s, "}");
		}
		appendStringInfoString(&s, "], ");

		/* Tuples */
		appendStringInfoString(&s, "\"tuples\": [");
		first_item = true;
		dlist_foreach(item_iter, &rel->tuples)
		{
			RWSetTuple *tup = dlist_container(RWSetTuple, node, item_iter.cur);

			if (!first_item)
				appendStringInfoString(&s, ", ");
			first_item = false;

			appendStringInfoString(&s, "{");
			appendStringInfo(&s, "\"blkno\": %d, ", ItemPointerGetBlockNumber(&tup->tid));
			appendStringInfo(&s, "\"offset\": %d", ItemPointerGetOffsetNumber(&tup->tid));
			appendStringInfoString(&s, "}");
		}
		appendStringInfoString(&s, "]");

		appendStringInfoString(&s, "}");
	}
	appendStringInfoString(&s, "]");

	appendStringInfoString(&s, "}");

	return s.data;
}
