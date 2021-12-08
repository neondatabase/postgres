/* contrib/remotexact/remotexact.c */
#include "postgres.h"

#include "access/xact.h"
#include "access/remotexact.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "rwset.h"
#include "storage/predicate.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "miscadmin.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/* GUCs */
char	   *remotexact_connstring;

typedef struct RxReorderBufferRelationKey
{
	Oid			relid;
} RxReorderBufferRelationKey;

typedef struct RxReorderBufferRelation
{
	RxReorderBufferRelationKey key;

	bool		is_index;
	int			csn;
	int			nitems;
	StringInfoData pages;
	StringInfoData tuples;
} RxReorderBufferRelation;

typedef struct RxReorderBuffer
{
	MemoryContext context;

	RWSetHeader header;
	HTAB	   *readRelations;
} RxReorderBuffer;

RxReorderBuffer *CurrentRxReorderBuffer = NULL;
PGconn	   *XactServerConn;
bool		Connected = false;

static void init_read_write_set(void);

static void rx_collect_read_tuple(Relation relation, ItemPointer tid, TransactionId tuple_xid);
static void rx_collect_seq_scan_relation(Relation relation);
static void rx_collect_index_scan_page(Relation relation, BlockNumber blkno);
static void rx_clear_rwset(void);
static void rx_send_rwset_and_wait(void);

static RxReorderBufferRelation *get_read_relation(Oid relid);
static bool connect_to_txn_server(void);

static void
init_read_write_set(void)
{
	MemoryContext old_context;
	HASHCTL		hash_ctl;

	Assert(!CurrentRxReorderBuffer);
	Assert(MemoryContextIsValid(TopTransactionContext));

	old_context = MemoryContextSwitchTo(TopTransactionContext);

	CurrentRxReorderBuffer = (RxReorderBuffer *) palloc(sizeof(RxReorderBuffer));
	CurrentRxReorderBuffer->context = TopTransactionContext;

	CurrentRxReorderBuffer->header.dbid = 0;
	CurrentRxReorderBuffer->header.xid = InvalidTransactionId;

	hash_ctl.hcxt = CurrentRxReorderBuffer->context;
	hash_ctl.keysize = sizeof(RxReorderBufferRelationKey);
	hash_ctl.entrysize = sizeof(RxReorderBufferRelation);
	CurrentRxReorderBuffer->readRelations = hash_create("read relations",
														max_predicate_locks_per_xact,
														&hash_ctl,
														HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemoryContextSwitchTo(old_context);
}

static void
rx_collect_read_tuple(Relation relation, ItemPointer tid, TransactionId tuple_xid)
{
	RxReorderBufferRelation *read_relation;
	StringInfo	buf = NULL;

	/*
	 * TODO(ctring): Assert the type of current relation. Do the same with
	 * other rx_collect_* functions
	 */

	/*
	 * Ignore current tuple if this relation is an index or current xact wrote
	 * it.
	 */
	if (relation->rd_index != NULL || TransactionIdIsCurrentTransactionId(tuple_xid))
		return;

	if (CurrentRxReorderBuffer == NULL)
		init_read_write_set();

	/* TODO(ctring): can dbid change across statements? */
	CurrentRxReorderBuffer->header.dbid = relation->rd_node.dbNode;

	read_relation = get_read_relation(relation->rd_id);
	read_relation->is_index = false;
	read_relation->nitems++;

	buf = &read_relation->tuples;
	pq_sendint32(buf, ItemPointerGetBlockNumber(tid));
	pq_sendint16(buf, ItemPointerGetOffsetNumber(tid));
}

static void
rx_collect_seq_scan_relation(Relation relation)
{
	RxReorderBufferRelation *read_relation;

	if (CurrentRxReorderBuffer == NULL)
		init_read_write_set();

	CurrentRxReorderBuffer->header.dbid = relation->rd_node.dbNode;

	read_relation = get_read_relation(relation->rd_id);
	read_relation->is_index = false;

	/* TODO(ctring): change this after CSN is introduced */
	read_relation->csn = 1;
}

static void
rx_collect_index_scan_page(Relation relation, BlockNumber blkno)
{
	RxReorderBufferRelation *read_relation;
	StringInfo	buf = NULL;

	if (CurrentRxReorderBuffer == NULL)
		init_read_write_set();

	CurrentRxReorderBuffer->header.dbid = relation->rd_node.dbNode;

	read_relation = get_read_relation(relation->rd_id);
	read_relation->is_index = true;
	read_relation->nitems++;

	buf = &read_relation->pages;
	pq_sendint32(buf, blkno);
	pq_sendint32(buf, 1);		/* TODO(ctring): change this after CSN is
								 * introduced */
}

static void
rx_clear_rwset(void)
{
	CurrentRxReorderBuffer = NULL;
}

static void
rx_send_rwset_and_wait(void)
{
	RWSet	   *rwset;
	RWSetHeader *header;
	RxReorderBufferRelation *read_relation;
	HASH_SEQ_STATUS status;
	int			read_len = 0;
	StringInfoData buf;

	if (CurrentRxReorderBuffer == NULL)
		return;

	if (!connect_to_txn_server())
		return;

	initStringInfo(&buf);

	/* Assemble the header */
	header = &CurrentRxReorderBuffer->header;
	pq_sendint32(&buf, header->dbid);
	pq_sendint32(&buf, header->xid);

	/* Cursor now points to where the length of the read section is stored */
	buf.cursor = buf.len;
	/* Read section length will be updated later */
	pq_sendint32(&buf, 0);

	/* Assemble the read set */
	hash_seq_init(&status, CurrentRxReorderBuffer->readRelations);
	while ((read_relation = (RxReorderBufferRelation *) hash_seq_search(&status)) != NULL)
	{
		StringInfo	items = NULL;

		read_len -= buf.len;

		if (read_relation->is_index)
		{
			pq_sendbyte(&buf, 'I');
			pq_sendint32(&buf, read_relation->key.relid);
			pq_sendint32(&buf, read_relation->nitems);
			items = &read_relation->pages;
		}
		else
		{
			pq_sendbyte(&buf, 'T');
			pq_sendint32(&buf, read_relation->key.relid);
			pq_sendint32(&buf, read_relation->nitems);
			pq_sendint32(&buf, read_relation->csn);
			items = &read_relation->tuples;
		}

		pq_sendbytes(&buf, items->data, items->len);

		read_len += buf.len;
	}

	/* Update the length of the read section */
	*(int *) (buf.data + buf.cursor) = pg_hton32(read_len);

	/* Actually send the buffer to the xact server */
	if (PQputCopyData(XactServerConn, buf.data, buf.len) <= 0 || PQflush(XactServerConn))
	{
		ereport(WARNING, errmsg("[remotexact] failed to send read/write set"));
	}

	/* TODO(ctring): remove this after the remote worker is implemented */
	rwset = RWSetAllocate();
	buf.cursor = 0;
	RWSetDecode(rwset, &buf);
	ereport(LOG, errmsg("[remotexact] sent: %s", RWSetToString(rwset)));
	RWSetFree(rwset);
}

static RxReorderBufferRelation *
get_read_relation(Oid relid)
{
	RxReorderBufferRelationKey key;
	RxReorderBufferRelation *relation;
	bool		found;
	MemoryContext old_context;

	Assert(CurrentRxReorderBuffer);

	key.relid = relid;

	relation = (RxReorderBufferRelation *) hash_search(CurrentRxReorderBuffer->readRelations,
													   &key, HASH_ENTER, &found);
	/* Initialize a new relation entry if not found */
	if (!found)
	{
		old_context = MemoryContextSwitchTo(CurrentRxReorderBuffer->context);

		relation->nitems = 0;
		relation->csn = 0;
		relation->is_index = false;
		initStringInfo(&relation->pages);
		initStringInfo(&relation->tuples);

		MemoryContextSwitchTo(old_context);
	}
	return relation;
}

static bool
connect_to_txn_server(void)
{
	PGresult   *res;

	/* Reconnect if the connection is bad for some reason */
	if (Connected && PQstatus(XactServerConn) == CONNECTION_BAD)
	{
		PQfinish(XactServerConn);
		XactServerConn = NULL;
		Connected = false;

		ereport(LOG, errmsg("[remotexact] connection to transaction server broken, reconnecting..."));
	}

	if (Connected)
	{
		ereport(LOG, errmsg("[remotexact] reuse existing connection to transaction server"));
		return true;
	}

	XactServerConn = PQconnectdb(remotexact_connstring);

	if (PQstatus(XactServerConn) == CONNECTION_BAD)
	{
		char	   *msg = pchomp(PQerrorMessage(XactServerConn));

		PQfinish(XactServerConn);
		ereport(WARNING,
				errmsg("[remotexact] could not connect to the transaction server"),
				errdetail_internal("%s", msg));
		return Connected;
	}

	/* TODO(ctring): send a more useful starting message */
	res = PQexec(XactServerConn, "start");
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		ereport(WARNING, errmsg("[remotexact] invalid response from transaction server"));
		return Connected;
	}
	PQclear(res);

	Connected = true;

	ereport(LOG, errmsg("[remotexact] connected to transaction server"));

	return Connected;
}

static const RemoteXactHook remote_xact_hook =
{
	.collect_read_tuple = rx_collect_read_tuple,
	.collect_seq_scan_relation = rx_collect_seq_scan_relation,
	.collect_index_scan_page = rx_collect_index_scan_page,
	.clear_rwset = rx_clear_rwset,
	.send_rwset_and_wait = rx_send_rwset_and_wait
};

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable("remotexact.connstring",
							   "connection string to the transaction server",
							   NULL,
							   &remotexact_connstring,
							   "postgresql://127.0.0.1:10000",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   NULL, NULL, NULL);

	if (remotexact_connstring && remotexact_connstring[0])
	{
		SetRemoteXactHook(&remote_xact_hook);

		ereport(LOG, errmsg("[remotexact] initialized"));
		ereport(LOG, errmsg("[remotexact] xactserver connection string \"%s\"", remotexact_connstring));
	}
}
