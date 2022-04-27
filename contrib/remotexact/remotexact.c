/* contrib/remotexact/remotexact.c */
#include "postgres.h"

#include "access/xact.h"
#include "access/remotexact.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "replication/logicalproto.h"
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

typedef struct ReadSetRelationKey
{
	Oid			relid;
} ReadSetRelationKey;

typedef struct ReadSetRelation
{
	ReadSetRelationKey key;

	bool		is_index;
	int			csn;
	int			nitems;

	StringInfoData pages;
	StringInfoData tuples;
} ReadSetRelation;

typedef struct RWSetCollectionBuffer
{
	MemoryContext context;

	RWSetHeader header;
	HTAB	   *read_relations;
	StringInfoData writes;
} RWSetCollectionBuffer;

static RWSetCollectionBuffer *rwset_collection_buffer = NULL;

PGconn	   *XactServerConn;
bool		Connected = false;

static void init_rwset_collection_buffer(Oid dbid);

static void rx_collect_relation(Oid dbid, Oid relid);
static void rx_collect_page(Oid dbid, Oid relid, BlockNumber blkno);
static void rx_collect_tuple(Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber tid);
static void rx_collect_insert(Relation relation, HeapTuple newtuple);
static void rx_collect_update(Relation relation, HeapTuple oldtuple, HeapTuple newtuple);
static void rx_collect_delete(Relation relation, HeapTuple oldtuple);
static void rx_clear_rwset_collection_buffer(void);
static void rx_send_rwset_and_wait(void);

static ReadSetRelation *get_read_relation(Oid relid);
static bool connect_to_txn_server(void);

static void
init_rwset_collection_buffer(Oid dbid)
{
	MemoryContext old_context;
	HASHCTL		hash_ctl;

	if (rwset_collection_buffer)
	{
		Oid old_dbid = rwset_collection_buffer->header.dbid; 
		if (old_dbid != dbid)
			ereport(ERROR,
					errmsg("Remotexact can access only one database"),
					errdetail("old dbid: %u, new dbid: %u", old_dbid, dbid));
		return;
	}

	old_context = MemoryContextSwitchTo(TopTransactionContext);

	rwset_collection_buffer = (RWSetCollectionBuffer *) palloc(sizeof(RWSetCollectionBuffer));
	rwset_collection_buffer->context = TopTransactionContext;

	rwset_collection_buffer->header.dbid = dbid;
	rwset_collection_buffer->header.xid = InvalidTransactionId;

	hash_ctl.hcxt = rwset_collection_buffer->context;
	hash_ctl.keysize = sizeof(ReadSetRelationKey);
	hash_ctl.entrysize = sizeof(ReadSetRelation);
	rwset_collection_buffer->read_relations = hash_create("read relations",
														  max_predicate_locks_per_xact,
														  &hash_ctl,
														  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	initStringInfo(&rwset_collection_buffer->writes);

	MemoryContextSwitchTo(old_context);
}

static void
rx_collect_relation(Oid dbid, Oid relid)
{
	ReadSetRelation *read_relation;

	init_rwset_collection_buffer(dbid);

	read_relation = get_read_relation(relid);
	read_relation->is_index = false;

	/* TODO(ctring): change this after CSN is introduced */
	read_relation->csn = 1;
}

static void
rx_collect_page(Oid dbid, Oid relid, BlockNumber blkno)
{
	ReadSetRelation *read_relation;
	StringInfo	buf = NULL;

	init_rwset_collection_buffer(dbid);

	read_relation = get_read_relation(relid);
	read_relation->is_index = true;
	read_relation->nitems++;

	buf = &read_relation->pages;
	pq_sendint32(buf, blkno);
	pq_sendint32(buf, 1);		/* TODO(ctring): change this after CSN is
								 * introduced */
}

static void
rx_collect_tuple(Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber offset)
{
	ReadSetRelation *read_relation;
	StringInfo	buf = NULL;

	init_rwset_collection_buffer(dbid);

	read_relation = get_read_relation(relid);
	read_relation->is_index = false;
	read_relation->nitems++;

	buf = &read_relation->tuples;
	pq_sendint32(buf, blkno);
	pq_sendint16(buf, offset);
}

static void
rx_collect_insert(Relation relation, HeapTuple newtuple)
{
	StringInfo buf = NULL;

	init_rwset_collection_buffer(relation->rd_node.dbNode);

	buf = &rwset_collection_buffer->writes;
	logicalrep_write_insert(buf, InvalidTransactionId, relation, newtuple, true /* binary */);
}

static void
rx_collect_update(Relation relation, HeapTuple oldtuple, HeapTuple newtuple)
{
	StringInfo buf = NULL;
	char	relreplident = relation->rd_rel->relreplident;

	init_rwset_collection_buffer(relation->rd_node.dbNode);

	// TOOD: We need to set the replica identity to something other than NOTHING
	// to collect the write set. Need to figure out a way to get rid of this step
	// or a check to prevent us from forgetting to do this step.
	if (relreplident != REPLICA_IDENTITY_DEFAULT &&
		relreplident != REPLICA_IDENTITY_FULL &&
		relreplident != REPLICA_IDENTITY_INDEX)
		return;

	buf = &rwset_collection_buffer->writes;
	logicalrep_write_update(buf, InvalidTransactionId, relation, oldtuple, newtuple, true /* binary */);
}

static void
rx_collect_delete(Relation relation, HeapTuple oldtuple)
{
	StringInfo buf = NULL;
	char	relreplident = relation->rd_rel->relreplident;

	init_rwset_collection_buffer(relation->rd_node.dbNode);

	// TOOD: We need to set the replica identity to something other than NOTHING
	// to collect the write set. Need to figure out a way to get rid of this step
	// or a check to prevent us from forgetting to do this step.
	if (relreplident != REPLICA_IDENTITY_DEFAULT &&
		relreplident != REPLICA_IDENTITY_FULL &&
		relreplident != REPLICA_IDENTITY_INDEX)
		return;

	if (oldtuple == NULL)
		return;

	buf = &rwset_collection_buffer->writes;
	logicalrep_write_delete(buf, InvalidTransactionId, relation, oldtuple, true /* binary */);
}


static void
rx_clear_rwset_collection_buffer(void)
{
	rwset_collection_buffer = NULL;
}

static void
rx_send_rwset_and_wait(void)
{
	RWSet	   *rwset;
	RWSetHeader *header;
	ReadSetRelation *read_relation;
	HASH_SEQ_STATUS status;
	int			read_len = 0;
	StringInfoData buf;

	if (rwset_collection_buffer == NULL)
		return;

	if (!connect_to_txn_server())
		return;

	initStringInfo(&buf);

	/* Assemble the header */
	header = &rwset_collection_buffer->header;
	pq_sendint32(&buf, header->dbid);
	pq_sendint32(&buf, header->xid);

	/* Cursor now points to where the length of the read section is stored */
	buf.cursor = buf.len;
	/* Read section length will be updated later */
	pq_sendint32(&buf, 0);

	/* Assemble the read set */
	hash_seq_init(&status, rwset_collection_buffer->read_relations);
	while ((read_relation = (ReadSetRelation *) hash_seq_search(&status)) != NULL)
	{
		StringInfo	items = NULL;

		/* Accumulate the length of the buffer used by each relation */
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

	pq_sendbytes(&buf, rwset_collection_buffer->writes.data, rwset_collection_buffer->writes.len);

	/* Actually send the buffer to the xact server */
	if (PQputCopyData(XactServerConn, buf.data, buf.len) <= 0 || PQflush(XactServerConn))
		ereport(WARNING, errmsg("[remotexact] failed to send read/write set"));

	/* TODO(ctring): This code is for debugging rwset remove all after the remote worker is implemented */
	rwset = RWSetAllocate();
	buf.cursor = 0;
	RWSetDecode(rwset, &buf);
	ereport(LOG, errmsg("[remotexact] sent: %s", RWSetToString(rwset)));
	RWSetFree(rwset);
}

static ReadSetRelation *
get_read_relation(Oid relid)
{
	ReadSetRelationKey key;
	ReadSetRelation *relation;
	bool		found;
	MemoryContext old_context;

	Assert(rwset_collection_buffer);

	key.relid = relid;

	relation = (ReadSetRelation *) hash_search(rwset_collection_buffer->read_relations,
											   &key, HASH_ENTER, &found);
	/* Initialize a new relation entry if not found */
	if (!found)
	{
		old_context = MemoryContextSwitchTo(rwset_collection_buffer->context);

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
	.collect_tuple = rx_collect_tuple,
	.collect_relation = rx_collect_relation,
	.collect_page = rx_collect_page,
	.clear_rwset = rx_clear_rwset_collection_buffer,
	.collect_insert = rx_collect_insert,
	.collect_update = rx_collect_update,
	.collect_delete = rx_collect_delete,
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
