/*-------------------------------------------------------------------------
 *
 * memstore.c -- simplistic in-memory page storage.
 *
 * Copyright (c) 2013-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/zenith/memstore.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/lwlock.h"
#include "utils/hsearch.h"
#include "storage/shmem.h"
#include "fmgr.h"
#include "access/xlogreader.h"
#include "access/xlog_internal.h"
#include "access/xlog.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/proc.h"
#include "storage/pagestore_client.h"
#include "miscadmin.h"

#include "libpq/libpq.h"
#include "libpq/pqformat.h"

#include "memstore.h"
#include "zenith_store.h"

MemStore *memStore;

void
memstore_init()
{
	Size		size = 0;
	size = add_size(size, 1000*1000*1000);
	size = MAXALIGN(size);
	RequestAddinShmemSpace(size);

	RequestNamedLWLockTranche("memStoreLock", 1);
}

void
memstore_init_shmem()
{
	HASHCTL		info;
	bool		found;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(PerPageWalHashKey);
	info.entrysize = sizeof(PerPageWalHashEntry);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	memStore = ShmemInitStruct("memStore",
								 sizeof(MemStore),
								 &found);

	if (!found)
		memStore->lock = &(GetNamedLWLockTranche("memStoreLock"))->lock;

	memStore->pages = ShmemInitHash("memStorePAges",
		10*1000, /* minsize */
		20*1000, /* maxsize */
		&info, HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
}

void
memstore_insert(PerPageWalHashKey key, XLogRecPtr lsn, XLogRecord *record)
{
	bool found;
	PerPageWalHashEntry *hash_entry;
	PerPageWalRecord *list_entry;

	/*
	 * XXX: could be done with one allocation, but that is prototype anyway so
	 * don't bother with offsetoff.
	 */
	list_entry = ShmemAlloc(sizeof(PerPageWalRecord));
	list_entry->record = ShmemAlloc(record->xl_tot_len);
	list_entry->lsn = lsn;
	list_entry->prev = NULL;
	list_entry->next = NULL;
	memcpy(list_entry->record, record, record->xl_tot_len);

	LWLockAcquire(memStore->lock, LW_EXCLUSIVE);

	hash_entry = (PerPageWalHashEntry *) hash_search(memStore->pages, &key, HASH_ENTER, &found);

	if (!found)
	{
		Assert(!hash_entry->newest);
		Assert(!hash_entry->oldest);
	}

	PerPageWalRecord *prev_newest = hash_entry->newest;
	hash_entry->newest = list_entry;
	list_entry->prev = prev_newest;
	if (prev_newest)
		prev_newest->next = list_entry;
	if (!hash_entry->oldest)
		hash_entry->oldest = list_entry;

	LWLockRelease(memStore->lock);
}


PerPageWalRecord *
memstore_get_oldest(PerPageWalHashKey *key)
{
	bool found;
	PerPageWalHashEntry *hash_entry;
	PerPageWalRecord *result = NULL;

	LWLockAcquire(memStore->lock, LW_SHARED);

	hash_entry = (PerPageWalHashEntry *) hash_search(memStore->pages, key, HASH_FIND, &found);

	if (found) {
		result = hash_entry->oldest;
	}

	LWLockRelease(memStore->lock);

	/*
	 * For now we never modify records or change their next field, so unlocked
	 * access would be okay.
	 */
	return result;
}

PG_FUNCTION_INFO_V1(zenith_store_get_page);

static void
get_page_by_key(PerPageWalHashKey *key, char **raw_page_data)
{
	PerPageWalRecord *record_entry = memstore_get_oldest(key);

	if (!record_entry)
	{
		memset(*raw_page_data, '\0', BLCKSZ);
		return;
	}

	/* recovery here */
	int			chain_len = 0;
	char	   *errormsg;
	for (; record_entry; record_entry = record_entry->next)
	{
		XLogReaderState reader_state = {
			.ReadRecPtr = 0,
			.EndRecPtr = record_entry->lsn,
			.decoded_record = record_entry->record
		};

		if (!DecodeXLogRecord(&reader_state, record_entry->record, &errormsg))
			zenith_log(ERROR, "failed to decode WAL record: %s", errormsg);

		InRecovery = true;
		RmgrTable[record_entry->record->xl_rmid].rm_redo(&reader_state);
		InRecovery = false;

		chain_len++;
	}
	zenith_log(RequestTrace, "Page restored: chain len is %d", chain_len);

	/* Take a verbatim copy of the page */
	Buffer		buf;
	BufferTag	newTag;			/* identity of requested block */
	uint32		newHash;		/* hash value for newTag */
	LWLock	   *newPartitionLock;	/* buffer partition lock for it */
	int			buf_id;
	bool		valid = false;
	BufferDesc *bufdesc = NULL;

	/* create a tag so we can lookup the buffer */
	INIT_BUFFERTAG(newTag, key->rnode, key->forknum, key->blkno);

	/* determine its hash code and partition lock ID */
	newHash = BufTableHashCode(&newTag);
	newPartitionLock = BufMappingPartitionLock(newHash);

	/* see if the block is in the buffer pool already */
	LWLockAcquire(newPartitionLock, LW_SHARED);
	buf_id = BufTableLookup(&newTag, newHash);

	if (buf_id >= 0)
	{
		bufdesc = GetBufferDescriptor(buf_id);
		valid = PinBuffer(bufdesc, NULL);
	}
	/* Can release the mapping lock as soon as we've pinned it */
	LWLockRelease(newPartitionLock);

	if (!valid)
		zenith_log(ERROR, "Can't pin buffer");

	buf = BufferDescriptorGetBuffer(bufdesc);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	memcpy(*raw_page_data, BufferGetPage(buf), BLCKSZ);
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buf);
}


Datum
zenith_store_get_page(PG_FUNCTION_ARGS)
{
	uint64		sysid = PG_GETARG_INT64(0);
	Oid			tspaceno = PG_GETARG_OID(1);
	Oid			dbno = PG_GETARG_OID(2);
	Oid			relno = PG_GETARG_OID(3);
	ForkNumber	forknum = PG_GETARG_INT32(4);
	int64		blkno = PG_GETARG_INT64(5);

	bytea	   *raw_page;
	char	   *raw_page_data;

	/* Initialize buffer to copy to */
	raw_page = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(raw_page, BLCKSZ + VARHDRSZ);
	raw_page_data = VARDATA(raw_page);

	PerPageWalHashKey key;
	memset(&key, '\0', sizeof(PerPageWalHashKey));
	key.system_identifier = sysid;
	key.rnode.spcNode = tspaceno;
	key.rnode.dbNode = dbno;
	key.rnode.relNode = relno;
	key.forknum = forknum;
	key.blkno = blkno;

	get_page_by_key(&key, &raw_page_data);

	PG_RETURN_BYTEA_P(raw_page);
}


PG_FUNCTION_INFO_V1(zenith_store_dispatcher);

Datum
zenith_store_dispatcher(PG_FUNCTION_ARGS)
{
	StringInfoData s;

	/* switch client to COPYBOTH */
	pq_beginmessage(&s, 'W');
	pq_sendbyte(&s, 0);			/* copy_is_binary */
	pq_sendint16(&s, 0);		/* numAttributes */
	pq_endmessage(&s);
	pq_flush();

	zenith_log(RequestTrace, "got connection");

	// MyProc->xmin = InvalidTransactionId;

	for (;;)
	{
		StringInfoData msg;
		initStringInfo(&msg);

		ModifyWaitEvent(FeBeWaitSet, 0, WL_SOCKET_READABLE, NULL);

		pq_startmsgread();
		pq_getbyte(); /* libpq message type 'd' */
		if (pq_getmessage(&msg, 0) != 0)
			zenith_log(ERROR, "failed to read client request");

		ZenithMessage *raw_req = zm_unpack(&msg);

		zenith_log(RequestTrace, "got page request: %s", ZenithMessageStr[raw_req->tag]);

		if (messageTag(raw_req) != T_ZenithExistsRequest
			&& messageTag(raw_req) != T_ZenithTruncRequest
			&& messageTag(raw_req) != T_ZenithUnlinkRequest
			&& messageTag(raw_req) != T_ZenithNblocksRequest
			&& messageTag(raw_req) != T_ZenithReadRequest)
		{
			zenith_log(ERROR, "invalid request");
		}
		ZenithRequest *req = (ZenithRequest *) raw_req;

		PerPageWalHashKey key;
		memset(&key, '\0', sizeof(PerPageWalHashKey));
		key.system_identifier = req->system_id;
		key.rnode = req->page_key.rnode;
		key.forknum = req->page_key.forknum;
		key.blkno = req->page_key.blkno;

		ZenithResponse resp;
		memset(&resp, '\0', sizeof(ZenithResponse));

		if (req->tag == T_ZenithExistsRequest)
		{
			resp.tag = T_ZenithStatusResponse;
			resp.ok = true;
		}
		else if (req->tag == T_ZenithTruncRequest)
		{
			resp.tag = T_ZenithStatusResponse;
			resp.ok = true;
		}
		else if (req->tag == T_ZenithUnlinkRequest)
		{
			resp.tag = T_ZenithStatusResponse;
			resp.ok = true;
		}
		else if (req->tag == T_ZenithNblocksRequest)
		{
			resp.tag = T_ZenithNblocksResponse;
			resp.n_blocks = 0;
			resp.ok = true;
		}
		else if (req->tag == T_ZenithReadRequest)
		{
			char *raw_page_data = (char *) palloc(BLCKSZ);
			get_page_by_key(&key, &raw_page_data);

			resp.tag = T_ZenithReadResponse;
			resp.page = raw_page_data;
			resp.ok = true;
		}
		else
			Assert(false);

		/* respond */
		StringInfoData resp_str = zm_pack((ZenithMessage *) &resp, false);
		resp_str.cursor = 'd';

	zenith_log(LOG, "[XXXXXX] sending copy data %x:%x:%x:%x:%x:%x l=%d",
		*resp_str.data & 0xff,
		*(resp_str.data + 1) & 0xff,
		*(resp_str.data + 2) & 0xff,
		*(resp_str.data + 3) & 0xff,
		*(resp_str.data + 4) & 0xff,
		*(resp_str.data + 5) & 0xff,
		resp_str.len
		);

		pq_endmessage(&resp_str);
		pq_flush();

		zenith_log(RequestTrace, "responded: %s", ZenithMessageStr[raw_req->tag]);

		CHECK_FOR_INTERRUPTS();
	}
}

