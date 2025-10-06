/* -------------------------------------------------------------------------
 *
 * pgstat_waitevent.c
 *	  Implementation of wait event statistics.
 *
 * This file contains the implementation of wait event statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_waitevent.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"

bool		have_wait_event_stats = false;

static PgStat_PendingWaitevent PendingWaitEventStats;

/*
 * Support function for the SQL-callable pgstat* functions. Returns
 * a pointer to the wait events statistics struct.
 */
PgStat_WaitEvent *
pgstat_fetch_stat_wait_event(uint32 wait_event_info)
{
	PgStat_WaitEvent *wait_event_entry;

	wait_event_entry = (PgStat_WaitEvent *) pgstat_fetch_entry(PGSTAT_KIND_WAIT_EVENT,
															   InvalidOid, (uint64) wait_event_info);

	return wait_event_entry;
}

/*
 * Returns a pointer to the first counter for a specific class.
 */
static PgStat_Counter *
waitEventGetClassCounters(int64 *waitEventStats, int classId)
{
	int			offset = WaitClassTable[classId].offSet;

	return &waitEventStats[offset];
}

/*
 * Returns a pointer to the counter for a specific wait event.
 */
static PgStat_Counter *
waitEventGetCounter(int64 *waitEventStats, int classId, int eventId)
{
	int64	   *classCounters;

	Assert(classId >= 0 && classId < NB_WAITCLASSTABLE_ENTRIES);
	Assert(eventId >= 0 && eventId < WaitClassTable[classId].numberOfEvents);

	classCounters = waitEventGetClassCounters(waitEventStats, classId);

	return &classCounters[eventId];
}

/*
 * Increment a wait event stat counter.
 */
inline void
waitEventIncrementCounter(uint32 wait_event_info)
{
	DecodedWaitInfo waitInfo;
	PgStat_Counter *counter;
	uint32		classId;
	uint16		eventId;

	classId = *my_wait_event_info & WAIT_EVENT_CLASS_MASK;
	eventId = *my_wait_event_info & WAIT_EVENT_ID_MASK;

	if (classId == 0 && eventId == 0)
		return;

	/* Don't take into account user defined LWLock in the stats */
	if (classId == PG_WAIT_LWLOCK && eventId >= LWTRANCHE_FIRST_USER_DEFINED)
		return;

	/* Don't take into account custom wait event extension in the stats */
	if (classId == PG_WAIT_EXTENSION && eventId >= WAIT_EVENT_CUSTOM_INITIAL_ID)
		return;

	/* Don't take account PG_WAIT_INJECTIONPOINT */
	if (classId == PG_WAIT_INJECTIONPOINT)
		return;

	WAIT_EVENT_INFO_DECODE(waitInfo, wait_event_info);

	counter = waitEventGetCounter(PendingWaitEventStats.counts, waitInfo.classId,
								  waitInfo.eventId);

	(*counter)++;

	have_wait_event_stats = true;
}

const char *
get_wait_event_name_from_index(int index)
{
	/* Iterate through the WaitClassTable */
	for (int classIdx = 0; classIdx < NB_WAITCLASSTABLE_ENTRIES; classIdx++)
	{
		int			classOffset = WaitClassTable[classIdx].offSet;
		int			classSize = WaitClassTable[classIdx].numberOfEvents;

		/* Skip empty entries */
		if (WaitClassTable[classIdx].numberOfEvents == 0)
			continue;

		/* Check if the index falls within this class section */
		if (index >= classOffset && index < classOffset + classSize)
		{
			/* Calculate the event ID within this class */
			int			eventId = index - classOffset;

			return WaitClassTable[classIdx].eventNames[eventId];
		}
	}

	Assert(false);
	return "unknown";
}

/*
 * Flush out locally pending wait event statistics
 *
 * Returns true if some statistics could not be flushed due to lock contention.
 */

bool
pgstat_wait_event_flush_cb(bool nowait)
{
	PgStat_EntryRef *entry_ref;
	bool		could_not_be_flushed = false;

	if (!have_wait_event_stats)
		return false;

	for (int classIdx = 0; classIdx < NB_WAITCLASSTABLE_ENTRIES; classIdx++)
	{
		WaitClassTableEntry *class;
		int			classOffset;
		int			classSize;

		/* Skip empty entries */
		if (WaitClassTable[classIdx].numberOfEvents == 0)
			continue;

		class = &WaitClassTable[classIdx];

		classOffset = class->offSet;
		classSize = class->numberOfEvents;

		for (int eventId = 0; eventId < classSize; eventId++)
		{
			const char *name;
			PgStatShared_WaitEvent *shwaiteventent;
			PgStat_Counter *shstat;
			PgStat_Counter pending_counter;
			uint32		wait_event_info;

			name = get_wait_event_name_from_index(classOffset + eventId);

			if (!name)
				continue;

			/* Build the wait_event_info */
			wait_event_info = ENCODE_WAIT_EVENT_INFO(classIdx, eventId);

			entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_WAIT_EVENT,
													InvalidOid, (uint64) wait_event_info, nowait);

			if (!entry_ref)
			{
				could_not_be_flushed = true;
				continue;
			}

			shwaiteventent = (PgStatShared_WaitEvent *) entry_ref->shared_stats;
			shstat = &shwaiteventent->stats.counts;
			pending_counter = PendingWaitEventStats.counts[classOffset + eventId];

			*shstat += pending_counter;

			pgstat_unlock_entry(entry_ref);
		}
	}

	/* done, clear the pending entry */
	MemSet(PendingWaitEventStats.counts, 0, sizeof(PendingWaitEventStats.counts));

	if (!could_not_be_flushed)
		have_wait_event_stats = false;

	return could_not_be_flushed;
}

void
pgstat_wait_event_reset_timestamp_cb(PgStatShared_Common *header, TimestampTz ts)
{
	((PgStatShared_WaitEvent *) header)->stats.stat_reset_timestamp = ts;
}

/*
 * Check if there any wait event stats waiting for flush.
 */
bool
pgstat_wait_event_have_pending_cb(void)
{
	return have_wait_event_stats;
}
