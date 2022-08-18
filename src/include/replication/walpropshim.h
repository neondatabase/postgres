/*
 * walpropshim.h
 *	  various hooks for the walproposer component of the Neon extension.
 */

#ifndef __WALPROPOSER_H__
#define __WALPROPOSER_H__

/*
 * Set to true only in standalone run of `postgres --sync-safekeepers`.
 * See also the top comment in contrib/neon/walproposer.c
 */
extern PGDLLIMPORT bool syncSafekeepers;
extern PGDLLIMPORT void (*WalProposerInit) (XLogRecPtr flushRecPtr, uint64 systemId);
extern PGDLLIMPORT void (*WalProposerStart) (void);

void       WalProposerSync(int argc, char *argv[]);

#endif
