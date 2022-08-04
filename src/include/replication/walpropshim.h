#ifndef __WALPROPOSER_H__
#define __WALPROPOSER_H__

/* Set to true only in standalone run of `postgres --sync-safekeepers` (see comment on top) */
extern PGDLLIMPORT bool syncSafekeepers;
extern PGDLLIMPORT void (*WalProposerInit) (XLogRecPtr flushRecPtr, uint64 systemId);
extern PGDLLIMPORT void (*WalProposerStart) (void);

void       WalProposerSync(int argc, char *argv[]);

#endif
