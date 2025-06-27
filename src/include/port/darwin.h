/* src/include/port/darwin.h */

#define __darwin__	1

#if HAVE_DECL_F_FULLFSYNC		/* not present before macOS 10.3 */
#define HAVE_FSYNC_WRITETHROUGH

#endif

/*
 * macOS has a platform-specific implementation of prefetching.
 */
#ifndef USE_PREFETCH
#define USE_PREFETCH
#endif