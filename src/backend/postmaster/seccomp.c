/*-------------------------------------------------------------------------
 *
 * seccomp.c
 *	  Secure Computing BPF API wrapper.
 *
 * Pageserver delegates complex WAL decoding duties to postgres,
 * which means that the latter might fall victim to carefully designed
 * malicious WAL records and start doing harmful things to the system.
 * To prevent this, it has been decided to limit possible interactions
 * with the outside world using the Secure Computing BPF mode.
 *
 * We use this mode to disable all syscalls not in the allowlist. This
 * approach has its pros & cons:
 *
 *  - We have to carefully handpick and maintain the set of syscalls
 *    required for the WAL redo process. Core dumps help with that.
 *    The method of trial and error seems to work reasonably well,
 *    but it would be nice to find a proper way to "prove" that
 *    the set in question is both necessary and sufficient.
 *
 *  - Once we enter the seccomp bpf mode, it's impossible to lift those
 *    restrictions (otherwise, what kind of "protection" would that be?).
 *    Thus, we have to either enable extra syscalls for the clean shutdown,
 *    or exit the process immediately via _exit() instead of proc_exit().
 *
 *  - Should we simply use SCMP_ACT_KILL_PROCESS, or implement a custom
 *    facility to deal with the forbidden syscalls? If we'd like to embed
 *    a startup security test, we should go with the latter; In that
 *    case, which one of the following options is preferable?
 *
 *      * Catch the denied syscalls with a signal handler using SCMP_ACT_TRAP.
 *        Provide a common signal handler with a static switch to override
 *        its behavior for the test case. The downside is that if an attacker
 *        is able to change the switch, they could probably gain more time to
 *        examine the running process and do something about the protection
 *        (since there might be bugs in the linux kernel). We could go further
 *        and remap the memory backing the switch as readonly, then ban
 *        mprotect().
 *
 *      * Yet again, catch the denied syscalls using SCMP_ACT_TRAP.
 *        Provide 2 different signal handlers: one for a test case,
 *        another for the main processing loop. Install the first one,
 *        enable seccomp, perform the test, switch to the second one,
 *        finally ban sigaction(), presto!
 *
 *      * Spoof the result of a syscall using SECCOMP_RET_ERRNO for the
 *        test, then ban it altogether with another filter. The downside
 *        of this solution is that we don't actually check that
 *        SCMP_ACT_KILL_PROCESS/SCMP_ACT_TRAP works.
 *
 *    Either approach seems to require two eBPF filter programs
 *    (sans the mprotect()-less variant of the first option),
 *    which is unfortunate: the man page tells this is uncommon.
 *    Maybe I (@funbringer) am missing something, though; I encourage
 *    any reader to get familiar with it and scrutinize my conclusions.
 *
 * TODOs and ideas in no particular order:
 *
 *  - Do something about mmap() in musl's malloc().
 *    Definitely not a priority if we don't care about musl.
 *
 *  - See if we can untangle PG's shutdown sequence (involving unlink()):
 *
 *      * Simplify (or rather get rid of) shmem setup in PG's WAL redo mode.
 *      * Investigate chroot() or mount namespaces for better FS isolation.
 *      * (Per Heikki) Simply call _exit(), no big deal.
 *      * Come up with a better idea?
 *
 *  - Make use of seccomp's argument inspection (for what?).
 *    Unfortunately, it views all syscall arguments as scalars,
 *    so it won't work for e.g. string comparison in unlink().
 *
 *  - Benchmark with bpf jit on/off, try seccomp_syscall_priority().
 *
 *  - Test against various linux distros & glibc versions.
 *    I suspect that certain libc functions might involve slightly
 *    different syscalls, e.g. select/pselect6/pselect6_time64/whatever.
 *
 *  - Test on any arch other than amd64 to see if it works there.
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/seccomp.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/seccomp.h"

#include <execinfo.h>
#include <fcntl.h>
#include <unistd.h>

static void die(const char *str) pg_attribute_noreturn();

static volatile sig_atomic_t seccomp_test_sighandler_done = false;
static void seccomp_test_sighandler(int signum, siginfo_t *info, void *cxt);
static void seccomp_deny_sighandler(int signum, siginfo_t *info, void *cxt);

static char seccomp_rules_origin[2048];
static int do_seccomp_load_rules(PgSeccompRule *rules, int count, uint32 def_action);

void seccomp_load_rules(PgSeccompRule *rules, int count, const char *origin)
{
#define raise_error(str) \
	ereport(FATAL, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("seccomp: " str)))

	struct sigaction action = { .sa_flags = SA_SIGINFO };
	PgSeccompRule rule;
	long fd;

	/*
	 * Install a test signal handler.
	 * XXX: pqsignal() is too restrictive for our purposes,
	 * since we'd like to examine the contents of siginfo_t.
	 */
	action.sa_sigaction = seccomp_test_sighandler;
	if (sigaction(SIGSYS, &action, NULL) != 0)
		raise_error("failed to install a test SIGSYS handler");

	/*
	 * First, check that open of a well-known file works.
	 * XXX: we use syscall() to call a very specific syscall.
	 */
	fd = syscall(SCMP_SYS(open), "/dev/null", O_RDONLY, 0);
	if (fd < 0 || seccomp_test_sighandler_done)
		raise_error("failed to open a test file");
	close((int)fd);

	/*
	 * Set a trap on open() to test seccomp bpf.
	 * XXX: this should be the same syscall as above and below.
	 */
	rule = PG_SCMP(open, SCMP_ACT_TRAP);
	if (do_seccomp_load_rules(&rule, 1, SCMP_ACT_ALLOW) != 0)
		raise_error("failed to load a test filter");

	/*
	 * Finally, check that open() now raises SIGSYS.
	 * XXX: we use syscall() to call a very specific syscall.
	 */
	(void)syscall(SCMP_SYS(open), "/dev/null", O_RDONLY, 0);
	if (!seccomp_test_sighandler_done)
		raise_error("SIGSYS handler doesn't seem to work");

	/*
	 * Now that everything seems to work, install a proper handler.
	 *
	 * Instead of silently crashing the process with
	 * a fake SIGSYS caused by SCMP_ACT_KILL_PROCESS,
	 * we'd like to receive a real SIGSYS to print the
	 * message and *then* immediately exit.
	 */
	action.sa_sigaction = seccomp_deny_sighandler;
	if (sigaction(SIGSYS, &action, NULL) != 0)
		raise_error("failed to install a proper SIGSYS handler");

	/* If this succeeds, any syscall not in the list will fire a SIGSYS */
	if (do_seccomp_load_rules(rules, count, SCMP_ACT_TRAP) != 0)
		raise_error("failed to enter seccomp mode");

	/*
	 * Copy the origin of those rules (e.g. function name)
	 * to print it when a violation takes place.
	 *
	 * We deliberately don't bother with a "stack of origins"
	 * to make things simpler, since that use case is unlikely.
	 */
	strncpy(seccomp_rules_origin, origin, strlen(origin));

#undef raise_error
}

/*
 * Enter seccomp mode with a BPF filter that will only allow
 * certain syscalls to proceed.
 */
static int
do_seccomp_load_rules(PgSeccompRule *rules, int count, uint32 def_action)
{
	scmp_filter_ctx ctx;
	int rc = -1;

	/* Create a context with a default action for syscalls not in the list */
	if ((ctx = seccomp_init(def_action)) == NULL)
		goto cleanup;

	for (int i = 0; i < count; i++)
	{
		PgSeccompRule *rule = &rules[i];
		if ((rc = seccomp_rule_add(ctx, rule->psr_action, rule->psr_syscall, 0)) != 0)
			goto cleanup;
	}

	/* Try building & loading the program into the kernel */
	if ((rc = seccomp_load(ctx)) != 0)
		goto cleanup;

cleanup:
	/*
	 * We don't need the context anymore regardless of the result,
	 * since either we failed or the eBPF program has already been
	 * loaded into the linux kernel.
	 */
	seccomp_release(ctx);
	return rc;
}

static inline void
die(const char *str)
{
	/*
	 * Best effort write to stderr.
	 * XXX: we use syscall() to call a very specific syscall
	 * which is guaranteed to be allowed by the filter.
	 */
	(void)syscall(SCMP_SYS(write), fileno(stderr), str, strlen(str));

	/* XXX: we don't want to run any atexit callbacks */
	_exit(1);
}

static void
seccomp_test_sighandler(int signum, siginfo_t *info, void *cxt pg_attribute_unused())
{
#define DIE_PREFIX "seccomp test signal handler: "

	/* Check that this signal handler is used only for a single test case */
	if (seccomp_test_sighandler_done)
		die(DIE_PREFIX "test handler should only be used for 1 test\n");
	seccomp_test_sighandler_done = true;

	if (signum != SIGSYS)
		die(DIE_PREFIX "bad signal number\n");

	/* TODO: maybe somehow extract the hardcoded syscall number */
	if (info->si_syscall != SCMP_SYS(open))
		die(DIE_PREFIX "bad syscall number\n");

#undef DIE_PREFIX
}

static void
seccomp_deny_sighandler(int signum, siginfo_t *info, void *cxt pg_attribute_unused())
{
	/*
	 * Unfortunately, we can't use seccomp_syscall_resolve_num_arch()
	 * to resolve the syscall's name, since it calls strdup()
	 * under the hood (wtf!).
	 */
	char buffer[4096];
	(void)snprintf(
			buffer, lengthof(buffer),
			"-------------------------------------------------------------\n"
			"Seccomp violation: failed to invoke syscall %1$d.\n"
			"This syscall is not in the allowlist.\n"
			"Use a tool (e.g. `ausyscall %1$d`) to get the syscall's name.\n"
			"Origin: %2$s\n"
			"-------------------------------------------------------------\n",
			(int)info->si_syscall,
			seccomp_rules_origin);

	die(buffer);
}
