#ifndef PG_SECCOMP_H
#define PG_SECCOMP_H

#include "postgres.h"

#ifdef HAVE_LIBSECCOMP
#include <seccomp.h>
#endif

typedef struct {
    int    psr_syscall; /* syscall number */
    uint32 psr_action;  /* libseccomp action, e.g. SCMP_ACT_ALLOW */
} PgSeccompRule;

#define PG_SCMP(syscall, action)                \
    (PgSeccompRule) {                           \
        .psr_syscall = SCMP_SYS(syscall),       \
        .psr_action = (action),                 \
    }

#define PG_SCMP_ALLOW(syscall) \
    PG_SCMP(syscall, SCMP_ACT_ALLOW)

void seccomp_load_rules(PgSeccompRule *syscalls, int count);

#endif /* PG_SECCOMP_H */
