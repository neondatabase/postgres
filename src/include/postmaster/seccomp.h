#ifndef PG_SECCOMP_H
#define PG_SECCOMP_H
#ifdef HAVE_LIBSECCOMP

#include "postgres.h"

#include <seccomp.h>

/* A struct representing a single libseccomp rule */
typedef struct {
    int    psr_syscall; /* syscall number */
    uint32 psr_action;  /* libseccomp action, e.g. SCMP_ACT_ALLOW */
} PgSeccompRule;

/* Build a rule to perform an action when syscall is invoked */
#define PG_SCMP(syscall, action)                \
    (PgSeccompRule) {                           \
        .psr_syscall = SCMP_SYS(syscall),       \
        .psr_action = (action),                 \
    }

#define PG_SCMP_ALLOW(syscall) \
    PG_SCMP(syscall, SCMP_ACT_ALLOW)

/* This wrapper pastes */
#define SECCOMP_LOAD_RULES(rules, count)        \
    {                                           \
        char seccomp_rules_origin[2048];        \
        snprintf(                               \
            seccomp_rules_origin,               \
            sizeof(seccomp_rules_origin),       \
            "%s:%d (%s)",                       \
            __FILE__, __LINE__, __func__);      \
                                                \
        seccomp_load_rules(                     \
            (rules), (count),                   \
            seccomp_rules_origin);              \
    }

void seccomp_load_rules(PgSeccompRule *rules, int count, const char *origin);

#endif
#endif /* PG_SECCOMP_H */
