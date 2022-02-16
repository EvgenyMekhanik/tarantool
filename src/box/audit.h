/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2021, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#include "trivia/config.h"

struct space;
struct port;

enum LOG_CODE {
        SPACE_SELECT,
        SPACE_INSERT,
        SPACE_REPLACE,
        SPACE_UPDATE,
        SPACE_UPSERT,
        SPACE_DELETE,
        SPACE_GET,
#if 0
        INDEX_RANDOM,
        INDEX_MIN,
        INDEX_MAX,
#endif
        AUTH_USER,
        NO_AUTH_USER,
        OPEN_CONNECT,
        CLOSE_CONNECT,
        USER_CREATED,
        USER_DELETED,
        ROLE_CREATED,
        ROLE_DELETED,
        USER_ENABLED,
        USER_DISABLED,
        USER_GRANT_RIGHTS,
        ROLE_GRANT_RIGHTS,
        USER_REVOKE_RIGHTS,
        ROLE_REVOKE_RIGHTS,
        PASSWORD_CHANGED,
        ACCESS_DENIED,
        MAX_LOG_CODE,
};

struct audit_on_select {
        struct space *space;
        struct port *port;
        enum LOG_CODE code;
};

#if defined(ENABLE_AUDIT_LOG)
# include "audit_impl.h"
#else /* !defined(ENABLE_AUDIT_LOG) */

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

<<<<<<< HEAD
=======
<<<<<<< HEAD
struct space;

>>>>>>> a22e1ef95... cfg: implement ability to set audit log format
int
audit_log_init(const char *init_str, int log_nonblock);
=======
void
audit_log_init(const char *init_str, int log_nonblock, const char *format);
>>>>>>> a793950aa... cfg: implement ability to set audit log format

static inline void
audit_log_free(void) {}

static inline void
audit_log_set_space_triggers(struct space *space)
{
        (void)space;
}

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* !defined(ENABLE_AUDIT_LOG) */
