/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2021, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "lua/audit.h"

LUA_API int
luaopen_audit_log(struct lua_State *L)
{
        (void)L;
        return 0;
}
