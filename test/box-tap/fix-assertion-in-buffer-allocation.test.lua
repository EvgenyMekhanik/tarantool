#!/usr/bin/env tarantool

local buffer = require('buffer')

local tap = require('tap')
local test = tap.test('fix-assertion-in-buffer-allocation')

test:plan(2)

local expected_errmsg =
	"builtin/buffer.lua:50: Failed to allocate 4294967296 bytes in ibuf"
local ibuf = buffer.ibuf()
local rc, errmsg = pcall(ibuf.alloc, ibuf, 2 ^ 32)
test:is(rc, false, "ibuf:alloc")
test:is(tostring(errmsg), expected_errmsg, "errmsg")

os.exit(test:check() and 0 or 1)