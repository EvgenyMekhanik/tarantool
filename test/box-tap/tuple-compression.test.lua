#!/usr/bin/env tarantool

local tap = require('tap')
local test = tap.test('tuple-compression')

test:plan(12)

box.cfg{}

local vinyl_space = box.schema.space.create('vinyl_space', {engine = 'vinyl'})
local memtx_space = box.schema.space.create('memtx_space', {engine = 'memtx'})
local field_format, rc, errmsg

-- Invalid compression type
field_format = {
	{name = 'field1', type = 'unsigned', compression = 'invalid'}
}
rc, errmsg = pcall(vinyl_space.format, vinyl_space, field_format)
test:is(rc, false, "invalid compression type")
test:is(tostring(errmsg), "Can't modify space 'vinyl_space': " ..
        "field 1 has unknown compression type", "errmsg")
rc, errmsg = pcall(memtx_space.format, memtx_space, field_format)
test:is(rc, false, "invalid compression type")
test:is(tostring(errmsg), "Can't modify space 'memtx_space': " ..
        "field 1 has unknown compression type", "errmsg")
-- Same check when format passed during space creation.
rc, errmsg = pcall(box.schema.space.create, 'tmp',
                   {engine = 'vinyl', format = field_format})
test:is(rc, false, "invalid compression type")
test:is(tostring(errmsg), "Failed to create space 'tmp': " ..
        "field 1 has unknown compression type", "errmsg")
rc, errmsg = pcall(box.schema.space.create, 'tmp',
                   {engine = 'memtx', format = field_format})
test:is(rc, false, "invalid compression type")
test:is(tostring(errmsg), "Failed to create space 'tmp': " ..
        "field 1 has unknown compression type", "errmsg")

-- Vinyl spaces doestn't support compression at all.
field_format = {
	{name = 'field1', type = 'unsigned', compression = 'zstd5'}
}
rc, errmsg = pcall(vinyl_space.format, vinyl_space, field_format)
test:is(rc, false, "vinyl space compression unsupported")
test:is(tostring(errmsg), "Vinyl does not support compression", "errmsg")
-- Same check when format passed during space creation.
rc, errmsg = pcall(box.schema.space.create, 'tmp',
                   {engine = 'vinyl', format = field_format})
test:is(rc, false, "vinyl space compression unsupported")
test:is(tostring(errmsg), "Vinyl does not support compression", "errmsg")

vinyl_space:drop()
memtx_space:drop()

os.exit(test:check() and 0 or 1)