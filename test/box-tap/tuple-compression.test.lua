#!/usr/bin/env tarantool

local tap = require('tap')
local test = tap.test('tuple-compression')

test:plan(25)

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
test:is(tostring(errmsg), "vinyl does not support compression", "errmsg")
-- Same check when format passed during space creation.
rc, errmsg = pcall(box.schema.space.create, 'tmp',
                   {engine = 'vinyl', format = field_format})
test:is(rc, false, "vinyl space compression unsupported")
test:is(tostring(errmsg), "vinyl does not support compression", "errmsg")

-- All other checks, because compression for vinyl spaces
-- doesn't supported.
vinyl_space:drop()

-- Unable to create format, with compressed field, if there is some index
-- with the same field.
primary_idx = memtx_space:create_index('primary',
    { parts = { {1, 'unsigned'}, {3, 'unsigned'} } }
)
field_format = {
    {name = 'field1', type = 'unsigned', compression = 'zstd5'}
}
rc, errmsg = pcall(memtx_space.format, memtx_space, field_format)
test:is(rc, false, "compression for indexed fields unsupported")
test:is(tostring(errmsg), "memtx does not support compression " ..
        "for indexed fields", "errmsg")
field_format = {
    {name = 'field1', type = 'unsigned'},
    {name = 'field2', type = 'unsigned'},
    {name = 'field3', type = 'unsigned', compression = 'zstd5'}
}
rc, errmsg = pcall(memtx_space.format, memtx_space, field_format)
test:is(rc, false, "compression for indexed fields unsupported")
test:is(tostring(errmsg), "memtx does not support compression " ..
        "for indexed fields", "errmsg")

-- Compressed field is not indexed, so it is valid format.
field_format = {
    {name = 'field1', type = 'unsigned'},
    {name = 'field2', type = 'unsigned', compression = 'zstd5'},
    {name = 'field3', type = 'unsigned'}
}
rc, errmsg = pcall(memtx_space.format, memtx_space, field_format)
test:is(rc, true, "format")
test:is(errmsg, nil, "errmsg")

-- Unable to create index, if space format has compression for
-- some of creating indexed fields.
rc, errmsg = pcall(memtx_space.create_index, memtx_space, 'secondary',
                   { parts = {2, 'unsigned'} }
)
test:is(rc, false, "compression for indexed fields unsupported")
test:is(tostring(errmsg), "memtx does not support compression " ..
        "for indexed fields", "errmsg")

memtx_space:replace{1, 1, 1}
memtx_space:replace{2, 2, 2}
memtx_space:replace{3, 3, 3}
field_format = {
    {name = 'field1', type = 'unsigned'},
    {name = 'field2', type = 'unsigned'},
    {name = 'field3', type = 'unsigned'}
}
rc, errmsg = pcall(memtx_space.format, memtx_space, field_format)
test:is(rc, true, "format")
test:is(errmsg, nil, "errmsg")
memtx_space:replace{4, 4, 4}
-- There are some tuples with old format with compressed field, so
-- we can't create this index.
rc, errmsg = pcall(memtx_space.create_index, memtx_space, 'secondary',
                   { parts = {2, 'unsigned'} }
)
test:is(rc, false, "compression for indexed fields unsupported")
test:is(tostring(errmsg), "memtx does not support compression " ..
        "for indexed fields", "errmsg")

memtx_space:delete{1, 1}
memtx_space:delete{2, 2}
memtx_space:delete{3, 3}
rc, errmsg = pcall(memtx_space.create_index, memtx_space, 'secondary',
                   { parts = {2, 'unsigned'} }
)
test:is(rc, true, "create index")


memtx_space:drop()

os.exit(test:check() and 0 or 1)
