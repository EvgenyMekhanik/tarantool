local t = require('luatest')
local server = require('test.luatest_helpers.server')
local net = require('net.box')

box.cfg{}

local g = t.group("invalid compression type")

g.test_invalid_compression_type_during_space_creation = function()
    local rc, errmsg, format
    format = {{name = 'x', type = 'unsigned', compression = 'invalid'}}
    rc, errmsg = pcall(box.schema.space.create, 'tmp',
                       {engine = 'memtx', format = format})
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "Failed to create space 'tmp': " ..
                    "field 1 has unknown compression type")
    rc, errmsg = pcall(box.schema.space.create, 'tmp',
                       {engine = 'vinyl', format = format})
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "Failed to create space 'tmp': " ..
                    "field 1 has unknown compression type")
end

g.before_test('test_invalid_compression_type_during_setting_format', function()
    box.schema.space.create('vinyl_space', {engine = 'vinyl'})
    box.schema.space.create('memtx_space', {engine = 'memtx'})
end)

g.test_invalid_compression_type_during_setting_format = function()
    local rc, errmsg, format
    local vinyl_space = box.space.vinyl_space
    local memtx_space = box.space.memtx_space
    format = {{name = 'x', type = 'unsigned', compression = 'invalid'}}
    rc, errmsg = pcall(vinyl_space.format, vinyl_space, format)
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "Can't modify space 'vinyl_space': " ..
                    "field 1 has unknown compression type")
    rc, errmsg = pcall(memtx_space.format, memtx_space, format)
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "Can't modify space 'memtx_space': " ..
                    "field 1 has unknown compression type")
end

g.after_test('test_invalid_compression_type_during_setting_format', function()
    box.space.vinyl_space:drop()
    box.space.memtx_space:drop()
    collectgarbage()
end)

g = t.group("vinyl doesn't support compression")

g.test_vinyl_does_not_support_compression_during_space_creation = function()
    local rc, errmsg, format
    format = {{name = 'x', type = 'unsigned', compression = 'zstd'}}
    rc, errmsg = pcall(box.schema.space.create, 'tmp',
                       {engine = 'vinyl', format = format})
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "Vinyl does not support compression")
end

g.test_vinyl_does_not_support_compression_during_setting_format = function()
    local rc, errmsg, format
    format = {{name = 'x', type = 'unsigned', compression = 'zstd'}}
    local vinyl_space = box.schema.space.create(
        'vinyl_space', {engine = 'vinyl'}
    )
    rc, errmsg = pcall(vinyl_space.format, vinyl_space, format)
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "Vinyl does not support compression")
    vinyl_space:drop()
end

g = t.group('format with compression for indexed fields unsupported')

g.before_each(function()
    local space = box.schema.space.create('memtx_space', {engine = 'memtx'})
    local pk = space:create_index('primary',
        { parts = { {1, 'unsigned'}, {3, 'unsigned'} } }
    )
end)

g.after_each(function() box.space.memtx_space:drop() collectgarbage() end)

g.test_unable_to_set_format_with_compression_for_indexed_fields = function()
    local rc, errmsg, format
    local memtx_space = box.space.memtx_space
    format = {{name = 'x', type = 'unsigned', compression = 'zstd'}}
    rc, errmsg = pcall(memtx_space.format, memtx_space, format)
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "Memtx does not support compression " ..
                    "for indexed fields")
    format = {
        {name = 'field1', type = 'unsigned'},
        {name = 'field2', type = 'unsigned'},
        {name = 'field3', type = 'unsigned', compression = 'zstd'}
    }
    rc, errmsg = pcall(memtx_space.format, memtx_space, format)
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "Memtx does not support compression " ..
                    "for indexed fields")
    memtx_space.index.primary:drop()
    -- After dropping index it's ok to set format with compression
    rc, errmsg = pcall(memtx_space.format, memtx_space, format)
    t.assert_equals(rc, true)
    t.assert_equals(errmsg, nil)
end

g.test_unable_to_create_index_for_compressed_fields = function()
    local rc, errmsg, format
    local memtx_space = box.space.memtx_space
    format = {
        {name = 'field1', type = 'unsigned'},
        {name = 'field2', type = 'unsigned', compression = 'zstd'},
        {name = 'field3', type = 'unsigned'}
    }
    rc, errmsg = pcall(memtx_space.format, memtx_space, format)
    t.assert_equals(rc, true)
    t.assert_equals(errmsg, nil)
    rc, errmsg = pcall(memtx_space.create_index, memtx_space, 'secondary',
                       {parts = {2, 'unsigned'}})
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "Engine does not support compression " ..
                    "for indexed fields")
    memtx_space:replace{1, 1, 1}
    memtx_space:replace{2, 2, 2}
    memtx_space:replace{3, 3, 3}
    format = {
        {name = 'field1', type = 'unsigned'},
        {name = 'field2', type = 'unsigned'},
        {name = 'field3', type = 'unsigned'}
    }
    rc, errmsg = pcall(memtx_space.format, memtx_space, format)
    t.assert_equals(rc, true)
    t.assert_equals(errmsg, nil)
    memtx_space:replace{4, 4, 4}
    -- There are some tuples in old format with compressed field, so
    -- we can't create this index.
    rc, errmsg = pcall(memtx_space.create_index, memtx_space, 'secondary',
                       { parts = {2, 'unsigned'}})
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "Engine does not support compression " ..
                    "for indexed fields")
    memtx_space:delete{1, 1}
    memtx_space:delete{2, 2}
    memtx_space:delete{3, 3}
    -- After deleting all tuples in the old format, you can create an index.
    rc = pcall(memtx_space.create_index, memtx_space, 'secondary',
               {parts = {2, 'unsigned'}})
    t.assert_equals(rc, true)
end
