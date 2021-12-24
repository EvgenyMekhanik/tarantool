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

g.test_vinyl_does_not_support_compression_during_space_creation = function()
    local rc, errmsg, format
    format = {{name = 'x', type = 'unsigned', compression = 'zstd5'}}
    rc, errmsg = pcall(box.schema.space.create, 'tmp',
                       {engine = 'vinyl', format = format})
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "vinyl does not support compression")
end

g.test_vinyl_does_not_support_compression_during_setting_format = function()
    local rc, errmsg, format
    format = {{name = 'x', type = 'unsigned', compression = 'zstd5'}}
    local vinyl_space = box.schema.space.create(
        'vinyl_space', {engine = 'vinyl'}
    )
    rc, errmsg = pcall(vinyl_space.format, vinyl_space, format)
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "vinyl does not support compression")
    vinyl_space:drop()
end

g = t.group('format with compression for indexed fields')

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
    format = {{name = 'x', type = 'unsigned', compression = 'zstd5'}}
    rc, errmsg = pcall(memtx_space.format, memtx_space, format)
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "memtx does not support compression " ..
                    "for indexed fields")
    format = {
        {name = 'field1', type = 'unsigned'},
        {name = 'field2', type = 'unsigned'},
        {name = 'field3', type = 'unsigned', compression = 'zstd5'}
    }
    rc, errmsg = pcall(memtx_space.format, memtx_space, format)
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "memtx does not support compression " ..
                    "for indexed fields")
end

g.test_unable_to_create_index_for_compressed_fields = function()
    local rc, errmsg, format
    local memtx_space = box.space.memtx_space
    format = {
        {name = 'field1', type = 'unsigned'},
        {name = 'field2', type = 'unsigned', compression = 'zstd5'},
        {name = 'field3', type = 'unsigned'}
    }
    rc, errmsg = pcall(memtx_space.format, memtx_space, format)
    t.assert_equals(rc, true)
    t.assert_equals(errmsg, nil)
    rc, errmsg = pcall(memtx_space.create_index, memtx_space, 'secondary',
                       {parts = {2, 'unsigned'}})
    t.assert_equals(rc, false)
    t.assert_equals(tostring(errmsg), "memtx does not support compression " ..
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
    t.assert_equals(tostring(errmsg), "memtx does not support compression " ..
                    "for indexed fields")
    memtx_space:delete{1, 1}
    memtx_space:delete{2, 2}
    memtx_space:delete{3, 3}
    -- After deleting all tuples in the old format, you can create an index.
    rc = pcall(memtx_space.create_index, memtx_space, 'secondary',
               {parts = {2, 'unsigned'}})
    t.assert_equals(rc, true)
end

g = t.group("space with compressed fields")

g.before_each(function()
    local space = box.schema.space.create('memtx_space', {engine = 'memtx'})
    local pk =  space:create_index('primary', { parts = {2, 'unsigned'} })
    box.space.memtx_space:format({
        {name = 'x', type = 'string', compression='zstd5'},
        {name = 'y', type = 'unsigned'},
        {name = 'z', type = 'unsigned', compression = 'zstd5'}
    })
end)

g.after_each(function() box.space.memtx_space:drop() collectgarbage() end)


g.test_insert_and_select_with_compression = function()
    local long_field = string.rep('x', 1000)
    local memtx_space = box.space.memtx_space
    local tuple = memtx_space:insert({long_field, 1, 1})
    t.assert_equals(tuple[1], long_field)
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
    local tuple_array = memtx_space:select{}
    t.assert_equals(#tuple_array, 1)
    t.assert_equals(tuple_array[1][1], long_field)
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
end

g.test_replace_and_select_with_compression = function()
    local long_field = string.rep('x', 1000)
    local memtx_space = box.space.memtx_space
    local tuple = memtx_space:replace({long_field, 1, 1})
    t.assert_equals(tuple[1], long_field)
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
    local tuple_array = memtx_space:select{}
    t.assert_equals(#tuple_array, 1)
    t.assert_equals(tuple_array[1][1], long_field)
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
end

g.test_update_and_select_with_compression = function()
    local long_field = string.rep('x', 1000)
    local update_long_field = string.rep('y', 1000)
    local memtx_space = box.space.memtx_space
    local tuple = memtx_space:replace({long_field, 1, 1})
    t.assert_equals(tuple[1], long_field)
    local tuple = memtx_space:update(1, {{'=', 1, update_long_field}})
    t.assert_equals(tuple[1], update_long_field)
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
    local tuple_array = memtx_space:select{}
    t.assert_equals(#tuple_array, 1)
    t.assert_equals(tuple_array[1][1], update_long_field)
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
end

g.test_upsert_and_select_with_compression = function()
    local long_field = string.rep('x', 1000)
    local update_long_field = string.rep('y', 1000)
    local memtx_space = box.space.memtx_space
    -- Insert new tuple.
    memtx_space:upsert({long_field, 1, 1}, {{'=', 1, update_long_field}})
    local tuple_array = memtx_space:select{}
    t.assert_equals(#tuple_array, 1)
    t.assert_equals(tuple_array[1][1], long_field)
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
    -- Update old tuple.
    memtx_space:upsert({long_field, 1, 1}, {{'=', 1, update_long_field}})
    local tuple_array = memtx_space:select{}
    t.assert_equals(#tuple_array, 1)
    t.assert_equals(tuple_array[1][1], update_long_field)
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
end

g = t.group("before_replace_triggers")

g.before_each(function()
    box.schema.space.create('memtx_space', {engine = 'memtx'})
    box.space.memtx_space:create_index('primary', { parts = {2, 'unsigned'} })
    box.space.memtx_space:format({
        {name = 'x', type = 'string', compression='zstd5'},
        {name = 'y', type = 'unsigned'},
        {name = 'z', type = 'unsigned', compression = 'zstd5'}
    })
    box.space.memtx_space:replace{string.rep('x', 1000), 1, 1}
    box.space.memtx_space:before_replace(function(old, new)
        if old and old[1] == string.rep('x', 1000) then
            return box.tuple.update(
                old, {{':', 1, 1, 1000, string.rep('!', 1000)}}
            )
        end
    end)
end)

g.after_each(function() box.space.memtx_space:drop() collectgarbage() end)

g.test_before_replace_trigger_during_insert = function()
    local long_field = string.rep('y', 1000)
    local memtx_space = box.space.memtx_space
    local tuple = memtx_space:insert{long_field, 1, 1}
    t.assert_equals(tuple[1], string.rep('!', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
end

g.test_before_replace_trigger_during_replace = function()
    local long_field = string.rep('y', 1000)
    local memtx_space = box.space.memtx_space
    local tuple = memtx_space:replace{long_field, 1, 1}
    t.assert_equals(tuple[1], string.rep('!', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
end

g.test_before_replace_trigger_during_delete = function()
    local memtx_space = box.space.memtx_space
    local tuple = memtx_space:delete{1}
    t.assert_equals(tuple[1], string.rep('!', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
end

g.test_before_replace_trigger_during_update = function()
    local long_field = string.rep('y', 1000)
    local memtx_space = box.space.memtx_space
    local tuple = memtx_space:update(
        1, {{'=', 1, long_field}}
    )
    t.assert_equals(tuple[1], string.rep('!', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
end

g.test_before_replace_trigger_during_upsert_without_old_tuple = function()
    local long_field = string.rep('y', 1000)
    local memtx_space = box.space.memtx_space
    memtx_space:upsert(
        {long_field, 2, 2}, {{'=', 1, long_field}}
    )
    local tuple_array = memtx_space:select({})
    t.assert_equals(#tuple_array, 2)
    t.assert_equals(tuple_array[1][1], string.rep('x', 1000))
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
    t.assert_equals(tuple_array[2][1], string.rep('y', 1000))
    t.assert_equals(tuple_array[2][2], 2)
    t.assert_equals(tuple_array[2][3], 2)
end

g.test_before_replace_trigger_during_upsert_with_old_tuple = function()
    local long_field = string.rep('y', 1000)
    local memtx_space = box.space.memtx_space
    memtx_space:upsert(
        {string.rep('x', 1000), 1, 1}, {{'=', 1, long_field}}
    )
    local tuple_array = memtx_space:select({})
    t.assert_equals(tuple_array[1][1], string.rep('!', 1000))
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
end

g = t.group("on_replace_triggers")

g.before_each(function()
    box.schema.space.create('memtx_space', {engine = 'memtx'})
    box.space.memtx_space:create_index('primary', { parts = {2, 'unsigned'} })
    box.space.memtx_space:format({
        {name = 'x', type = 'string', compression='zstd5'},
        {name = 'y', type = 'unsigned'},
        {name = 'z', type = 'unsigned', compression = 'zstd5'}
    })
    box.space.memtx_space:replace{string.rep('x', 1000), 1, 1}
    box.space.memtx_space:on_replace(function(old, new, s, op)
        if old and old[1] == string.rep('x', 1000) then
            box.tuple.update(old, {{':', 1, 1, 1000, string.rep('!', 1000)}})
        end
        if new and new[1] == string.rep('x', 1000) then
            box.tuple.update(new, {{':', 1, 1, 1000, string.rep('!', 1000)}})
        end
    end)
end)

g.after_each(function() box.space.memtx_space:drop() collectgarbage() end)

g.test_on_replace_trigger_during_insert = function()
    local long_field = string.rep('x', 1000)
    local memtx_space = box.space.memtx_space
    local tuple = memtx_space:insert{long_field, 2, 2}
    t.assert_equals(tuple[1], string.rep('x', 1000))
    t.assert_equals(tuple[2], 2)
    t.assert_equals(tuple[3], 2)
end

g.test_on_replace_trigger_during_replace = function()
    local long_field = string.rep('y', 1000)
    local memtx_space = box.space.memtx_space
    local tuple = memtx_space:replace{long_field, 1, 1}
    t.assert_equals(tuple[1], string.rep('y', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
end

g.test_on_replace_trigger_during_delete = function()
    local memtx_space = box.space.memtx_space
    local tuple = memtx_space:delete{1}
    t.assert_equals(tuple[1], string.rep('x', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
end

g.test_on_replace_trigger_during_update = function()
    local long_field = string.rep('y', 1000)
    local memtx_space = box.space.memtx_space
    local tuple = memtx_space:update(
        1, {{'=', 1, long_field}}
    )
    t.assert_equals(tuple[1], string.rep('y', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
end

g.test_on_replace_trigger_during_upsert_without_old_tuple = function()
    local long_field = string.rep('y', 1000)
    local memtx_space = box.space.memtx_space
    memtx_space:upsert(
        {long_field, 2, 2}, {{'=', 1, long_field}}
    )
    local tuple_array = memtx_space:select({})
    t.assert_equals(#tuple_array, 2)
    t.assert_equals(tuple_array[1][1], string.rep('x', 1000))
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
    t.assert_equals(tuple_array[2][1], string.rep('y', 1000))
    t.assert_equals(tuple_array[2][2], 2)
    t.assert_equals(tuple_array[2][3], 2)
end

g.test_on_replace_trigger_during_upsert_with_old_tuple = function()
    local long_field = string.rep('y', 1000)
    local memtx_space = box.space.memtx_space
    memtx_space:upsert(
        {string.rep('x', 1000), 1, 1}, {{'=', 1, long_field}}
    )
    local tuple_array = memtx_space:select({})
    t.assert_equals(tuple_array[1][1], string.rep('y', 1000))
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
end

g = t.group("iproto")

g.before_each(function()
    g.server = server:new({alias = 'master'})
    g.server:start()
    g.server:exec(function()
        box.schema.space.create('memtx_space', {engine = 'memtx'})
        box.space.memtx_space:create_index('primary',
            { parts = {2, 'unsigned'} }
        )
        box.space.memtx_space:format({
            {name = 'x', type = 'string', compression='zstd5'},
            {name = 'y', type = 'unsigned'},
            {name = 'z', type = 'unsigned', compression = 'zstd5'}
        })
        box.space.memtx_space:replace{string.rep('x', 1000), 1, 1}
        box.space.memtx_space:replace{string.rep('y', 1000), 2, 2}
    end)
end)

g.after_each(function()
    g.server:stop()
end)

local function check_tuple_array(tuple_array)
    t.assert_equals(#tuple_array, 3)
    t.assert_equals(tuple_array[1][1], string.rep('x', 1000))
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
    t.assert_equals(tuple_array[2][1], string.rep('y', 1000))
    t.assert_equals(tuple_array[2][2], 2)
    t.assert_equals(tuple_array[2][3], 2)
    t.assert_equals(tuple_array[3][1], string.rep('z', 1000))
    t.assert_equals(tuple_array[3][2], 3)
    t.assert_equals(tuple_array[3][3], 3)
end

g.test_insert_and_select_over_iproto = function()
    local c = net.connect(g.server.net_box_uri)
    c.space.memtx_space:insert({string.rep('z', 1000), 3, 3})
    local tuple_array = c.space.memtx_space:select({})
    check_tuple_array(tuple_array)
end

g.test_replace_and_select_over_iproto = function()
    local c = net.connect(g.server.net_box_uri)
    c.space.memtx_space:replace({string.rep('z', 1000), 3, 3})
    local tuple_array = c.space.memtx_space:select({})
    check_tuple_array(tuple_array)
end

g.test_update_and_select_over_iproto = function()
    local c = net.connect(g.server.net_box_uri)
    c.space.memtx_space:update(1, {{'=', 1, string.rep('z', 1000)}})
    local tuple_array = c.space.memtx_space:select({})
    t.assert_equals(#tuple_array, 2)
    t.assert_equals(tuple_array[1][1], string.rep('z', 1000))
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
    t.assert_equals(tuple_array[2][1], string.rep('y', 1000))
    t.assert_equals(tuple_array[2][2], 2)
    t.assert_equals(tuple_array[2][3], 2)
end

g.test_upsert_and_select_over_iproto_without_old_tuple = function()
    local c = net.connect(g.server.net_box_uri)
    c.space.memtx_space:upsert({string.rep('z', 1000), 3, 3},
        {{'=', 1, string.rep('z', 1000)}}
    )
    local tuple_array = c.space.memtx_space:select({})
    check_tuple_array(tuple_array)
end

g.test_upsert_and_select_over_iproto_with_old_tuple = function()
    local c = net.connect(g.server.net_box_uri)
    c.space.memtx_space:upsert({string.rep('x', 1000), 1, 1},
        {{'=', 1, string.rep('z', 1000)}}
    )
    local tuple_array = c.space.memtx_space:select({})
    t.assert_equals(#tuple_array, 2)
    t.assert_equals(tuple_array[1][1], string.rep('z', 1000))
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
    t.assert_equals(tuple_array[2][1], string.rep('y', 1000))
    t.assert_equals(tuple_array[2][2], 2)
    t.assert_equals(tuple_array[2][3], 2)
end

g.test_snapshot_recovery = function()
    local tuple_array = g.server:exec(function()
        return box.space.memtx_space:select({})
    end)
    t.assert_equals(#tuple_array, 2)
    t.assert_equals(tuple_array[1][1], string.rep('x', 1000))
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
    t.assert_equals(tuple_array[2][1], string.rep('y', 1000))
    t.assert_equals(tuple_array[2][2], 2)
    t.assert_equals(tuple_array[2][3], 2)
    g.server:stop()
    g.server:start()
    local tuple_array = g.server:exec(function()
        return box.space.memtx_space:select({})
    end)
    t.assert_equals(#tuple_array, 2)
    t.assert_equals(tuple_array[1][1], string.rep('x', 1000))
    t.assert_equals(tuple_array[1][2], 1)
    t.assert_equals(tuple_array[1][3], 1)
    t.assert_equals(tuple_array[2][1], string.rep('y', 1000))
    t.assert_equals(tuple_array[2][2], 2)
    t.assert_equals(tuple_array[2][3], 2)
end
