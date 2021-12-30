local t = require('luatest')
local server = require('test.luatest_helpers.server')
local net = require('net.box')

box.cfg{}

g = t.group("space with compressed fields")

g.before_each(function()
    local space = box.schema.space.create('memtx_space', {engine = 'memtx'})
    local pk =  space:create_index('primary', { parts = {2, 'unsigned'} })
    box.space.memtx_space:format({
        {name = 'x', type = 'string', compression='zstd'},
        {name = 'y', type = 'unsigned'},
        {name = 'z', type = 'unsigned', compression = 'zstd'}
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
        {name = 'x', type = 'string', compression='zstd'},
        {name = 'y', type = 'unsigned'},
        {name = 'z', type = 'unsigned', compression = 'zstd'}
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
    box.schema.space.create('storage')
    box.space.storage:create_index('primary', { parts = {2, 'unsigned'} })
    box.schema.space.create('memtx_space', {engine = 'memtx'})
    box.space.memtx_space:create_index('primary', { parts = {2, 'unsigned'} })
    box.space.memtx_space:format({
        {name = 'x', type = 'string', compression='zstd'},
        {name = 'y', type = 'unsigned'},
        {name = 'z', type = 'unsigned', compression = 'zstd'}
    })
    box.space.memtx_space:replace{string.rep('x', 1000), 1, 1}
    box.space.memtx_space:on_replace(function(old, new, s, op)
        local tmp
        if old and old[1] == string.rep('x', 1000) then
            tmp = box.tuple.update(old, {{':', 1, 1, 1000,
                string.rep('!', 1000)}}
            )
        elseif new and new[1] == string.rep('x', 1000) then
            tmp = box.tuple.update(new, {{':', 1, 1, 1000,
                string.rep('?', 1000)}}
            )
        else
            tmp = box.tuple.new{string.rep('#', 1000), 1, 1}
        end
        box.space.storage:replace(tmp)
    end)
end)

g.after_each(function()
    box.space.memtx_space:drop()
    box.space.storage:drop()
    collectgarbage()
end)

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
            {name = 'x', type = 'string', compression='zstd'},
            {name = 'y', type = 'unsigned'},
            {name = 'z', type = 'unsigned', compression = 'zstd'}
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
