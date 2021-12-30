local t = require('luatest')
local server = require('test.luatest_helpers.server')
local net = require('net.box')

box.cfg{}

g = t.group("CRUD operations")

g.before_each(function()
    box.schema.space.create('memtx_space', {engine = 'memtx'})
    box.space.memtx_space:create_index('primary', { parts = {
        {2, 'unsigned'},
        {4, 'unsigned'}
    }})
    box.space.memtx_space:format({
        {name = 'a', type = 'string', compression='zstd'},
        {name = 'b', type = 'unsigned'},
        {name = 'c', type = 'unsigned', compression = 'zstd'},
        {name = 'd', type = 'unsigned'}
    })
end)

g.after_each(function() box.space.memtx_space:drop() collectgarbage() end)

g.test_insert = function()
    local tuple = box.space.memtx_space:insert({string.rep('a', 1000), 1, 1, 1})
    t.assert_equals(tuple[1], string.rep('a', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
end

g.test_replace = function()
    local tuple = box.space.memtx_space:replace({string.rep('a', 1000), 1, 1, 1})
    t.assert_equals(tuple[1], string.rep('a', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
end

g.test_update = function()
    box.space.memtx_space:replace({string.rep('a', 1000), 1, 1, 1})
    local tuple = box.space.memtx_space:update({1, 1}, {
        {'=', 1, string.rep('b', 1000)}
    })
    t.assert_equals(tuple[1], string.rep('b', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
end

g.test_upsert = function()
    -- Insert new tuple
    box.space.memtx_space:upsert({string.rep('a', 1000), 1, 1, 1},
        {{'=', 1, string.rep('b', 1000)}
    })
    local tuples = box.space.memtx_space:select({})
    t.assert_equals(#tuples, 1)
    t.assert_equals(tuples[1][1], string.rep('a', 1000))
    t.assert_equals(tuples[1][2], 1)
    t.assert_equals(tuples[1][3], 1)
    t.assert_equals(tuples[1][4], 1)
    -- Update old tuple
    box.space.memtx_space:upsert({string.rep('a', 1), 1, 1, 1},
        {{'=', 1, string.rep('b', 1000)}
    })
    tuples = box.space.memtx_space:select({})
    t.assert_equals(#tuples, 1)
    t.assert_equals(tuples[1][1], string.rep('b', 1000))
    t.assert_equals(tuples[1][2], 1)
    t.assert_equals(tuples[1][3], 1)
    t.assert_equals(tuples[1][4], 1)
end

g.test_select = function()
   box.space.memtx_space:replace({string.rep('a', 1000), 1, 1, 1})
   box.space.memtx_space:replace({string.rep('b', 1000), 2, 2, 2})
   box.space.memtx_space:replace({string.rep('c', 1000), 3, 3, 3})
   local tuples = box.space.memtx_space:select({})
   t.assert_equals(#tuples, 3)
   t.assert_equals(tuples[1][1], string.rep('a', 1000))
   t.assert_equals(tuples[1][2], 1)
   t.assert_equals(tuples[1][3], 1)
   t.assert_equals(tuples[1][4], 1)
   t.assert_equals(tuples[2][1], string.rep('b', 1000))
   t.assert_equals(tuples[2][2], 2)
   t.assert_equals(tuples[2][3], 2)
   t.assert_equals(tuples[2][4], 2)
   t.assert_equals(tuples[3][1], string.rep('c', 1000))
   t.assert_equals(tuples[3][2], 3)
   t.assert_equals(tuples[3][3], 3)
   t.assert_equals(tuples[3][4], 3)
end

g.test_delete = function()
    box.space.memtx_space:replace({string.rep('a', 1000), 1, 1, 1})
    local tuples = box.space.memtx_space:select({})
    t.assert_equals(#tuples, 1)
    t.assert_equals(tuples[1][1], string.rep('a', 1000))
    t.assert_equals(tuples[1][2], 1)
    t.assert_equals(tuples[1][3], 1)
    t.assert_equals(tuples[1][4], 1)
    local tuple = box.space.memtx_space:delete({1, 2})
    t.assert_equals(tuple, nil)
    local tuple = box.space.memtx_space:delete({1, 1})
    t.assert_equals(tuple[1], string.rep('a', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
    local tuples = box.space.memtx_space:select({})
end

g.test_count = function()
    box.space.memtx_space:replace({string.rep('a', 1000), 1, 1, 1})
    box.space.memtx_space:replace({string.rep('b', 1000), 2, 2, 21})
    box.space.memtx_space:replace({string.rep('c', 1000), 2, 2, 22})
    box.space.memtx_space:replace({string.rep('c', 1000), 3, 3, 3})
    local count = box.space.memtx_space:count(1, {iterator='EQ'})
    t.assert_equals(count, 1)
    count = box.space.memtx_space:count(1, {iterator='GE'})
    t.assert_equals(count, 4)
    count = box.space.memtx_space:count(1, {iterator='LE'})
    t.assert_equals(count, 1)
    count = box.space.memtx_space:count(2, {iterator='EQ'})
    t.assert_equals(count, 2)
    count = box.space.memtx_space:count(2, {iterator='GE'})
    t.assert_equals(count, 3)
    count = box.space.memtx_space:count(2, {iterator='LE'})
    t.assert_equals(count, 3)
    count = box.space.memtx_space:count(3, {iterator='EQ'})
    t.assert_equals(count, 1)
    count = box.space.memtx_space:count(3, {iterator='LE'})
    t.assert_equals(count, 4)
    count = box.space.memtx_space:count(3, {iterator='GE'})
    t.assert_equals(count, 1)
end

g.test_frommap = function()
    local tuple = box.space.memtx_space:frommap({
        a = string.rep('a', 1000), b = 1, c = 1, d = 1}
    )
    t.assert_equals(tuple[1], string.rep('a', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
end

g.test_get = function()
    box.space.memtx_space:replace({string.rep('a', 1000), 1, 1, 1})
    local tuple = box.space.memtx_space:get{1, 1}
    t.assert_equals(tuple[1], string.rep('a', 1000))
    t.assert_equals(tuple['a'], string.rep('a', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple['b'], 1)
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple['c'], 1)
    t.assert_equals(tuple[4], 1)
    t.assert_equals(tuple['d'], 1)
end

g.test_len = function()
    box.space.memtx_space:replace({string.rep('a', 1000), 1, 1, 1})
    box.space.memtx_space:replace({string.rep('b', 1000), 2, 2, 2})
    local len = box.space.memtx_space:len()
    t.assert_equals(len, 2)
end

g.test_pairs = function()
    box.space.memtx_space:replace({string.rep('a', 1000), 1, 1, 1})
    box.space.memtx_space:replace({string.rep('b', 1000), 2, 2, 2})
    box.space.memtx_space:replace({string.rep('c', 1000), 3, 3, 3})
    local num = 0
    local ascii_a = 97
    for _, v in box.space.memtx_space:pairs() do
        t.assert_equals(v[1], string.rep(string.char(ascii_a + num), 1000))
        num = num + 1
        t.assert_equals(v[2], num)
        t.assert_equals(v[3], num)
        t.assert_equals(v[4], num)
    end
end

g.test_create_check_constraint = function()
    box.space.memtx_space:create_check_constraint('c1',
        [["b" = "c" AND "b" = "d"]]
    )
    box.space.memtx_space:create_check_constraint('c2',
        string.format([[NOT "a" LIKE '%s']], string.rep('a', 1000))
    )
    local rc, errmsg = pcall(box.space.memtx_space.replace,
        box.space.memtx_space,
        {string.rep('a', 1000), 1, 1, 1}
    )
    t.assert_equals(rc, false)
    t.assert_equals(string.match(tostring(errmsg),
        "Check constraint failed 'c2'"),
        "Check constraint failed 'c2'"
    )
    rc, errmsg = pcall(box.space.memtx_space.replace,
        box.space.memtx_space,
        {string.rep('b', 1000), 1, 2, 1}
    )
    t.assert_equals(rc, false)
    t.assert_equals(string.match(tostring(errmsg),
        "Check constraint failed 'c1'"),
        "Check constraint failed 'c1'"
    )
    local tuple = box.space.memtx_space:replace({string.rep('b', 1000), 1, 1, 1})
    t.assert_equals(tuple[1], string.rep('b', 1000))
    t.assert_equals(tuple[2], 1)
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
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
