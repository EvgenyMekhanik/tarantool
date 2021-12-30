local t = require('luatest')
local server = require('test.luatest_helpers.server')
local net = require('net.box')

g = t.group("space operations", {
    {is_local = false, temporary = false},
    {is_local = true, temporary = false},
    {is_local = false, temporary = true}
})

g.before_all(function(cg)
    cg.server = server:new({alias = 'master'})
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:stop()
end)

g.before_each(function(cg)
    cg.server:exec(function(is_local, temporary)
        box.schema.space.create('memtx_space', {
            engine = 'memtx',
            is_local = is_local,
            temporary = temporary
        })
        box.space.memtx_space:create_index('primary', { parts = {
            {1, 'unsigned'},
            {3, 'unsigned'},
            {5, 'unsigned'}
        }})
        box.space.memtx_space:format({
            {name = 'a', type = 'unsigned'},
            {name = 'b', type = 'string', compression='zstd'},
            {name = 'c', type = 'unsigned'},
            {name = 'd', type = 'unsigned', compression = 'zstd'},
            {name = 'e', type = 'unsigned'}
        })
    end, {cg.params.is_local, cg.params.temporary})
end)

g.after_each(function(cg)
    cg.server:exec(function()
        box.space.memtx_space:drop()
        collectgarbage()
    end)
end)

g.test_insert = function(cg)
    local tuple = cg.server:exec(function()
        return box.space.memtx_space:insert({1,
            string.rep('a', 1000), 1, 1, 1
        })
    end)
    t.assert_equals(tuple[1], 1)
    t.assert_equals(tuple[2], string.rep('a', 1000))
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
    t.assert_equals(tuple[5], 1)
end

g.test_replace = function(cg)
    local tuple = cg.server:exec(function()
        return box.space.memtx_space:replace({1,
            string.rep('a', 1000), 1, 1, 1
        })
    end)
    t.assert_equals(tuple[1], 1)
    t.assert_equals(tuple[2], string.rep('a', 1000))
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
    t.assert_equals(tuple[5], 1)
end

g.test_put = function(cg)
    local tuple = cg.server:exec(function()
        return box.space.memtx_space:put({1,
            string.rep('a', 1000), 1, 1, 1
        })
    end)
    t.assert_equals(tuple[1], 1)
    t.assert_equals(tuple[2], string.rep('a', 1000))
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
    t.assert_equals(tuple[5], 1)
end

g.test_update = function(cg)
    cg.server:exec(function()
        box.space.memtx_space:replace({1,
            string.rep('a', 1000), 1, 1, 1
        })
    end)
    local tuple = cg.server:exec(function()
        return box.space.memtx_space:update({1, 1, 1}, {
            {'=', 2, string.rep('b', 1000)}
        })
    end)
    t.assert_equals(tuple[1], 1)
    t.assert_equals(tuple[2], string.rep('b', 1000))
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
    t.assert_equals(tuple[5], 1)
end

g.test_upsert = function(cg)
    -- Insert new tuple
    cg.server:exec(function()
        box.space.memtx_space:upsert({1, string.rep('a', 1000), 1, 1, 1},
            {{'=', 2, string.rep('b', 1000)}
        })
    end)
    local tuples = cg.server:exec(function()
        return box.space.memtx_space:select({})
    end)
    t.assert_equals(#tuples, 1)
    t.assert_equals(tuples[1][1], 1)
    t.assert_equals(tuples[1][2], string.rep('a', 1000))
    t.assert_equals(tuples[1][3], 1)
    t.assert_equals(tuples[1][4], 1)
    t.assert_equals(tuples[1][5], 1)
    -- Update old tuple
    cg.server:exec(function()
        box.space.memtx_space:upsert({1, string.rep('a', 1), 1, 1, 1},
            {{'=', 2, string.rep('b', 1000)}
        })
    end)
    local tuples = cg.server:exec(function()
        return box.space.memtx_space:select({})
    end)
    t.assert_equals(#tuples, 1)
    t.assert_equals(tuples[1][1], 1)
    t.assert_equals(tuples[1][2], string.rep('b', 1000))
    t.assert_equals(tuples[1][3], 1)
    t.assert_equals(tuples[1][4], 1)
    t.assert_equals(tuples[1][5], 1)
end

g.test_select = function(cg)
    cg.server:exec(function()
        box.space.memtx_space:replace({1, string.rep('a', 1000), 1, 1, 1})
        box.space.memtx_space:replace({2, string.rep('b', 1000), 2, 2, 2})
        box.space.memtx_space:replace({3, string.rep('c', 1000), 3, 3, 3})
    end)
    local tuples = cg.server:exec(function()
        return box.space.memtx_space:select({})
    end)
    t.assert_equals(#tuples, 3)
    t.assert_equals(tuples[1][1], 1)
    t.assert_equals(tuples[1][2], string.rep('a', 1000))
    t.assert_equals(tuples[1][3], 1)
    t.assert_equals(tuples[1][4], 1)
    t.assert_equals(tuples[1][5], 1)
    t.assert_equals(tuples[2][1], 2)
    t.assert_equals(tuples[2][2], string.rep('b', 1000))
    t.assert_equals(tuples[2][3], 2)
    t.assert_equals(tuples[2][4], 2)
    t.assert_equals(tuples[2][5], 2)
    t.assert_equals(tuples[3][1], 3)
    t.assert_equals(tuples[3][2], string.rep('c', 1000))
    t.assert_equals(tuples[3][3], 3)
    t.assert_equals(tuples[3][4], 3)
    t.assert_equals(tuples[3][5], 3)
end

g.test_delete = function(cg)
    cg.server:exec(function()
        box.space.memtx_space:replace({1, string.rep('a', 1000), 1, 1, 1})
    end)
    local tuples = cg.server:exec(function()
        return box.space.memtx_space:select({})
    end)
    t.assert_equals(#tuples, 1)
    t.assert_equals(tuples[1][1], 1)
    t.assert_equals(tuples[1][2], string.rep('a', 1000))
    t.assert_equals(tuples[1][3], 1)
    t.assert_equals(tuples[1][4], 1)
    t.assert_equals(tuples[1][5], 1)
    local tuple = cg.server:exec(function()
        return box.space.memtx_space:delete({1, 2, 1})
    end)
    t.assert_equals(tuple, nil)
    local tuple = cg.server:exec(function()
        return box.space.memtx_space:delete({1, 1, 1})
    end)
    t.assert_equals(tuple[1], 1)
    t.assert_equals(tuple[2], string.rep('a', 1000))
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
    t.assert_equals(tuple[5], 1)
    local tuples = cg.server:exec(function()
        return box.space.memtx_space:select({})
    end)
    t.assert_equals(#tuples, 0)
end

g.test_auto_increment = function(cg)
    cg.server:exec(function()
        box.space.memtx_space:replace({1, string.rep('a', 1000), 1, 1, 1})
        box.space.memtx_space:auto_increment({string.rep('b', 1000), 2, 2, 2})
    end)
    local tuples = cg.server:exec(function()
        return box.space.memtx_space:select({})
    end)
    t.assert_equals(#tuples, 2)
    t.assert_equals(tuples[1][1], 1)
    t.assert_equals(tuples[1][2], string.rep('a', 1000))
    t.assert_equals(tuples[1][3], 1)
    t.assert_equals(tuples[1][4], 1)
    t.assert_equals(tuples[1][5], 1)
    t.assert_equals(tuples[2][1], 2)
    t.assert_equals(tuples[2][2], string.rep('b', 1000))
    t.assert_equals(tuples[2][3], 2)
    t.assert_equals(tuples[2][4], 2)
    t.assert_equals(tuples[2][5], 2)
end

g.test_count = function(cg)
    cg.server:exec(function()
        box.space.memtx_space:replace({1, string.rep('a', 1000), 1, 1, 1})
        box.space.memtx_space:replace({2, string.rep('b', 1000), 2, 2, 21})
        box.space.memtx_space:replace({2, string.rep('c', 1000), 2, 2, 22})
        box.space.memtx_space:replace({3, string.rep('c', 1000), 3, 3, 3})
    end)
    local count = cg.server:exec(function()
        return box.space.memtx_space:count(1, {iterator='EQ'})
    end)
    t.assert_equals(count, 1)
    local count = cg.server:exec(function()
        return box.space.memtx_space:count(1, {iterator='GE'})
    end)
    t.assert_equals(count, 4)
    local count = cg.server:exec(function()
        return box.space.memtx_space:count(1, {iterator='LE'})
    end)
    t.assert_equals(count, 1)
    local count = cg.server:exec(function()
        return box.space.memtx_space:count(2, {iterator='EQ'})
    end)
    t.assert_equals(count, 2)
    local count = cg.server:exec(function()
        return box.space.memtx_space:count(2, {iterator='GE'})
    end)
    t.assert_equals(count, 3)
    local count = cg.server:exec(function()
        return box.space.memtx_space:count(2, {iterator='LE'})
    end)
    t.assert_equals(count, 3)
    local count = cg.server:exec(function()
        return box.space.memtx_space:count(3, {iterator='EQ'})
    end)
    t.assert_equals(count, 1)
    local count = cg.server:exec(function()
        return box.space.memtx_space:count(3, {iterator='LE'})
    end)
    t.assert_equals(count, 4)
    local count = cg.server:exec(function()
        return box.space.memtx_space:count(3, {iterator='GE'})
    end)
    t.assert_equals(count, 1)
end

g.test_frommap = function(cg)
    local tuple = cg.server:exec(function()
        return box.space.memtx_space:frommap({
            a = 1, b = string.rep('a', 1000), c = 1, d = 1, e = 1
        })
    end)
    t.assert_equals(tuple[1], 1)
    t.assert_equals(tuple[2], string.rep('a', 1000))
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
    t.assert_equals(tuple[5], 1)
end
--[=[
g.test_get = function()
    box.space.memtx_space:replace({1, string.rep('a', 1000), 1, 1, 1})
    local tuple = box.space.memtx_space:get{1, 1, 1}
    t.assert_equals(tuple[1], 1)
    t.assert_equals(tuple['a'], 1)
    t.assert_equals(tuple[2], string.rep('a', 1000))
    t.assert_equals(tuple['b'], string.rep('a', 1000))
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple['c'], 1)
    t.assert_equals(tuple[4], 1)
    t.assert_equals(tuple['d'], 1)
    t.assert_equals(tuple[5], 1)
    t.assert_equals(tuple['e'], 1)
end

g.test_len_and_count = function()
    box.space.memtx_space:replace({1, string.rep('a', 1000), 1, 1, 1})
    box.space.memtx_space:replace({2, string.rep('b', 1000), 2, 2, 2})
    local len = box.space.memtx_space:len()
    t.assert_equals(len, 2)
end

g.test_pairs = function()
    box.space.memtx_space:replace({1, string.rep('a', 1000), 1, 1, 1})
    box.space.memtx_space:replace({2, string.rep('b', 1000), 2, 2, 2})
    box.space.memtx_space:replace({3, string.rep('c', 1000), 3, 3, 3})
    local num = 1
    local ascii_a = 97
    for _, v in box.space.memtx_space:pairs() do
        t.assert_equals(v[1], num)
        t.assert_equals(v[2], string.rep(string.char(ascii_a + num - 1), 1000))
        t.assert_equals(v[3], num)
        t.assert_equals(v[4], num)
        t.assert_equals(v[5], num)
        num = num + 1
    end
end
]]--
g.test_create_check_constraint = function()
    box.space.memtx_space:create_check_constraint('c1',
        [["c" = "d" AND "c" = "e"]]
    )
    box.space.memtx_space:create_check_constraint('c2',
        string.format([[NOT "b" LIKE '%s']], string.rep('a', 1000))
    )
    local rc, errmsg = pcall(box.space.memtx_space.replace,
        box.space.memtx_space,
        {1, string.rep('a', 1000), 1, 1, 1}
    )
    t.assert_equals(rc, false)
    t.assert_equals(string.match(tostring(errmsg),
        "Check constraint failed 'c2'"),
        "Check constraint failed 'c2'"
    )
    rc, errmsg = pcall(box.space.memtx_space.replace,
        box.space.memtx_space,
        {1, string.rep('b', 1000), 1, 2, 1}
    )
    t.assert_equals(rc, false)
    t.assert_equals(string.match(tostring(errmsg),
        "Check constraint failed 'c1'"),
        "Check constraint failed 'c1'"
    )
    local tuple = box.space.memtx_space:replace({1, string.rep('b', 1000), 1, 1, 1})
    t.assert_equals(tuple[1], 1)
    t.assert_equals(tuple[2], string.rep('b', 1000))
    t.assert_equals(tuple[3], 1)
    t.assert_equals(tuple[4], 1)
    t.assert_equals(tuple[5], 1)
end

g.test_bsize = function(cg)
    box.schema.space.create('space_no_compression', {
        engine = 'memtx',
        is_local = cg.params.is_local,
        temporary = cg.params.temporary
    })
    box.space.space_no_compression:create_index('primary', { parts = {
        {1, 'unsigned'},
        {3, 'unsigned'},
        {5, 'unsigned'}
    }})
    box.space.space_no_compression:format({
        {name = 'a', type = 'unsigned'},
        {name = 'b', type = 'string'},
        {name = 'c', type = 'unsigned'},
        {name = 'd', type = 'unsigned'},
        {name = 'e', type = 'unsigned'}
    })
    box.space.memtx_space:replace{1, string.rep('a', 1000), 1, 1, 1}
    box.space.space_no_compression:replace{1, string.rep('a', 1000), 1, 1, 1}
    t.assert_lt(
        box.space.memtx_space:bsize(),
        box.space.space_no_compression:bsize()
    )
    box.space.space_no_compression:drop()
    collectgarbage()
end

--[[
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
]=]--