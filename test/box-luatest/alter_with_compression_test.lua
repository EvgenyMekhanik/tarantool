local server = require('test.luatest_helpers.server')
local t = require('luatest')

local g = t.group()

g.before_all(function(cg)
    cg.server = server:new({alias = 'master'})
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:stop()
end)

g.test_setting_compression_during_space_creation = function(cg)
    local format

    format = {{name = 'x', type = 'unsigned', compression = 'zstd'}}
    t.assert_error_msg_content_equals(
        "Failed to create space 'space': field 1 has unknown compression type",
        function()
            cg.server:exec(function(format)
                return box.schema.space.create('space', {
                   format = format
                })
            end, {format})
        end
    )
    format = {{name = 'x', type = 'unsigned', compression = 'none'}}
    t.assert_equals(cg.server:exec(function(format)
        return box.schema.space.create('space', {format = format})
    end, {format}), nil)
end

--[==[
 g.server:exec(function()
        box.schema.space.create('space')
    end)
    t.assert_error_msg_content_equals(
        "Can't modify space 'space': field 1 has unknown compression type",
        function()
            cg.server:exec(function(format)
                return box.space.space:format(format)
            end, {format})
        end
    )
    g.server:exec(function()
        box.schema.space.drop()
    end)


g.test_invalid_compression_type_during_space_creation = function(cg)
    local format = {{name = 'x', type = 'unsigned', compression = 'invalid'}}

    t.assert_error_msg_content_equals(
        "Failed to create space 'T': field 1 has unknown compression type",
        function()
            cg.server:exec(function(engine, format)
                return box.schema.space.create('T', {
                    engine = engine, format = format
                })
            end, {cg.params.engine, format})
        end
    )
end

g.before_test('test_invalid_compression_type_during_setting_format', function(cg)
    cg.server:exec(function(engine)
        box.schema.space.create('space', {engine = engine})
    end, {cg.params.engine})
end)

g.test_invalid_compression_type_during_setting_format = function(cg)
    local format = {{name = 'x', type = 'unsigned', compression = 'invalid'}}

    t.assert_error_msg_content_equals(
        "Can't modify space 'space': field 1 has unknown compression type",
        function()
            cg.server:exec(function(format)
                return box.space.space:format(format)
            end, {format})
        end
    )
end

g.after_test('test_invalid_compression_type_during_setting_format', function(cg)
    cg.server:exec(function()
        box.space.space:drop()
        collectgarbage()
    end)
end)

g = t.group("vinyl doesn't support compression")

g.before_all = function()
    g.server = server:new({alias = 'master'})
    g.server:start()
end

g.after_all = function()
    g.server:stop()
end

g.test_vinyl_does_not_support_compression_during_space_creation = function()
    local format = {{name = 'x', type = 'unsigned', compression = 'zstd'}}

    t.assert_error_msg_content_equals(
        "Vinyl does not support compression",
        function()
            g.server:exec(function(format)
                return box.schema.space.create('T', {
                    engine = 'vinyl', format = format
            })
            end, {format})
        end
    )
end

g.test_vinyl_does_not_support_compression_during_setting_format = function()
    local format = {{name = 'x', type = 'unsigned', compression = 'zstd'}}
    g.server:exec(function()
        box.schema.space.create('vinyl_space', {engine = 'vinyl'})
    end)

    t.assert_error_msg_content_equals(
        "Vinyl does not support compression",
        function()
            g.server:exec(function(format)
                return box.space.vinyl_space:format(format)
            end, {format})
        end
    )
    t.assert_error_msg_content_equals(
        "Vinyl does not support compression",
        function()
            g.server:exec(function(format)
                return box.space.vinyl_space:alter({format = format})
            end, {format})
        end
    )

    g.server:exec(function()
        box.space.vinyl_space:drop()
        collectgarbage()
    end)
end

g = t.group('format with compression for indexed fields unsupported')

g.before_all = function()
    g.server = server:new({alias = 'master'})
    g.server:start()
end

g.after_all = function()
    g.server:stop()
end

g.before_each(function()
    g.server:exec(function()
        box.schema.space.create('memtx_space', {engine = 'memtx'})
        box.space.memtx_space:create_index('primary',
            { parts = { {1, 'unsigned'}, {3, 'unsigned'} } }
        )
    end)
end)

g.after_each(function()
    g.server:exec(function()
        box.space.memtx_space:drop()
        collectgarbage()
    end)
end)

g.test_unable_to_set_format_with_compression_for_indexed_fields = function()
    local format

    format = {{name = 'x', type = 'unsigned', compression = 'zstd'}}
    t.assert_error_msg_content_equals(
        "Index field does not support compression",
        function()
            g.server:exec(function(format)
                return box.space.memtx_space:format(format)
            end, {format})
        end
    )
    format = {
        {name = 'field1', type = 'unsigned'},
        {name = 'field2', type = 'unsigned'},
        {name = 'field3', type = 'unsigned', compression = 'zstd'}
    }
    t.assert_error_msg_content_equals(
        "Index field does not support compression",
        function()
            g.server:exec(function(format)
                return box.space.memtx_space:format(format)
            end, {format})
        end
    )
    g.server:exec(function()
        box.space.memtx_space.index.primary:drop()
    end)
    -- After dropping index it's ok to set format with compression
    t.assert_equals(g.server:exec(function(format)
            return box.space.memtx_space:format(format)
        end, {format}), nil
    )
end

g.test_unable_to_create_index_for_compressed_fields = function()
    local format

    format = {
        {name = 'field1', type = 'unsigned'},
        {name = 'field2', type = 'unsigned', compression = 'zstd'},
        {name = 'field3', type = 'unsigned'}
    }
    t.assert_equals(g.server:exec(function(format)
            return box.space.memtx_space:format(format)
        end, {format}), nil
    )
    t.assert_error_msg_content_equals(
        "Index field does not support compression",
        function()
            g.server:exec(function()
                return box.space.memtx_space:create_index('secondary',
                    {parts = {2, 'unsigned'}}
                )
            end)
        end
    )
    g.server:exec(function()
        box.space.memtx_space:replace{1, 1, 1}
        box.space.memtx_space:replace{2, 2, 2}
        box.space.memtx_space:replace{3, 3, 3}
    end)
    format = {
        {name = 'field1', type = 'unsigned'},
        {name = 'field2', type = 'unsigned'},
        {name = 'field3', type = 'unsigned'}
    }
    t.assert_equals(g.server:exec(function(format)
            return box.space.memtx_space:format(format)
        end, {format}), nil
    )
    g.server:exec(function()
        box.space.memtx_space:replace{4, 4, 4}
    end)
    -- There are some tuples in old format with compressed field, so
    -- we can't create this index.
    t.assert_error_msg_content_equals(
        "Index field does not support compression",
        function()
            g.server:exec(function()
                return box.space.memtx_space:create_index('secondary',
                    {parts = {2, 'unsigned'}}
                )
            end)
        end
    )
    g.server:exec(function()
        box.space.memtx_space:delete{1, 1}
        box.space.memtx_space:delete{2, 2}
        box.space.memtx_space:delete{3, 3}
    end)
    -- After deleting all tuples in the old format, you can create an index.
   local index = g.server:exec(function()
        return box.space.memtx_space:create_index('secondary',
            {parts = {2, 'unsigned'}}
        ), nil
    end)
    t.assert_equals(type(index), "table")
end

g = t.group('altering of existing space')

g.before_all = function()
    g.server = server:new({alias = 'master'})
    g.server:start()
end

g.after_all = function()
    g.server:stop()
end

g.before_each(function()
    g.server:exec(function()
        box.schema.space.create('memtx_space', {engine = 'memtx'})
        box.space.memtx_space:create_index('primary', {
            parts = {2, 'unsigned'}
        })
        box.space.memtx_space:format({
            {name = 'x', type = 'string', compression='zstd'},
            {name = 'y', type = 'unsigned'},
            {name = 'z', type = 'unsigned', compression = 'zstd'}
        })
    end)
end)

g.after_each(function()
    g.server:exec(function()
        box.space.memtx_space:drop()
        collectgarbage()
    end)
end)

-- Same checks as previously but for space method `alter`
g.test_alter_space_with_compression = function()
    local format

    g.server:exec(function()
        box.space.memtx_space:replace{string.rep('a', 1000), 1, 1}
        box.space.memtx_space:replace{string.rep('b', 1000), 2, 2}
        box.space.memtx_space:replace{string.rep('c', 1000), 3, 3}
    end)
    format = {
        {name = 'x', type = 'string'},
        {name = 'y', type = 'unsigned'},
        {name = 'z', type = 'unsigned'}
    }
    t.assert_equals(g.server:exec(function(format)
            return box.space.memtx_space:alter({format = format})
        end, {format}), nil
    )
    g.server:exec(function()
        box.space.memtx_space:replace{string.rep('d', 1000), 4, 4}
    end)
    format = {
        {name = 'x', type = 'string', compression = 'zstd'},
        {name = 'y', type = 'unsigned'},
        {name = 'z', type = 'unsigned'}
    }
    t.assert_equals(g.server:exec(function(format)
            return box.space.memtx_space:alter({format = format})
        end, {format}), nil
    )
    g.server:exec(function()
        box.space.memtx_space:replace{string.rep('e', 1000), 5, 5}
    end)
    format = {
        {name = 'x', type = 'string'},
        {name = 'y', type = 'unsigned', compression = 'zstd'},
        {name = 'z', type = 'unsigned'}
    }
    t.assert_error_msg_content_equals(
        "Index field does not support compression",
        function()
            g.server:exec(function(format)
                return box.space.memtx_space:alter({format = format})
            end, {format})
        end
    )
    t.assert_error_msg_content_equals(
        "Index field does not support compression",
        function()
            g.server:exec(function()
                return box.space.memtx_space:create_index('secondary',
                    {parts = {1, 'string'}}
                )
            end)
        end
    )
    t.assert_error_msg_content_equals(
        "Index field does not support compression",
        function()
            g.server:exec(function()
                return box.space.memtx_space:create_index('secondary',
                    {parts = {3, 'unsigned'}}
                )
            end)
        end
    )
    format = {
        {name = 'x', type = 'string', compression = 'zstd'},
        {name = 'y', type = 'unsigned'},
        {name = 'z', type = 'unsigned'}
    }
    t.assert_equals(g.server:exec(function(format)
            return box.space.memtx_space:alter({format = format})
        end, {format}), nil
    )
    g.server:exec(function()
        box.space.memtx_space:delete{1}
        box.space.memtx_space:delete{2}
        box.space.memtx_space:delete{3}
    end)
    local index = g.server:exec(function()
        return box.space.memtx_space:create_index('secondary',
            {parts = {3, 'unsigned'}}
        ), nil
    end)
    t.assert_equals(type(index), "table")
    g.server:exec(function()
        box.space.memtx_space.index.secondary:drop()
        box.space.memtx_space:delete{5}
    end)
    t.assert_error_msg_content_equals(
        "Index field does not support compression",
        function()
            g.server:exec(function()
                return box.space.memtx_space:create_index('secondary',
                    {parts = {1, 'string'}}
                )
            end)
        end
    )
    format = {
        {name = 'x', type = 'string'},
        {name = 'y', type = 'unsigned'},
        {name = 'z', type = 'unsigned'}
    }
    t.assert_equals(g.server:exec(function(format)
            return box.space.memtx_space:alter({format = format})
        end, {format}), nil
    )
    local index = g.server:exec(function()
        return box.space.memtx_space:create_index('secondary',
            { parts = {1, 'string'}}
        ), nil
    end)
    t.assert_equals(type(index), "table")
end

-- Same checks as previously but for index method `alter`
g.test_alter_index_with_compression = function()
   local format

    g.server:exec(function()
        box.space.memtx_space:replace{string.rep('a', 1000), 1, 1}
        box.space.memtx_space:replace{string.rep('b', 1000), 2, 2}
        box.space.memtx_space:replace{string.rep('c', 1000), 3, 3}
    end)
    t.assert_error_msg_content_equals(
        "Index field does not support compression",
        function()
            g.server:exec(function()
                return box.space.memtx_space.index.primary:alter({
                    parts = {
                       {field = 1, type = 'string'},
                       {field = 2, type = 'unsigned'}
                   }
                })
            end)
        end
    )
    format = {
        {name = 'x', type = 'string'},
        {name = 'y', type = 'unsigned'},
        {name = 'z', type = 'unsigned'}
    }
    t.assert_equals(g.server:exec(function(format)
            return box.space.memtx_space:alter({format = format})
        end, {format}), nil
    )
    g.server:exec(function()
        box.space.memtx_space:replace{string.rep('d', 1000), 4, 4}
    end)
     g.server:exec(function()
        box.space.memtx_space:delete{1}
        box.space.memtx_space:delete{2}
        box.space.memtx_space:delete{3}
    end)
    local index = g.server:exec(function()
        return box.space.memtx_space:create_index('secondary', {
            parts = {
                {field = 1, type = 'string'},
                {field = 2, type = 'unsigned'}
            }
        }), nil
    end)
    t.assert_equals(type(index), "table")
    format = {
        {name = 'x', type = 'string', compression = 'zstd'},
        {name = 'y', type = 'unsigned'},
        {name = 'z', type = 'unsigned'}
    }
    t.assert_error_msg_content_equals(
        "Index field does not support compression",
        function()
            g.server:exec(function(format)
                return box.space.memtx_space:format(format)
            end, {format})
        end
    )
end
]==]--