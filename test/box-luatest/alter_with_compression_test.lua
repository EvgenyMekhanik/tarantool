local server = require('test.luatest_helpers.server')
local t = require('luatest')

local g = t.group("invalid compression type", t.helpers.matrix({
    engine = {'memtx', 'vinyl'},
    compression = {'zstd', 'lz4'}
}))

g.before_all(function(cg)
    cg.server = server:new({alias = 'master'})
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:stop()
end)

g.test_invalid_compression_type_during_space_creation = function(cg)
    local format = {{
        name = 'x', type = 'unsigned', compression = cg.params.compression
    }}

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
    local format = {{
        name = 'x', type = 'unsigned', compression = cg.params.compression
    }}

    t.assert_error_msg_content_equals(
        "Can't modify space 'space': field 1 has unknown compression type",
        function()
            cg.server:exec(function(format)
                return box.space.space:format(format)
            end, {format})
        end
    )

    t.assert_error_msg_content_equals(
        "Can't modify space 'space': field 1 has unknown compression type",
        function()
            cg.server:exec(function(format)
                return box.space.space:alter({format = format})
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

g = t.group("none compression", t.helpers.matrix({
    engine = {'memtx', 'vinyl'},
}))

g.before_all(function(cg)
    cg.server = server:new({alias = 'master'})
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:stop()
end)

g.test_none_compression_during_space_creation = function(cg)
    local format = {{
        name = 'x', type = 'unsigned', compression = 'none'
    }}

    cg.server:exec(function(engine, format)
        local t = require('luatest')
        t.assert(box.schema.space.create('T', {
            engine = engine, format = format
        }))
    end, {cg.params.engine, format})
end

g.before_test('test_none_compression_during_setting_format', function(cg)
    cg.server:exec(function(engine)
        box.schema.space.create('space', {engine = engine})
    end, {cg.params.engine})
end)

g.test_none_compression_during_setting_format = function(cg)
    local format = {{
        name = 'x', type = 'unsigned', compression = 'none'
    }}

    cg.server:exec(function(format)
        local t = require('luatest')
        t.assert_equals(box.space.space:format(format), nil)
    end, {format})

    cg.server:exec(function(format)
        local t = require('luatest')
        t.assert_equals(box.space.space:alter({format = format}), nil)
    end, {format})
end

g.after_test('test_none_compression_during_setting_format', function(cg)
    cg.server:exec(function()
        box.space.space:drop()
        collectgarbage()
    end)
end)
