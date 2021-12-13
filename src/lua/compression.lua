-- compression.lua (internal file)

local ffi = require("ffi")
local builtin = ffi.C

ffi.cdef[[
enum compression_type {
        COMPRESSION_TYPE_NONE = 0,
        COMPRESSION_TYPE_ZSTD5,
        compression_type_MAX
};
struct tt_compression;
struct tt_compression *
tt_compression_new(void);
void
tt_compression_delete(struct tt_compression *ttc);
int
tt_compression_init_for_compress(struct tt_compression *ttc,
                                 enum compression_type type,
                                 uint32_t size, char *data);
]]
local compression_t = ffi.typeof('struct tt_compression')

local function compression_new()
    local ttc = builtin.tt_compression_new()
    ttc = ffi.cast('struct tt_compression &', ttc)
    ttc = ffi.gc(ttc, builtin.tt_compression_delete)
    return ttc
end

local function checkcompression(ttc, method)
    if not ffi.istype(compression_t, ttc) then
        error('Attempt to call method without object, compression:%s()',
              method)
    end
end

local function compression_eq(lhs, rhs)
    if not rhs or lhs.size ~= rhs.size then
        return false
    end
    return builtin.memcmp(lhs.data, rhs.data, lhs.size) == 0
end

local function compression_init(ttc, type, size, data)
    checkcompression(ttc)
    if builtin.tt_compression_init_for_compress(ttc, type, size, data) ~= 0 then
        box.error.last() -- TODO
    end
end


local compression_methods = {
    init = compression_init;
};

local compression_mt = {
    __eq = compression_eq;
    __index = compression_methods;
};

ffi.metatype(compression_t, compression_mt);

return setmetatable({
    new         = compression_new;
}, {})
