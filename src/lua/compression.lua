-- compression.lua (internal file)

local ffi = require("ffi")
local buffer = require('buffer')
local builtin = ffi.C

ffi.cdef[[
void
tt_compression_create(struct tt_compression *ttc, char *data,
		      uint32_t size, int type);
]]

local ttc_t = ffi.typeof('struct tt_compression')

local function compression_new(buf, size, type)
    local ttc = ffi.new(ttc_t)
    builtin.tt_compression_create(ttc, buf, size, type)
    return ttc
end

return setmetatable({
    new         = compression_new;
}, {})
