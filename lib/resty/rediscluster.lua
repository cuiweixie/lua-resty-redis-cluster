local ffi = require 'ffi'


local ffi_new = ffi.new
local C = ffi.C
local crc32 = ngx.crc32_short
local setmetatable = setmetatable
local floor = math.floor
local pairs = pairs
local tostring = tostring
local tonumber = tonumber


ffi.cdef[[
int lua_redis_crc16(char *key, int keylen);
]]


local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end


--
-- Find shared object file package.cpath, obviating the need of setting
-- LD_LIBRARY_PATH
-- Or we should add a little patch for ffi.load ?
--
local function load_shared_lib(so_name)
    local string_gmatch = string.gmatch
    local string_match = string.match
    local io_open = io.open
    local io_close = io.close

    local cpath = package.cpath

    for k, _ in string_gmatch(cpath, "[^;]+") do
        local fpath = string_match(k, "(.*/)")
        fpath = fpath .. so_name

        -- Don't get me wrong, the only way to know if a file exist is trying
        -- to open it.
        local f = io_open(fpath)
        if f ~= nil then
            io_close(f)
            return ffi.load(fpath)
        end
    end
end


local _M = {}
local mt = { __index = _M }


local clib = load_shared_lib("libredis_slot.so")
if not clib then
    error("can not load libredis_slot.so")
end

local function redis_slot(str)
    return clib.lua_redis_crc16(ffi.cast("char *", str), #str)
end

local redis = require "resty.redis"

redis.add_commands("cluster")
local commands = {
    "append",            --[["auth",]]        --[["bgrewriteaof",]]
    --[["bgsave",]]      --[["blpop",]]    --[["brpop",]]
    --[["brpoplpush",]]  --[["config", ]]   --[["dbsize",]]
    --[["debug", ]]      "decr",              "decrby",
    --[["del",]]         --[["discard",           "echo",]]
    --[["eval",]]              "exec",              "exists",
    --[["expire",            "expireat",          "flushall",
    "flushdb",]]           "get",               "getbit",
    "getrange",          "getset",            "hdel",
    "hexists",           "hget",              "hgetall",
    "hincrby",           "hkeys",             "hlen",
    "hmget",             "hmset",             "hset",
    "hsetnx",            "hvals",             "incr",
    "incrby",           --[["info",]]         --[["keys",]]
    --[["lastsave", ]]  "lindex",            "linsert",
    "llen",              "lpop",              "lpush",
    "lpushx",            "lrange",            "lrem",
    "lset",              "ltrim",             "mget",
    "monitor",           --[["move",]]        "mset",
    "msetnx",            --[[["multi",]]      --[["object",]]
    --[["persist",]]     --[["ping",]]        --[["psubscribe",]]
   --[[ "publish",           "punsubscribe",      "quit",]]
    --[["randomkey",         "rename",            "renamenx",]]
    "rpop",              --[["rpoplpush",]]   "rpush",
    "rpushx",            "sadd",              --[["save",]]
    "scard",             --[["script",]]
    --[["sdiff",             "sdiffstore",]]
    --[["select",]]            "set",               "setbit",
    "setex",             "setnx",             "setrange",
    --[["shutdown",          "sinter",            "sinterstore",
    "sismember",         "slaveof",           "slowlog",]]
    "smembers",          "smove",             "sort",
    "spop",              "srandmember",       "srem",
    "strlen",            --[["subscribe",]]         "sunion",
    "sunionstore",       --[["sync",]]              "ttl",
    "type",              --[["unsubscribe",]]       --[["unwatch",
    "watch",]]             "zadd",              "zcard",
    "zcount",            "zincrby",           "zinterstore",
    "zrange",            "zrangebyscore",     "zrank",
    "zrem",              "zremrangebyrank",   "zremrangebyscore",
    "zrevrange",         "zrevrangebyscore",  "zrevrank",
    "zscore",            --[["zunionstore",    "evalsha"]]
}

local _M = {}
local mt = { __index = _M }

local slot_cache = {}
--local slot_state = {}
--local WAIT = 0
--local FIN = 1


function _M.fetch_slots(self)
    local serv_list = self.config.serv_list
    local red = redis:new()
    for i=1,#serv_list do
        local ip = serv_list[i].ip
        local port = serv_list[i].port
        local ok, err = red:connect(ip, port)
        if ok then
            local slot_info, err = red:cluster("slots")
            if slot_info then
                local slots = {}
                for i=1,#slot_info do
                    local item = slot_info[i]
                    for slot = item[1],item[2] do
                        local list = {serv_list={}, cur = 1}
                       for j = 3,#item do
                            list.serv_list[#list.serv_list + 1] = {ip = item[j][1], port = item[j][2]}
                            slots[slot] = list
                        end
                    end
                end
                slot_cache[self.config.name] = slots
                --self.slots = slots
                --debug_log("fetch_slots", self)
            end
        end
    end
end



function _M.init_slots(self)
    if slot_cache[self.config.name] then
        return
    end
    self:fetch_slots()
end

function _M.new(self, config)
    local inst = {}
    inst.config = config
    inst = setmetatable(inst, mt)
    inst:init_slots()
    return inst
end

function _M.close(self)

end

local function next_index(cur, size)
    cur = cur + 1
    if cur > size then
        cur = 1
    end
    return cur
end

local MAGIC_TRY = 3
local DEFUALT_KEEPALIVE_TIMEOUT = 1000
local DEFAULT_KEEPALIVE_CONS = 200

local function _do_cmd(self, cmd, key, ...)
    if self._reqs then
        local args = {...}
        local t = {cmd = cmd, key=key, args=args}
        table.insert(self._reqs, t)
        return
    end
    local config = self.config

    key = tostring(key)
    local slot = redis_slot(key)

    for k=1, MAGIC_TRY do
        local slots = slot_cache[self.config.name]
        local serv_list = slots[slot].serv_list
        local index =slots[slot].cur
        for i=1,#serv_list do
            local ip = serv_list[index].ip
            local port = serv_list[index].port
            redis_client = redis:new()
            local ok, err = redis_client:connect(ip, port)
            if ok then
                slots[slot].cur = index
                local res, err = redis_client[cmd](redis_client, key, ...)
                redis_client:set_keepalive(config.keepalive_timeout or DEFUALT_KEEPALIVE_TIMEOUT,
                                           config.keepalove_cons or DEFAULT_KEEPALIVE_CONS)
                if err and string.sub(err, 1, 5) == "MOVED" then
                    self:fetch_slots()
                    break
                end
                return res, err
            else
                index = next_index(index, #serv_list)
            end
        end
    end
    return nil, "oops! please contact cuiweixie"
end

for i = 1, #commands do
    local cmd = commands[i]

    _M[cmd] =
        function (self, ...)
            return _do_cmd(self, cmd, ...)
        end
end

function _M.init_pipeline(self)
    self._reqs = {}
end

local INTIT = 0
local FIND = 1
local FIN  = 2
function _M.commit_pipeline(self)
    if not self._reqs or #self._reqs == 0 then return end
    local reqs = self._reqs
    self._reqs = nil
    local config = self.config
    local slots = slot_cache[config.name]
    local map_ret = {}
    local map = {}
    for i=1,#reqs do
        reqs[i].origin_index = i
        local key = reqs[i].key
        local slot = redis_slot(tostring(key))
        local slot_item = slots[slot]
        local ip = slot_item.serv_list[slot_item.cur].ip
        local port = slot_item.serv_list[slot_item.cur].port
        local inst_key = ip..tostring(key)
        if not map[ins_key] then
            map[inst_key] = {ip=ip,port=port,reqs={}}
            map_ret[ins_key] = {}
        end
        local ins_req = map[inst_key].reqs
        ins_req[#ins_req+1] = reqs[i]
    end
    for k, v in pairs(map) do
        local ip = v.ip
        local port = v.port
        local ins_reqs = v.reqs
        local ins = redis:new()
        local ok, err = ins:connect(ip, port)

        if ok then
            ins:init_pipeline()
            for i=1,#ins_reqs do
                local req = ins_reqs[i]
                if #req.args > 0 then
                    ins[req.cmd](ins, req.key, unpack(req.args))
                else
                    ins[req.cmd](ins, req.key)
                end
            end
            local res, err = ins:commit_pipeline()
            redis_client:set_keepalive(config.keepalive_timeout or DEFUALT_KEEPALIVE_TIMEOUT,
                                           config.keepalove_cons or DEFAULT_KEEPALIVE_CONS)
            if err then
                return nil, err.." return from "..tostring(ip)..":"..tostring(port)
            end
            map_ret[k] = res
        else
            return nil, "commit failed while connecting to "..tostring(ip)..":"..tostring(port)
        end
    end
    local ret = {}
    for k,v in pairs(map_ret) do
        local ins_reqs = map[k].reqs
        local res = v
        for i=1,#ins_reqs do
            req[ins_reqs[i].origin_index] =res[i]
        end
    end
    return res
end

function _M.cancel_pipeline(self)
    self._reqs = nil
end

return _M
