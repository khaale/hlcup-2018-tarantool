#!/usr/bin/env tarantool
local log = require('log')
local os = require('os')
local fio = require('fio')
local json = require('json')
local box = require('box')
local msgpack = require('msgpack')
local fiber = require('fiber')
--local bit32 = require('bit32')
--local digest = require('digest')

local refdata = require('refdata')
local refs = refdata.new()

local db = require('db')

local function tostring_or_nil(value)
    if value == nil then
        return msgpack.NULL
    else
        return tostring(value)
    end
end

local function extract_email_domain(email)
    return string.match(email, '@(.*)')
end

local function extract_year(ts)
    local year = os.date('%Y', ts)
    return tonumber(year)
end

local function extract_phone_code(phone)
    if (phone == nil) then
        return msgpack.NULL
    else
        return tonumber(string.match(phone, '%((%d%d%d)%)'))
    end
end

local current_time = os.time()

log.info('Configure schema')
db.configure(box, log)

log.info('Unpacking data..')
local accounts_json_path = '/tmp/accounts'
os.execute('mkdir /tmp/accounts && unzip -d /tmp/accounts /tmp/data/data.zip && (cd /tmp/accounts && ls -R -lah)')

log.info('Loading data..')
local account_files = fio.listdir(accounts_json_path)
log.info(account_files)
for _, account_file in ipairs(account_files) do
    local f = fio.open(accounts_json_path..'/'..account_file, {'O_RDONLY' })
    local txt = f:read()
    local js = json.decode(txt)
    for _, account_js in ipairs(js['accounts']) do
        --log.info(account_js)
        local premium_start = msgpack.NULL
        local premium_finish = msgpack.NULL
        local premium_now = 0
        if (account_js['premium'] ~= nil) then
            premium_start = tonumber(account_js['premium']['start'])
            premium_finish = tonumber(account_js['premium']['finish'])
            if (current_time > premium_start and current_time < premium_finish) then premium_now = 1 end
        end
        local likes = {}
        local likes_mask = 0
        --[[
        if (account_js['likes'] ~= nil) then
            for _,like in ipairs(account_js['likes']) do
                local hash = tonumber(digest.murmur(tostring(like['id'])))
                likes_mask = bit32.bor(likes_mask, hash)
                table.insert(likes, hash)-- {like['id'], like['ts']})
            end
        end
        ]]
        local account = {
            tonumber(account_js['id']),
            account_js['email'],
            extract_email_domain(account_js['email']),
            tostring_or_nil(account_js['fname']),
            tostring_or_nil(account_js['sname']),
            tostring_or_nil(account_js['phone']),
            extract_phone_code(account_js['phone']),
            refs:get_value('sex', account_js['sex']),
            tonumber(account_js['birth']),
            extract_year(account_js['birth']),
            refs:get_or_add_value('country', account_js['country']),
            refs:get_or_add_value('city', account_js['city']),
            tonumber( account_js['joined']),
            refs:get_value('status', account_js['status']),
            premium_start,
            premium_finish,
            premium_now,
            likes,
            likes_mask
        }

        --log.info(record)
        box.space.accounts:insert(account)

        if (account_js['interests'] ~= nil) then
            for _,interest in ipairs(account_js['interests']) do
                local interest_id = refs:get_or_add_value('interest', interest)
                box.space.interests:insert({ account[1], interest_id })
            end
        end

        if (account_js['likes'] ~= nil) then
            for _,like in ipairs(account_js['likes']) do
                box.space.likes:upsert(
                    {account[1], like['id']},
                    {{'=', 2, like['id']}}
                )
            end
        end
    end
end

db.print_stats(box, log)

log.info('Memory before and after GC: ')
log.info(box.info.memory())
collectgarbage('collect')
log.info(box.info.memory())

-- make snapshot for debug purposes
-- box.snapshot()

refs:build_lookups()

local function dummy_accounts_handler(req)
    local resp = req:render({text = '{"accounts": []}' })
    resp.status = 200
    return resp
end

local function dummy_groups_handler(req)
    local resp = req:render({text = '{"groups": []}' })
    resp.status = 200
    return resp
end

local function dummy_empty_handler(req)
    local resp = req:render({text = '{}' })
    resp.status = 200
    return resp
end


local httpd = require('http.server').new(nil, 80, { log_requests=false })

local get_account_filter_handler = require('handle_get_accounts_filter').new(refs, box, log);
httpd:route({ path = '/accounts/filter', method = 'GET' }, function(req) return get_account_filter_handler:handle(req) end)

httpd:route({ path = '/accounts/group', method = 'GET' }, dummy_groups_handler)

httpd:route({ path = '/accounts/new', method = 'POST' }, dummy_empty_handler)

httpd:route({ path = '/accounts/likes', method = 'POST' }, dummy_empty_handler)

httpd:route({ path = '/accounts/:id/recommend', method = 'GET' }, dummy_accounts_handler)

httpd:route({ path = '/accounts/:id/suggest', method = 'GET' }, dummy_accounts_handler)

httpd:route({ path = '/accounts/:id/suggest', method = 'GET' }, dummy_accounts_handler)

httpd:route({ path = '/accounts/:id', method = 'POST' }, dummy_empty_handler)

local function warmup_http()
    fiber.sleep(1)
    --log.info("Warming up..")
    local http_client = require('http.client').new({max_connections = 5})
    local resp = http_client:request('GET','http://0.0.0.0/accounts/filter/?limit=5')
    log.info(resp)
end

fiber.create(warmup_http)

log.info('Starting http server..')
httpd:start()