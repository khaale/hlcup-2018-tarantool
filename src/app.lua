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


log.info("No interests and new likes")

local bitindex = require('bitindex')
local likes_index = bitindex.create()
local account_index = {}
local indexes = {
    likes_index = likes_index
}

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
            local like_ids = {}
            for _,like in ipairs(account_js['likes']) do
                table.insert(like_ids, like['id'])
                --box.space.likes:upsert(
                --    {account[1], like['id']},
                --   {{'=', 2, like['id']}}
                --)
            end
            likes_index:add_values(account[1], like_ids)
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

-- -- interests stats
-- log.info(box.sql.execute([[
--     SELECT COUNT(*) FROM "interests" GROUP BY "account_id" ORDER BY COUNT(*) DESC LIMIT 10;
-- ]]))
-- log.info(box.sql.execute([[
--     SELECT * FROM "interests" WHERE "account_id" IN (
--         SELECT "account_id" FROM "interests" GROUP BY "account_id" ORDER BY COUNT(*) DESC LIMIT 1);
-- ]]))
-- -- likes stats
-- log.info(box.sql.execute([[
--     SELECT COUNT(*) FROM "likes" GROUP BY "liker_id" ORDER BY COUNT(*) DESC LIMIT 10;
-- ]]))
-- log.info(box.sql.execute([[
--     SELECT * FROM "likes" WHERE "liker_id" IN (
--         SELECT "liker_id" FROM "likes" GROUP BY "liker_id" ORDER BY COUNT(*) DESC LIMIT 1);
-- ]]))
-- log.info(box.sql.execute([[
--     SELECT COUNT(*) FROM "likes" GROUP BY "likee_id" ORDER BY COUNT(*) DESC LIMIT 10;
-- ]]))
-- log.info(box.sql.execute([[
--     SELECT * FROM "likes" WHERE "likee_id" IN (
--         SELECT "likee_id" FROM "likes" GROUP BY "likee_id" ORDER BY COUNT(*) DESC LIMIT 1);
-- ]]))


log.info('creating lowcard table..')
local keys = box.sql.execute([[
       SELECT "country", "city", "sex", "status", "premium_now"
       FROM "accounts"
       GROUP BY "country", "city", "sex", "status", "premium_now";
]])

for _,key in ipairs(keys) do
    local ids =  box.sql.execute(string.format([[
        SELECT "id" FROM "accounts" WHERE "country" = '%s' AND "city" = '%s' AND "sex" = '%s' AND "status" = '%s' AND "premium_now" = '%s' ORDER BY "id" DESC LIMIT 100;
    ]],
    key[1],key[2],key[3],key[4],key[5]
    ))
    local record = { 0 }
    for _,v in ipairs(key) do
    --    if (v == msgpack.NULL) then 
    --         table.insert(record, 0)
    --    else
             table.insert(record, v)
    --    end
    end
    for _, v in ipairs(ids) do
        record[1] = v[1]
        --log.info(record)
        box.space.lowcard:insert(record)
    end
end
log.info(string.format(
    'created lowcard table. Keys count: %d, rows count: %d',
    #keys,
    box.space.lowcard:count()))
--box.space.lowcard:create_index('snd', { type = 'tree', parts = { 'country', 'city', 'sex', 'status', 'premium_now' }, unique = false })

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

local get_account_filter_handler = require('handle_get_accounts_filter').new(refs, box, indexes, log);
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


local function print_stats()

    while(true) do
        fiber.sleep(5)
        log.info(box.info.memory())
    end
end
--fiber.create(print_stats)

log.info('Starting http server..')
httpd:start()