#!/usr/bin/env tarantool
local log = require('log')
local os = require('os')
local fio = require('fio')
local json = require('json')
local box = require('box')
local msgpack = require('msgpack')
local fiber = require('fiber')

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


local dicts = {}
dicts['sex'] = {}
dicts['sex']['f'] = 1
dicts['sex']['m'] = 2
dicts['status'] = {}
dicts['status']['свободны'] = 1
dicts['status']['заняты'] = 2
dicts['status']['всё сложно'] = 3
dicts['country'] = {}
dicts['city'] = {}

local dict_counters = {}
dict_counters['country'] = 1
dict_counters['city'] = 1

local function to_dict_with_addition(dict_name, key)

    if (key == nil) then
        return msgpack.NULL
    end

    if (dicts[dict_name][key] ~= nil) then
        return dicts[dict_name][key];
    end

    local curr_idx = dict_counters[dict_name]
    dicts[dict_name][key] = curr_idx
    dict_counters[dict_name] = curr_idx + 1
    return curr_idx

end

local function to_dict(dict_name, key)
    return dicts[dict_name][key]
end

local function make_dict_lookup()
    local lookups = {}
    for dict_key,dict in pairs(dicts) do
        lookups[dict_key] = {}
        for item_key,idx in pairs(dict) do
            lookups[dict_key][idx] = item_key
        end
    end
    return lookups;
end

log.info('Configure schema')
box.cfg{}
box.once('init', function()
    local s = box.schema.create_space('accounts')
    s:format({
        { name = 'id', type = 'unsigned' }, -- уникальный внешний идентификатор пользователя. Устанавливается тестирующей системой и используется затем, для проверки ответов сервера. Тип - 32-разрядное целое число.
        { name = 'email', type = 'string' }, -- адрес электронной почты пользователя. Тип - unicode-строка длиной до 100 символов. Гарантируется уникальность.
        { name = 'email_domain', type = 'string' }, -- !домен из адреса электронной почты
        { name = 'fname', type = 'string', is_nullable=true }, -- имя и фамилия соответственно. Тип - unicode-строки длиной до 50 символов. Поля опциональны и могут отсутствовать в конкретной записи.
        { name = 'sname', type = 'string', is_nullable=true }, -- имя и фамилия соответственно. Тип - unicode-строки длиной до 50 символов. Поля опциональны и могут отсутствовать в конкретной записи.
        { name = 'phone', type = 'string',  is_nullable=true }, -- номер мобильного телефона. Тип - unicode-строка длиной до 16 символов. Поле является опциональным, но для указанных значений гарантируется уникальность. Заполняется довольно редко.
        { name = 'phone_code', type = 'unsigned', is_nullable=true }, -- !код страны из телефонного номера
        { name = 'sex', type = 'unsigned' }, -- unicode-строка "m" означает мужской пол, а "f" - женский.
        { name = 'birth', type = 'unsigned' }, -- дата рождения, записанная как число секунд от начала UNIX-эпохи по UTC (другими словами - это timestamp). Ограничено снизу 01.01.1950 и сверху 01.01.2005-ым.
        { name = 'birth_year', type = 'unsigned' }, -- дата рождения, записанная как число секунд от начала UNIX-эпохи по UTC (другими словами - это timestamp). Ограничено снизу 01.01.1950 и сверху 01.01.2005-ым.
        { name = 'country', type = 'unsigned', is_nullable=true }, -- страна проживания. Тип - unicode-строка длиной до 50 символов. Поле опционально.
        { name = 'city', type = 'unsigned', is_nullable=true }, -- город проживания. Тип - unicode-строка длиной до 50 символов. Поле опционально и указывается редко. Каждый город расположен в определённой стране.
        { name = 'joined', type = 'unsigned' }, -- дата регистрации в системе. Тип - timestamp с ограничениями: снизу 01.01.2011, сверху 01.01.2018.
        { name = 'status', type = 'unsigned' }, -- текущий статус пользователя в системе. Тип - одна строка из следующих вариантов: "свободны", "заняты", "всё сложно"
        { name = 'premium_start', type = 'unsigned', is_nullable=true }, -- начало и конец премиального периода в системе
        { name = 'premium_finish', type = 'unsigned', is_nullable=true }, -- начало и конец премиального периода в системе
        { name = 'premium_now', type = 'unsigned' }, -- начало и конец премиального периода в системе
    })
    s:create_index('primary', { type = 'tree', parts = {'id'} })
    --s:create_index('snd_sex', { type = 'hash', parts = {'sex'} })
    s:create_index('snd_email', { type = 'tree', parts = {'email'}, unique = false })
    s:create_index('snd_email_domain', { type = 'tree', parts = {'email_domain'}, unique = false })
    -- s:create_index('snd_status', { type = 'hash', parts = {'status'}, unique = false })
    s:create_index('snd_fname', { type = 'tree', parts = {'fname'}, unique = false })
    s:create_index('snd_sname', { type = 'tree', parts = {'sname'}, unique = false })
    s:create_index('snd_phone_code', { type = 'tree', parts = {'phone_code'}, unique = false })
    s:create_index('snd_country', { type = 'tree', parts = {'country'}, unique = false })
    s:create_index('snd_city', { type = 'tree', parts = {'city'}, unique = false })
    s:create_index('snd_birth', { type = 'tree', parts = {'birth'}, unique = false })
    s:create_index('snd_birth_year', { type = 'tree', parts = {'birth_year'}, unique = false })
    s:create_index('snd_premium_now', { type = 'tree', parts = {'premium_now'}, unique = false })
end)

log.info(box.info.memory())
log.info(box.info.gc())

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

        local record = {
            tonumber(account_js['id']),
            account_js['email'],
            extract_email_domain(account_js['email']),
            tostring_or_nil(account_js['fname']),
            tostring_or_nil(account_js['sname']),
            tostring_or_nil(account_js['phone']),
            extract_phone_code(account_js['phone']),
            to_dict('sex', account_js['sex']),
            tonumber(account_js['birth']),
            extract_year(account_js['birth']),
            to_dict_with_addition('country', account_js['country']),
            to_dict_with_addition('city', account_js['city']),
            tonumber( account_js['joined']),
            to_dict('status', account_js['status']),
            premium_start,
            premium_finish,
            premium_now
        }
        --log.info(record)
        box.space.accounts:insert(record)
    end
end

local cnt = box.space.accounts:count()
log.info('Records inserted '..tostring(cnt))
log.info('Sample:')
log.info(box.space.accounts.index.primary:random())

log.info('Memory before and after GC: ')
log.info(box.info.memory())
collectgarbage('collect')
log.info(box.info.memory())

local function print_field_stats(field_name)
    local uniq_query = 'SELECT COUNT(DISTINCT "'..field_name..'") FROM "accounts";'
    local uniq_cnt = box.sql.execute(uniq_query)[1][1];
    local null_query = 'SELECT COUNT(*) FROM "accounts" WHERE "'..field_name..'" IS NULL;'
    local null_cnt = box.sql.execute(null_query)[1][1];
    log.info('Unique '..field_name..' values count: '..tostring(uniq_cnt)..', null values count: '..tostring(null_cnt))
end

local function get_data_stats()
    print_field_stats('id')
    print_field_stats('email')
    print_field_stats('email_domain')
    print_field_stats('fname')
    print_field_stats('sname')
    print_field_stats('phone_code')
    print_field_stats('country')
    print_field_stats('city')
    print_field_stats('birth')
    print_field_stats('premium_now')
    --log.info(box.sql.execute('SELECT DISTINCT "premium_now" FROM "accounts";'))
end

--log.info(dicts)
--log.info(dict_counters)
local lookups = make_dict_lookup(dicts)
--log.info(lookups)

get_data_stats()

local function dummy_accounts_handler(req)
    log.info(req:query_param(nil))
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

local function parse_req_params(req_params)
    local result = {}
    for p,v in pairs(req_params) do
        if (p ~= 'limit' and p ~= 'query_id') then
            local p_name = string.match(p, '(.*)_')
            local p_pred = string.match(p, '_(.*)')
            local val = string.gsub(v, '%+', ' ')
            result[p_name] = { p_pred, val}
        end
    end
    return result
end

local pred_map = {}
pred_map['eq'] = '='
pred_map['neq'] = '<>'
pred_map['lt'] = '<'
pred_map['gt'] = '>'

local function build_accounts_query(params, limit)
    local filter_clause = ''
    local columns_clause = ''

    for col,p in pairs(params) do
        if (col ~= 'likes' and col ~= 'interests') then

            local pred = p[1]
            local val = p[2]

            if (col == 'premium') then
                columns_clause = columns_clause..', "premium_start", "premium_finish"'
            elseif (col ~= 'id' or col ~= 'email') then
                columns_clause = columns_clause..', "'..col..'"'
            end

            if (col == 'birth' and pred == 'year') then
                filter_clause = string.format([[%s AND "birth_year" = '%s' ]], filter_clause, val)
            elseif (col == 'phone' and pred == 'code') then
                filter_clause = string.format([[%s AND "phone_code" = '%s' ]], filter_clause, val)
            elseif (col == 'email' and pred == 'domain') then
                filter_clause = string.format([[%s AND "email_domain" = '%s' ]], filter_clause, val)
            elseif (col == 'premium' and pred == 'now') then
                filter_clause = string.format([[%s AND "premium_now" = '1' ]], filter_clause, val)
            elseif pred_map[pred] ~= nil then
                if (dicts[col] ~= nil) then
                    val = dicts[col][val]
                end

                filter_clause = string.format([[%s AND %q %s '%s' ]], filter_clause, col, pred_map[pred], val)
            elseif (pred == 'null') then
                if (col == 'premium') then
                    col = 'premium_start'
                end

                if (val == "1") then
                    filter_clause = string.format([[%s AND %q IS NULL ]], filter_clause, col)
                else
                    filter_clause = string.format([[%s AND NOT %q IS NULL ]], filter_clause, col)
                end
            end
        end
    end

    if (filter_clause ~= '') then
        filter_clause = "WHERE "..string.sub(filter_clause, 5)
    end

    return string.format([[SELECT
        "id", "email" %s
    FROM "accounts"
    %s
    ORDER BY "id" DESC
    LIMIT %d;]], columns_clause, filter_clause, limit)
end

local function make_accounts_response(results, params)
    local result = {}
    result['accounts'] = {}
    local cols = { 'id', 'email' }

    for col, _ in pairs(params) do
        if (col ~= 'id' or col ~= 'email') then
            cols[#cols + 1] = col
        end
    end

    for i, record in ipairs(results) do
        result['accounts'][i] = {}
        for j, col in ipairs(cols) do

            local val = record[j]

            if (val ~= msgpack.NULL) then
                if (val ~= msgpack.NULL and dicts[col] ~= nil) then
                    val = lookups[col][val]
                end

                if (col == 'premium') then
                    result['accounts'][i]['premium'] = {}
                    result['accounts'][i]['premium']['start'] = val
                    result['accounts'][i]['premium']['finish'] = record[j+1]
                else
                    result['accounts'][i][col] = val
                end
            end
        end
    end

    return json.encode(result)
end


local function accounts_filter_handler(req)
    local req_params = req:query_param(nil)
    local params = parse_req_params(req_params)
    --log.info(params)
    local limit = tonumber(req_params['limit'])

    local query = build_accounts_query(params, limit)
    --log.info(query)

    local result = box.sql.execute(query)
    --log.info(result)

    local response = make_accounts_response(result, params)

    local resp = req:render({text = response })
    resp.status = 200
    return resp
end

local httpd = require('http.server').new(nil, 80, { log_requests=false })
httpd:route({ path = '/accounts/filter', method = 'GET' }, accounts_filter_handler)
httpd:route({ path = '/accounts/group', method = 'GET' }, dummy_groups_handler)
httpd:route({ path = '/accounts/new', method = 'POST' }, dummy_empty_handler)
httpd:route({ path = '/accounts/likes', method = 'POST' }, dummy_empty_handler)
httpd:route({ path = '/accounts/:id/recommend', method = 'GET' }, dummy_accounts_handler)
httpd:route({ path = '/accounts/:id/suggest', method = 'GET' }, dummy_accounts_handler)
httpd:route({ path = '/accounts/:id/suggest', method = 'GET' }, dummy_accounts_handler)
httpd:route({ path = '/accounts/:id', method = 'POST' }, dummy_empty_handler)


local function warmup_http()
    fiber.sleep(1000)
    log.info("Warming up..")
    local http_client = require('http.client').new({max_connections = 5})
    local resp = http_client:request('GET','http://0.0.0.0/accounts/filter/?limit=5')
    log.info(resp)
end

fiber.create(warmup_http)

log.info('Starting http server..')
httpd:start()