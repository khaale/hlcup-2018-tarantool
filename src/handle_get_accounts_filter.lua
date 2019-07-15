local msgpack = require('msgpack')
local json = require('json')
local os = require('os')

local pred_map = {}
pred_map['eq'] = '='
pred_map['neq'] = '<>'
pred_map['lt'] = '<'
pred_map['gt'] = '>'

local function revert_ipairs(dict)
    local result = {}
    for k,v in ipairs(dict) do
        result[v] = k
    end
    return result
end

local allowed_params_map = {}
allowed_params_map['sex'] = revert_ipairs{'eq'}
allowed_params_map['email'] = revert_ipairs{'domain','lt','gt'}
allowed_params_map['status'] = revert_ipairs{'eq', 'neq'}
allowed_params_map['fname'] = revert_ipairs{'eq', 'any', 'null'}
allowed_params_map['sname'] = revert_ipairs{'eq', 'starts', 'null'}
allowed_params_map['phone'] = revert_ipairs{'code', 'null'}
allowed_params_map['country'] = revert_ipairs{'eq', 'null'}
allowed_params_map['city'] = revert_ipairs{'eq', 'any', 'null'}
allowed_params_map['birth'] = revert_ipairs{'lt', 'gt', 'year'}
allowed_params_map['interests'] = revert_ipairs{'contains', 'any'}
allowed_params_map['likes'] = revert_ipairs{'contains'}
allowed_params_map['premium'] = revert_ipairs{'now', 'null'}


local function get_items_list(self, item_name, val, quote)
    local result = ''
    local cnt = 0
    for word in string.gmatch(val, '[^,]+') do
        local key = quote..word..quote
        if (self._refs:contains(item_name)) then
            key = self._refs:get_value(item_name, word)
        end
        result = result..', '..key
        cnt = cnt + 1
    end

    return string.sub(result, 2), cnt
end

local function join(items, quote)
    local result = ''
    for _,item in ipairs(items) do
        result = result..', '..quote..tostring(item)..quote
    end
    return string.sub(result, 2)
end

local function split(self, item_name, val)
    local result = {}
    for word in string.gmatch(val, '[^,]+') do
        local key = word
        if (self._refs:contains(item_name)) then
            key = self._refs:get_value(item_name, word)
        end
        table.insert(result, key)
    end

    return result
end

local function execute_account_likes_query(self, val, limit)
    local values = split(self, 'like', val)
    return self._indexes.likes_index:search_contains_all(values, tonumber(limit))
end

local lowcard_cols = revert_ipairs({ "country", "city", "sex", "status", "premium_now" })
local function is_lowcard(params)
    if (#params == 0) then return false end

    for col,_ in pairs(params) do
        if (lowcard_cols[col] ~= nil) then
            return false
        end
    end
    return true
end

local function build_column_clause(params)
    local columns_clause = '"id", "email"'
    for col,p in pairs(params) do
        if (col == 'premium') then
            columns_clause = columns_clause..', "premium_start", "premium_finish"'
        elseif (col ~= 'likes' and col ~= 'interests') then
            columns_clause = columns_clause..', "'..col..'"'
        end
    end
    return columns_clause
end

local function build_filter_clause(self, params)
    local filter_clause = ''

    for col,p in pairs(params) do
        local pred = p[1]
        local val = p[2]

        if (col == 'likes') then
            local ids = execute_account_likes_query(self, val, 100)
            local ids_string = join(ids, '')
            filter_clause = filter_clause..string.format(' AND "id" in (%s)', ids_string)
            --[[
            local item_list, item_count = get_items_list(self, 'like', val, '')
            local fc = string.format(
                    ' AND %d = (SELECT COUNT(DISTINCT "likee_id") FROM "likes" WHERE "accounts"."id" = "liker_id" and "likee_id" IN (%s) LIMIT 1)',
                    item_count,
                    item_list
                    )
            filter_clause = filter_clause..fc
            ]]
        elseif (col == 'interests') then
            --[[
            local item_list, item_count = get_items_list(self, 'interest', val, '')

            local fc
            if (pred == 'any') then
                fc = string.format(
                    ' AND EXISTS (SELECT 1 FROM "interests" WHERE "accounts"."id" = "account_id" and "interest_id" IN (%s) LIMIT 1)',
                    item_list
                    )
            elseif (pred == 'contains') then
                fc = string.format(
                    ' AND %d = (SELECT COUNT(DISTINCT "interest_id") FROM "interests" WHERE "accounts"."id" = "account_id" and "interest_id" IN (%s) LIMIT 1)',
                    item_count,
                    item_list
                    )
            end
            filter_clause = filter_clause..fc
            ]]
        else
            if (col == 'birth' and pred == 'year') then
                filter_clause = string.format([[%s AND "birth_year" = '%s' ]], filter_clause, val)
            elseif (col == 'phone' and pred == 'code') then
                filter_clause = string.format([[%s AND "phone_code" = '%s' ]], filter_clause, val)
            elseif (col == 'email' and pred == 'domain') then
                filter_clause = string.format([[%s AND "email_domain" = '%s' ]], filter_clause, val)
            elseif (col == 'premium' and pred == 'now') then
                filter_clause = string.format([[%s AND "premium_now" = '1' ]], filter_clause, val)
            elseif pred_map[pred] ~= nil then
                if (self._refs:contains(col)) then
                    val = self._refs:get_value(col, val)
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
            elseif (pred == 'any') then
                local item_list, _ = get_items_list(self, col, val, '\'')
                filter_clause = string.format([[%s AND %q IN (%s) ]], filter_clause, col, item_list)
            end
        end
    end

    if (filter_clause ~= '') then
        filter_clause = "WHERE "..string.sub(filter_clause, 5)
    end

    return filter_clause
end

local function build_lowcard_filter_clause(self, params)
    local filter = ''
    for 
end

local function execute_accounts_query(self, params, limit)
    local columns_clause = build_column_clause(params)
    local filter_clause
    if (is_lowcard(params)) then
        filter_clause = build_lowcard_filter_clause(self, params)
    else
        filter_clause = build_filter_clause(self, params)
    end

    local query = string.format([[SELECT
        %s
    FROM "accounts"
    %s
    ORDER BY "id" DESC
    LIMIT %d;]], columns_clause, filter_clause, limit)
    self._log.info(query)
    return self._box.sql.execute(query);
end

local function make_accounts_response(self, results, params)
    local result = {}
    result['accounts'] = {}
    local cols = { 'id', 'email' }

    for col, _ in pairs(params) do
        if (col ~= 'id' and col ~= 'email' and col ~= 'likes' and col ~= 'interests') then
            cols[#cols + 1] = col
        end
    end

    for i, record in ipairs(results) do
        result['accounts'][i] = {}
        for j, col in ipairs(cols) do

            local val = record[j]

            if (val ~= msgpack.NULL) then
                if (val ~= msgpack.NULL and self._refs:contains(col)) then
                    val = self._refs:get_key(col, val)
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

local function try_parse_req_params(req_params)
    local result = {}
    for p,v in pairs(req_params) do
        if (p ~= 'limit' and p ~= 'query_id') then
            local p_name = string.match(p, '(.*)_')
            local p_pred = string.match(p, '_(.*)')
            local val = string.gsub(v, '%+', ' ')

            if (allowed_params_map[p_name] == nil or allowed_params_map[p_name][p_pred] == nil) then
                return false, { p_name, p_pred }
            end

            result[p_name] = { p_pred, val}
        end
    end
    return true, result
end

local function accounts_filter_handler(self, req)
    --local start_time = os.clock()

    local req_params = req:query_param(nil)
    local status, params = try_parse_req_params(req_params)
    if (not status) then
        local resp = req:render({ text = ''})
        resp.status = 400
        return resp
    end
    --log.info(params)
    local limit = tonumber(req_params['limit'])
    if (limit == nil) then
        local resp = req:render({ text = ''})
        resp.status = 400
        return resp
    end

    local result = execute_accounts_query(self, params, limit)

    local response = make_accounts_response(self, result, params)

    --local finish_time = os.clock()
    --self._log.info('Time elapsed: '..tostring(finish_time - start_time)..' seconds')

    local resp = req:render({text = response })
    resp.status = 200
    return resp
end

return {
    new = function(refs, box, indexes, log)
        local handler = {
            _refs = refs,
            _box = box,
            _log = log,
            _indexes = indexes,
            handle = accounts_filter_handler
        }
        return handler
    end
}