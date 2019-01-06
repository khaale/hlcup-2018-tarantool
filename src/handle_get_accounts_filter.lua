local msgpack = require('msgpack')
local json = require('json')

local pred_map = {}
pred_map['eq'] = '='
pred_map['neq'] = '<>'
pred_map['lt'] = '<'
pred_map['gt'] = '>'

local function build_accounts_query(self, params, limit)
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

local function make_accounts_response(self, results, params)
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

local function accounts_filter_handler(self, req)
    local req_params = req:query_param(nil)
    local params = parse_req_params(req_params)
    --log.info(params)
    local limit = tonumber(req_params['limit'])

    local query = build_accounts_query(self, params, limit)
    --log.info(query)

    local result = self._box.sql.execute(query)
    --log.info(result)

    local response = make_accounts_response(self, result, params)

    local resp = req:render({text = response })
    resp.status = 200
    return resp
end

return {
    new = function(refs, box, log)
        local handler = {
            _refs = refs,
            _box = box,
            _log = log,
            handle = accounts_filter_handler
        }
        return handler
    end
}