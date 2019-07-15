local roaring = require('roaring')
local log = require('log')
local fiber = require('fiber')

local function make_bitmap(values)
    local bitmap = roaring.create_bitmap()
    for _,value in ipairs(values) do
        roaring.add_to_bitmap(bitmap, tonumber(value))
    end
    return bitmap
end

local function add_values(self, id, values)
    local bitmap = make_bitmap(values)
    table.insert(self._data, { id, bitmap })
end

local function search_contains_all(self, values, limit)
    log.info(values)
    local bitmap_input = make_bitmap(values)
    local values_cnt = #values
    local result = {}
    local found_cnt = 0
    local counter = 0;
    for acc,bitmap in pairs(self._data) do
        if (roaring.and_cardinality(bitmap[2], bitmap_input) == values_cnt) then
            table.insert(result, acc)
            found_cnt = found_cnt + 1
            if (found_cnt > limit) then break end
        end

        counter = counter + 1
        if (counter % 1000 == 0) then
            fiber.yield()
        end
    end
    log.info(counter)
    log.info(result)
    return result
end

return {
    create = function()
        return {
            _data = {},
            add_values = add_values,
            search_contains_all = search_contains_all,
        }
    end
}