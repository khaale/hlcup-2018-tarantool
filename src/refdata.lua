local msgpack = require('msgpack')

local function get(self, name)
    return self._dicts[name]
end

local function get_value(self, name, key)
    return self._dicts[name][key]
end

local function contains(self, name)
    return self._dicts[name] ~= nil
end

local function get_or_add_value(self, dict_name, key)
    if (key == nil) then
        return msgpack.NULL
    end

    local dict = self._dicts[dict_name]
    if (dict[key] ~= nil) then
        return dict[key];
    end

    local curr_idx = self._dict_counters[dict_name]
    dict[key] = curr_idx
    self._dict_counters[dict_name] = curr_idx + 1
    return curr_idx
end

local function build_lookups(self)
    local dicts = self._dicts
    local lookups = {}
    for dict_key,dict in pairs(dicts) do
        lookups[dict_key] = {}
        for item_key,idx in pairs(dict) do
            lookups[dict_key][idx] = item_key
        end
    end
    self._lookups = lookups;
end

local function get_key(self, name, value)
    return self._lookups[name][value]
end


local refdata = {}
refdata.new = function()
    local self = {}

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

    self._dicts = dicts
    self._dict_counters = dict_counters
    self.get = get
    self.get_value = get_value
    self.contains = contains
    self.get_or_add_value = get_or_add_value
    self.build_lookups = build_lookups
    self.get_key = get_key

    return self
end

return refdata