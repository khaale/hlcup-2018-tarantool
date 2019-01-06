local function configure(box, log)
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
end


local function print_field_stats(field_name, box, log)
    local uniq_query = 'SELECT COUNT(DISTINCT "'..field_name..'") FROM "accounts";'
    local uniq_cnt = box.sql.execute(uniq_query)[1][1];
    local null_query = 'SELECT COUNT(*) FROM "accounts" WHERE "'..field_name..'" IS NULL;'
    local null_cnt = box.sql.execute(null_query)[1][1];
    log.info('Unique '..field_name..' values count: '..tostring(uniq_cnt)..', null values count: '..tostring(null_cnt))
end

local function print_stats(box, log)
    print_field_stats('id', box, log)
    print_field_stats('email', box, log)
    print_field_stats('email_domain', box, log)
    print_field_stats('fname', box, log)
    print_field_stats('sname', box, log)
    print_field_stats('phone_code', box, log)
    print_field_stats('country', box, log)
    print_field_stats('city', box, log)
    print_field_stats('birth', box, log)
    print_field_stats('premium_now', box, log)
end

return {
    configure = configure,
    print_stats = print_stats
}

