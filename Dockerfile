FROM tarantool/tarantool:2

COPY app.lua /opt/tarantool/app.lua

EXPOSE 80

CMD tarantool app.lua