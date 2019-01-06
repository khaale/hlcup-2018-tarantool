FROM tarantool/tarantool:2

COPY src/ /opt/tarantool/

EXPOSE 80

CMD tarantool app.lua