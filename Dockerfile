FROM tarantool/tarantool:2

RUN apk add --no-cache --virtual .build-deps \
        git \
        cmake \
        make \
        coreutils \
        gcc \
        g++ \
        postgresql-dev \
        lua-dev \
        musl-dev \
        cyrus-sasl-dev \
        mosquitto-dev \
        libev-dev \
        libressl-dev

RUN luarocks install bit32

COPY src/ /opt/tarantool/

EXPOSE 80

CMD tarantool app.lua