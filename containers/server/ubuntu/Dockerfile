ARG OS_VERSION=latest
ARG OS_ARCH=x86_64
ARG NODE_VERSION=lts/hydrogen
FROM ghcr.io/comparative-inc/lcdb-node-addon:ubuntu-${OS_VERSION}-${OS_ARCH} AS lcdb-node-addon-base

RUN if [ ! -d /lcdb-server ]; then mkdir /lcdb-server; fi

WORKDIR /lcdb-server

RUN mkdir impl

COPY ./buffer_reader.js /lcdb-server/
COPY ./buffer_writer.js /lcdb-server/
COPY ./constants.js /lcdb-server/
COPY ./package.json /lcdb-server/
COPY ./package-lock.json /lcdb-server/
COPY ./packet_reader.js /lcdb-server/
COPY ./pg_client.js /lcdb-server/
COPY ./pg_server.js /lcdb-server/
COPY ./pg_type_writer.js /lcdb-server/
COPY ./example/simple_server.js /lcdb-server/impl

ARG NODE_VERSION
RUN . $NVM_DIR/nvm.sh && nvm use ${NODE_VERSION} && npm install
RUN . $NVM_DIR/nvm.sh && nvm use ${NODE_VERSION} && npm link ../lcdb/nodejs-addon/

ENTRYPOINT [ "node", "impl/simple_server.js" ]
