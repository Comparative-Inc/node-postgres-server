ARG OS_VERSION=latest
ARG OS_ARCH=x86_64
ARG NODE_VERSION=lts/hydrogen
FROM ghcr.io/comparative-inc/lcdb-node-addon:ubuntu-${OS_VERSION}-${OS_ARCH} AS lcdb-node-addon-base

RUN if [ ! -d /lcdb-client ]; then mkdir /lcdb-client; fi

WORKDIR /lcdb-client

COPY ./example/simple_client.js /lcdb-client/

ARG NODE_VERSION
RUN . $NVM_DIR/nvm.sh && nvm use ${NODE_VERSION} && npm init -y
RUN . $NVM_DIR/nvm.sh && nvm use ${NODE_VERSION} && npm i pg minimist

ENTRYPOINT [ "node", "simple_client.js" ]