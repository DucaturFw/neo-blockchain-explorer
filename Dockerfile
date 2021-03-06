FROM node:10-alpine

ARG NEO_EXPLORER_NEO_NODE
ARG NEO_EXPLORER_TABLE_BLOCKS
ARG NEO_EXPLORER_TABLE_TXS
ARG NEO_EXPLORER_DB_NAME
ARG NEO_EXPLORER_RETHINK

COPY . /proj/

WORKDIR /proj

RUN yarn

ENTRYPOINT [ "yarn", "start" ]