FROM postgres:16.2

RUN apt-get update && apt-get install -y git make gcc make postgresql-server-dev-16

RUN cd /tmp && \
    git clone --branch v0.6.2 https://github.com/pgvector/pgvector.git && \
    cd pgvector && \
    make; \
    make install

# FIXME - seems to have no effect
COPY ./src/postgres.sql /docker-entrypoint-initdb.d/init.sql
