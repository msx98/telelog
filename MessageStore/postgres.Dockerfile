FROM postgres:16.2

RUN apt-get update && apt-get install -y git make gcc make

RUN cd /tmp && \
    git clone --branch v0.6.2 https://github.com/pgvector/pgvector.git && \
    cd pgvector && \
    make; \
    make install

COPY ./src/postgres.sql /docker-entrypoint-initdb.d/init.sql
