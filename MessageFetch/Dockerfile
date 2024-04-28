FROM python:3.11-slim

RUN pip3 install pyrogram tgcrypto pymongo deep-translator mysql-connector-python python-dotenv

RUN apt update && apt install -y nano vim procps net-tools iputils-ping wget curl

ADD ./init.sh /root/init.sh

WORKDIR /app

ENTRYPOINT ["bash", "/root/init.sh"]
