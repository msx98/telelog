FROM python:3.11-slim

RUN pip3 install pyrogram tgcrypto pymongo deep-translator mysql-connector-python python-dotenv

ADD ./init.sh /root/init.sh

WORKDIR /app

ENTRYPOINT ["bash", "/root/init.sh"]
