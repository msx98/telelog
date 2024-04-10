FROM python:3.11-slim

RUN pip3 install pyrogram tgcrypto pymongo deep-translator mysql-connector-python python-dotenv

ADD ./src/*.py /app/

ADD ./src/listener.session /app/

ADD ./src/listener.session-journal /app/

WORKDIR /app

ENTRYPOINT ["python", "./main.py"]
