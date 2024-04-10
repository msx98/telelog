FROM python:3.11-slim

RUN pip3 install pyrogram tgcrypto pymongo deep-translator mysql-connector-python

ADD src/main.py /app/main.py

ADD src/session/listener.session* /app/

ENTRYPOINT ["python", "/app/main.py"]
