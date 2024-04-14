#!/usr/bin/env python3

import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector import connection as MySQLConnection
import os
import time
from typing import List, Dict
from mysql_backend import normalize_message

cur = None
conn = None
def query(q: str, n: int = None):
    global cur
    global conn
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
    conn = mysql.connector.connect(host=MYSQL_HOST,
                                database=MYSQL_DATABASE,
                                user=MYSQL_USER,
                                password=MYSQL_PASSWORD,
                                port=MYSQL_PORT,)
    if not conn.is_connected():
        raise Exception("Could not connect")
    cur = conn.cursor()
    cur.execute(q)
    i = 0
    if n is not None:
        while i < n:
            try:
                row = next(cur)
                yield row
                i += 1
            except:
                break
    else:
        for row in cur:
            yield row

def create_insert_query(d: Dict, table: str):
    cols = list(d.keys())
    template = ",".join(["%s" for _ in cols])
    cols_str = ",".join(cols)
    ordered_vals = tuple([d[x] for x in cols])
    return f"INSERT INTO {table} ({cols_str}) VALUES ({template})", ordered_vals

def insert(d: List[Dict], table: str):
    global cur
    cur = conn.cursor()
    for i in d:
        q, v = create_insert_query(i, table)
        cur.execute(q, v)
    conn.commit()
    cur.close()

def get_cols(table: str):
    return (tuple([x[0] for x in (query(f"DESCRIBE {table}"))]))


#!/usr/bin/env python3

from pyrogram import Client, filters
from pyrogram.enums import ChatType
from pyrogram.types import Message, User, Chat
from mysql_backend import MySQLBackend
from mongo_backend import MongoBackend
import datetime
from consts import *
import signal
import sys

print("Connecting to DB")
db = MongoBackend(**config)


print("Connecting to account")
app = Client("listener_fetch", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, phone_number=TELEGRAM_PHONE, password=TELEGRAM_PASS)


print("Defining handlers")


def signal_handler(sig, frame):
	print("Closing DB")
	db.close()
	print("Closing account")
	app.stop()
	sys.exit(0)


print("Adding signal handlers")
signal.signal(signal.SIGINT, signal_handler)


print("Launching app")
client = app.start()


channels = [
    #(-1001221122299, 'דיווחים בזמן אמת', 152960),
    (-1001143765178, 'אבו עלי אקספרס', 371460),
    #(-1001425850587, 'אבו צאלח הדסק הערבי', 108616),
    #(-1001406113886, 'חדשות מהשטח בטלגרם', 467476),
    #(-1001613161072, 'דניאל עמרם ללא צנזורה', 373393),
    #(-1001221122299, 'דיווחים בזמן אמת', 152960),
    #(-1001474443960, 'מבזקי רעם - מבזקי חדשות בזמן אמת', 116313),
]
channel_counts = [0 for i in range(len(channels))]
current_channel = None

def get_channel_counts() -> str:
    global current_channel
    s = f"""Currently working on "{current_channel}"\n"""
    for channel_num, channel in enumerate(channels):
        channel_id, channel_title, channel_members = channel
        s += f""""{channel_title}": {channel_counts[channel_num]}\n"""
    return s


#it = client.get_chat_history(channels[0][0])
#row = next(it)
#from enum import Enum
#primitive_types = (bool, int, str, float, datetime.datetime, type(None))

async def main():
    global current_channel
    global channel_counts
    for channel_num, channel in enumerate(channels):
        channel_id, channel_title, channel_members = channel
        print(f"Working on channel {channel_title}")
        current_channel = channel_title
        last_announced = time.time()
        async for row in client.get_chat_history(channel_id):
            db.add_message(row)
            channel_counts[channel_num] += 1
            if time.time() - last_announced > 5:
                print(f"Added {channel_counts[channel_num]} rows to {channel_title}")
                last_announced = time.time()


@app.on_message(filters.private & filters.command("status"))
async def status_handler(client: Client, message: Message):
    status = get_channel_counts()
    await message.reply_text(status)


if __name__ == "__main__":
    app.loop.run_until_complete(main())
    app.stop()
    db.close()


main()