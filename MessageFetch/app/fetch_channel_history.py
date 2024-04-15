#!/usr/bin/env python3

import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector import connection as MySQLConnection
import os
import time
from typing import List, Dict, Tuple
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
from pyrogram.types import Message, User, Chat, Dialog
from pyrogram.errors import RPCError, FloodWait, Flood
from mysql_backend import MySQLBackend
from mongo_backend import MongoBackend
import datetime
import time
from consts import *
import signal
import sys
from typing import Optional

print("Connecting to DB")
config["MONGO_DATABASE"] = "messages_new"
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


top_channels = [
    (-1001425940518, "חדשות בזמן בטלגרם", 6969),
    (-1001221122299, 'דיווחים בזמן אמת', 152960),
    (-1001143765178, 'אבו עלי אקספרס', 371460),
    (-1001425850587, 'אבו צאלח הדסק הערבי', 108616),
    (-1001406113886, 'חדשות מהשטח בטלגרם', 467476),
    (-1001613161072, 'דניאל עמרם ללא צנזורה', 373393),
    (-1001221122299, 'דיווחים בזמן אמת', 152960),
    (-1001474443960, 'מבזקי רעם - מבזקי חדשות בזמן אמת', 116313),
]


def get_channel_counts(
        d_names: Dict[int, str],
        d_counts: Dict[int, int],
        channels: List[int],
) -> str:
    s = ""
    for channel_id in channels:
        s += f"""- {(d_names[channel_id])[:12]}: {d_counts.get(channel_id, "SKIP")}\n"""
    if not s:
        return "<< DONE >>"
    return s


def get_status(
        d_names: Dict[int, str],
        d_counts: Dict[int, int],
        pending: List[int],
        finished: List[int],
        current: int
) -> str:
    sum_counts = sum([v for k, v in d_counts.items() if k in finished]) + (d_counts[current] if current is not None else 0)
    s = ""
    s += "Pending:\n"
    s += get_channel_counts(d_names, d_counts, [current] + pending) + "\n\n"
    if current is not None:
        s += f"""Currently working on "{d_names[current]}"\n"""
    s += f"""Finished {len(finished)}, pending {len(pending)}, total {len(d_names)}\n"""
    s += f"""Total messages: {sum_counts}"""
    return s


async def get_dialogs() -> List[Dialog]:
    my_dialogs = app.get_dialogs()
    dialogs_list = list()
    async for dialog in my_dialogs:
        dialogs_list.append(dialog)
    return dialogs_list


async def main():
    channel_counts: Dict[int, int] = dict()
    pending: List[int] = []
    finished: List[int] = []
    status_msg: Message = await app.send_message(DEBUG_CHAT_ID, "Starting up")
    all_dialogs: List[Dialog] = await get_dialogs()
    all_channels: List[int] = [dialog.chat.id for dialog in all_dialogs if dialog.chat.type == ChatType.CHANNEL]
    id_to_name: Dict[int, str] = {dialog.chat.id: dialog.chat.title for dialog in all_dialogs}
    await status_msg.edit_text(f"Got {len(all_channels)} channels, checking which ones are already fetched")
    id_to_max_message: Dict[int, int] = {dialog.chat.id: db.max_message_id(dialog) for dialog in all_dialogs}
    id_to_leftovers: Dict[int, int] = {dialog.chat.id: ((dialog.top_message.id if dialog.top_message else -1) - id_to_max_message[dialog.chat.id]) for dialog in all_dialogs}
    sorted_channels = sorted(id_to_leftovers.items(), key=lambda x: x[1], reverse=False) # sort in ascending order
    all_channels = [x[0] for x in sorted_channels if x[0] in all_channels]
    pending = [x for x in all_channels if id_to_leftovers[x] > 0]
    #finished = [x[0] for x in top_channels]
    finished = [x for x in all_channels if x not in pending]
    channel_counts = {channel_id: 0 for channel_id in pending}
    while pending:
        channel_id = pending.pop(0)
        next_possible = time.time() + 0
        async for row in client.get_chat_history(channel_id):
            if row.id <= id_to_max_message[channel_id]:
                break
            db.add_message(row)
            channel_counts[channel_id] += 1
            if time.time() >= next_possible:
                try:
                    await status_msg.edit_text(get_status(id_to_name, channel_counts, pending, finished, channel_id))
                    next_possible = time.time() + 10
                except FloodWait as e:
                    print(f"Got floodwait with {e.value}")
                    next_possible = time.time() + float(e.value) + 1
                    continue
                except Exception as e:
                    print(f"Got exception {e}")
                    next_possible = time.time() + 10
                    pass
        finished.append(channel_id)
    await status_msg.edit_text(get_status(id_to_name, channel_counts, pending, finished, None))


#@app.on_message(filters.private & filters.command("status"))
#async def status_handler(client: Client, message: Message):
#    status = get_channel_counts()
#    await message.reply_text(status)


if __name__ == "__main__":
    app.loop.run_until_complete(main())
    app.send_message(DEBUG_CHAT_ID, "Finished fetching messages")
    app.stop()
    db.close()


main()