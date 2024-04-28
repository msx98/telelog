#!/usr/bin/env python3

import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector import connection as MySQLConnection
import os
import time
from typing import List, Dict, Tuple
from mysql_backend import normalize_message
import json
from pyrogram import Client, filters
from pyrogram.enums import ChatType
from pyrogram.types import Message, User, Chat, Dialog
from pyrogram.errors import RPCError, FloodWait, Flood
from mysql_backend import MySQLBackend
from mongo_backend import MongoBackend
from pg_backend import PostgresBackend
import datetime
import time
from consts import *
import signal
import sys
from typing import Optional


print("Connecting to DB")
db = MongoBackend()


print("Connecting to account")
app = Client("listener_fetch", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, phone_number=TELEGRAM_PHONE, password=TELEGRAM_PASS)


print("Defining handlers")
def signal_handler(sig, frame):
	print("Closing DB")
	db.close()
	print("Closing account")
	app.stop()
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


print("Launching app")
client = app.start()


additional_channels = {
    -1001425940518, # חדשות בזמן בטלגרם - קבוצת החדשות
    -1002137332812,
}


def get_channel_counts_string(
        d_dialogs: Dict[int, Dialog],
        d_counts: Dict[int, int],
        channels: List[int],
) -> str:
    s = ""
    for channel_id in channels:
        if channel_id is not None:
            s += f"""- {(d_dialogs[channel_id].chat.title)[:12]}: {d_counts.get(channel_id, "SKIP")}\n"""
    if not s:
        return "<< DONE >>"
    return s


async def get_dialogs() -> Dict[int, Dialog]:
    my_dialogs = app.get_dialogs()
    d: Dict[int, Dialog] = dict()
    async for dialog in my_dialogs:
        d[dialog.chat.id] = dialog
    return d


from mongo_backend import StoredDialog


class StatusMessage:
    def __init__(self, update_interval: float = 5):
        self.message = None
        self.config = dict()
        self.update_interval = update_interval
        self.last_s = None
        self.announce_update()
    
    async def start(self):
        assert self.message is None
        if os.path.exists(f"{SESSION_DIR}/.status_message.tmp"):
            with open(f"{SESSION_DIR}/.status_message.tmp", "r") as f:
                messages = await app.get_messages(DEBUG_CHAT_ID, int(f.read().strip()))
                if isinstance(messages, list) and (len(messages) == 1):
                    self.message = messages[0]
                elif isinstance(messages, Message):
                    self.message = messages
        else:
            self.message = await app.send_message(DEBUG_CHAT_ID, "Starting up")
            with open(f"{SESSION_DIR}/.status_message.tmp", "w") as f:
                f.write(str(self.message.id))
    
    def announce_update(self):
        self.last_update = time.time()
        self.next_update = self.last_update + self.update_interval
    
    async def update(self, force: bool = None, **kwargs):
        self.config |= kwargs
        if (time.time() >= self.next_update) or force:
            try:
                await self._update(**self.config)
                self.announce_update()
            except FloodWait as e:
                print(f"Got floodwait with {e.value}")
                self.next_update = time.time() + float(e.value) + self.update_interval
            except Exception as e:
                print(f"Got exception {e}")
                self.next_update = time.time() + self.update_interval
    
    async def announce_recover(self):
        await self.update(last_recover=time.time())
    
    async def announce_finish(self):
        await self.update(last_finish=time.time())
    
    async def _update(
            self,
            *,
            d_dialogs: Dict[int, Dialog] = dict(),
            d_counts: Dict[int, int] = dict(),
            pending: List[int] = [],
            finished: List[int] = [],
            current: int = None,
            kicked: List[str] = [],
            last_finish: float = None,
            last_recover: float = None,
            **kwargs,
    ):
        sum_counts = 0
        if d_counts:
            sum_counts += sum([v for k, v in d_counts.items() if k in finished])
            sum_counts += d_counts[current] if current is not None else 0
        s = ""
        if last_finish is not None:
            s += f"""Last finish: {datetime.datetime.fromtimestamp(last_finish).strftime('%H:%M:%S')}\n"""
        if last_recover is not None:
            s += f"""Last recover: {datetime.datetime.fromtimestamp(last_recover).strftime('%H:%M:%S')}\n"""
        if kicked:
            sep = '\n'
            s += f"""Kicked from {len(kicked)} channels:\n{sep.join(kicked)}\n\n"""
        if (current is not None) or pending:
            s += "Pending:\n"
            s += get_channel_counts_string(d_dialogs, d_counts, ([current] if current is not None else []) + (pending or [])) + "\n\n"
        if current is not None:
            s += f"""Currently working on "{d_dialogs[current].chat.title}"\n"""
        if finished or pending or d_dialogs:
            s += f"""Finished {len(finished)}, pending {len(pending)}, total {len(d_dialogs)}\n"""
        if sum_counts:
            s += f"""Total messages: {sum_counts}\n"""
        s += "Updated " + datetime.datetime.now().strftime("%H:%M:%S") + "\n"
        s = s.strip()
        if s:
            if s != self.last_s:
                await self.message.edit_text(s)
                self.last_s = s
            else:
                pass
        else:
            print(f"No channels yet")
    
    async def close(self):
        await self.message.delete()
        self.message = None


async def main():
    status_msg = StatusMessage()
    await status_msg.start()

    while True:
        channel_counts: Dict[int, int] = dict()
        pending: List[int] = []
        finished: List[int] = []
        print("Fetching dialogs")
        current_dialogs: Dict[int, Dialog] = await get_dialogs()
        print(f"Got {len(current_dialogs)} dialogs")
        await status_msg.update(d_dialogs=current_dialogs, d_counts=channel_counts, pending=pending, finished=finished, current=None)
        
        result = db.delete_last_write()
        if result is not None:
            channel_id, channel_name, delete_count = result
            print(f"Deleted {delete_count} messages from the write attempt to {channel_name}")
            await status_msg.announce_recover()

        stored_dialogs_fast: Dict[int, StoredDialog] = db.get_stored_dialogs_fast()
        print(f"Got {len(stored_dialogs_fast)} stored dialogs")
        stored_dialogs: Dict[int, StoredDialog] = stored_dialogs_fast#db.get_stored_dialogs()
        #missing_dialogs: Dict[int, StoredDialog] = {k: stored_dialogs[k] for k in stored_dialogs.keys() if k not in stored_dialogs_fast.keys()}
        kicked_channels = [v.title for k,v in stored_dialogs.items() if k not in current_dialogs.keys()]
        await status_msg.update(kicked=kicked_channels)
        all_dialogs = {k:dialog for k,dialog in current_dialogs.items() if (dialog.chat.type == ChatType.CHANNEL) or (dialog.chat.id in additional_channels)}
        await status_msg.update(d_dialogs=all_dialogs)
        id_to_max_message: Dict[int, int] = {k: v.max_id for k, v in stored_dialogs.items() if k in all_dialogs.keys()}
        id_to_leftovers: Dict[int, int] = {dialog.chat.id: ((dialog.top_message.id if dialog.top_message else -1) - id_to_max_message.get(dialog.chat.id,-1)) for dialog in all_dialogs.values()}
        sorted_channels = sorted(id_to_leftovers.items(), key=lambda x: x[1], reverse=False) # sort in ascending order
        sorted_channels_with_names = [(x[0], all_dialogs[x[0]].chat.title, x[1]) for x in sorted_channels]
        all_channels = [x[0] for x in sorted_channels if x[0] in all_dialogs.keys()]
        pending += [x for x in all_channels if id_to_leftovers[x] > 0]
        finished += [x for x in all_channels if x not in pending]
        channel_counts |= {channel_id: 0 for channel_id in pending}
        await status_msg.update()
        print(f"Pending: {len(pending)}")
        while pending:
            channel_id = pending.pop(0)
            assert channel_id is not None
            await status_msg.update(current=channel_id)
            db.select_channel(all_dialogs[channel_id])
            async for row in client.get_chat_history(channel_id):
                diff = row.id - id_to_max_message.get(channel_id, -1)
                #assert diff < 30
                if diff <= 0:
                    break
                db.add_message(row)
                channel_counts[channel_id] += 1
                await status_msg.update()
            finished.append(channel_id)
            db.unselect_channel()
        await status_msg.announce_finish()
        print("Reached finish")


if __name__ == "__main__":
    app.loop.run_until_complete(main())
    app.stop()
    db.close()


main()
