#!/usr/bin/env python3

import os
import time
from typing import List, Dict, Tuple
from common.utils import load_pyrogram_session
from pyrogram.enums import ChatType
from pyrogram.types import Message, User, Chat, Dialog, ChatPreview
from pyrogram.errors import RPCError, FloodWait, Flood
from common.backend.pg_backend import PostgresBackend
from common.backend.base_backend import StoredDialog
import datetime
import time
from common.consts import consts
from common.backend.config import Config
import signal
import sys
from typing import Optional
import random
import asyncio
import logging
logging.basicConfig(level=logging.INFO)


SHOULD_FETCH_GROUPS = False


additional_channels = {
    # specify channel IDs below...
    -1001425940518,
}


def get_channel_counts_string(
        d_dialogs: Dict[int, Dialog],
        d_counts: Dict[int, int],
        channels: List[int],
        max_display: int = 10,
) -> str:
    s = ""
    display_count = min(len(channels), max_display)
    for i in range(display_count):
        channel_id = channels[i]
        if channel_id is not None and isinstance(d_dialogs[channel_id].chat.title, str):
            s += f"""- {(d_dialogs[channel_id].chat.title)[:12]}: {d_counts.get(channel_id, "SKIP")}\n"""
    if display_count < len(channels):
        s += f"""... and {len(channels) - display_count} more\n"""
    if not s:
        return "<< DONE >>"
    return s


async def get_dialogs(app) -> Dict[int, Dialog]:
    my_dialogs = app.get_dialogs()
    d: Dict[int, Dialog] = dict()
    async for dialog in my_dialogs:
        d[dialog.chat.id] = dialog
    return d


class StatusMessage:
    def __init__(self, app, config: Config, update_interval: float = 60):
        self.app = app
        self._config = config
        self.message = None
        self.message_config = dict()
        self.update_interval = update_interval
        self.last_s = None
        self.finish_msg = None
        self.announce_update()
    
    async def start(self):
        assert self.message is None
        try:
            last_message = int(self._config.get("status_message", None))
            messages = await self.app.get_messages(int(os.environ["DEBUG_CHAT_ID"]), last_message)
            if isinstance(messages, list) and (len(messages) == 1):
                self.message = messages[0]
            elif isinstance(messages, Message):
                self.message = messages
        except:
            logging.info(f"Sending new message to {os.environ['DEBUG_CHAT_ID']} with last_message {None}")
            self.message = await self.app.send_message(int(os.environ["DEBUG_CHAT_ID"]), "Starting up")
            self._config.set("status_message", self.message.id)
    
    def announce_update(self):
        self.last_update = time.time()
        self.next_update = self.last_update + self.update_interval
    
    async def update(self, force: bool = None, **kwargs):
        self.message_config |= kwargs
        if (time.time() >= self.next_update) or force:
            try:
                await self._update(**self.message_config)
                self.announce_update()
            except FloodWait as e:
                logging.info(f"Got floodwait with {e.value}")
                self.next_update = time.time() + float(e.value) + self.update_interval
            except Exception as e:
                logging.info(f"Got exception 2 {e}")
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
            msg_sum = sum([x.top_message.id for x in d_dialogs.values() if x.top_message])
            s += f"""Total messages (from dialogs): {msg_sum}\n"""
        if self.finish_msg:
            s += self.finish_msg + "\n"
        s += "Updated " + datetime.datetime.now().strftime("%H:%M:%S") + "\n"
        s = s.strip()
        if s:
            if s != self.last_s:
                await self.message.edit_text(s)
                self.last_s = s
            else:
                pass
        else:
            logging.info(f"No channels yet")
    
    async def update_text(self, s: str, force: bool = None):
        if not s:
            return
        if s == self.last_s:
            return
        if (time.time() < self.next_update) and not force:
            return
        if len(s.encode('utf-8')) > 4096:
            logging.info(f"String too long: {len(s)}")
            return
        try:
            await self.message.edit_text(s)
            self.announce_update()
        except FloodWait as e:
            logging.info(f"Got floodwait with {e.value}")
            self.next_update = time.time() + float(e.value) + self.update_interval
        except Exception as e:
            logging.info(f"Got exception that looks like: {e}")
            self.next_update = time.time() + self.update_interval

    async def close(self):
        await self.message.delete()
        self.message = None


async def main(*, app = None, db = None):

    started_app, started_db = False, False

    if db is None:
        logging.info("Connecting to DB")
        db = PostgresBackend()
        started_db = True
    
    config = Config(project="MessageFetch")

    if app is None:
        logging.info("Connecting to account")
        app = load_pyrogram_session(config)
        started_app = True

    await app.start()
    config.set("pyrogram_session_string", await app.export_session_string())

    status_msg = StatusMessage(app, config)
    await status_msg.start()
    first_run = False

    channel_counts: Dict[int, int] = dict()
    pending: List[int] = []
    finished: List[int] = []
    logging.info("Fetching dialogs")
    current_dialogs: Dict[int, Dialog] = await get_dialogs(app)
    logging.info(f"Got {len(current_dialogs)} dialogs")
    db.add_channel(list(current_dialogs.values()), update_top_message_id=False)
    logging.info(f"Inserted {len(current_dialogs)} dialogs to DB without updating top message ID")

    if os.path.exists(f".force_commit_top_message_id"):
        from Pipeline.common.backend.pg_backend import perform_update_top_message_id
        stored_dialogs_actual: Dict[int, StoredDialog] = db.get_stored_dialogs()
        cur = db._conn.cursor()
        for chat_id, v in stored_dialogs_actual.items():
            top_message_id = v.max_id
            perform_update_top_message_id(cur, chat_id, top_message_id)
        db._conn.commit()
        cur.close()
        os.remove(f".force_commit_top_message_id")

    await status_msg.update(d_dialogs=current_dialogs, d_counts=channel_counts, pending=pending, finished=finished, current=None)
    logging.info("Updated msg")
    
    result = db.delete_last_write()
    logging.info(f"Deleted last write result {result}")
    if result is not None:
        channel_id, channel_name, delete_count = result
        logging.info(f"Deleted {delete_count} messages from the write attempt to {channel_name}")
        await status_msg.announce_recover()

    stored_dialogs_fast: Dict[int, StoredDialog] = db.get_stored_dialogs_committed()
    logging.info(f"Got {len(stored_dialogs_fast)} stored dialogs")
    stored_dialogs: Dict[int, StoredDialog] = stored_dialogs_fast#db.get_stored_dialogs()
    #missing_dialogs: Dict[int, StoredDialog] = {k: stored_dialogs[k] for k in stored_dialogs.keys() if k not in stored_dialogs_fast.keys()}
    kicked_channels = [v.title or "UNKNOWN TITLE" for k,v in stored_dialogs.items() if k not in current_dialogs.keys()]
    await status_msg.update(kicked=kicked_channels)
    all_dialogs = {k:dialog for k,dialog in current_dialogs.items() if SHOULD_FETCH_GROUPS or (dialog.chat.type == ChatType.CHANNEL) or (dialog.chat.id in additional_channels)}
    await status_msg.update(d_dialogs=all_dialogs)
    id_to_max_message: Dict[int, int] = {k: v.max_id or 0 for k, v in stored_dialogs.items() if k in all_dialogs.keys()}
    # "or 0" because some channels are banned so they have no messages
    id_to_leftovers: Dict[int, int] = {dialog.chat.id: ((dialog.top_message.id if dialog.top_message else -1) - id_to_max_message.get(dialog.chat.id,-1)) for dialog in all_dialogs.values()}
    sorted_channels = sorted(id_to_leftovers.items(), key=lambda x: x[1], reverse=False) # sort in ascending order
    sorted_channels_with_names = [(x[0], all_dialogs[x[0]].chat.title, x[1]) for x in sorted_channels]
    all_channels = [x[0] for x in sorted_channels if x[0] in all_dialogs.keys()]
    pending += [x for x in all_channels if id_to_leftovers[x] > 0]
    finished += [x for x in all_channels if x not in pending]
    channel_counts |= {channel_id: 0 for channel_id in pending}
    msg_saved_init, _ = db.count_messages()
    msg_saved_count = msg_saved_init
    msg_total_count = sum(id_to_leftovers.values())
    t_start = time.time()
    update_interval_seconds = 10
    next_rate_update = time.time() + update_interval_seconds
    await status_msg.update()
    logging.info(f"Pending: {len(pending)}")
    while pending:
        channel_id = pending.pop(0)
        assert channel_id is not None
        await status_msg.update(current=channel_id)
        db.select_channel(all_dialogs[channel_id])
        async for row in app.get_chat_history(channel_id):
            diff = row.id - id_to_max_message.get(channel_id, -1)
            #assert diff < 30
            if diff <= 0:
                break
            db.add_message(row)
            channel_counts[channel_id] += 1
            msg_saved_count += 1
            if time.time() >= next_rate_update:
                rate = (msg_saved_count - msg_saved_init) / (time.time() - t_start)
                expected_time_delta_seconds = (msg_total_count - msg_saved_count) / rate
                expected_time_delta = datetime.timedelta(seconds=expected_time_delta_seconds)
                #expected_time = datetime.datetime.fromtimestamp(time.time() + expected_time_delta_seconds).strftime("%Y-%m-%d %H:%M:%S")
                finish_msg = f"Expected finish within {expected_time_delta} (rate = {rate:.2f})"
                status_msg.finish_msg = finish_msg
                next_rate_update = time.time() + update_interval_seconds
                await status_msg.update()
        finished.append(channel_id)
        db.unselect_channel()
    await status_msg.announce_finish()
    logging.info("Reached finish")

    if started_app:
        await app.stop()
    
    if started_db:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
