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
TIME_PERIOD_FOR_GROUPS = datetime.timedelta(weeks=12)


should_fetch_full = {
    # specify channel IDs below...
    -1001425940518,
    -1001746834350,
    -1001974205315,
    -1001534580776,
    -1001974568317,
    -1001710504678,

}


def get_channel_counts_string(
        d_dialogs: Dict[int, Dialog],
        d_counts: Dict[int, int],
        id_to_leftovers: Dict[int, int],
        channels: List[int],
        max_display: int = 10,
) -> str:
    s = ""
    display_count = min(len(channels), max_display)
    for i in range(display_count):
        channel_id = channels[i]
        if channel_id is not None and isinstance(d_dialogs[channel_id].chat.title, str):
            s += f"""- {(d_dialogs[channel_id].chat.title)}: {d_counts.get(channel_id, "SKIP")} / {id_to_leftovers.get(channel_id, "UNK")}\n"""
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
        self.msg_saved_count = 0
        self.can_write = None
        self.id_to_leftovers = dict()
        self.announce_update()
    
    async def start(self):
        assert self.message is None
        try:
            last_message = int(self._config.get("status_message", None))
            messages = await self.app.get_messages(int(os.environ["TELEGRAM_DEBUG_CHAT_ID"]), last_message)
            if isinstance(messages, list) and (len(messages) == 1):
                self.message = messages[0]
            elif isinstance(messages, Message):
                self.message = messages
        except:
            logging.info(f"Sending new message to {os.environ['TELEGRAM_DEBUG_CHAT_ID']} with last_message {None}")
            self.message = await self.app.send_message(int(os.environ["TELEGRAM_DEBUG_CHAT_ID"]), "Starting up")
            self._config.set("status_message", self.message.id)
    
    def announce_update(self):
        self.last_update = time.time()
        self.next_update = self.last_update + self.update_interval
    
    async def update(self, force: bool = None, **kwargs):
        self.message_config |= kwargs
        self.can_write = (time.time() >= self.next_update) or force
        if True:
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
            s += get_channel_counts_string(d_dialogs, d_counts, self.id_to_leftovers, ([current] if current is not None else []) + (pending or [])) + "\n\n"
        if current is not None:
            s += f"""Currently working on "{d_dialogs[current].chat.title}"\n"""
        if finished or pending or d_dialogs:
            s += f"""Finished {len(finished)}, pending {len(pending)}, total {len(d_dialogs)}\n"""
        if self.msg_saved_count is not None:
            s += f"""Total messages: {self.msg_saved_count}\n"""
            msg_sum = sum([x.top_message.id for x in d_dialogs.values() if x.top_message])
            s += f"""Total messages (from dialogs): {msg_sum}\n"""
        if self.finish_msg:
            s += self.finish_msg + "\n"
        s += "Updated " + datetime.datetime.now().strftime("%H:%M:%S") + "\n"
        s = s.strip()
        with open("/tmp/msg.txt", "w") as f:
            f.write(s)
        if s:
            if self.can_write and (s != self.last_s):
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


async def main(*, session_name: str):

    started_app, started_db = False, False

    logging.info("Connecting to DB")
    db = PostgresBackend()
    config = Config(project="MessageFetch")

    logging.info("Connecting to account")
    app = load_pyrogram_session(session_name)
    await app.start()

    status_msg = StatusMessage(app, config)
    await status_msg.start()

    channel_counts: Dict[int, int] = dict()
    pending: List[int] = []
    finished: List[int] = []
    logging.info("Fetching dialogs")
    current_dialogs: Dict[int, Dialog] = await get_dialogs(app)
    logging.info(f"Got {len(current_dialogs)} dialogs")
    db.add_channel(list(current_dialogs.values()))
    logging.info(f"Inserted {len(current_dialogs)} dialogs to DB without updating top message ID")
    # FIXME - REMOVE 2 LINES BELOW ONCE FIRST BATCH HAS FINISHED
    chats_with_reactions = db.get_chats_with_reactions_bak()
    current_dialogs = {k: v for k,v in current_dialogs.items() if k in chats_with_reactions}

    await status_msg.update(d_dialogs=current_dialogs, d_counts=channel_counts, pending=pending, finished=finished, current=None)
    logging.info("Updated msg")
    
    # FIXME - change to get_offsets_from_ongoing_writes() once first batch has finished
    offsets = db.get_offsets_from_ongoing_writes_reactions()
    logging.info(f"Offsets: {offsets}")

    stored_dialogs: Dict[int, StoredDialog] = db.get_stored_dialogs_committed()
    logging.info(f"Got {len(stored_dialogs)} stored dialogs")
    kicked_channels = [v.title or "UNKNOWN TITLE" for k,v in stored_dialogs.items() if k not in current_dialogs.keys()]
    logging.info(f"Kicked from {len(kicked_channels)} channels: {kicked_channels}")
    kicked_channels = kicked_channels[:10]
    await status_msg.update(kicked=kicked_channels)
    all_dialogs = {
        k: dialog
        for k,dialog in current_dialogs.items() if
        dialog.top_message is not None
        and dialog.chat is not None
        and (not dialog.chat.is_restricted) # this means there are no messages
        and (
            SHOULD_FETCH_GROUPS
            or (dialog.chat.type == ChatType.CHANNEL)
            or (dialog.chat.id in should_fetch_full)
        )
    }
    assert set(all_dialogs.keys()).issubset(set(stored_dialogs.keys()))

    await status_msg.update(d_dialogs=all_dialogs)

    id_to_max_committed_message: Dict[int, int] = {k: stored_dialogs[k].max_id if stored_dialogs[k].max_id is not None else -1 for k in all_dialogs.keys()}
    id_to_top_available_message: Dict[int, int] = {k: offsets.get(k, dialog.top_message.id) for k, dialog in all_dialogs.items()}
    status_msg.id_to_leftovers: Dict[int, int] = {k: (id_to_top_available_message[k] - id_to_max_committed_message[k]) for k in all_dialogs}
    sorted_channels = sorted(status_msg.id_to_leftovers.items(), key=lambda x: x[1], reverse=False) # sort in ascending order
    all_channels = [x[0] for x in sorted_channels]
    all_channels = [x for x in all_channels if x in offsets.keys()] + [x for x in all_channels if x not in offsets.keys()]
    pending += [x for x in all_channels if status_msg.id_to_leftovers[x] > 0]
    finished += [x for x in all_channels if x not in pending]
    channel_counts |= {channel_id: 0 for channel_id in pending}
    msg_saved_init, _ = db.count_messages()
    status_msg.msg_saved_count = msg_saved_init
    msg_total_count = sum(status_msg.id_to_leftovers.values())
    t_start = time.time()
    update_interval_seconds = 60
    next_rate_update = time.time() + update_interval_seconds
    await status_msg.update()
    logging.info(f"Pending: {len(pending)}")
    while pending:
        channel_id = pending.pop(0)
        assert channel_id is not None
        await status_msg.update(current=channel_id)
        assert channel_id in all_dialogs, f"Attempted to fetch {channel_id} which is not fully visible to us"
        offset = id_to_top_available_message[channel_id]
        if all_dialogs[channel_id].chat.type in [ChatType.GROUP, ChatType.SUPERGROUP] and channel_id not in should_fetch_full:
            min_date = datetime.datetime.now() - TIME_PERIOD_FOR_GROUPS
        else:
            min_date = datetime.datetime(year=1970, month=1, day=1)
        logging.info(f"Fetching {channel_id}: {stored_dialogs[channel_id].title}, starting from {offset}, and we are fetching until {id_to_max_committed_message.get(channel_id, -1)}. Max detected message is {all_dialogs[channel_id].top_message.id}")
        async for row in app.get_chat_history(channel_id, offset_id=offset):
            if offset is not None and row.id > offset:
                continue
            if row.id <= id_to_max_committed_message.get(channel_id, -1):
                logging.info(f"Reached max committed message for {channel_id}: {stored_dialogs[channel_id].title} at {row.id} <= {id_to_max_committed_message.get(channel_id, -1)}")
                break
            if row.date < min_date:
                logging.info(f"Reached in date for {channel_id}")
                break
            db.add_message(row)
            channel_counts[channel_id] += 1
            status_msg.msg_saved_count += 1
            if time.time() >= next_rate_update:
                total_saved = sum(channel_counts.values())
                rate = total_saved / (time.time() - t_start)
                expected_time_delta_seconds = (msg_total_count - total_saved) / rate
                expected_time_delta = datetime.timedelta(seconds=expected_time_delta_seconds)
                status_msg.finish_msg = f"Expected finish within {expected_time_delta} (rate = {rate:.2f})"
                next_rate_update = time.time() + update_interval_seconds
                await status_msg.update()
        finished.append(channel_id)
        logging.info(f"Finished {channel_id}: {stored_dialogs[channel_id].title} after {channel_counts[channel_id]} messages")
        db.set_top_message_id_to_db_max(channel_id)
    await status_msg.announce_finish()
    logging.info("Reached finish")

    await app.stop()
    db.close()


if __name__ == "__main__":
    session_names = os.environ["TELEGRAM_FETCH_WITH"].split(",")
    asyncio.run(main(session_name=session_names[0]))
