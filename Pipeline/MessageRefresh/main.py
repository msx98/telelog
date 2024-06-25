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


current_date = datetime.datetime.now()
expected_runtime_limit = datetime.timedelta(hours=6)
lookback = datetime.timedelta(days=7)
run_over = datetime.timedelta(days=1) + expected_runtime_limit
run_every = datetime.timedelta(days=1)
assert run_every < run_over
refresh_interval = (
    current_date - lookback - run_over,
    current_date - lookback
)


async def main(db):

    started_app, started_db = False, False
    fetch_min_date, fetch_max_date = refresh_interval
    
    config = Config(project="MessageRefresh")
    previous_run = config.get("previous_run", current_date - run_every - datetime.timedelta(days=1))
    if previous_run > current_date - run_every:
        logging.info("Not running - last run was too recent")
        return

    logging.info("Connecting to account")
    app = load_pyrogram_session(os.environ["TELEGRAM_FETCH_WITH"].split(",")[0])
    await app.start()

    stored_dialogs: Dict[int, StoredDialog] = db.get_stored_dialogs_committed()

    logging.info(f"Iterating over dialogs")
    async for dialog in app.get_dialogs():
        if dialog.chat is None:
            continue
        chat: Chat = dialog.chat
        if chat.id not in stored_dialogs:
            logging.warning(f"Chat {chat.id} not found in DB - skipping")
            continue
        if stored_dialogs[chat.id].max_id is None:
            logging.warning(f"Chat {chat.id} has no messages - skipping")
            continue
        
        logging.info(f"Fetching messages for {chat.id}")
        async for row in app.get_chat_history(chat.id, offset_date=fetch_max_date):
            if row.id > stored_dialogs[chat.id].max_id:
                logging.warning(f"Message {row.id} is newer than max_id {stored_dialogs[chat.id].max_id} - this shouldn't happen - skipping")
                break
            if row.date < fetch_min_date:
                break
            db.add_message(row)
        logging.info(f"Finished fetching messages for {chat.id}")
    
    config.set("previous_run", current_date) # current_time, not now(), so we don't have to account for shifting due to runtime duration
    logging.info("Reached finish")

    await app.stop()


if __name__ == "__main__":
    logging.info("Connecting to DB")
    db = PostgresBackend()
    asyncio.run(main(db))
    logging.info("Closing DB")
    db.close()
    logging.info("Bye")
