#!/usr/bin/env python3

import os
import time
from typing import List, Dict, Tuple
from common.utils import load_pyrogram_session, create_postgres_engine, upsert
from pyrogram.enums import ChatType
from pyrogram.types import Message, User, Chat, Dialog, ChatPreview
from pyrogram.errors import RPCError, FloodWait, Flood
from common.backend.pg_backend import PostgresBackend
from common.backend.base_backend import StoredDialog
import datetime
from collections import defaultdict
import time
from common.consts import consts
from common.backend.config import Config
import signal
import sys
from typing import Optional
import random
import asyncio
import common.backend.models as models
import logging
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from common.backend.pg_backend import compose_insert_chat_dict_query, clean_dict, aggregate_chat_dict_queries
logging.basicConfig(level=logging.INFO)


SHOULD_RETRY_ALREADY_FETCHED = False
MAX_ATTEMPTS_PER_CHAT = 3
SLEEP_SECONDS_AFTER_FETCH_CHAT = 2.5


def get_chat_ids_without_invite_link(engine) -> List[int]:
    logging.info("Fetching dialogs without invite_link (null)")
    with Session(engine) as sess:
        stmt = select(models.Chats.chat_id).select_from(models.Chats)
        if SHOULD_RETRY_ALREADY_FETCHED:
            stmt = stmt.where(models.Chats.invite_link == None or models.Chats.invite_link == "")
        else:
            stmt = stmt.where(models.Chats.invite_link == None)
        result = sess.execute(stmt)
        dialogs = result.fetchall()
        chat_ids = [dialog[0] for dialog in dialogs]
        logging.info(f"Got {len(chat_ids)} dialogs without invite_link")
    return chat_ids


async def main(*, app = None, db = None):

    logging.info(f"Connecting to DB")
    engine = create_postgres_engine()
    config = Config(project="MessageFetch")

    if not get_chat_ids_without_invite_link(engine):
        logging.info("All dialogs have invite_links, exiting")
        exit(0)

    logging.info("Connecting to account")
    app = load_pyrogram_session(config, default_session_string=consts["TELEGRAM_SESSION_STRING_MAIN"])
    await app.start()

    logging.info("Fetching dialogs")
    dialog_dicts = [compose_insert_chat_dict_query(clean_dict(dialog.chat)) async for dialog in app.get_dialogs()]
    for i in range(len(dialog_dicts)):
        dialog_dicts[i].pop("top_message", None)
        dialog_dicts[i].pop("top_message_id", None)
    dialog_dicts = list(aggregate_chat_dict_queries("chat_id", dialog_dicts, set()).values())
    logging.info(f"Got {len(dialog_dicts)} dialogs")
    distinct_ids = set([dialog["chat_id"] for dialog in dialog_dicts])
    logging.info(f"Got {len(distinct_ids)} distinct dialogs")
    if len(distinct_ids) != len(dialog_dicts):
        logging.error(f"Duplicate chat_ids found")
        exit(1)

    logging.info("Inserting dialogs to DB")
    with Session(engine) as sess:
        result = upsert(sess, models.Chats, dialog_dicts)
        logging.info(f"Inserted {result.rowcount} dialogs to DB")
    
    logging.info("Fetching dialogs without invite_link (null)")
    chat_ids: List[int] = get_chat_ids_without_invite_link(engine)
    
    logging.info("Filling invite links")
    dialog_count = len(chat_ids)
    dialogs_left = dialog_count
    for chat_id in chat_ids:
        attempts_left = MAX_ATTEMPTS_PER_CHAT
        while attempts_left:
            attempts_left -= 1
            try:
                chat = await app.get_chat(chat_id)
                logging.info(f"Got chat {chat_id} with invite link: {chat.invite_link}")
                chat_dict = clean_dict(chat)
                chat_dict["invite_link"] = chat.invite_link or ""
                chat_dict.pop("top_message", None)
                chat_dict.pop("top_message_id", None)
                insert_query = compose_insert_chat_dict_query(chat_dict)
                logging.info(f"Inserting chat {chat_id} to DB: {insert_query}")
                with Session(engine) as sess:
                    upsert(sess, models.Chats, [insert_query])
                    sess.flush()
                    sess.commit()
                attempts_left = 0
            except FloodWait as e:
                logging.error(f"Flood wait: {e}")
                time.sleep(e.x)
            except Flood as e:
                logging.error(f"Flood error: {e}")
                time.sleep(e.x)
            except RPCError as e:
                logging.error(f"Error fetching chat {chat_id}: {e}")
            except Exception as e:
                logging.error(f"Unknown error: {e}")
                exit(1)
        dialogs_left -= 1
        logging.info(f"Dialogs left: {dialogs_left}/{dialog_count}")
        time.sleep(SLEEP_SECONDS_AFTER_FETCH_CHAT)

    await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
