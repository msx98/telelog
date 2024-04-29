#!/usr/bin/env python3
print("Started execution")
import sys
import os
homedir=os.environ["HOME"]
sys.path.insert(0, f"{homedir}/docker/telenews/MessageFetch/app")
os.chdir(f"{homedir}/docker/telenews/Experiments")
from typing import Tuple, Dict, Any, Optional, List
from pyrogram.types import Message, Chat, Dialog
from pg_backend import PostgresBackend, TableNames
import datetime
from typing import Tuple, Dict, Any, Optional, List
from collections import OrderedDict
import pyrogram.types
from pyrogram.types import Message, User, Chat, MessageReactions, Reaction
from pyrogram.enums import ChatType, MessageMediaType
from pyrogram.errors import FloodWait
import time
import os
import pyrogram
from pymongo import MongoClient
from enum import Enum
import datetime
import pandas as pd
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pyrogram
from pyrogram import Client
from consts import *
import json
import asyncio
NullFormatter = ticker.NullFormatter
MONGO_HOST="mongodb://127.0.0.1:27017/"
MONGO_INITDB_ROOT_USERNAME="root"
MONGO_INITDB_ROOT_PASSWORD="example"
MONGO_DATABASE="messages_new"
print("Imported")

def sleeper():
    time.sleep(15)

db = PostgresBackend(debug_read_only=True)
mongoconn = MongoClient(MONGO_HOST, username=MONGO_INITDB_ROOT_USERNAME, password=MONGO_INITDB_ROOT_PASSWORD)
mongodb = mongoconn[MONGO_DATABASE]
print("Connected to dbs")

chats_table = mongodb["chats_man"]
def insert_chat(chat: Chat, linked_chat_id = None):
    if chat.linked_chat:
        linked_chat_id = chat.linked_chat.id
    chats_table.insert_one({
        "chat_id": chat.id,
        "linked_chat_id": linked_chat_id,
        "title": chat.title,
        "username": chat.username,
        "invite_link": chat.invite_link,
    })
def insert_chat_full(chat: Chat):
    if chat.linked_chat:
        insert_chat(chat, chat.linked_chat.id)
        insert_chat(chat.linked_chat, chat.id)
    else:
        insert_chat(chat, None)

async def main():
    app = Client("listener_pgtests", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, phone_number=TELEGRAM_PHONE, password=TELEGRAM_PASS)
    client = await app.start()
    print("Connected to Telegram")

    joined_dialogs = dict()
    linked_chats = dict()
    linked_chats_inv = dict()
    missing_dialogs_with_titles = dict()
    missing_dialogs_with_invite = dict()
    missing_dialogs_with_nothing = dict()
    all_invites = dict()

    joined_dialogs_gen = app.get_dialogs()
    async for dialog in joined_dialogs_gen:
        joined_dialogs[dialog.chat.id] = dialog
        print(dialog.chat.id, dialog.chat.title)
    print(f"Got {len(joined_dialogs)} dialogs")

    all_dialogs_mongo = mongodb["dialogs"].find({})
    for dialog in all_dialogs_mongo:
        chat_id = dialog["chat"]["id"]
        if chat_id in joined_dialogs:
            continue
        chat_title = dialog["chat"].get("title",None)
        username = dialog["chat"].get("username", None)
        invite_link = dialog["chat"].get("invite_link",None)
        invite = (username, invite_link)
        if username or invite_link:
            missing_dialogs_with_invite[chat_id] = (username, invite_link)
            all_invites[chat_id] = (username, invite_link)
        elif chat_title:
            missing_dialogs_with_titles[chat_id] = chat_title
        else:
            missing_dialogs_with_nothing[chat_id] = dialog

    all_enc = dict()
    with open("all_enc.json", "r") as f:
        all_enc = json.load(f)
    if not all_enc:
        print("couldnt load all_enc")
        exit(1)
    all_enc = {k: v for k, v in all_enc}
    for chat_id, chat_title in all_enc.items():
        if chat_id in joined_dialogs|missing_dialogs_with_invite:
            continue
        missing_dialogs_with_titles[chat_id] = chat_title
        missing_dialogs_with_nothing.pop(chat_id, None)

    for chat_id, dialog in joined_dialogs.items():
        chat_title = dialog.chat.title
        print(f"{chat_id}: {chat_title}")
        chat = chats_table.find_one({"chat_id": chat_id})
        if chat:
            print(f"{chat_id}: found in mongo")
            if chat_id not in all_invites:
                all_invites[chat_id] = (chat["username"], chat["invite_link"])
            linked_chat_id = chat["linked_chat_id"]
            if not linked_chat_id:
                print(f"{chat_id}: skipping")
                continue
            linked_chat = chats_table.find_one({"chat_id": linked_chat_id})
            assert linked_chat
            if linked_chat_id not in all_invites:
                all_invites[linked_chat_id] = (linked_chat["username"], linked_chat["invite_link"])
            linked_chats[linked_chat_id] = (chat_id, linked_chat)
            linked_chats_inv[chat_id] = (linked_chat_id, chat)
            print(f"{chat_id}: continuing")
            continue

        for i in range(3):
            try:
                chat = await app.get_chat(chat_id)
            except FloodWait as e:
                print(f"{chat_id}: get_chat got floodwait for {e.value} seconds")
                time.sleep(e.value)
                sleeper()
            except Exception as e:
                print(e)
                sleeper()
        if not chat:
            print(f"{chat_id}: Could not obtain chat")
            continue
        print(f"{chat_id}: Got chat, inserting")
        insert_chat_full(chat)
        linked_chat = chat.linked_chat
        if linked_chat:
            print(f"{chat_id}: Linked chat: {linked_chat.id} {linked_chat.title}")
            linked_chats[linked_chat.id] = (chat_id, linked_chat)
            linked_chats_inv[chat_id] = linked_chat.id
            if linked_chat.id not in all_invites:
                all_invites[linked_chat.id] = (linked_chat.username, linked_chat.invite_link)
        else:
            print(f"{chat_id}: No linked chat")
        print("Sleeping")
        sleeper()

    print("Fixing dicts")
    for chat_id, handle in linked_chats.items():
        father_id, linked_chat = handle
        missing_dialogs_with_titles.pop(chat_id, None)
        missing_dialogs_with_nothing.pop(chat_id, None)
        if chat_id not in missing_dialogs_with_invite:
            assert isinstance(linked_chat,(dict,Chat))
            missing_dialogs_with_invite[chat_id] = linked_chat.id if hasattr(linked_chat, "id") else linked_chat["chat_id"]

    missing_dialogs = missing_dialogs_with_titles|missing_dialogs_with_invite|missing_dialogs_with_nothing
    all_dialogs = joined_dialogs|missing_dialogs

    print(f"Total dialogs: {len(all_dialogs)}")
    print(f"Joined dialogs: {len(joined_dialogs)}")
    print(f"Missing dialogs with titles: {len(missing_dialogs_with_titles)}")
    print(f"Missing dialogs with invite: {len(missing_dialogs_with_invite)}")
    print(f"Missing dialogs with nothing: {len(missing_dialogs_with_nothing)}")
    print(f"Linked chats: {len(linked_chats)}")
    print(f"All invites: {len(all_invites)}")

    for chat_id in missing_dialogs:
        if chat_id not in all_invites:
            continue
        if chat_id in joined_dialogs:
            continue
        print(f"{chat_id}: trying to join")
        username, invite_link = all_invites[chat_id]
        invite = username or invite_link or chat_id
        did_join = False
        for i in range(3):
            try:
                chat = await app.join_chat(invite)
                did_join = True
                print(f"{chat_id}: joined {chat}")
                break
            except FloodWait as e:
                print(f"{chat_id}: join failed: got floodwait for {e.value} seconds")
                time.sleep(e.value)
                sleeper()
            except Exception as e:
                print(f"{chat_id}: join failed: {e}")
                break
        if not did_join:
            print(f"{chat_id}: could not join after 3 attempts")
        sleeper()

    print("Trying to join those that aren't missing")
    for chat_id in all_invites:
        if chat_id in missing_dialogs|joined_dialogs:
            continue
        print(f"{chat_id}: trying to join 2")
        username, invite_link = all_invites[chat_id]
        invite = username or invite_link or chat_id
        for i in range(3):
            try:
                chat = await app.join_chat(invite)
                print(f"{chat_id}: joined {chat}")
                break
            except FloodWait as e:
                print(f"{chat_id}: join 2 failed: got floodwait for {e.value} seconds")
                time.sleep(e.value)
                sleeper()
            except Exception as e:
                print(f"{chat_id}: join 2 failed: {e}")
                break
        sleeper()

    print("All done!")


if __name__ == "__main__":
    asyncio.run(main())
