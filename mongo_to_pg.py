#!/usr/bin/env python3

print("Launched, importing")

from typing import Tuple, Dict, Any, Optional, List
from collections import OrderedDict
import pyrogram.types
from pyrogram.types import Message, User, Chat, MessageReactions, Reaction
from pyrogram.enums import ChatType, MessageMediaType
import os
import pyrogram
from pymongo import MongoClient
from enum import Enum
import datetime
import pandas as pd
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import time
import threading
import signal
import psycopg2
from emoji_map import EmojiMap
from MessageFetch.app.consts import *
from utils import *

print("Defining environment")

get_pipeline = lambda chat_id: [
    {
        "$match": {
            "chat.id": chat_id,
            "$or": [
                {"text": {"$ne": None}},
                {"caption": {"$ne": None}},
            ],
        }
    },
    {
        "$project": {
            "cid": "$chat.id",
            "mid": "$id",
            "text": {"$ifNull": ["$text", "$caption"]},
            "reactions": 1,
            "views": 1,
            "forwards": 1,
            "date": 1,
            "forward_from_chat_id": {"$ifNull": ["$forward_from_chat.id", None]},
            "forward_from_message_id": {"$ifNull": ["$forward_from_message_id", None]},
            "reply_to_message_id": 1,
        }
    },
]

print("Connecting to Mongo")
mongoconn = MongoClient(MONGO_HOST, username=MONGO_INITDB_ROOT_USERNAME, password=MONGO_INITDB_ROOT_PASSWORD)
mongodb = mongoconn["messages_new"]

print("Connecting to postgres")
pgconn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    database=POSTGRES_DB,
)
message_table = "messages"

print("Finding channels")
cursor = mongodb["dialogs"].find({})
top_channels = list(cursor)
top_channels.sort(key=lambda x: (x["top_message"]["id"]), reverse=True)
top_message_id = {channel["chat"]["id"]: channel["top_message"]["id"] for channel in top_channels}
count_messages = sum(chat["top_message"]["id"] for chat in top_channels if "top_message" in chat)
print(f"Found {len(top_channels)} channels with <= {count_messages}")

print("Setting handlers and launching tracker")

start_time = time.time()
processed_count = 0
processed_count_channel = 0
top_message_sum = 0
count_messages_channel = 0
is_running = True
break_signal = False

def signal_handler(sig, frame):
    global break_signal
    if break_signal:
        print("Exiting forcefully if necessary - waiting")
        time.sleep(1)
        if break_signal:
            print("Exiting forcefully")
            exit(1)
        else:
            print("Exiting peacefully (2)")
    else:
        break_signal = True
        print("Exiting peacefully (1)")


signal.signal(signal.SIGINT, signal_handler)


def normalize_reactions(mongo_message: Dict):
    reactions = mongo_message.get("reactions", {"reactions": []})
    if type(reactions) is list:
        mongo_message["reactions"] = reactions
    elif type(reactions) is dict:
        reactions = reactions.get("reactions", [])
        mongo_message["reactions"] = []
        for r in reactions:
            if "emoji" in r:
                mongo_message["reactions"].append([r["emoji"], r["count"]])
            elif "custom_emoji_id" in r:
                mongo_message["reactions"].append([r["custom_emoji_id"], r["count"]])
            else:
                raise Exception(f"Unknown reaction type: {r} in {reactions}")
    else:
        mongo_message["reactions"] = []
    mongo_message["reactions"] = [[EmojiMap.to_int(r[0]), r[1]] for r in mongo_message["reactions"]]


def normalize_media(mongo_message: Dict):
    # {'document', 'audio', 'voice', 'video', 'sticker', 'animation', 'new_chat_photo', 'video_note', 'photo'}
    file_id: str = None
    media_type: str = None
    if "document" in mongo_message:
        file_id = mongo_message["document"]["file_id"]
        media_type = "document"
    elif "audio" in mongo_message:
        file_id = mongo_message["audio"]["file_id"]
        media_type = "audio"
    elif "voice" in mongo_message:
        file_id = mongo_message["voice"]["file_id"]
        media_type = "voice"
    elif "video" in mongo_message:
        file_id = mongo_message["video"]["file_id"]
        media_type = "video"
    elif "sticker" in mongo_message:
        file_id = mongo_message["sticker"]["file_id"]
        media_type = "sticker"
    elif "animation" in mongo_message:
        file_id = mongo_message["animation"]["file_id"]
        media_type = "animation"
    elif "video_note" in mongo_message:
        file_id = mongo_message["video_note"]["file_id"]
        media_type = "video_note"
    elif "photo" in mongo_message:
        file_id = mongo_message["photo"]["file_id"]
        media_type = "photo"
    elif "new_chat_photo" in mongo_message:
        file_id = mongo_message["new_chat_photo"]["file_id"]
        media_type = "new_chat_photo"
    mongo_message["media_type"] = media_type
    mongo_message["file_id"] = file_id


results = None
inserted_total = 0
batch_time_total = 0
spm_avg = 0
batch_i = 0
batch_size = 1024
batch = [dict() for _ in range(batch_size)]

def add_query_reactions(cur, msg_dict):
    for r in msg_dict["reactions"]:
        r_norm = r[1] / msg_dict["reactions_vote_count"] if msg_dict["reactions_vote_count"] else 0
        cur.execute(f"""
                    INSERT INTO reactions (chat_id, message_id, reaction_id, reaction_votes_norm, reaction_votes_abs) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (chat_id, message_id, reaction_id) DO UPDATE SET reaction_votes_norm = EXCLUDED.reaction_votes_norm, reaction_votes_abs = EXCLUDED.reaction_votes_abs
                    """,
                    (msg_dict["cid"], msg_dict["mid"], r[0], r_norm, r[1]))


def add_query_poll(cur, msg_dict):
    if msg_dict.get("poll", None) is None:
        return
    for l,p in enumerate(msg_dict["poll_options"]):
        p_norm = (p["voter_count"] / msg_dict["poll_vote_count"]) if msg_dict["poll_vote_count"] else 0
        cur.execute(f"""
                    INSERT INTO polls (chat_id, message_id, poll_option_id, poll_option_text, poll_option_votes_norm, poll_option_votes_abs) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chat_id, message_id, poll_option_id) DO UPDATE SET poll_option_votes_norm = EXCLUDED.poll_option_votes_norm, poll_option_votes_abs = EXCLUDED.poll_option_votes_abs
                    """,
                    (msg_dict["cid"], msg_dict["mid"], l, p["text"], p_norm, p["voter_count"]))

def add_query_message(cur, msg_dict):
    cur.execute(f"""
                INSERT INTO {message_table} (chat_id, message_id, text, date, views, forwards, reactions_vote_count, poll_vote_count, forward_from_chat_id, forward_from_message_id, reply_to_message_id, media_type, file_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chat_id, message_id) DO UPDATE SET text = EXCLUDED.text, date = EXCLUDED.date, views = EXCLUDED.views, forwards = EXCLUDED.forwards, reactions_vote_count = EXCLUDED.reactions_vote_count, poll_vote_count = EXCLUDED.poll_vote_count, file_id = EXCLUDED.file_id, media_type = EXCLUDED.media_type
                """,
                (msg_dict["cid"], msg_dict["mid"], msg_dict["text"], msg_dict["date"], msg_dict["views"], msg_dict["forwards"], msg_dict["reactions_vote_count"], msg_dict["poll_vote_count"], msg_dict["forward_from_chat_id"], msg_dict["forward_from_message_id"], msg_dict["reply_to_message_id"], msg_dict["media_type"], msg_dict["file_id"]))


def add_query(cur, msg_dict):
    add_query_message(cur, msg_dict)
    add_query_reactions(cur, msg_dict)
    add_query_poll(cur, msg_dict)


def send_batch():
    global batch, batch_i, processed_count, processed_count_channel, inserted_total, batch_time_total, spm_avg
    t_start = time.time()
    i = batch_i
    if i == 0:
        return
    cur = pgconn.cursor()
    cur.execute("BEGIN")
    for j in range(i):
        add_query(cur, batch[j])
    cur.execute("COMMIT")
    cur.close()
    processed_count += i
    processed_count_channel += i
    batch_i = 0
    batch_time_total += time.time() - t_start
    inserted_total += i
    spm_avg = batch_time_total / inserted_total
    mps_avg = inserted_total / batch_time_total
    left_channel = (count_messages_channel - processed_count_channel) * spm_avg
    left_total = (count_messages - processed_count) * spm_avg
    print(f"{cid}: {count_messages} > {count_messages_channel} > {processed_count_channel}; Rate: {mps_avg:.2f} mps; Left: cha={pretty_time(left_channel)}, tot={pretty_time(left_total)}")


print(f"Deleting {message_table} from postgres if neccessary")
cur = pgconn.cursor()
if False:
    cur.execute("SELECT EXISTS(SELECT * FROM pg_extension WHERE extname = 'vector');")
    if not cur.fetchone()[0]:
        cur.execute("CREATE EXTENSION vector;")
if False:
    cur.execute(f"DROP INDEX IF EXISTS idx_chat")
    cur.execute(f"DROP INDEX IF EXISTS idx_date")
    cur.execute(f"DROP INDEX IF EXISTS idx_reaction")
    cur.execute(f"DROP TABLE IF EXISTS polls")
    cur.execute(f"DROP TABLE IF EXISTS reactions")
    cur.execute(f"DROP TABLE IF EXISTS {message_table}")
    cur.execute(f"DROP TABLE IF EXISTS chats")
    cur.execute(f"DROP TYPE IF EXISTS chat_type")
    cur.execute(f"CREATE TYPE chat_type AS ENUM ('private', 'bot', 'group', 'supergroup', 'channel')")
    cur.execute(f"""
            CREATE TABLE IF NOT EXISTS chats (
                chat_id BIGINT NOT NULL,
                top_message_id BIGINT,
                title TEXT,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                invite_link TEXT,
                type chat_type,
                members_count INTEGER CHECK (members_count >= 0),
                is_verified BOOLEAN,
                is_restricted BOOLEAN,
                is_scam BOOLEAN,
                is_fake BOOLEAN,
                is_support BOOLEAN,
                linked_chat_id BIGINT,
                PRIMARY KEY (chat_id)
            );
            """)
    cur.execute(f"""
                CREATE TYPE IF NOT EXISTS media_type AS ENUM (
                    'document', 
                    'audio', 
                    'voice', 
                    'video', 
                    'sticker', 
                    'animation', 
                    'new_chat_photo', 
                    'video_note', 
                    'photo'
                );""")
    cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {message_table} (
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                sender_id BIGINT,
                text TEXT,
                embedding vector(768),
                date TIMESTAMP,
                views INTEGER CHECK (views >= 0),
                forwards INTEGER CHECK (forwards >= 0),
                forward_from_chat_id BIGINT,
                forward_from_message_id BIGINT,
                reply_to_message_id BIGINT,
                poll_vote_count INTEGER CHECK (poll_vote_count >= 0),
                reactions_vote_count INTEGER CHECK (reactions_vote_count >= 0),
                media_type media_type,
                file_id TEXT,
                PRIMARY KEY (chat_id, message_id)
            );-- PARTITION BY RANGE (chat_id)
            """)
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_chat ON {message_table} (chat_id)")
    cur.execute(f"""
    CREATE INDEX IF NOT EXISTS idx_date ON {message_table} (
        (date_part('year', date)),
        (date_part('month', date)),
        (date_part('day', date))
    )
    """)
    cur.execute(f"""
            CREATE TABLE IF NOT EXISTS reactions (
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                reaction_id INTEGER,
                reaction_votes_norm REAL CHECK (reaction_votes_norm >= 0),
                reaction_votes_abs INTEGER CHECK (reaction_votes_abs >= 0),
                PRIMARY KEY (chat_id, message_id, reaction_id)
            );-- PARTITION BY RANGE (chat_id);
            ALTER TABLE reactions ADD CONSTRAINT fk_reactions_message FOREIGN KEY (chat_id, message_id) REFERENCES {message_table} (chat_id, message_id);
            """)
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_reaction ON reactions (reaction_id)")
    cur.execute(f"""
            CREATE TABLE IF NOT EXISTS polls (
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                poll_option_id INTEGER,
                poll_option_text TEXT,
                poll_option_votes_norm REAL CHECK (poll_option_votes_norm >= 0),
                poll_option_votes_abs INTEGER CHECK (poll_option_votes_norm >= 0),
                poll_option_text_embedding REAL[],
                PRIMARY KEY (chat_id, message_id, poll_option_id)
            );-- PARTITION BY RANGE (chat_id);
            ALTER TABLE polls ADD CONSTRAINT fk_polls_message FOREIGN KEY (chat_id, message_id) REFERENCES {message_table} (chat_id, message_id);
""")
cur.close()

results = mongodb["messages"].find({})
cid = None
print("Going over messages")
first_iteration = True
for result in results:
    if break_signal:
        break
    cid, mid = result["chat"]["id"], result["id"]
    if mid > top_message_id[cid]:
        continue
    batch[batch_i]["cid"] = cid
    batch[batch_i]["mid"] = mid
    batch[batch_i]["text"] = result.get("text", result.get("caption", result.get("poll", {}).get("question", None)))
    batch[batch_i]["reactions"] = result.get("reactions", dict())
    batch[batch_i]["views"] = result.get("views", 0)
    batch[batch_i]["forwards"] = result.get("forwards", 0)
    batch[batch_i]["date"] = result["date"]
    batch[batch_i]["forward_from_chat_id"] = result.get("forward_from_chat", {}).get("id", None)
    batch[batch_i]["forward_from_message_id"] = result.get("forward_from_message_id", None)
    batch[batch_i]["reply_to_message_id"] = result.get("reply_to_message_id", None)
    batch[batch_i]["poll_options"] = result.get("poll", {}).get("options", None)
    batch[batch_i]["poll_vote_count"] = sum(p["voter_count"] for p in batch[batch_i]["poll_options"]) if batch[batch_i]["poll_options"] is not None else None
    normalize_reactions(batch[batch_i])
    batch[batch_i]["reactions_vote_count"] = sum(r[1] for r in batch[batch_i]["reactions"])
    normalize_media(batch[batch_i])
    batch_i += 1
    if batch_i == batch_size:
        send_batch()
if batch_i:
    send_batch()


print("SEE TOP MESSAGES:")
print(top_message_id)

print("WRITING DIALOGS")

for chat_id in top_message_id:
    dialog = mongodb["dialogs"].find_one({"chat.id": chat_id})["chat"]
    channel = {
        "title": dialog.get("title", None),
        "top_message_id": top_message_id[chat_id],
        "first_name": None,
        "last_name": None,
        "username": dialog.get("username", None),
        "invite_link": None, # FIXME - need to fetch
        "type": dialog.get("type", None).lower() if dialog.get("type", None) is not None else None,
        "members_count": dialog.get("members_count", None),
        "is_verified": dialog.get("is_verified", None),
        "is_restricted": dialog.get("is_restricted", None),
        "is_scam": dialog.get("is_scam", None),
        "is_fake": dialog.get("is_fake", None),
        "is_support": dialog.get("is_support", None),
        "linked_chat_id": None, # FIXME - need to fetch
    }
    cur = pgconn.cursor()
    cur.execute(f"""
                INSERT INTO chats (chat_id, top_message_id, title, first_name, last_name, username, invite_link, type, members_count, is_verified, is_restricted, is_scam, is_fake, is_support, linked_chat_id) VALUES (%
                ON CONFLICT (chat_id) DO UPDATE SET top_message_id = EXCLUDED.top_message_id, title = EXCLUDED.title, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, username = EXCLUDED.username, invite_link = EXCLUDED.invite_link, type = EXCLUDED.type, members_count = EXCLUDED.members_count, is_verified = EXCLUDED.is_verified, is_restricted = EXCLUDED.is_restricted, is_scam = EXCLUDED.is_scam, is_fake = EXCLUDED.is_fake, is_support = EXCLUDED.is_support, linked_chat_id = EXCLUDED.linked_chat_id
                """,
                (chat_id, channel["top_message_id"], channel["title"], channel["first_name"], channel["last_name"], channel["username"], channel["invite_link"], channel["type"], channel["members_count"], channel["is_verified"], channel["is_restricted"], channel["is_scam"], channel["is_fake"], channel["is_support"], channel["linked_chat_id"]))
    pgconn.commit()
    cur.close()

print("COMMITTED TO EVERYTHING")

break_signal = False
exit(0)


print("Starting to work")
for chat in top_channels:
    if break_signal:
        break
    if "top_message" not in chat:
        continue
    cid = chat["chat"]["id"]
    top_message_id = chat["top_message"]["id"]
    count_messages_channel = top_message_id
    channel_name = chat["chat"]["title"]
    print(f"{cid}: Working on {channel_name}")
    results = mongodb["messages"].aggregate(get_pipeline(cid))
    print(f"{cid}: Going over messages")
    processed_count_channel = 0
    batch_i = 0
    for result in results:
        if break_signal:
            break
        batch[batch_i] = result
        batch_i += 1
        if batch_i == batch_size:
            send_batch()
    if batch_i > 0:
        send_batch()
    top_message_sum += top_message_id
    processed_count = top_message_sum

break_signal = False

print("ALL DONE")
