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
from transformers.utils.import_utils import is_nltk_available
from sentence_transformers import SentenceTransformer
import time
import threading
import signal

print("Defining environment")

NullFormatter = ticker.NullFormatter
get_pipeline = lambda chat_id: [
    {
        "$match": {
            "chat.id": chat_id,
            "$or": [
                {"text": {"$ne": None}},
                {"caption": {"$ne": None}},
            ],
            "reply_to_message_id": None,
        }
    },
    {
        "$project": {
            "cid": "$chat.id",
            "mid": "$id",
            "text": 1,
            "caption": 1,
            "reactions": 1,
            #"views": 1,
            "forwards": 1,
        }
    },
    {
        "$addFields": {
            "reactions_string": {
                "$reduce": {
                    "input": {
                        "$map": {
                            "input": "$reactions.reactions",
                            "as": "reaction",
                            "in": {
                                "$concat": [
                                    "$$reaction.emoji",
                                    ":",
                                    {"$toString": "$$reaction.count"},
                                    ", "
                                ]
                            }
                        }
                    },
                    "initialValue": "",
                    "in": {
                        "$concat": ["$$value", "$$this"]
                    }
                }
            }
        }
    },
    {
        "$addFields": {
            "text_with_reactions": {
                "$concat": [
                    "<<TEXT>>",
                    {"$replaceAll": {
                        "input": {"$ifNull": ["$text", "$caption"]},
                        "find": "\n\nכדי להגיב לכתבה לחצו כאן",
                        "replacement": "",
                    }},
                    "<<REACTIONS>>",
                    {"$rtrim": {"input": "$reactions_string", "chars": ", "}},
                    "<<FORWARDS>>",
                    {"$toString": "$forwards"},
                ]
            }
        }
    },
    {
        "$project": {
            "cid": 1,
            "mid": 1,
            "text": "$text_with_reactions",
        }
    },
    #{"$limit": 100}
]
MONGO_HOST="mongodb://127.0.0.1:27017/"
MONGO_INITDB_ROOT_USERNAME="root"
MONGO_INITDB_ROOT_PASSWORD="example"
MONGO_DATABASE="messages_new"

print("Connecting to Mongo")

conn = MongoClient(MONGO_HOST, username=MONGO_INITDB_ROOT_USERNAME, password=MONGO_INITDB_ROOT_PASSWORD)
db = conn[MONGO_DATABASE]
messages = db["messages"]

print("Finding channels")

cursor = db["dialogs"].find({})#"chat.id": {"$in": hebrew_channel_ids}})
top_channels = list(cursor)
top_channels.sort(key=lambda x: (x["top_message"]["id"]), reverse=True)

print(f"Found {len(top_channels)} channels")

count_messages = sum(chat["top_message"]["id"] for chat in top_channels if "top_message" in chat)
print(f"Total messages: {count_messages}")

print("Loading model")
model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')

print("Setting handlers and launching tracker")

start_time = time.time()
processed_count = 0
processed_count_channel = 0
top_message_count = 0
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

week_seconds = 60 * 60 * 24 * 7
day_seconds = 60 * 60 * 24
hour_seconds = 60 * 60
minute_seconds = 60

def pretty_time(seconds: float) -> str:
    # (Weeks: {weeks}, )?(Days: {days}, )?)(Hours: {hours}, )?(Minutes: {minutes}, )?(Seconds: seconds)?
    weeks = int(seconds // week_seconds)
    seconds -= weeks * week_seconds
    days = int(seconds // day_seconds)
    seconds -= days * day_seconds
    hours = int(seconds // hour_seconds)
    seconds -= hours * hour_seconds
    minutes = int(seconds // minute_seconds)
    seconds -= minutes * minute_seconds
    s = ""
    if weeks > 0:
        s += f"{weeks}w, "
    if s or (days > 0):
        s += f"{days}d, "
    if s or (hours > 0):
        s += f"{hours}h, "
    if s or (minutes > 0):
        s += f"{minutes}m, "
    s += f"{seconds:.2f}s"
    return s

def recalc_rate():
    global is_running, processed_count_channel, processed_count, count_messages, count_messages_channel
    current_time = time.time()
    last_count = processed_count
    last_recalc = current_time
    RECALC_EVERY_SECS = 3
    next_recalc = current_time + RECALC_EVERY_SECS
    while is_running:
        current_time = time.time()
        if current_time >= next_recalc:
            next_recalc = current_time + RECALC_EVERY_SECS
            new_count = processed_count - last_count
            elapsed_intermediate = (current_time - last_recalc)
            #rate_mps = new_count / elapsed_intermediate
            if new_count == 0:
                print(f"{cid}: Rate > {elapsed_intermediate:.2f} spm")
                continue
            #assert new_count > 0
            rate_spm = elapsed_intermediate / new_count
            last_count = processed_count
            last_recalc = current_time
            left_channel = (count_messages_channel - processed_count_channel) * rate_spm
            left_total = (count_messages - processed_count) * rate_spm
            print(f"{cid}: {count_messages} > {count_messages_channel} > {processed_count_channel}; Rate: {rate_spm:.2f} spm; Left: cha={left_channel:.2f}, tot={left_total:.2f}")
        time.sleep(0.1)


t = threading.Thread(target=recalc_rate)

batch_i = 0
batch_size = 1000
batch = [{"cid": None, "mid": None, "text": None, "embed": None} for _ in range(batch_size)]
current_msgs = set()

embed_db = db["embeddings_5"]
#embed_db.create_index([("cid", 1), ("mid", 1)], unique=True)
def send_batch():
    global batch, batch_i, processed_count, processed_count_channel
    i = batch_i
    if i == 0:
        return
    texts = [batch[j]["text"] for j in range(i)]
    embeds = model.encode(texts)
    for j in range(i):
        batch[j]["embed"] = embeds[j].tolist()
    db["embeddings_5"].insert_many(batch[:i])
    processed_count += i
    processed_count_channel += i
    batch_i = 0


print("Warming up - stage 1")
warmup_start = time.time()
warmup_count_1 = 10
for i in range(warmup_count_1):
    _ = model.encode(["בוקר טוב עולם, מה שלום כולם?"])
warmup_elapsed_1 = time.time() - warmup_start
warmup_spm_1 = warmup_elapsed_1 / warmup_count_1
print(f"Warmup stage 1 took {warmup_elapsed_1:.2f} seconds")
print("Warming up - stage 2")
warmup_start = time.time()
warmup_count_2 = batch_size
_ = model.encode(["בוקר טוב עולם, מה שלום כולם?"]*warmup_count_2)
warmup_elapsed_2 = time.time() - warmup_start
warmup_spm_2 = warmup_elapsed_2 / warmup_count_2
print(f"Warmup stage 2 took {warmup_elapsed_2:.2f} seconds")
warmup_parallel_gain = warmup_spm_2 / warmup_spm_1
print(f"Warmup parallel gain: {warmup_parallel_gain:.2f}")


print("Starting to embed")
t.start()
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
    results = db["messages"].aggregate(get_pipeline(cid))
    print(f"{cid}: Going over messages")
    processed_count_channel = 0
    batch_i = 0
    current_msgs.clear()
    for result in results:
        if break_signal:
            break
        if result["mid"] in current_msgs:
            pass#continue
        current_msgs.add(result["mid"])
        batch[batch_i]["cid"] = result["cid"]
        batch[batch_i]["mid"] = result["mid"]
        batch[batch_i]["text"] = result["text"]
        batch_i += 1
        if batch_i == batch_size:
            send_batch()
    if batch_i > 0:
        send_batch()
    top_message_count += top_message_id
    processed_count = top_message_count

is_running = False

print("ALL DONE")

print("Joining thread")
t.join()

# FIXME:
# this is an intermediate solution
# eventually we want:
# - a good encoder that we finetuned on this kind of data
# - a separate encoder for the sentiment (reactions & forwards)
# - and a model to unify them into a single representation
# also, there is no date, and this is terrible!
# date can be multidimensional because different "stories" have different meanings for different dates
# how do we fix this?

break_signal = False
print("Exiting")
