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
import psycopg2

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
            "text": {"$ifNull": ["$text", "$caption"]},
            "reactions": 1,
            "views": 1,
            "forwards": 1,
            "date": 1,
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
                        "input": "$text",
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
            "reactions": 1,
            "views": 1,
            "forwards": 1,
            "date": 1,
        }
    },
    #{"$limit": 100}
]
def process_msg(result: Dict[str, Any]) -> str:
    # cid, mid, text, caption, reactions, views, forwards
    text = result.get("text", result.get("caption", None))
    if text is None:
        return None
    reactions_str = ", ".join([f"{r['emoji']}:{r['count']}" for r in result.get("reactions", {"reactions": []}).get("reactions", [])])
    forwards_str = f"FORWARDS:{result.get('forwards', 0)};" if "forwards" in result else ""
    views_str = f"VIEWS:{result.get('views', 0)};" if "views" in result else ""
    return f"<<TEXT>>{text}<<SENTIMENT>>{views_str}{forwards_str}{reactions_str}"

get_pipeline_fast = lambda chat_id: get_pipeline(chat_id)[:2]

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

print("Connecting to postgres")
import psycopg2
POSTGRES_HOST="127.0.0.1"
POSTGRES_PORT="5432"
POSTGRES_USER="user"
POSTGRES_PASSWORD="password"
POSTGRES_DB="telegram"

pgconn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    database=POSTGRES_DB,
)

print("Loading model")
model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')

print("Setting handlers and launching tracker")

def emoji_to_int(emoji: str) -> int:
    return int.from_bytes(emoji.encode("utf-32"), "little")

def int_to_emoji(i: int) -> str:
    return i.to_bytes(8, "little").decode("utf-32")

class EmojiMap:
    def __init__(self, emoji_set: Set[str|int] = None, *, path: str = "emoji_map.txt", convert: bool = True):
        self.path = path
        self.convert = convert
        self.emoji_map = dict()
        if emoji_set:
            for emoji in emoji_set:
                self._add_emoji(emoji)
        self._reload_map()
    
    def _reload_map(self):
        if not os.path.exists(self.path):
            return
        with open(self.path, "r") as f:
            i = 0
            for line in f:
                emoji = line.strip()
                if not emoji:
                    continue
                emoji = emoji.split(",")
                assert len(emoji) == 2
                is_custom, emoji = int(emoji[0]), int(emoji[1])
                if not is_custom:
                    emoji = int_to_emoji(emoji)
                self.emoji_map[emoji] = i
                i += 1

    def _save_map(self):
        with open(self.path, "w") as f:
            emoji_list = sorted(self.emoji_map.items(), key=lambda x: x[1])
            for emoji, _ in emoji_list:
                is_custom = isinstance(emoji, int)
                if not is_custom:
                    emoji = emoji_to_int(emoji)
                f.write(f"{is_custom},{emoji}\n")
    
    def _add_emoji(self, emoji: str|int):
        assert emoji not in self.emoji_map
        self.emoji_map[emoji] = len(emoji_map)
        assert isinstance(emoji, (str, int))
        is_custom = isinstance(emoji, int)
        if not is_custom:
            emoji = emoji_to_int(emoji)
        with open(path, "w+"):
            f.write(f"{is_custom},{emoji}\n")
    
    def __getitem__(self, emoji: str|int) -> int:
        if isinstance(emoji, str):
            if emoji not in emoji_map:
                self._add_emoji(emoji)
            return emoji_map[emoji]
        elif isinstance(emoji, int):
            return emoji
        else:
            raise ValueError(f"Unknown emoji type: {emoji} = {type(emoji)}")


emoji_map = EmojiMap()

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

import random
from math import ceil
hebrew_alphabet = "םןץףךאבגדהוזחטיכלמנסעפצקרשת"
def calculate_warmup_gain(batch_size: int, do_print: bool = True) -> float:
    min_reps = 128
    total_reps = max(min_reps, batch_size)
    total_reps_2 = int(ceil(total_reps / batch_size))
    total_reps_1 = total_reps_2 * batch_size
    random_strings_warmup_0 = ["".join(random.choices(hebrew_alphabet, k=20)) for _ in range(1)]
    random_strings_warmup_1 = ["".join(random.choices(hebrew_alphabet, k=20)) for _ in range(total_reps_1)]
    random_strings_warmup_2 = [["".join(random.choices(hebrew_alphabet, k=20)) for _ in range(batch_size)] for _ in range(total_reps_2)]
    if do_print:
        print("Warming up - stage 0")
    _ = model.encode(random_strings_warmup_0)
    if do_print:
        print(f"Warming up - stage 1 with {total_reps_1} strings")
    warmup_count_1 = len(random_strings_warmup_1)
    warmup_start = time.time()
    for i in range(warmup_count_1):
        _ = model.encode(random_strings_warmup_1[i])
    warmup_elapsed_1 = time.time() - warmup_start
    warmup_spm_1 = warmup_elapsed_1 / warmup_count_1
    warmup_mps_1 = warmup_count_1 / warmup_elapsed_1
    if do_print:
        print(f"Warmup stage 1 took {warmup_elapsed_1:.2f} seconds")
        print(f"Warming up - stage 2 with {total_reps_2} batches of {batch_size} strings each ({total_reps_1} strings)")
    warmup_count_2 = batch_size * total_reps_2
    warmup_start = time.time()
    for i in range(total_reps_2):
        encodings = model.encode(random_strings_warmup_2[i])
    warmup_elapsed_2 = time.time() - warmup_start
    assert len(encodings) == batch_size
    del encodings
    warmup_spm_2 = warmup_elapsed_2 / warmup_count_2
    warmup_mps_2 = warmup_count_2 / warmup_elapsed_2
    if do_print:
        print(f"Warmup stage 2 took {warmup_elapsed_2:.2f} seconds")
    warmup_parallel_gain = warmup_mps_2 / warmup_mps_1
    if do_print:
        print(f"Warmup parallel gain: {warmup_parallel_gain:.2f}")
    return warmup_parallel_gain, warmup_elapsed_2


def find_optimal_batch_size() -> int:
    print("Searching for optimal gain")
    max_warmup_parallel_gain = None
    batch_size = 1
    batch_size_left, batch_size_right = batch_size, batch_size
    max_warmup_parallel_gain = -1
    while True:
        print(f"Trying batch size {batch_size}")
        warmup_parallel_gain_cur, _ = calculate_warmup_gain(batch_size, do_print=False)
        print(f"Batch size {batch_size}: warmup gain: {warmup_parallel_gain_cur:.2f}")
        if warmup_parallel_gain_cur >= max_warmup_parallel_gain:
            batch_size_left = batch_size
            batch_size *= 2
            batch_size_right = batch_size
            max_warmup_parallel_gain = max(warmup_parallel_gain_cur, max_warmup_parallel_gain)
        else:
            break
    print(f"Searching between {batch_size_left} and {batch_size_right}")
    while batch_size_left < batch_size_right:
        batch_size = (batch_size_left + batch_size_right) // 2
        warmup_parallel_gain_cur, _ = calculate_warmup_gain(batch_size, do_print=False)
        print(f"Batch size {batch_size}: warmup gain: {warmup_parallel_gain_cur:.2f}")
        if warmup_parallel_gain_cur > max_warmup_parallel_gain:
            batch_size_left = batch_size
            max_warmup_parallel_gain = warmup_parallel_gain_cur
        else:
            batch_size_right = batch_size - 1
    print(f"Optimal batch size: {batch_size_left}")
    return batch_size_left

#print(calculate_warmup_gain(1000, do_print=True))
batch_size = 32#find_optimal_batch_size()
print("Calculating warmup_elapsed_2")
warmup_parallel_gain, warmup_elapsed_2 = calculate_warmup_gain(batch_size, do_print=False)


signal.signal(signal.SIGINT, signal_handler)


results = None
inserted_total = 0
batch_time_total = 0
spm_avg = 0
batch_i = 0
batch = [{"cid": None, "mid": None, "text": None, "embed": None} for _ in range(batch_size)]


embed_db = db["embeddings_6"]
#embed_db.create_index([("cid", 1), ("mid", 1)], unique=True)
def send_batch():
    global batch, batch_i, processed_count, processed_count_channel, inserted_total, batch_time_total, spm_avg
    t_start = time.time()
    i = batch_i
    if i == 0:
        return
    texts = [batch[j]["text"] for j in range(i)]
    #embeds = model.encode(texts)
    for j in range(i):
        #batch[j]["embed"] = embeds[j].tolist()
        reactions = batch[j].get("reactions", {"reactions": []})
        if type(reactions) is list:
            batch[j]["reactions"] = reactions
        elif type(reactions) is dict:
            batch[j]["reactions"] = [[r["emoji"], r["count"]] for r in reactions.get("reactions", [])]
        else:
            batch[j]["reactions"] = []
        batch[j]["reactions"] = [[emoji_map[r[0]], int(isinstance(r[0], int)) r[1]] for r in batch[j]["reactions"]]
    cur = pgconn.cursor()
    cur.execute("BEGIN")
    for j in range(i):
        cur.execute("""
                    INSERT INTO messages_test (chat_id, message_id, text, text_embedding, date, views, forwards, reactions) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chat_id, message_id) DO UPDATE SET text = EXCLUDED.text, text_embedding = EXCLUDED.text_embedding, date = EXCLUDED.date, views = EXCLUDED.views, forwards = EXCLUDED.forwards, reactions = EXCLUDED.reactions
                    """,
                    (batch[j]["cid"], batch[j]["mid"], batch[j]["text"], batch[j]["embed"], batch[j]["date"], batch[j]["views"], batch[j]["forwards"], batch[j]["reaction_emojis"], batch[j]["reaction_counts"]))
    cur.execute("COMMIT")
    cur.close()
    processed_count += i
    processed_count_channel += i
    batch_i = 0
    batch_time_total += time.time() - t_start
    inserted_total += i
    spm_avg = batch_time_total / inserted_total
    left_channel = (count_messages_channel - processed_count_channel) * spm_avg
    left_total = (count_messages - processed_count) * spm_avg
    print(f"{cid}: {count_messages} > {count_messages_channel} > {processed_count_channel}; Rate: {spm_avg:.2f} spm; Left: cha={pretty_time(left_channel)}, tot={pretty_time(left_total)}")


print("Starting to embed")
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
    results = db["messages"].aggregate(get_pipeline_fast(cid))
    print(f"{cid}: Going over messages")
    processed_count_channel = 0
    batch_i = 0
    for result in results:
        if break_signal:
            break
        batch[batch_i] |= result
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
