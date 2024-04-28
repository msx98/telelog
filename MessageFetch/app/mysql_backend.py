from typing import Tuple, Dict, Any, Optional, List
from collections import OrderedDict
from pyrogram.types import Message, User, Chat
from pyrogram.enums import ChatType, MessageMediaType
import os
import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector import connection as MySQLConnection
from base_backend import BaseBackendWithQueue, clean_dict, StoredDialog, chat_type_dict, media_type_dict
import threading
import queue
import time
import datetime
from consts import *


def normalize_message(message: Message) -> Optional[Dict[str, Optional[str|int]]]:
    if message.outgoing:
        return None
    message_dict = OrderedDict()
    if hasattr(message, "text") and message.text is not None:
        message_dict["text"] = message.text
    elif hasattr(message, "caption") and message.caption is not None:
        message_dict["text"] = message.caption
    elif hasattr(message, "poll") and hasattr(message.poll, "question") and message.poll.question is not None:
        return None # FIXME - not implemented yet
    else:
        return None
    if hasattr(message, "media"):
        message_dict["media_type"] = media_type_dict.get(message.media, 0) if message.media is not None else None
    message_dict["date"] = message.date
    if message.chat:
        message_dict["chat_id"] = message.chat.id
        message_dict["chat_name"] = message.chat.title
    message_dict["message_id"] = message.id
    if message.from_user:
        message_dict["sender_type"] = 0
        message_dict["sender_id"] = message.from_user.id
        message_dict["sender_first_name"] = message.from_user.first_name
        message_dict["sender_last_name"] = message.from_user.last_name
        message_dict["sender_username"] = message.from_user.username
        message_dict["sender_is_verified"] = message.from_user.is_premium
        message_dict["sender_is_scam"] = message.from_user.is_scam or message.from_user.is_fake or message.from_user.is_deleted
        message_dict["sender_is_restricted"] = message.from_user.is_restricted
    elif message.sender_chat:
        message_dict["sender_type"] = chat_type_dict[message.sender_chat.type]
        message_dict["sender_id"] = message.sender_chat.id
        message_dict["sender_first_name"] = message.sender_chat.title
        message_dict["sender_username"] = message.sender_chat.username
        message_dict["sender_is_verified"] = message.sender_chat.is_verified
        message_dict["sender_is_scam"] = message.sender_chat.is_scam or message.sender_chat.is_fake
        message_dict["sender_is_restricted"] = message.sender_chat.is_restricted
    if message.reply_to_message:
        if message.reply_to_message.chat:
            message_dict["reply_to_chat_id"] = message.reply_to_message.chat.id
        message_dict["reply_to_message_id"] = message.reply_to_message_id
        message_dict["reply_to_top_message_id"] = message.reply_to_top_message_id
    if message.forward_from_chat:
        message_dict["forward_chat_id"] = message.forward_from_chat.id
        message_dict["forward_message_id"] = message.forward_from_message_id
        message_dict["forward_date"] = message.forward_date
    # FIXME - do we also need to check for forward_from_user or something?
    if message.reactions and message.reactions.reactions:
        message_dict["reactions"] = ",".join([f"{r.emoji}:{r.count}" for r in message.reactions.reactions])
    message_dict["views"] = message.views
    message_dict["forwards"] = message.forwards
    # text, date, chat_id, chat_name, message_id, sender_id, sender_first_name, sender_last_name, sender_username, reply_to_chat_id, reply_to_message_id, reply_to_top_message_id, forward_chat_id, forward_message_id, forward_date, reactions, views, forwards
    return message_dict


def normalize_chat(chat: Chat) -> Dict[str, Optional[str|int]]:
    chat_dict = OrderedDict()
    chat_dict["id"] = chat.id
    chat_dict["title"] = chat.title
    chat_dict["chat_type"] = chat_type_dict[chat.type]
    chat_dict["chat_username"] = chat.username
    chat_dict["chat_description"] = chat.description
    chat_dict["chat_members_count"] = getattr(chat, "members_count", None)
    chat_dict["chat_invite_link"] = getattr(chat, "invite_link", None)
    chat_dict["chat_is_verified"] = chat.is_verified
    chat_dict["chat_is_scam"] = chat.is_scam
    chat_dict["chat_is_support"] = chat.is_support
    return chat_dict


class MessageQueue:
    def __init__(self, db, max_queue_size):
        self.db = db
        self.max_queue_size = max_queue_size
        self.queue = queue.Queue()
        self.lock = db.lock
        self.last_push = time.time()
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def add_message(self, message):
        #print(f"Trying to add message to queue")
        with self.lock:
            self.queue.put(message)
        #print(f"Added message to queue")

    def run(self):
        while True:
            if self.queue.qsize() >= self.max_queue_size or time.time() - self.last_push > 5:
                #print(f"Reached condition - gonna wait for lock")
                with self.lock:
                    messages = [self.queue.get() for _ in range(self.queue.qsize())]
                    #print(f"Pushing {len(messages)} messages to DB")
                    self.db._add_messages(messages)
                    self.last_push = time.time()
            time.sleep(1)


class MySQLBackend(BaseBackendWithQueue):
    conn: MySQLConnection = None

    def __init__(
        self,
        *,
        host = None,
        port = None,
        user = None,
        password = None,
        database = None,
        **kwargs,
    ):
        self._conn = mysql.connector.connect(host = host or MYSQL_HOST,
                                            database = database or MYSQL_DATABASE,
                                            user = user or MYSQL_USER,
                                            password = password or MYSQL_PASSWORD,
                                            port = port or MYSQL_PORT,)
        super().__init__(**kwargs)

    def _add_messages(self, messages):
        #print(f"Inserting {len(messages)} messages")
        cur = self._conn.cursor()
        for message in messages:
            message_norm = normalize_message(message)
            if not message_norm:
                continue
            message_norm_keys, message_norm_vals = list(zip(*message_norm.items()))
            message_norm_keys = ",".join(message_norm_keys)
            message_norm_vals_almost = ",".join("%s" for _ in message_norm_vals)
            update_str = ", ".join([f"{k}=VALUES({k})" for k in message_norm.keys()])
            cur.execute(f"INSERT INTO messages ({message_norm_keys}) VALUES ({message_norm_vals_almost}) ON DUPLICATE KEY UPDATE {update_str}", message_norm_vals)
        self._conn.commit()
        cur.close()
        print(f"Inserted {len(messages)} messages")
    
    def count_messages(self) -> Tuple[int, datetime.datetime]:
        with self.lock:
            cur = self._conn.cursor()
            cur.execute("SELECT COUNT(*), MAX(date) FROM messages")
            n, date = cur.fetchone()
            cur.close()
            return n, date
