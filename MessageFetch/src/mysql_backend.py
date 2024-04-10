from typing import Tuple, Dict, Any, Optional, List
from collections import OrderedDict
import pyrogram.types
from pyrogram.types import Message, User, Chat
from pyrogram.enums import ChatType
import os
import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector import connection as MySQLConnection
import threading
import queue
import time


chat_type_dict = {
    ChatType.CHANNEL: 1,
    ChatType.GROUP: 2,
    ChatType.SUPERGROUP: 3,
    ChatType.PRIVATE: 4,
    ChatType.BOT: 5,
}


def normalize_message(message: Message) -> Optional[Dict[str, Optional[str|int]]]:
    if message.text is None or message.outgoing:
        return None
    message_dict = OrderedDict()
    message_dict["text"] = message.text
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
    def __init__(self, db):
        self.db = db
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
            if self.queue.qsize() >= 10:
                #print(f"Reached condition - gonna wait for lock")
                with self.lock:
                    messages = [self.queue.get() for _ in range(self.queue.qsize())]
                    #print(f"Pushing {len(messages)} messages to DB")
                    self.db._add_messages(messages)
                    self.last_push = time.time()
            time.sleep(1)


class MySQLBackend:
    conn: MySQLConnection = None

    def __init__(
        self,
        *,
        MYSQL_HOST = os.environ["MYSQL_HOST"],
        MYSQL_PORT = os.environ["MYSQL_PORT"],
        MYSQL_USER = os.environ["MYSQL_USER"],
        MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"],
        MYSQL_DATABASE = os.environ["MYSQL_DATABASE"],
    ):
        self.conn = mysql.connector.connect(host=MYSQL_HOST,
                                            database=MYSQL_DATABASE,
                                            user=MYSQL_USER,
                                            password=MYSQL_PASSWORD,
                                            port=MYSQL_PORT,)
        self.lock = threading.Lock()
        if not self.conn.is_connected():
            raise Exception("Could not connect")
        self.message_queue = MessageQueue(self)
        print(f"Connected to MySQL at {MYSQL_HOST}:{MYSQL_PORT}")

    def add_message(self, message: Message):
        self.message_queue.add_message(message)

    def _add_messages(self, messages):
        #print(f"Inserting {len(messages)} messages")
        cur = self.conn.cursor()
        for message in messages:
            message_norm = normalize_message(message)
            if not message_norm:
                continue
            message_norm_keys, message_norm_vals = list(zip(*message_norm.items()))
            message_norm_keys = ",".join(message_norm_keys)
            message_norm_vals_almost = ",".join("%s" for _ in message_norm_vals)
            cur.execute(f"INSERT INTO messages ({message_norm_keys}) VALUES ({message_norm_vals_almost})", message_norm_vals)
        self.conn.commit()
        cur.close()
        print(f"Inserted {len(messages)} messages")
    
    def sample_query(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM messages LIMIT 1")
        print(cur.fetchone())
        cur.close()
    
    def get_historic_chats(self) -> List[int]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT DISTINCT chat_id FROM messages 
            UNION 
            SELECT DISTINCT sender_id AS chat_id FROM messages WHERE sender_type <> 0
        """)
        chat_ids = [r[0] for r in cur.fetchall()]
        cur.close()
        return chat_ids
    
    def refresh_chats(self, chat_info: Dict[int, Chat]):
        cur = self.conn.cursor()
        for chat_id, chat in chat_info.items():
            chat_type = chat_type_dict[chat.type]
            cur.execute("""
                        INSERT INTO chats (chat_id, chat_name, chat_type) VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE chat_name=%s, chat_type=%s""", (chat_id, chat.title, chat_type, chat.title, chat_type))
        self.conn.commit()
        cur.close()
    
    def count_messages(self) -> int:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM messages")
            n = cur.fetchone()[0]
            cur.close()
            return n
