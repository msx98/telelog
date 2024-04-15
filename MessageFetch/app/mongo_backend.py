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


chat_type_dict = {
    ChatType.CHANNEL: 1,
    ChatType.GROUP: 2,
    ChatType.SUPERGROUP: 3,
    ChatType.PRIVATE: 4,
    ChatType.BOT: 5,
}


media_type_dict = {
    MessageMediaType.AUDIO: 1,
    MessageMediaType.DOCUMENT: 2,
    MessageMediaType.PHOTO: 3,
    MessageMediaType.STICKER: 4,
    MessageMediaType.VIDEO: 5,
    MessageMediaType.ANIMATION: 6,
    MessageMediaType.VOICE: 7,
    MessageMediaType.VIDEO_NOTE: 8,
    MessageMediaType.CONTACT: 9,
    MessageMediaType.LOCATION: 10,
    MessageMediaType.VENUE: 11,
    MessageMediaType.POLL: 12,
    MessageMediaType.WEB_PAGE: 13,
    MessageMediaType.DICE: 14,
    MessageMediaType.GAME: 15,
}


primitive_types = (bool, int, str, float, datetime.datetime, type(None))
unparseable_types = (pyrogram.client.Client,)
def clean_dict(d_old, rec_level=1, max_rec=20) -> Dict:
    if isinstance(d_old, unparseable_types):
        return None
    if rec_level > max_rec:
        raise Exception()
    #print(f"Working on {type(d_old)}, {d_old}")
    if isinstance(d_old, primitive_types):
        return d_old
    elif isinstance(d_old, Enum):
        return d_old.name
    elif isinstance(d_old, list):
        return [clean_dict(x,rec_level+1,max_rec) for x in d_old]
    elif isinstance(d_old, dict):
        d = dict()
        for k, v in d_old.items():
            v_new = clean_dict(v,rec_level+1,max_rec)
            if v_new is not None:
                d[k] = v_new
        return d
    elif hasattr(d_old, "__dict__"):
        return clean_dict(d_old.__dict__,rec_level+1,max_rec)
    else:
        return None


def normalize_user(from_user: User):
    return {
        "sender_type": 0,
        "sender_id": from_user.id,
        "sender_first_name": from_user.first_name,
        "sender_last_name": from_user.last_name,
        "sender_username": from_user.username,
        "sender_is_verified": from_user.is_premium,
        "sender_is_scam": from_user.is_scam or from_user.is_fake or from_user.is_deleted,
        "sender_is_restricted": from_user.is_restricted
    }


def normalize_chat(sender_chat: Chat):
    return {
        "sender_type": chat_type_dict[sender_chat.type],
        "sender_id": sender_chat.id,
        "sender_first_name": sender_chat.title,
        "sender_username": sender_chat.username,
        "sender_is_verified": sender_chat.is_verified,
        "sender_is_scam": sender_chat.is_scam or sender_chat.is_fake,
        "sender_is_restricted": sender_chat.is_restricted
    }


def normalize_reactions(reactions: MessageReactions) -> List[Dict[str, int]]:
    if not reactions or not reactions.reactions:
        return []
    return [{r.emoji: r.count} for r in reactions.reactions]


class MongoBackend:
    conn: MongoClient = None

    def __init__(
        self,
        *,
        MONGO_HOST,
        MONGO_INITDB_ROOT_USERNAME,
        MONGO_INITDB_ROOT_PASSWORD,
        MONGO_DATABASE,
        **kwargs,
    ):
        self.conn = MongoClient(MONGO_HOST, username=MONGO_INITDB_ROOT_USERNAME, password=MONGO_INITDB_ROOT_PASSWORD)
        if not self.conn:
            raise Exception("Could not connect")
        self.db = self.conn[MONGO_DATABASE]

    def add_message(self, message: Message):
        if message.outgoing:
            return
        message_norm = clean_dict(message)
        if not message_norm:
            return
        #print(f"Adding message: {message_norm}")
        self.db["messages"].insert_one(message_norm)
    
    def check_if_needs_update(self, dialog: pyrogram.types.Dialog) -> bool:
        channel_id = dialog.chat.id
        m = self.db["messages"].find_one({"chat.id": channel_id}, sort=[("id", -1)])
        if m is None:
            return True
        current_top_message = dialog.top_message
        return current_top_message.id > m["id"]

    def max_message_id(self, dialog: pyrogram.types.Dialog) -> int:
        channel_id = dialog.chat.id
        m = self.db["messages"].find_one({"chat.id": channel_id}, sort=[("id", -1)])
        if m is None:
            return -1
        return m["id"]

    def close(self):
        assert self.conn is not None
        self.conn.close()
