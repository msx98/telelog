from typing import Tuple, Dict, Any, Optional, List
from collections import OrderedDict
import pyrogram.types
from pyrogram.types import Message, User, Chat
from pyrogram.enums import ChatType
import os
from pymongo import MongoClient


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
    message_dict = dict()
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
        message_dict["reactions"] = message.reactions.reactions
    message_dict["views"] = message.views
    message_dict["forwards"] = message.forwards
    # text, date, chat_id, chat_name, message_id, sender_id, sender_first_name, sender_last_name, sender_username, reply_to_chat_id, reply_to_message_id, reply_to_top_message_id, forward_chat_id, forward_message_id, forward_date, reactions, views, forwards
    return message_dict


def normalize_message(message: Message) -> Dict[str, Optional[str|int]]:
    # just fetch as dict
    d = message.__dict__
    d.pop("_client", None)
    return d


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
        message_norm = normalize_message(message)
        if not message_norm:
            return
        print(f"Adding message: {message_norm}")
        self.db["messages"].insert_one(message_norm)

    def close(self):
        assert self.conn is not None
        self.conn.close()
