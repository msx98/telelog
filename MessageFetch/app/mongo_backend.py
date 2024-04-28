from typing import Tuple, Dict, Any, Optional, List, Iterable
from collections import OrderedDict
import pyrogram.types
from pyrogram.types import Message, User, Chat, MessageReactions, Reaction, Dialog
from pyrogram.enums import ChatType, MessageMediaType
import os
import pyrogram
from pymongo import MongoClient
from enum import Enum
import datetime
from base_backend import BaseBackend, clean_dict, StoredDialog
import json
from consts import *


class MongoBackend(BaseBackend):
    _conn: MongoClient = None

    def __init__(
        self,
        *,
        name = "mongo",
        host = None,
        user = None,
        password = None,
        database = None,
        **kwargs,
    ):
        self._conn = MongoClient(
            host = host or MONGO_HOST,
            username = user or MONGO_INITDB_ROOT_USERNAME,
            password = password or MONGO_INITDB_ROOT_PASSWORD,
        )
        self.db = self._conn[database or MONGO_DATABASE]
        super().__init__(name, **kwargs)
    
    @property
    def is_connected(self) -> bool:
        return self._conn is not None

    def add_message(self, message: Message):
        if message.outgoing:
            return
        if message.chat:
            assert self._selected_channel is not None
            assert message.chat.id == self._selected_channel.chat.id
        message_norm = clean_dict(message)
        if not message_norm:
            return
        #print(f"Adding message: {message_norm}")
        self.db["messages"].insert_one(message_norm)

    def add_messages(self, messages: Iterable[Message]):
        message_list = []
        for message in messages:
            if message.outgoing:
                continue
            if message.chat:
                assert self._selected_channel is not None
                assert message.chat.id == self._selected_channel.chat.id
            message_norm = clean_dict(message)
            if not message_norm:
                continue
            message_list.append(message_norm)
        self.db["messages"].insert_many(message_list)

    def add_channel(self, dialog: Dialog):
        cleaned_dialog = clean_dict(dialog)
        self.db["dialogs"].update_one(
            {'_id': cleaned_dialog['chat']['id']}, 
            {'$set': cleaned_dialog}, 
            upsert=True,
        )
    
    def delete_messages(self, channel_id: int, id_min: int, id_max: int):
        assert self._selected_channel is None
        r_count = self.db["messages"].delete_many({"chat.id": channel_id, "id": {"$gt": id_min, "$lte": id_max}})
    
    def delete_channel(self, channel_id: int):
        assert self._selected_channel is None
        self.db["messages"].delete_many({"chat.id": channel_id})
        self.db["dialogs"].delete_many({"_id": channel_id})
    
    def find_channel(self, search_str: str) -> List[Dict[str, Any]]:
        cursor = self.db["dialogs"].find({"chat.title": {"$regex": search_str, "$options": "i"}})
        return list(cursor)
    
    def get_stored_dialogs(self) -> Dict[int, StoredDialog]:
        # Return: d[channel_id] = (channel_name, max_message_id)
        cursor = self.db["messages"].aggregate([
            {"$group": {"_id": "$chat.id", "max_id": {"$max": "$id"}, "channel_name": {"$first": "$chat.title"}}}
        ])
        #cursor = self.db["dialogs"].find() # FIXME - use this once dialogs collection is fixed
        d = dict()
        for row in cursor:
            d[row["_id"]] = StoredDialog(title=row["channel_name"], max_id=row["max_id"])
            #d[row["_id"]] = StoredDialog(title=row["chat"]["title"], max_id=row["top_message"]["id"] if ("top_message" in row) else -1)
        self._stored_dialogs = d
        return d

    def get_stored_dialogs_fast(self) -> Dict[int, StoredDialog]:
        # Return: d[channel_id] = (channel_name, max_message_id)
        cursor = self.db["dialogs"].find() # FIXME - use this once dialogs collection is fixed
        d = dict()
        for row in cursor:
            d[row["_id"]] = StoredDialog(title=row["chat"]["title"], max_id=row["top_message"]["id"] if ("top_message" in row) else -1)
        self._stored_dialogs = d
        return d

    def count_messages(self) -> int:
        return self.db["messages"].count_documents({})

    def count_dialogs(self) -> int:
        return self.db["dialogs"].count_documents({})
