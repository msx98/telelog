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
from base_backend import clean_dict, StoredDialog
import json


class MongoBackend:
    conn: MongoClient = None

    def __init__(
        self,
        *,
        MONGO_HOST,
        MONGO_INITDB_ROOT_USERNAME,
        MONGO_INITDB_ROOT_PASSWORD,
        MONGO_DATABASE,
        SESSION_DIR,
        **kwargs,
    ):
        self.conn = MongoClient(MONGO_HOST, username=MONGO_INITDB_ROOT_USERNAME, password=MONGO_INITDB_ROOT_PASSWORD)
        if not self.conn:
            raise Exception("Could not connect")
        self.db = self.conn[MONGO_DATABASE]
        self._stored_dialogs: Dict[int, StoredDialog] = None
        self.selected_channel: pyrogram.types.Dialog = None
        self.session_dir = SESSION_DIR

    def add_message(self, message: Message):
        if message.outgoing:
            return
        if message.chat:
            assert self.selected_channel is not None
            assert message.chat.id == self.selected_channel.chat.id
        message_norm = clean_dict(message)
        if not message_norm:
            return
        #print(f"Adding message: {message_norm}")
        self.db["messages"].insert_one(message_norm)
    
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

    def close(self):
        assert self.conn is not None
        self.conn.close()
    
    def select_channel(self, dialog: pyrogram.types.Dialog):
        assert self.selected_channel is None
        self.selected_channel = dialog
        assert self._stored_dialogs
        max_id = self._stored_dialogs[dialog.chat.id].max_id if dialog.chat.id in self._stored_dialogs else -1
        channel_id = dialog.chat.id
        with open(f"{self.session_dir}/.last_write.json", "w") as f:
            f.write(json.dumps({
                "channel_id": channel_id,
                "channel_name": dialog.chat.title,
                "id_end": max_id,
                "id_start": dialog.top_message.id if dialog.top_message else max_id
            }))
    
    def delete_channel(self, channel_id: int):
        assert self.selected_channel is None
        self.db["messages"].delete_many({"chat.id": channel_id})
        self.db["dialogs"].delete_many({"_id": channel_id})
    
    def find_channel(self, search_str: str) -> List[Dict[str, Any]]:
        cursor = self.db["dialogs"].find({"chat.title": {"$regex": search_str, "$options": "i"}})
        return list(cursor)

    def count_messages(self) -> int:
        return self.db["messages"].count_documents({})

    def count_dialogs(self) -> int:
        return self.db["dialogs"].count_documents({})
    
    def unselect_channel(self):
        assert self.selected_channel is not None
        cleaned_dialog = clean_dict(self.selected_channel)
        self.db["dialogs"].update_one(
            {'_id': cleaned_dialog['chat']['id']}, 
            {'$set': cleaned_dialog}, 
            upsert=True,
        )
        self.selected_channel = None
        os.remove(f"{self.session_dir}/.last_write.json")
    
    def delete_last_write(self):
        if not os.path.exists(f"{self.session_dir}/.last_write.json"):
            return None
        with open(f"{self.session_dir}/.last_write.json", "r") as f:
            data = json.loads(f.read())
        channel_id = data["channel_id"]
        channel_name = data["channel_name"]
        id_end = data["id_end"]
        id_start = data["id_start"]
        assert id_end <= id_start # we receive messages in reverse
        r = self.db["messages"].delete_many({"chat.id": channel_id, "id": {"$gt": id_end, "$lte": id_start}})
        delete_count = r.deleted_count
        return (channel_id, channel_name, delete_count)
