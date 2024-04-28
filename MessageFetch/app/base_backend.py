from typing import Dict, List, Iterable, Any
from enum import Enum
import queue
import threading
import time
import abc
from pyrogram.enums import ChatType, MessageMediaType
from pyrogram.types import Dialog, Chat, Message
import datetime
import pyrogram.client
import json
from consts import *
from collections import namedtuple
StoredDialog = namedtuple("Dialog", ["title", "max_id"])


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


class BaseBackend(abc.ABC):
    _conn = None
    _selected_channel: Dialog = None
    _stored_dialogs: Dict[int, StoredDialog] = None
    _session_dir = None

    def __init__(self, name: str, **kwargs):
        if not self.is_connected:
            raise Exception("self._conn is inactive")
        self._selected_channel: Dialog = None
        self._stored_dialogs: Dict[int, StoredDialog] = None
        self._session_dir = kwargs.get("SESSION_DIR", SESSION_DIR) + f"/{name}"
        os.makedirs(self._session_dir, exist_ok=True)

    def select_channel(self, dialog: pyrogram.types.Dialog):
        assert self._selected_channel is None
        self._selected_channel = dialog
        assert self._stored_dialogs
        max_id = self._stored_dialogs[dialog.chat.id].max_id if dialog.chat.id in self._stored_dialogs else -1
        channel_id = dialog.chat.id
        with open(f"{self._session_dir}/.last_write.json", "w") as f:
            f.write(json.dumps({
                "channel_id": channel_id,
                "channel_name": dialog.chat.title,
                "id_end": max_id,
                "id_start": dialog.top_message.id if dialog.top_message else max_id
            }))

    def unselect_channel(self):
        assert self._selected_channel is not None
        self.add_channel(self._selected_channel, update_top_message_id=True)
        self._selected_channel = None
        os.remove(f"{self._session_dir}/.last_write.json")
    
    def delete_last_write(self):
        if not os.path.exists(f"{self._session_dir}/.last_write.json"):
            return None
        with open(f"{self._session_dir}/.last_write.json", "r") as f:
            data = json.loads(f.read())
        channel_id = data["channel_id"]
        channel_name = data["channel_name"]
        id_end = data["id_end"]
        id_start = data["id_start"]
        assert id_end <= id_start # we receive messages in reverse
        self.delete_messages(channel_id, id_end, id_start)
        return (channel_id, channel_name, id_end-id_start+1)

    def close(self):
        print("db.close() - start")
        if not hasattr(self, "_conn"):
            raise NotImplementedError()
        if not hasattr(self._conn, "close"):
            raise NotImplementedError()
        if self._conn:
            print(f"db.close() - closing connection")
            self._conn.close()
            self._conn = None
        else:
            print(f"db.close() - connection already closed")
        print("db.close() - end")
    
    @property
    @abc.abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def add_message(self, message: Message):
        raise NotImplementedError()
    
    @abc.abstractmethod
    def add_channel(self, dialog: Dialog, update_top_message_id: bool):
        raise NotImplementedError()
    
    @abc.abstractmethod
    def add_messages(self, messages: Iterable[Message]):
        raise NotImplementedError()

    @abc.abstractmethod
    def delete_messages(self, channel_id: int, id_min: int, id_max: int):
        raise NotImplementedError()

    @abc.abstractmethod
    def delete_channel(self, channel_id: int):
        raise NotImplementedError()
    
    @abc.abstractmethod
    def get_stored_dialogs(self) -> Dict[int, StoredDialog]:
        raise NotImplementedError()
    
    @abc.abstractmethod
    def get_stored_dialogs_fast(self) -> Dict[int, StoredDialog]:
        raise NotImplementedError()
    
    @abc.abstractmethod
    def find_channel(self, search_str: str) -> List[Dict[str, Any]]:
        raise NotImplementedError()
    
    @abc.abstractmethod
    def count_messages(self) -> int:
        raise NotImplementedError()

    @abc.abstractmethod
    def count_dialogs(self) -> int:
        raise NotImplementedError()


class MessageQueue:
    def __init__(self, db: BaseBackend, lock: threading.Lock, max_queue_size: int):
        self.db = db
        self.max_queue_size = max_queue_size
        self.queue = queue.Queue()
        self.lock = lock
        self.last_push = time.time()
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def add_message(self, message):
        # Don't add to queue while queue is being processed
        with self.lock:
            self.queue.put(message)

    def run(self):
        while True:
            if self.queue.qsize() >= self.max_queue_size or time.time() - self.last_push > 5:
                #print(f"Reached condition - gonna wait for lock")
                with self.lock:
                    messages = [self.queue.get() for _ in range(self.queue.qsize())]
                    #print(f"Pushing {len(messages)} messages to DB")
                    self.db.add_messages(messages)
                    self.last_push = time.time()
            time.sleep(1)


class BaseBackendWithQueue(BaseBackend):
    _queue: MessageQueue
    _lock: threading.Lock

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        self._lock = threading.Lock()
        self._queue = MessageQueue(self, self._lock, kwargs.get("max_queue_size", 10))
    
    @property
    def lock(self):
        return self._lock
    
    def close(self):
        with self._lock:
            super().close()

    def add_message(self, message: Message):
        self._queue.add_message(message)
