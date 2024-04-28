from types import Dict
from enum import Enum
import queue
import threading
import time
import abc
from pyrogram.enums import ChatType, MessageMediaType
import datetime
import pyrogram.client
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
    def __init__(self, **kwargs):
        self._lock = threading.Lock()

    @property
    def lock(self) -> threading.Lock:
        return self._lock

    @abc.abstractmethod
    def _add_messages(self, messages):
        raise NotImplementedError()


class MessageQueue:
    def __init__(self, db: BaseBackend, max_queue_size: int):
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
