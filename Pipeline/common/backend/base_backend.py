from typing import Dict, List, Iterable, Any, Callable, Optional
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
from common.consts import consts
from common.backend.config import Config
from collections import namedtuple, deque
import asyncio
import logging
StoredDialog = namedtuple("Dialog", ["title", "max_id", "username", "invite_link"])


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
def clean_dict(d_old, rec_level=1, max_rec=25) -> Dict:
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
    _config: Config = None

    def __init__(self, name: str, **kwargs):
        if not self.is_connected:
            raise Exception("self._conn is inactive")
        self._selected_channel: Dialog = None
        self._stored_dialogs: Dict[int, StoredDialog] = None
        self._config = kwargs.get("config", Config(project="sessionstore"))

    def select_channel(self, dialog: pyrogram.types.Dialog):
        assert self._selected_channel is None
        self._selected_channel = dialog
        #assert self._stored_dialogs
        max_id = self._stored_dialogs[dialog.chat.id].max_id if dialog.chat.id in self._stored_dialogs else -1
        if max_id is None:
            max_id = -1
        channel_id = dialog.chat.id
        self._config.set("last_write", json.dumps({
                "channel_id": channel_id,
                "channel_name": dialog.chat.title,
                "id_end": max_id,
                "id_start": dialog.top_message.id if dialog.top_message else max_id
            }))

    def unselect_channel(self):
        assert self._selected_channel is not None
        self.add_channel(self._selected_channel, update_top_message_id=True)
        self._selected_channel = None
        self._config.unset("last_write")
    
    def delete_last_write(self):
        data = self._config.get("last_write", None)
        if data is None:
            return None
        data = json.loads(data)
        channel_id = data["channel_id"]
        channel_name = data["channel_name"]
        id_end = data["id_end"]
        id_start = data["id_start"]
        assert id_end <= id_start # we receive messages in reverse
        self.delete_messages(channel_id, id_end, id_start)
        self._config.unset("last_write")
        return (channel_id, channel_name, id_start-id_end+1)

    def close(self):
        print("BaseBackend.close() - start")
        if not hasattr(self, "_conn"):
            raise NotImplementedError()
        if not hasattr(self._conn, "close"):
            raise NotImplementedError()
        if self._conn:
            print(f"BaseBackend.close() - closing connection")
            self._conn.close()
            self._conn = None
        else:
            print(f"BaseBackend.close() - connection already closed")
        print("BaseBackend.close() - end")
    
    def add_admin(self, user_id: int):
        admins = self._config.get("admins", "")
        if str(user_id) not in admins:
            self._config.set("admins", admins + f"{user_id},")

    def is_admin(self, user_id: int) -> bool:
        admins = self._config.get("admins", "").strip().split(",")[:-1]
        return str(user_id) in admins
    
    @property
    @abc.abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def add_message(self, message: Message|Callable):
        raise NotImplementedError()
    
    @abc.abstractmethod
    def add_channel(self, dialog: Dialog):
        raise NotImplementedError()
    
    @abc.abstractmethod
    def add_messages(self, messages: Iterable[Message|Callable]):
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
    def get_stored_dialogs_committed(self) -> Dict[int, StoredDialog]:
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
    def __init__(
            self,
            db: BaseBackend,
            lock: threading.Lock,
            *,
            batch_fill_timeout: float,
            batch_size: int,
    ):
        self.db = db
        self.max_queue_size = 10 * batch_size
        self.batch_size = batch_size
        self.batch_fill_timeout = batch_fill_timeout
        self.queue: deque = deque(maxlen=self.max_queue_size)
        self.lock = lock
        self.quick_lock = threading.Lock()
        self.last_push = time.time()
        self.last_count = 0
        self.messages: List[Optional[Message|Callable]] = [None for _ in range(batch_size)]
        self.stopped = False
        self.thread = threading.Thread(target=self._loop)
        self.start()

    def add_message(self, message: Message|Callable):
        if self.stopped:
            raise Exception("MessageQueue is stopped")
        self.queue.append(message)

    def start(self):
        self.thread.start()

    def stop(self):
        logging.info(f"MessageQueue.stop() - changing stopped to True")
        self.stopped = True
        logging.info(f"MessageQueue.stop() - waiting for thread to join")
        self.thread.join()
        logging.info(f"MessageQueue.stop() - thread joined - done")

    def _loop(self):
        i = 0
        while not self.stopped:
            i = 0
            #logging.info("Going for another batch")
            next_force_push = time.time() + self.batch_fill_timeout
            while i < self.batch_size:
                try:
                    self.messages[i] = self.queue.popleft()
                    i += 1
                except IndexError:
                    if time.time() > next_force_push:
                        break
                    #logging.info("Got index error, sleeping")
                    time.sleep(1)
            if i:
                #logging.info("Calling self.db.add_messages")
                self.db.add_messages(self.messages[:i])
            #logging.info("Obtaining lock")
            with self.quick_lock:
                self.last_push = time.time()
                self.last_count = i


class BaseBackendWithQueue(BaseBackend):
    _queue: MessageQueue
    _lock: threading.Lock

    def __init__(self, name: str, *, batch_fill_timeout: float = 1, batch_size: int = 10000, **kwargs):
        super().__init__(name, **kwargs)
        self._lock = threading.Lock()
        if kwargs.get("debug_read_only", None) is True:
            self._queue = None
        else:
            self._queue = MessageQueue(
                self,
                self._lock,
                batch_fill_timeout = batch_fill_timeout,
                batch_size = batch_size,
            )
    
    @property
    def lock(self):
        return self._lock
    
    def close(self):
        logging.info(f"BaseBackendWithQueue.close() - waiting for lock")
        with self._lock:
            logging.info(f"BaseBackendWithQueue.close() - got lock, stopping queue")
            self._queue.stop()
            logging.info(f"BaseBackendWithQueue.close() - stopped queue, waiting for queue to flush batches")
            self.wait_for_queue_flush_full_sync()
            logging.info(f"BaseBackendWithQueue.close() - stopped queue, calling super().close()")
            super().close()
        logging.info(f"BaseBackendWithQueue.close() - end")
    
    def __del__(self):
        logging.info(f"BaseBackendWithQueue.__del__() - start")
        try:
            self.close()
        except Exception as e:
            logging.warning(f"BaseBackendWithQueue.__del__() - error: {e}")
        logging.info(f"BaseBackendWithQueue.__del__() - end")

    def add_message(self, message: Message|Callable):
        self._queue.add_message(message)
    
    async def wait_for_queue_flush_batch(self, *, max_tries: int = 3):
        assert isinstance(max_tries, int) and max_tries > 0
        start_wait = time.time()
        for i in range(1, max_tries+1):
            if not self._queue.quick_lock.acquire(timeout = 1): # should be quick
                raise TimeoutError("Timed out waiting for lock")
            last_push = self._queue.last_push
            self._queue.quick_lock.release()
            if last_push > start_wait:
                return
            if i == max_tries:
                raise TimeoutError("Timed out waiting for queue to flush batch")
            else:
                await asyncio.sleep(self._queue.batch_fill_timeout)
    
    def wait_for_queue_flush_full_sync(self):
        max_batches = 1 + int(self._queue.max_queue_size / self._queue.batch_size)
        max_tries = int(max_batches * 1.25)
        assert max_batches >= 1
        assert max_tries >= 1
        assert self._queue.stopped # so we can't receive any more messages
        for i in range(1, max_tries+1):
            if not self._queue.quick_lock.acquire(timeout = 1): # should be quick
                raise TimeoutError("Timed out waiting for lock")
            queue_len = len(self._queue.queue)
            self._queue.quick_lock.release()
            if queue_len == 0:
                return
            if i == max_tries:
                raise TimeoutError("Timed out waiting for queue to flush all batches")
            else:
                time.sleep(self._queue.batch_fill_timeout)
