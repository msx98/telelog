#!/usr/bin/env python3

import os
import time
from typing import List, Dict, Tuple, Set, Any, Union
from common.utils import load_pyrogram_session
from pyrogram.enums import ChatType
from pyrogram.types import Message, User, Chat, Dialog, ChatPreview
from pyrogram import Client
from pyrogram.errors import RPCError, FloodWait, Flood, UserAlreadyParticipant
from common.backend.pg_backend import PostgresBackend
from common.backend.base_backend import StoredDialog
import datetime
import time
from common.consts import consts
from common.backend.config import Config
import threading
import signal
import sys
from typing import Optional
import random
import asyncio
from collections import namedtuple, defaultdict
import logging
logging.basicConfig(level=logging.INFO)


SHOULD_FETCH_GROUPS = int(os.environ.get("TELEGRAM_FETCH_GROUPS", 0))
TIME_PERIOD_FOR_GROUPS = datetime.timedelta(weeks=4)
TRY_TO_JOIN_MISSING_CHANNELS = False


should_fetch_full = {int(x) for x in os.environ.get("TELEGRAM_FETCH_FULL", "").split(",") if x}


class StatusMessage:
    def __init__(self, app: Client, config, session_name: str, update_interval: float = 60):
        self._params = dict()
        self._app: Client = app
        self._config = config
        self._session_name = session_name
        self._message: Message = None
        self._active: bool = False
        self._main_loop_task: asyncio.Task = None
        self._lock = asyncio.Lock()
        self._update_interval = update_interval
        self._prev_text = None
    
    def set_params(self, **kwargs):
        self._params |= kwargs

    def get_params_string(self) -> str:
        s = "\n".join([f"{k}: {v}" for k, v in self._params.items()] or ["<< EMPTY >>"])
        s += f"\n\nLast updated: {datetime.datetime.now().strftime('%H:%M:%S')}"
        return s

    @property
    def text(self) -> str:
        return self.get_params_string()

    async def _edit_text(self, text: str):
        if text == self._prev_text:
            return
        await self._message.edit_text(self.text)
        self._prev_text = text
    
    async def _refresh_text(self):
        await self._edit_text(self.text)
    
    async def start(self):
        assert self._message is None
        assert not self._active
        logging.info(f"Sending message")
        try:
            last_msg = self._config.get(f"last_msg_{self._session_name}", None)
            if last_msg is None:
                raise ValueError()
            self._message = await self._app.get_messages(int(os.environ["TELEGRAM_DEBUG_CHAT_ID"]), int(last_msg))
            assert isinstance(self._message, Message), "couldnt obtain msg"
        except Exception as e:
            logging.warning(f"creating new msg because {e}")
            self._message = await self._app.send_message(int(os.environ["TELEGRAM_DEBUG_CHAT_ID"]), self.text)
            self._config.set(f"last_msg_{self._session_name}", self._message.id)
        logging.info(f"Creating task")
        self._main_loop_task = asyncio.create_task(self._main_loop())
    
    async def _main_loop(self):
        self._active = True
        while self._active:
            try:
                await self._refresh_text()
            except FloodWait as e:
                logging.info(f"Got floodwait with {e.value}")
                await asyncio.sleep(float(e.value))
            except Exception as e:
                logging.warning(f"Failed to edit message, error: {e}")
            await asyncio.sleep(self._update_interval)
    
    async def stop(self):
        self._main_loop_task.cancel()
        try:
            await self._main_loop_task
        except asyncio.CancelledError:
            pass


class WorkerQueue:

    def __init__(self, session_names: List[str]):
        self.db = PostgresBackend()
        self.all_dialogs: Dict[int, Dialog] = dict()
        self.lock = asyncio.Lock()
        self.session_names = session_names
        self.workers = []
        self.offsets = self.db.get_offsets_from_ongoing_writes()
        self.channel_counts: Dict[int, int] = defaultdict(lambda: 0)
        self.compatible_channels: Dict[str, Set[int]] = defaultdict(set) # for every worker, compatible channels
        self.channel_busy: Dict[int, bool] = defaultdict(lambda: False)
        self.pending: List[int] = list()
        self.app: Dict[str, Client] = dict()
        self.status_msg: Dict[str, StatusMessage] = dict()
        self.stored_dialogs: Dict[int, StoredDialog] = dict()
        self.joined_channels: Dict[str, Set[int]] = defaultdict(set)
        self.id_to_max_committed_message = None
        self.task = None
        self.stopped = False
        self.stop_lock = threading.Lock()

    async def try_join(self, session_name: str, join_links: str|int|List[str|int], timeout: float = 5) -> bool:
        app = self.app[session_name]
        if isinstance(join_links, (str, int)):
            join_links = [join_links]
        if set(join_links) & self.joined_channels[session_name]:
            return True # already joined
        return True
        assert isinstance(join_links, list)
        logging.info(f"{session_name} JOIN: Trying to join using {join_links}")
        while join_links:
            try:
                link = join_links.pop(0)
                if not link:
                    continue
                logging.info(f"{session_name} JOIN: Trying to join {link}")
                task = asyncio.create_task(app.join_chat(link))
                try:
                    await asyncio.wait_for(task, timeout=timeout)
                    result = task.result()
                    logging.info(f"{session_name} JOIN: Successfully joined {link} with result: {result}")
                except asyncio.TimeoutError:
                    logging.warning(f"{session_name} JOIN: Timeout for {link}")
                    await task.cancel()
                    raise
                return True
            except UserAlreadyParticipant:
                return True
            except Exception as e:
                logging.warning(f"{session_name} JOIN: Failed to join {link}, error: {e}")
                if not join_links:
                    return False
                await asyncio.sleep(1)
        return False
    
    def _add_channel(self, session_name: str, dialog: Dialog):
        #logging.info(f"Adding {dialog.chat.id}")
        self.all_dialogs[dialog.chat.id] = dialog
        self.compatible_channels[session_name].add(dialog.chat.id)
        self.joined_channels[session_name].add(dialog.chat.id)

    async def _fetch_channel(self, session_name: str, chat_id: int) -> bool:
        dialog = self.all_dialogs[chat_id]
        app = self.app[session_name]
        status_msg = self.status_msg[session_name]
        stored_dialog = self.stored_dialogs[chat_id]
        status_msg.set_params(status="trying to join")
        if chat_id not in self.joined_channels[session_name]:
            assert TRY_TO_JOIN_MISSING_CHANNELS, "We should not be here if we are not trying to join missing channels"
            success = await self.try_join(session_name, [chat_id, stored_dialog.invite_link, stored_dialog.username])
            if not success:
                status_msg.set_params(status="failed to join")
                return False
        status_msg.set_params(status=f"joined channel {chat_id}")
        t_start = time.time()
        try:
            offset = self.offsets.get(dialog.chat.id, dialog.top_message.id)
            if dialog.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP] and dialog.chat.id not in should_fetch_full:
                min_date = datetime.datetime.now() - TIME_PERIOD_FOR_GROUPS
            else:
                min_date = datetime.datetime(year=1970, month=1, day=1)
            async for row in app.get_chat_history(dialog.chat.id, offset_id=offset):
                #if self.channel_counts[dialog.chat.id] % 1000 == 0:
                #    logging.info(f"{session_name} fetched {self.channel_counts[dialog.chat.id]} msgs from {chat_id}")
                if row.id <= self.id_to_max_committed_message.get(dialog.chat.id, -1):
                    break
                if row.date < min_date:
                    break
                self.db.add_message(row)
                self.channel_counts[dialog.chat.id] += 1
            logging.info(f"{session_name} finished fetching {chat_id} in {time.time() - t_start:.2f} seconds")
            self.db.set_top_message_id_to_db_max(dialog.chat.id)
            return True
        except Exception as e:
            logging.warning(f"{session_name} failed to fetch {chat_id}, error: {e}")
            self.channel_counts[dialog.chat.id] = 0
            return False
    
    async def _run_worker_1_gather_dialogs(self, session_name: str):
        logging.info(f"{session_name} entered gather dialogs")
        try:
            async for dialog in self.app[session_name].get_dialogs():
                #logging.info(f"{session_name} got dialog {dialog.chat.id}")
                self._add_channel(session_name, dialog)
        except:
            raise

    async def _run_worker_2_perform_fetching_loop(self, session_name: str):
        logging.info(f"{session_name} entered fetching loop with {len(self.pending)} pending")
        while self.pending and self.compatible_channels[session_name]:
            selected_chat_id = None
            async with self.lock:
                q = [(i,chat_id) for i, chat_id in enumerate(self.pending)]
                for i, chat_id in q:
                    if chat_id not in self.compatible_channels[session_name]:
                        continue
                    if self.channel_busy[chat_id]:
                        continue
                    self.channel_busy[chat_id] = True
                    selected_chat_id = chat_id
                    break
            self.status_msg[session_name].set_params(selected_chat_id = selected_chat_id)
            if selected_chat_id is None:
                await asyncio.sleep(1)
                continue
            logging.info(f"{session_name} selected {selected_chat_id}")
            success = await self._fetch_channel(session_name, selected_chat_id)
            logging.info(f"{session_name} finished fetch loop with success={success}")
            async with self.lock:
                #logging.info(f"{session_name} acquired lock")
                if success:
                    self.pending.remove(selected_chat_id)
                else:
                    self.compatible_channels[session_name].remove(selected_chat_id)
                    self.channel_counts[selected_chat_id] = 0
                self.channel_busy[selected_chat_id] = False
        if self.pending:
            async with self.lock:
                logging.warning(f"Worker {session_name} died prematurely")
        else:
            logging.info(f"Worker {session_name} finished")
    
    async def _run_worker_0_db_sync(self, barrier_1: asyncio.Barrier, barrier_2: asyncio.Barrier):
        logging.info(f"Worker 0 waiting for barrier 1")
        await barrier_1.wait()
        logging.info(f"Beginning worker 0 db sync, got {len(self.all_dialogs)} dialogs")
        assert self.all_dialogs, f"Something weird happened - we have no dialogs"
        self.db.add_channel(list(self.all_dialogs.values()))
        logging.info(f"Finished worker 0 db sync")
        self.stored_dialogs: Dict[int, StoredDialog] = self.db.get_stored_dialogs_committed()
        logging.info(f"Fetched {len(self.stored_dialogs)} stored dialogs")
        self.stored_dialogs = {
            k: v for k, v in self.stored_dialogs.items()
            if k in self.all_dialogs
            and self.all_dialogs[k].top_message is not None
            and self.all_dialogs[k].chat is not None
            and (not self.all_dialogs[k].chat.is_restricted) # this means there are no messages
            and (
                self.all_dialogs[k].chat.type in [ChatType.CHANNEL]
                or k in should_fetch_full
                or (
                    self.all_dialogs[k].chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]
                    and SHOULD_FETCH_GROUPS
                )
            )
        }
        logging.info(f"Filtered down to {len(self.stored_dialogs)} stored dialogs")
        self.id_to_max_committed_message: Dict[int, int] = {k: self.stored_dialogs[k].max_id if self.stored_dialogs[k].max_id is not None else -1 for k in self.stored_dialogs}
        self.id_to_top_available_message: Dict[int, int] = {k: self.offsets.get(k, self.all_dialogs[k].top_message.id) for k in self.stored_dialogs}
        self.id_to_leftovers: Dict[int, int] = {k: (self.id_to_top_available_message[k] - self.id_to_max_committed_message[k]) for k in self.stored_dialogs}
        interesting = dict()
        for k,v in self.id_to_leftovers.items():
            if v > 0:
                interesting[k] = (self.id_to_top_available_message[k], self.id_to_max_committed_message[k], v)
        print(interesting)
        sorted_channels = sorted(self.id_to_leftovers.items(), key=lambda x: x[1], reverse=False) # sort in ascending order
        self.all_channels = [x[0] for x in sorted_channels]
        self.all_channels = [x for x in self.all_channels if x in self.offsets.keys()] + [x for x in self.all_channels if x not in self.offsets.keys()]
        self.all_channels = [x for x in self.all_channels if self.id_to_leftovers[x] > 0]
        self.pending = self.all_channels
        if TRY_TO_JOIN_MISSING_CHANNELS:
            for chat_id in self.all_channels:
                for session_name in self.session_names:
                    self.compatible_channels[session_name].add(chat_id)
        total_left_count: int = sum(list(self.id_to_leftovers.values()))
        logging.info(f"Total left count: {total_left_count}")
        await barrier_2.wait()
        logging.info(f"Passed barrier 2 with pending {len(self.pending)}")
        start_time = time.time()
        saved_count = sum(list(self.channel_counts.values()))
        while self.pending:
            saved_count = sum(list(self.channel_counts.values()))
            #saved_count_diff = max(new_saved_count-saved_count, 0)
            rate_mps = saved_count / (time.time() - start_time)
            #start_time = time.time()
            left_seconds = (total_left_count - saved_count) / rate_mps if rate_mps != 0 else float("inf")
            left_time = datetime.timedelta(seconds=left_seconds) if rate_mps != 0 else "INFINITY"
            logging.info(f"{saved_count}, rate {rate_mps:.2f} mps, left {left_time}")
            for session_name in self.session_names:
                self.status_msg[session_name].set_params(
                    pending = len(self.pending),
                    saved_count = saved_count,
                    rate_mps = rate_mps,
                    left_time = left_time
                )
            await asyncio.sleep(1)
        logging.info(f"Worker 0 all done (no more pending)")

    async def _run_worker_full(self, barrier_1: asyncio.Barrier, barrier_2: asyncio.Barrier, session_name: str):
        logging.info(f"{session_name} creating app")
        self.app[session_name] = load_pyrogram_session(session_name)
        logging.info(f"{session_name} starting app")
        await self.app[session_name].start()
        logging.info(f"{session_name} gathering dialogs")
        await self._run_worker_1_gather_dialogs(session_name)
        logging.info(f"{session_name} waiting for barrier 1")
        await barrier_1.wait()
        logging.info(f"{session_name} starting status message")
        result = await self.try_join(session_name, [os.environ["TELEGRAM_DEBUG_CHAT_LINK"], os.environ["TELEGRAM_DEBUG_CHAT_ID"]])
        if result is False:
            raise Exception("Could not join chat")
        self.status_msg[session_name] = StatusMessage(self.app[session_name], self.db._config, session_name)
        logging.info(f"{session_name} starting status msg")
        await self.status_msg[session_name].start()
        logging.info(f"{session_name} waiting for barrier 2")
        await barrier_2.wait()
        logging.info(f"{session_name} performing loop")
        await self._run_worker_2_perform_fetching_loop(session_name)
        logging.info(f"{session_name} done, releasing context")
        await self.app[session_name].stop()
        await self.status_msg[session_name].stop()
        logging.info(f"{session_name} released context")
    
    async def _run_workers(self):
        barrier_1 = asyncio.Barrier(len(self.session_names) + 1)
        barrier_2 = asyncio.Barrier(len(self.session_names) + 1)
        logging.info("Gathering tasks")
        tasks = [self._run_worker_0_db_sync(barrier_1, barrier_2)]
        for session_name in self.session_names:
            tasks.append(self._run_worker_full(barrier_1, barrier_2, session_name))
        logging.info("Calling gather")
        await asyncio.gather(*tasks)
        logging.info("Closing DB")
        self.db.close()
        logging.info("_run_workers out")
    
    async def start(self):
        logging.info("WorkerQueue.start() - called")
        with self.stop_lock:
            self.task = asyncio.create_task(self._run_workers())
        try:
            logging.info("WorkerQueue.start() - awaiting task")
            await self.task
        except Exception:
            logging.info("WorkerQueue.start() - cancelling")
            for session_name in self.session_names:
                await self.status_msg[session_name].stop()
                await self.app[session_name].stop()
            logging.info("WorkerQueue.start() - stopped apps")
            raise
        logging.info("WorkerQueue.start() - done")
    
    def stop(self):
        with self.stop_lock:
            if self.stopped:
                logging.warning(f"WorkerQueue.stop() - already stopped - NOP")
            self.stopped = True
        logging.info("WorkerQueue.stop() - canceling task")
        self.task.cancel()
        logging.info("WorkerQueue.stop() - stopping DB")
        self.db.close()
        logging.info(f"WorkerQueue.stop() - done, please wait for the start task to finish")


async def main():
    session_names = os.environ["TELEGRAM_FETCH_WITH"].split(",")
    logging.info(f"Initializing with {session_names}")
    workers = WorkerQueue(session_names=session_names)
    logging.info(f"Adding signal handler")
    for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGABRT]:
        signal.signal(sig, lambda sig, frame: workers.stop())
    logging.info("Starting workers")
    try:
        await asyncio.create_task(workers.start())
        logging.info("Sleeping for a while so we don't overwhelm the system")
        await asyncio.sleep(1800)
    except asyncio.CancelledError:
        logging.info("Task is cancelled!")
    logging.info("Finished")


if __name__ == "__main__":
    asyncio.run(main())
