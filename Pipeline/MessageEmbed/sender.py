from typing import List, Dict
import logging
import numpy as np
import zmq
import sys
import os
import threading
import queue
from collections import defaultdict
import time


class SenderQueue:
    def __init__(self):
        self.host = os.environ["ZMQ_HOST"]
        self.port = os.environ["ZMQ_PORT"]
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(f"tcp://{self.host}:{self.port}")
        self.lock: threading.Lock = threading.Lock()
        self.queue: Dict[int, queue.Queue] = defaultdict(queue.Queue)
        self.thread = threading.Thread(target=self._runner, daemon=True)
        self.started = False
        self.killed = False
        self.n_pending, self.n_sent, self.n_ack = 0, 0, 0

    def _send(self, chat_id: int, message_ids: List[int], embeddings: List[np.ndarray]):
        n = len(message_ids)
        if n == 0:
            return True
        assert len(embeddings) == n
        metadata = f"{chat_id}:{n}:{','.join([str(x) for x in message_ids])}"
        metadata_bytes = metadata.encode()
        self.socket.send(metadata_bytes)
        
        for i in range(n):
            embedding = embeddings[i]
            embedding_bytes = embedding.tobytes()
            self.socket.send(embedding_bytes)
            self.n_sent += 1
            #self.n_ack += 1
        return True

    def add(self, chat_ids: List[int], message_ids: List[List[int]], embeddings: List[np.ndarray]):
        if self.killed:
            raise RuntimeError("Queue is dead")
        if not self.started:
            logging.info("Starting thread")
            self.started = True
            self.thread.start()
        with self.lock:
            logging.info(f"Obtained lock")
            for chat_id, message_id, embedding in zip(chat_ids, message_ids, embeddings):
                self.queue[chat_id].put((message_id, embedding))
                self.n_pending += 1
    
    def kill(self):
        self.killed = True
        logging.info("Killing SenderQueue")
        with self.lock:
            for q in self.queue.values():
                q.put(None)  # Sentinel value to stop the consumer loop
        self.thread.join()
        logging.info("SenderQueue killed")
    
    def _monitor(self):
        while not self.killed:
            logging.info(f"Pending: {self.n_pending}, Sent: {self.n_sent}, Ack: {self.n_ack}")
            time.sleep(5)

    def _runner(self):
        while not self.killed:
            with self.lock:
                chat_ids = list(self.queue.keys())
            for chat_id in chat_ids:
                q = self.queue[chat_id]
                if len(q) < 1024:
                    continue
                all_message_ids = []
                all_embeddings = []
                with self.lock:
                    while not q.empty():
                        message_id, embedding = q.get()
                        all_message_ids.append(message_id)
                        all_embeddings.append(embedding)
                try:
                    self._send(chat_id, all_message_ids, all_embeddings)
                except Exception as e:
                    logging.error(f"Error sending data: {e}")
                    # Handle error (e.g., re-queue data, log, etc.)
                finally:
                    q.task_done()
        logging.info("SenderQueue runner exited")
    
    def __del__(self):
        logging.info("SenderQueue destructor called")
        self.kill()
