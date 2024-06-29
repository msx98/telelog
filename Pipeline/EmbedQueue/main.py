from typing import List
import logging
import zmq
import os
import numpy as np
from sqlalchemy.orm import Session
from common.utils import create_postgres_engine
from common.backend.models import MessageChainEmbeddingsHegemmav2 
logging.basicConfig(level=logging.INFO)


class EmbeddingReceiver:
    def __init__(self):
        port = os.environ["ZMQ_PORT"]
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{port}")
        self.engine = create_postgres_engine()

    def receive_and_process(self):
        logging.info("Receiver started. Waiting for embeddings...")

        while True:
            # Receive total size
            total_size_str = self.socket.recv().decode()
            self.socket.send(b"OK")  # Ready for size
            total_size = int(total_size_str)

            # Receive metadata
            metadata_bytes = self.socket.recv(total_size)
            self.socket.send(b"OK")  # Ready for metadata
            chat_id, message_ids_str = metadata_bytes.decode().split(":")
            message_ids = [int(x) for x in message_ids_str.split(",")]

            # Calculate remaining size for embeddings
            remaining_size = total_size - len(metadata_bytes)

            # Receive embeddings
            embeddings_bytes = b""
            while remaining_size > 0:
                chunk = self.socket.recv(remaining_size)
                embeddings_bytes += chunk
                remaining_size -= len(chunk)

            # Reconstruct embeddings from bytes
            embeddings = np.frombuffer(embeddings_bytes, dtype=np.float32)

            # Process embeddings (you can expand on this)
            self.process_embeddings(chat_id, message_ids, embeddings)

    def process_embeddings(self, chat_id: str, message_ids: List[int], embeddings: np.ndarray):
        try:
            with Session(self.engine) as session:
                for message_id, embedding in zip(message_ids, embeddings):
                    new_embedding = MessageChainEmbeddingsHegemmav2(
                        chat_id=int(chat_id),
                        last_message_id=message_id,
                        embedding=embedding.tolist()
                    )
                    session.add(new_embedding)
                session.commit()
            logging.info(f"Processed and stored embeddings for chat_id: {chat_id}, message_ids: {message_ids}")
            self.socket.send(b"OK")  # Send acknowledgment
        except Exception as e:
            logging.error(f"Error processing data: {e}")
            self.socket.send(b"ERROR")  # Send error signal


def main():
    receiver = EmbeddingReceiver()
    receiver.receive_and_process()


if __name__ == "__main__":
    main()