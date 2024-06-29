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
            metadata = self.socket.recv().decode()
            chat_id, n, message_ids = metadata.split(":")
            message_ids = [int(x) for x in message_ids.split(",")]
            embeddings = []
            for i in range(n):
                embedding_bytes = self.socket.recv()
                embeddings.append(np.frombuffer(embedding_bytes, dtype=np.float32))
            if len(embeddings) != len(message_ids):
                logging.error(f"Received {len(embeddings)} embeddings and {len(message_ids)} message_ids. Skipping.")
                continue
            self.process_embeddings(chat_id, message_ids, embeddings)

    def process_embeddings(self, chat_id: str, message_ids: List[int], embeddings: List[np.ndarray]):
        try:
            with Session(self.engine) as session:
                for message_id, embedding in zip(message_ids, embeddings):
                    new_embedding = MessageChainEmbeddingsHegemmav2(
                        chat_id=int(chat_id),
                        last_message_id=message_id,
                        embedding=embedding
                    )
                    session.add(new_embedding)
                session.commit()
            logging.info(f"Processed and stored embeddings for chat_id: {chat_id}, message_ids: {message_ids}")
        except Exception as e:
            logging.error(f"Error processing data: {e}")


def main():
    receiver = EmbeddingReceiver()
    receiver.receive_and_process()


if __name__ == "__main__":
    main()