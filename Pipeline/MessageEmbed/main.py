#!/usr/bin/env python3

from typing import List
import logging
import os
import numpy as np
from reader import MessageReader
from sender import SenderQueue
from common.backend.models import MessageChain, MessageChainEmbeddingsHegemmav2
from common.utils import upsert
from common.utils import create_postgres_engine, load_embedding_model
from transformers import AutoTokenizer, AutoModel
logging.basicConfig(level=logging.INFO)


class MessageEmbedder:
    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained("yam-peleg/Hebrew-Gemma-11B-V2", device_map="auto")
        self.model = AutoModel.from_pretrained("yam-peleg/Hebrew-Gemma-11B-V2", device_map="auto")
    
    def embed(self, texts: List[str]) -> List[np.ndarray]:
        inputs = self.tokenizer(texts, return_tensors="pt", truncation=True, padding=True, max_length=512)
        outputs = self.model(**inputs)
        embeddings = outputs.last_hidden_state[:, 0, :].detach().numpy()
        embedding_rows = []
        for embedding in embeddings:
            embedding_rows.append(embedding)
        return embedding_rows


def main():
    logging.info("Creating reader")
    reader = MessageReader()
    logging.info("Creating sender")
    queue = SenderQueue()
    logging.info("Creating engine")
    embed = MessageEmbedder()

    logging.info("Starting embedding process")
    for batch in reader.get_messages(batch_size=128):
        logging.info(f"Sending {len(batch)} messages to embed")
        chat_ids = [chat_id for chat_id, _, _ in batch]
        last_message_ids = [last_message_id for _, last_message_id, _ in batch]
        texts = [text for _, _, text in batch]
        embeddings = embed.embed(texts)
        logging.info(f"Adding {len(embeddings)} embeddings to queue")
        queue.add(chat_ids, last_message_ids, embeddings)
    
    logging.info("Done")


if __name__ == "__main__":
    main()