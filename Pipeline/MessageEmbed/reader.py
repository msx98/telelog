from typing import Optional, List, Tuple, Generator
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from common.utils import create_postgres_engine
from common.backend.models import MessageChain, MessageChainEmbeddingsHegemmav2

class MessageReader:
    
    def __init__(self):
        self.engine = create_postgres_engine()
        self.ongoing_session = None
        self.killed = False

    def get_messages(self, *, batch_size: int = 128, require_channel_responses: bool = False) -> Generator[List[Tuple[int, int, str]], None, None]:
        """
        Generator yielding the next batch of messages.
        
        Yields:
            List of tuples: A list of (chat_id, last_message_id, chain) tuples.
        """
        logging.info("Starting message reader")
        with Session(self.engine) as session:
            self.ongoing_session = session
            offset = 0
            where_channel_responses = f"""
                            WHERE (chat_id IN (select chat_id from hebrew_chats where type='supergroup'))
                            AND (sent_by_linked_chat IS TRUE)
                            AND (chain_len>5)
                            """ if require_channel_responses else ""
            while not self.killed:

                stmt = text(f"""
                    SELECT m.chat_id, m.last_message_id, m.chain
                    FROM (
                            SELECT * FROM {MessageChain.__tablename__}
                            {where_channel_responses}
                    ) m
                    LEFT JOIN {MessageChainEmbeddingsHegemmav2.__tablename__} e
                    USING (chat_id, last_message_id)
                    WHERE e.embedding IS NULL
                    LIMIT :batch_size
                    OFFSET :offset
                """).bindparams(batch_size=batch_size, offset=offset)
                result = session.execute(stmt)
                messages = result.fetchall()
                # chat_id, last_message_id, chain
                messages = [(row[0], row[1], row[2]) for row in messages]

                if not messages:
                    break  # No more messages to process

                yield messages  # Yield the batch

                offset += batch_size 
    
    def kill(self):
        self.killed = True
        if self.ongoing_session:
            self.ongoing_session.close()
            self.ongoing_session = None
    
    def __del__(self):
        self.kill()


def get_messages(
        path_messages: str,
        path_chats: str,
        add_chat_name: bool = False,
) -> pd.DataFrame:
    logging.info(f"Reading chats from {path_chats}")
    chats_df = pd.read_parquet(path_chats)
    logging.info(f"Reading messages from {path_messages}")
    messages_df = pd.read_parquet(path_messages)
    if add_chat_name:
        messages_df = messages_df.merge(chats_df[["chat_id", "title"]], on="chat_id", how="left")
    return messages_df
