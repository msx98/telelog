#!/usr/bin/env python3

from typing import List, Optional, Tuple, Callable
import logging
import sys
import os
import sqlalchemy
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from sqlalchemy.orm import Session
from common.utils import create_postgres_engine
from common.backend.models import MessageChain
from common.utils import upsert
logging.basicConfig(level=logging.INFO)


def main(input_path: str):
    engine = create_postgres_engine()
    logging.info("Initialized engine")
    input_stream = pq.ParquetFile(input_path)
    assert {x.name for x in list(input_stream.schema)[1:]} == {"chain", "chat_id", "first_message_id", "last_message_id", "sent_by_linked_chat", "chain_len", }
    total_size = input_stream.metadata.num_rows
    logging.info(f"Working on {total_size} rows")
    n_inserted = 0
    for df in input_stream.iter_batches(batch_size=384):
        df = df.to_pandas()
        df = df[df["chain"].notna()]
        with Session(engine) as session:
            n_inserted += upsert(session, MessageChain, df.to_dict(orient="records")).rowcount
            session.flush()
            session.commit()
        logging.info(f"Finished {n_inserted}/{total_size} rows")
    logging.info("Closing session")
    session.close()
    logging.info("Done")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error(f"Usage: {sys.argv[0]} <input_path>")
        sys.exit(1)
    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        logging.error("File does not exist")
        sys.exit(1)
    main(file_path)