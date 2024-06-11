#!/usr/bin/env python3

import logging
logging.basicConfig(level=logging.DEBUG)
from sqlalchemy import update, bindparam
from common.backend.models import Messages
from common.backend.config import Config
from common.utils import create_postgres_engine, load_embedding_model


def main():
    engine = create_postgres_engine()
    read_conn = engine.connect()
    write_conn = engine.connect()


    config = Config(conn=read_conn)
    embed_chat_list = config.get("embed_chat_list", [])
    lookback_days = int(config.get("embed_lookback_days", 7))
    read_batch_size = int(config.get("embed_read_batch_size", 4))
    write_batch_size = int(config.get("embed_write_batch_size", 4))
    logging.debug("Connected to DB")


    if not embed_chat_list:
        logging.error("No chat list provided. Exiting.")
        read_conn.close()
        write_conn.close()
        exit(1)


    model = load_embedding_model()


    logging.debug(f"Fetching messages without embeddings")
    rd_cursor = read_conn.connection.cursor(name='read_cursor')
    chat_list_query = f"""AND chat_id IN ({",".join(embed_chat_list)})"""
    query = f"""
                    SELECT chat_id, message_id, text FROM messages
                    WHERE embedding IS NULL AND text IS NOT NULL
                    {chat_list_query}
                    AND date > now() - interval '{lookback_days} days'
                    ORDER BY date DESC
                    """
    rd_cursor.execute(query)
    logging.debug("Retrieved messages")

    counter = 0
    try:
        while True:
            if counter % write_batch_size == 0:
                if write_conn.in_transaction():
                    write_conn.commit()
            logging.debug("Fetching batch")
            batch = rd_cursor.fetchmany(size=read_batch_size)
            logging.debug("Fetched batch")
            if not batch:
                break
            batch_texts = [row[2] for row in batch]
            batch_embeddings = model.encode(batch_texts)
            logging.debug(f"Embedded {len(batch_embeddings)} messages")
            stmt = (
                update(Messages)
                .where(Messages.chat_id == bindparam('_chat_id'))
                .where(Messages.message_id == bindparam('_message_id'))
                .values({
                    "embedding": bindparam("embedding")
                })
            )
            values = [
                {"_chat_id": row[0], "_message_id": row[1], "embedding": embedding}
                for row, embedding in zip(batch, batch_embeddings)
            ]
            write_conn.execute(stmt, values)
            counter += len(batch)
            logging.debug(f"Processed {counter} messages.")
        if write_conn.in_transaction():
            write_conn.commit()
        logging.debug("Embeddings generated and uploaded successfully!")
        write_conn.close()
        read_conn.close()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        # Close the connection
        try:
            if write_conn.in_transaction():
                write_conn.commit()
        except:
            pass
        try:
            rd_cursor.close()
        except:
            pass
        write_conn.close()
        read_conn.close()


if __name__ == "__main__":
    main()
