#!/usr/bin/env python3

from typing import List, Optional, Tuple, Callable
import logging
import os
import click
import datetime
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.orm import Session
from common.utils import create_postgres_engine
from common.backend.models import ChatType
logging.basicConfig(level=logging.INFO)


default_days_ago = 90
default_end_date = datetime.date.today()
all_chat_types = [chat_type.name for chat_type in ChatType]
default_allowed_chat_types = [ChatType.channel.name, ChatType.group.name]
default_chats_table = "hebrew_chats"


@click.command()
@click.help_option("--help", "-h")
@click.option("--output-path", default=None, help="Output file path (default: autodetermined)")
@click.option("--recursive-replies", type=bool, default=False, help="Trace replies recursively")
@click.option("--recursive-forwards", type=bool, default=False, help="Trace forwards recursively")
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(default_end_date), help="Exclusive end date")
@click.option("--period-days", type=int, default=default_days_ago, help="Number of days before end date to export messages")
#@click.option("--allowed-chat-types", type=click.Choice(all_chat_types), multiple=True, default=default_allowed_chat_types, help="Allowed chat types")
@click.option("--select-chats", type=int, multiple=True, default=[], help="Select only specific chat IDs and disallow everything else")
@click.option("--chats-table", type=click.Choice(["chats", "hebrew_chats", "arabic_chats", "english_chats"]), default=default_chats_table, help="Table to select chats from")
def main(
    output_path: str,
    recursive_replies: bool,
    recursive_forwards: bool,
    end_date: datetime.date,
    period_days: int,
    #allowed_chat_types: List[str],
    select_chats: List[int],
    chats_table: str,
):
    start_date = end_date - datetime.timedelta(days=period_days)
    logging.info(f"Exporting messages from {start_date} to {end_date}")

    output_path, format, save = get_valid_output_path(output_path, chats_table, end_date, period_days)
    logging.info(f"Output path: {output_path}")

    engine = create_postgres_engine()
    session = Session(engine)
    logging.info("Initialized session")

    required_columns = "m.chat_id, m.message_id, m.sender_id, m.text, m.reply_to_message_id, m.forward_from_chat_id, m.forward_from_message_id"
    query_columns = required_columns

    query = f"""SELECT {query_columns} FROM messages m WHERE date BETWEEN '{start_date}' AND '{end_date}'"""
    if select_chats:
        assert chats_table == default_chats_table
        query += f""" AND chat_id IN ({', '.join([str(chat_id) for chat_id in select_chats])})"""
    else:
        query += f""" AND chat_id IN (SELECT chat_id FROM {chats_table})"""
    query += f""" AND text IS NOT NULL"""

    if recursive_replies or recursive_forwards:
        recursive_queries = []
        if recursive_replies:
            recursive_queries.append(f"""(m.chat_id = mt.chat_id AND m.message_id = mt.reply_to_message_id)""")
        if recursive_forwards:
            recursive_queries.append(f"""(m.chat_id = mt.forward_from_chat_id AND m.message_id = mt.forward_from_message_id)""")
        recursive_query = " OR ".join(recursive_queries)
        query = f"""
WITH RECURSIVE message_tree AS (
    {query}
    UNION ALL
    SELECT {query_columns} FROM messages m JOIN message_tree mt ON {recursive_query}
)
SELECT {required_columns} FROM message_tree m"""

    logging.info(f"Executing query: `{query}`")
    df = pd.read_sql(query, engine, chunksize=100000)#, dtype={"chat_id": np.int64, "message_id": np.int64, "reply_to_message_id": np.int64, "forward_from_chat_id": np.int64, "forward_from_message_id": np.int64})
    is_first = True
    for chunk in df:
        logging.info(f"Exporting {len(chunk)} messages into {output_path}")
        save(chunk, engine="fastparquet", append=not is_first)
        is_first = False
    
    logging.info(f"Closing session")
    session.close()

    logging.info(f"Done")


def get_valid_output_path(output_path: Optional[str], chats_table: str, end_date: datetime.date, days_ago: int) -> Tuple[str, str, Callable]:
    """
    :return: output_path, format, save_func
    """
    if not output_path:
        output_path_prefix = "_".join(chats_table.split("_")[:-1] + [""])
        output_path = f"./output/{output_path_prefix}messages_{end_date.strftime('%Y-%m-%d')}_{days_ago}d.parquet"
    if os.path.exists(output_path):
        raise FileExistsError(f"Output file already exists: {output_path}")
    _, format = os.path.splitext(output_path)
    format = format[1:]
    if format == 'csv':
        save_func = lambda df, **kwargs: df.to_csv(output_path, index=False, **kwargs)
    elif format == 'parquet':
        save_func = lambda df, **kwargs: df.to_parquet(output_path, index=False, **kwargs)
    elif format == 'pickle':
        save_func = lambda df, **kwargs: df.to_pickle(output_path, **kwargs)
    else:
        raise ValueError(f"Unknown format: {format}")
    base_dir = os.path.dirname(output_path)
    os.makedirs(base_dir, exist_ok=True)
    open(output_path, 'a').close() # touch file
    os.remove(output_path)
    return output_path, format, save_func


if __name__ == "__main__":
    main()