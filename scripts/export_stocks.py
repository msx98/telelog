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
from common.backend.models import GranularityType
logging.basicConfig(level=logging.INFO)


default_days_ago = 30
default_end_date = datetime.date.today()
all_granularity_types = [x.name for x in GranularityType]
default_allowed_granularity_types = all_granularity_types


@click.command()
@click.help_option("--help", "-h")
@click.option("--output-path", default=None, help="Output file path (default: autodetermined)")
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(default_end_date), help="Exclusive end date")
@click.option("--period-days", type=int, default=default_days_ago, help="Number of days before end date to export stocks")
def main(
    output_path: str,
    end_date: datetime.date,
    period_days: int,
):
    start_date = end_date - datetime.timedelta(days=period_days)
    logging.info(f"Exporting stocks from {start_date} to {end_date}")

    save = get_save_func(output_path, end_date, period_days)

    engine = create_postgres_engine()
    session = Session(engine)
    logging.info("Initialized session")

    query = f"""SELECT * FROM stocks m WHERE date BETWEEN '{start_date}' AND '{end_date}'"""
    logging.info(f"Executing query: `{query}`")
    df = pd.read_sql(query, engine)#, dtype={"chat_id": np.int64, "message_id": np.int64, "reply_to_message_id": np.int64, "forward_from_chat_id": np.int64, "forward_from_message_id": np.int64})

    logging.info(f"Exporting {len(df)} stocks")
    save(df)
    
    logging.info(f"Closing session")
    session.close()

    logging.info(f"Done")


def get_save_func(output_path: Optional[str], end_date: datetime.date, days_ago: int) -> Callable:
    if not output_path:
        output_path = f"./output/stocks_{end_date.strftime('%Y-%m-%d')}_{days_ago}d.parquet"
    if os.path.exists(output_path):
        raise FileExistsError(f"Output file already exists: {output_path}")
    _, format = os.path.splitext(output_path)
    format = format[1:]
    if format == 'csv':
        save_func = lambda df: df.to_csv(output_path, index=False)
    elif format == 'parquet':
        save_func = lambda df: df.to_parquet(output_path, index=False, compression="gzip")
    elif format == 'pickle':
        save_func = lambda df: df.to_pickle(output_path)
    else:
        raise ValueError(f"Unknown format: {format}")
    def save_func_wrap(df):
        logging.info(f"Saving to {output_path}")
        save_func(df)
        logging.info(f"Saved to {output_path}")
    base_dir = os.path.dirname(output_path)
    os.makedirs(base_dir, exist_ok=True)
    open(output_path, 'a').close() # touch file
    os.remove(output_path)
    return save_func_wrap


if __name__ == "__main__":
    main()