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
logging.basicConfig(level=logging.INFO)


@click.command()
@click.help_option("--help", "-h")
@click.option("--output-path", default=None, help="Output file path (default: autodetermined)")
def main(
    output_path: str,
):
    save = get_save_func(output_path)

    engine = create_postgres_engine()
    session = Session(engine)
    logging.info("Initialized session")

    query = f"""SELECT * FROM chats"""
    df = pd.read_sql(query, engine)

    logging.info(f"Exporting {len(df)} chats")
    save(df)
    
    logging.info(f"Closing session")
    session.close()

    logging.info(f"Done")


def get_save_func(output_path: Optional[str]) -> Callable:
    if not output_path:
        output_path = f"./output/chats.parquet"
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