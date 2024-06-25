#!/usr/bin/env python3

import yfinance as yf
import os
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import datetime
import pytz
import numpy as np
import googlefinance as gf

from common.utils import create_postgres_engine
from common.backend.pg_backend import upsert
import common.backend.models as models
from StockFetch.main_sql import repair_yf, write_to_pg


def add_ticker_sheets_csv(engine, path: str):
    df = pd.read_csv(path)
    symbol = df["Name"][0]
    symbol = {
        "CURRENCY:BTCUSD": "BTC-USD",
        "CURRENCY:ETHUSD": "ETH-USD",
        "CURRENCY:USDARS": "ARS=X",
        "CURRENCY:USDCNY": "CNY=X",
        "CURRENCY:USDEUR": "EUR=X",
        "CURRENCY:USDILS": "ILS=X",
        "CURRENCY:USDJPY": "JPY=X",
        "CURRENCY:USDVES": "VES=X",
        "GCW00": "GC=F",
        "TLV:137": "^TA125.TA",
    }.get(symbol, symbol)
    if symbol.startswith("TLV:"):
        symbol = symbol.split(":")[1] + ".TA"
    assert ":" not in symbol
    interval = df["Interval"][0]
    interval = {
        "DAILY": "1d",
    }[interval]
    df = df.drop(columns=["Name", "Interval"])
    df["Date"] = pd.to_datetime(df["Date"])
    # replace "#N/A" with np.nan
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].replace("#N/A", np.nan)
    df = repair_yf(df, symbol)
    write_to_pg(engine, df, interval, symbol)


def main():
    engine = create_postgres_engine()
    for sheet_path in os.listdir("sheets/"):
        add_ticker_sheets_csv(engine, f"sheets/{sheet_path}")
    pass


if __name__ == '__main__':
    main()