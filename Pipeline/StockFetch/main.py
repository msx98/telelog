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
import time

#from common.utils import create_postgres_engine
#from common.backend.pg_backend import upsert
#import common.backend.models as models


stock_data_path = os.environ.get("STOCK_DATA_PATH_OVERRIDE", os.environ.get("STOCK_DATA_PATH", None))
assert os.path.exists(stock_data_path), f"Stock data path {stock_data_path} does not exist"
stock_data_candle_path = f"{stock_data_path}/candles"
os.makedirs(stock_data_candle_path, exist_ok=True)
possible_intervals = {
    "1m": datetime.timedelta(days=7),
    "2m": datetime.timedelta(days=60),
    "5m": datetime.timedelta(days=60),
    "15m": datetime.timedelta(days=60),
    "30m": datetime.timedelta(days=60),
    "90m": datetime.timedelta(days=60),
    "1h": datetime.timedelta(days=730),
    "1d": datetime.timedelta(days=365*98),
}
stock_data_candle_paths = {
    interval: f"{stock_data_candle_path}/candles_{interval}"
    for interval in possible_intervals
}
for path in stock_data_candle_paths.values():
    os.makedirs(path, exist_ok=True)


def add_ticker_yfinance(ticker: str):
    assert isinstance(ticker, str)
    df = yf.download(
        ticker,
        interval="1m",
        period="1d",
        keepna=True,
    )
    if df is None or len(df) == 0:
        return
    df = repair_yf(df, ticker)
    write_to_parquet(df, "1m", ticker)


def repair_yf(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    df = df.reset_index()
    df["symbol"] = symbol
    renames = {
        "Datetime": "date",
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "Adj Close": "adj_close",
        "Stock Splits": "stock_splits",
        "Dividends": "dividends",
    }
    df = df.rename(columns=renames)
    for old in set(renames.values())-{"date"}:
        if old not in df.columns:
            df[old] = np.nan
    return df


def read_partitions(interval: str, symbol: str, min_yyyymm: int, max_yyyymm: int) -> pd.DataFrame:
    dfs = []
    for yyyymm in range(min_yyyymm, max_yyyymm + 1):
        path = f"{stock_data_candle_paths[interval]}/symbol={symbol}/yyyymm={yyyymm}"
        if not os.path.exists(path):
            continue
        df = pd.read_parquet(path)
        df["yyyymm"] = yyyymm
        df["symbol"] = symbol
        year, month = divmod(yyyymm, 100)
        df["year"] = year
        df["month"] = month
        df["date"] = pd.to_datetime(df[["year", "month", "day", "hour", "minute", "second"]])
        dfs.append(df)
    if len(dfs) == 0:
        return None
    return pd.concat(dfs)


def write_to_parquet(df: pd.DataFrame, interval: str, ticker: str, *, path: str = None):
    if df["date"].dt.tz:
        df["date"] = df["date"].dt.tz_convert("UTC").dt.tz_localize(None)
    df["yyyymm"] = (((df["date"].dt.year * 100) + df["date"].dt.month)).astype(np.uint32)
    df["day"] = df["date"].dt.day.astype(np.uint8)
    df["hour"] = df["date"].dt.hour.astype(np.uint8)
    df["minute"] = df["date"].dt.minute.astype(np.uint8)
    df["second"] = df["date"].dt.second.astype(np.uint8)
    old_df = read_partitions(interval, ticker, df["yyyymm"].min(), df["yyyymm"].max())
    if old_df is not None:
        df = old_df.set_index("date").combine_first(df.set_index("date")).reset_index()
    df[[
            "symbol",
            "yyyymm",
            "day",
            "hour",
            "minute",
            "second",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "adj_close"
    ]].to_parquet(
        path or stock_data_candle_paths[interval],
        index=False,
        compression="gzip",
        partition_cols=["symbol", "yyyymm"],
        existing_data_behavior="delete_matching",
    )


def main():
    for ticker in os.environ["TICKERS"].split(","):
        add_ticker_yfinance(ticker)
    print(f"Sleeping")
    time.sleep(3600)


if __name__ == '__main__':
    main()