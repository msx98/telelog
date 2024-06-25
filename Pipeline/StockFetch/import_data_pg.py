#!/usr/bin/env python3

import yfinance as yf
import os
from sqlalchemy import text, select, func
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import datetime
import pytz
import numpy as np
import googlefinance as gf
from collections import OrderedDict

from common.utils import create_postgres_engine
from common.backend.pg_backend import upsert
import common.backend.models as models


possible_intervals = OrderedDict(dict())
possible_intervals["1m"] = datetime.timedelta(days=7)
possible_intervals["2m"] = datetime.timedelta(days=60)
possible_intervals["5m"] = datetime.timedelta(days=60)
possible_intervals["15m"] = datetime.timedelta(days=60)
possible_intervals["30m"] = datetime.timedelta(days=60)
possible_intervals["90m"] = datetime.timedelta(days=60)
possible_intervals["1h"] = datetime.timedelta(days=730)
possible_intervals["1d"] = datetime.timedelta(days=365*98)


def add_ticker_yfinance(engine, ticker: str):
    assert isinstance(ticker, str)
    for interval, max_fetch in possible_intervals.items():
        try:
            start_date = datetime.datetime.now(pytz.timezone("America/New_York")) - max_fetch
            df = yf.download(
                ticker,
                interval=interval,
                start=start_date,
                keepna=True,
            )
            if df is None or len(df) == 0:
                continue
            df = repair_yf(df, ticker)
            years = df["date"].dt.year.unique()
            with Session(engine) as session:
                # CREATE TABLE IF NOT EXISTS stocks_2020 PARTITION OF stocks FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
                for year in years:
                    table_name = f"stocks_{year}"
                    session.execute(text(f"CREATE TABLE IF NOT EXISTS {table_name} PARTITION OF stocks FOR VALUES FROM ('{year}-01-01') TO ('{year+1}-01-01')"))
                upsert(session, models.Stocks, df.to_dict(orient="records"))
                session.flush()
                session.commit()
        except Exception as e:
            print(f"Failed to download {ticker} with interval {interval} and fetch {max_fetch}: {e}")


def add_ticker_sheets_csv(path: str, symbol: str):
    df = pd.read_csv(path)
    


def repair_yf(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    df = df.reset_index()
    df["symbol"] = symbol
    df = df.rename(columns={
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
    })
    if "stock_splits" not in df.columns:
        df["stock_splits"] = np.nan
    if "dividends" not in df.columns:
        df["dividends"] = np.nan
    return df


def write_to_parquet(hist_all: pd.DataFrame, path: str):
    hist_all["yyyymm"] = (((hist_all["date"].dt.year * 100) + hist_all["date"].dt.month)).astype(np.uint32)
    hist_all["day"] = hist_all["date"].dt.day.astype(np.uint8)
    hist_all["hour"] = hist_all["date"].dt.hour.astype(np.uint8)
    hist_all["minute"] = hist_all["date"].dt.minute.astype(np.uint8)
    hist_all["second"] = hist_all["date"].dt.second.astype(np.uint8)
    hist_all[[
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
        path,
        index=False,
        compression="gzip",
        partition_cols=["symbol", "yyyymm"],
    )


def main():
    engine = create_postgres_engine()
    #add_ticker("AAPL")
    add_ticker_yfinance(engine, "TA35.TA")
    pass


if __name__ == '__main__':
    main()