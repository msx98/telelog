#!/usr/bin/env python3

from typing import Dict
import yfinance as yf
import os
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import datetime
import pytz
import numpy as np
import time
from dotenv import load_dotenv

from common.utils import create_postgres_engine
from common.backend.pg_backend import upsert
import common.backend.models as models


def add_ticker_yfinance(engine, ticker: str, *, interval: str = "1m", period: str = "1d", start: datetime.datetime = None):
    assert isinstance(ticker, str)
    df = None
    periods_to_try = [period, "5d", "1d"]
    while (df is None or len(df) == 0) and periods_to_try:
        df = yf.download(
            ticker,
            interval=interval,
            period=periods_to_try.pop(0),
            start=start,
            keepna=True,
        )
    if df is None or len(df) == 0:
        return
    df = repair_yf(df, ticker)
    write_to_pg(engine, df, interval, ticker)


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
    if df["date"].dt.tz:
        df["date"] = df["date"].dt.tz_convert("UTC").dt.tz_localize(None)
    return df


def write_to_pg(engine, df: pd.DataFrame, interval: str, symbol: str):
    df["symbol"] = symbol
    df["granularity"] = interval
    df = df[[
            "symbol",
            "granularity",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "adj_close"
    ]].drop_duplicates()
    df_list = df.to_dict(orient="records")
    nan_to_None = lambda x: x if not np.isnan(x) else None
    df_list = [
        {
            "symbol": x["symbol"],
            "granularity": x["granularity"],
            "date": x["date"],
            "open": nan_to_None(x["open"]),
            "high": nan_to_None(x["high"]),
            "low": nan_to_None(x["low"]),
            "close": nan_to_None(x["close"]),
            "volume": nan_to_None(x["volume"]),
            "adj_close": nan_to_None(x["adj_close"]),
        }
        for x in df_list
    ]
    with Session(engine) as sess:
        upsert(sess, models.Stocks, df_list)
        sess.flush()
        sess.commit()


def main():
    load_dotenv(".stocks.env")
    engine = create_postgres_engine()
    with Session(engine) as sess:
        tickers = [x[0] for x in sess.execute(select(models.Stocks.symbol).distinct()).fetchall()]
    tickers = list(set(tickers) | set(os.environ["TICKERS"].split(",")))
    for ticker in tickers:
        print(f"Adding {ticker}")
        add_ticker_yfinance(engine, ticker, interval="1m", period="max")
        add_ticker_yfinance(engine, ticker, interval="60m", period="2y")
        start_date = datetime.datetime.now() - datetime.timedelta(days=60)
        start_date += datetime.timedelta(minutes=1)
        if ".TA" in ticker:
            start_date += datetime.timedelta(hours=3)
        if "=X" in ticker:
            start_date += datetime.timedelta(hours=3)
        if "-USD" in ticker:
            start_date += datetime.timedelta(hours=3)
        #from yfinance.exceptions import YFChartError, YFTzMissingError, YFTickerMissingError, YFInvalidPeriodError
        add_ticker_yfinance(engine, ticker, interval="2m", period="max", start=start_date)
    print(f"Sleeping")
    time.sleep(3600)


if __name__ == '__main__':
    main()
