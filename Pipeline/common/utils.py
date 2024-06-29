from typing import *
import random
import time

from sqlalchemy import UniqueConstraint, inspect, text
from sqlalchemy.dialects.postgresql import insert
from common.consts import consts
from common.backend.config import Config
import logging
import asyncio
import threading


def collect_list(
	it,
	max_elements: int = 1000,
	*,
	delay: float = 0.00,
	random_lower_limit: float = 0,
	random_upper_limit: float = 0.1,
) -> list:
	l = list()
	for i in it:
		time.sleep(delay + random.uniform(random_lower_limit, random_upper_limit))
		l.append(i)
		if max_elements and len(l) >= max_elements:
			break
	return l

week_seconds = 60 * 60 * 24 * 7
day_seconds = 60 * 60 * 24
hour_seconds = 60 * 60
minute_seconds = 60

def pretty_time(seconds: float) -> str:
    # (Weeks: {weeks}, )?(Days: {days}, )?)(Hours: {hours}, )?(Minutes: {minutes}, )?(Seconds: seconds)?
    weeks = int(seconds // week_seconds)
    seconds -= weeks * week_seconds
    days = int(seconds // day_seconds)
    seconds -= days * day_seconds
    hours = int(seconds // hour_seconds)
    seconds -= hours * hour_seconds
    minutes = int(seconds // minute_seconds)
    seconds -= minutes * minute_seconds
    s = ""
    if weeks > 0:
        s += f"{weeks}w, "
    if s or (days > 0):
        s += f"{days}d, "
    if s or (hours > 0):
        s += f"{hours}h, "
    if s or (minutes > 0):
        s += f"{minutes}m, "
    s += f"{seconds:.2f}s"
    return s


def create_postgres_engine():
    from sqlalchemy import create_engine
    import os
    postgres_host = os.environ.get("POSTGRES_HOST_OVERRIDE", os.environ["POSTGRES_HOST"])
    postgres_port = os.environ.get("POSTGRES_PORT_OVERRIDE", os.environ["POSTGRES_PORT"])
    return create_engine(f"postgresql://{consts['POSTGRES_USER']}:{consts['POSTGRES_PASSWORD']}@{postgres_host}:{postgres_port}/{consts['POSTGRES_DB']}")


def create_postgres_connection():
    return create_postgres_engine().connect()


def load_embedding_model():
    from sentence_transformers import SentenceTransformer
    import torch
    SENTENCE_TRANSFORMERS_MODEL_NAME = consts["SENTENCE_TRANSFORMERS_MODEL_NAME"]
    logging.debug(f"Loading model {SENTENCE_TRANSFORMERS_MODEL_NAME}")
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logging.debug(f"Selected device: {device}")
    model = SentenceTransformer(SENTENCE_TRANSFORMERS_MODEL_NAME)
    logging.debug(f"Moving model to {device}")
    model = model.to(device)
    logging.debug(f"Loaded model {SENTENCE_TRANSFORMERS_MODEL_NAME} into {device}")
    return model


def load_pyrogram_session(session_name: str):
    from pyrogram import Client
    import os
    session_string = os.environ[f"TELEGRAM_SESSION_STRING__{session_name}"]
    proxy = {k:v for k,v in {
        "scheme": os.environ.get(f"TELEGRAM_PROXY_SCHEME__{session_name}", None),
        "hostname": os.environ.get(f"TELEGRAM_PROXY_HOSTNAME__{session_name}", None),
        "port": os.environ.get(f"TELEGRAM_PROXY_PORT__{session_name}", None),
        "username": os.environ.get(f"TELEGRAM_PROXY_USERNAME__{session_name}", None),
        "password": os.environ.get(f"TELEGRAM_PROXY_PASSWORD__{session_name}", None),
    }.items() if v is not None}
    if proxy:
        client = Client("session", session_string=session_string, proxy=proxy)
    else:
        client = Client("session", session_string=session_string)
    return client


async def run_async(func: Callable, *, check_interval: float = 1, timeout: float = None):
    result = [None]
    returned = [False]
    error = [None]
    def runner():
        try:
            result[0] = func()
            returned[0] = True
        except Exception as e:
            logging.error(f"Error in async runner: {e}")
            returned[0] = True
            error[0] = e
    t = threading.Thread(target=runner)
    t.start()
    timeout_time = time.time() + timeout if timeout else None
    if timeout_time is None:
        while not returned[0]:
            await asyncio.sleep(check_interval)
    else:
        while (not returned[0]) and (time.time() < timeout_time):
            await asyncio.sleep(check_interval)
    if not returned[0]:
        raise TimeoutError()
    if error[0] is not None:
        raise error[0]
    else:
        return result[0]

def upsert(sess, model, rows):
    table = model.__table__
    stmt = insert(table)
    primary_keys = [key.name for key in inspect(table).primary_key]
    #from sqlalchemy import case
    update_dict = {c.name: text(f"CASE WHEN EXCLUDED.{c.name} IS NOT NULL THEN EXCLUDED.{c.name} ELSE {table.name}.{c.name} END")
                   for c in stmt.excluded
                   if not c.primary_key and not c.computed
                   and (c.name not in {"has_poll", "has_text", "has_media", "has_reactions"})}

    if not update_dict:
        raise ValueError("insert_or_update resulted in an empty update_dict")

    stmt = stmt.on_conflict_do_update(
        index_elements=primary_keys,
        set_=update_dict,
    )

    seen = set()
    foreign_keys = {col.name: list(col.foreign_keys)[0].column for col in table.columns if col.foreign_keys}
    unique_constraints = [c for c in table.constraints if isinstance(c, UniqueConstraint)]
    def handle_foreignkeys_constraints(row):
        for c_name, c_value in foreign_keys.items():
            foreign_obj = row.pop(c_value.table.name, None)
            row[c_name] = getattr(foreign_obj, c_value.name) if foreign_obj else None

        for const in unique_constraints:
            unique = tuple([const,] + [row[col.name] for col in const.columns])
            if unique in seen:
                return None
            seen.add(unique)

        return row

    rows = list(filter(None, (handle_foreignkeys_constraints(row) for row in rows)))
    return sess.execute(stmt, rows)
