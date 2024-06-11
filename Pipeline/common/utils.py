from typing import *
import random
import time
from common.consts import consts
from common.backend.config import Config
import logging


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
    import subprocess
    import os
    postgres_host, postgres_port = os.environ.get("POSTGRES_HOST"), os.environ.get("POSTGRES_PORT")
    postgres_host_ext, postgres_port_ext = os.environ.get("POSTGRES_HOST_EXTERNAL"), os.environ.get("POSTGRES_PORT_EXTERNAL")
    try:
        subprocess.check_output("ping -c 1 {}".format(postgres_host).split(" "))
    except Exception as e:
        postgres_host, postgres_port = postgres_host_ext, postgres_port_ext
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


def load_pyrogram_session(config: Config, default_session_string: Optional[str] = None):
    from pyrogram import Client
    import os
    session_string = config.get("pyrogram_session_string") or default_session_string
    if session_string:
        client = Client("session", session_string=session_string)
    else:
        client = Client("listener_fetch", api_id=os.environ["TELEGRAM_API_ID"], api_hash=os.environ["TELEGRAM_API_HASH"], phone_number=os.environ["TELEGRAM_PHONE"], password=os.environ["TELEGRAM_PASS"])
    return client
