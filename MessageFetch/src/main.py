#!/usr/bin/env python3

from pyrogram import Client, filters
from pyrogram.raw.functions.chatlists import get_chatlist_updates
from pyrogram.raw.functions.channels.get_channels import GetChannels
from pyrogram.enums import ChatType
from pymongo import MongoClient
import os
from mysql_backend import MySQLBackend
from mongo_backend import MongoBackend
from dotenv import load_dotenv
import random
import time


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


load_dotenv()
db = MySQLBackend()#MYSQL_HOST="127.0.0.1", MYSQL_PORT="3308")
#db = MongoBackend()#HOST="mongodb://localhost:27017/")


TELEGRAM_API_ID = os.environ["TELEGRAM_API_ID"]
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELEGRAM_PHONE = os.environ["TELEGRAM_PHONE"]
TELEGRAM_PASS = os.environ["TELEGRAM_PASS"]


app = Client("listener", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, phone_number=TELEGRAM_PHONE, password=TELEGRAM_PASS)


@app.on_message(filters.text & filters.channel)
def handle_message_from_channel(bot, message):
	db.add_message(message)
 

@app.on_message(filters.text & filters.group)
def handle_message_from_group(bot, message):
	db.add_message(message)

#from pyrogram.types import Message, User, Chat
@app.on_message(filters.text & filters.private)
def handle_message_from_private(bot, message):
	n = db.count_messages()
	bot.send_message(
		chat_id=message.chat.id,
		text=f"Total messages: {n}",
		reply_to_message_id=message.id
	)


app.run()