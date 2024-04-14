#!/usr/bin/env python3

from pyrogram import Client, filters
from pyrogram.enums import ChatType
from pyrogram.types import Message, User, Chat
from mysql_backend import MySQLBackend
from mongo_backend import MongoBackend
import datetime
from consts import *

print("Connecting to DB")
db = MySQLBackend(**config)

print("Connecting to account")
app = Client("listener", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, phone_number=TELEGRAM_PHONE, password=TELEGRAM_PASS)

print("Defining handlers")

@app.on_message(filters.text)
def handle_message(bot, message):
	if not hasattr(message, "text"):
		return
	db.add_message(message)


@app.on_message(filters.text & filters.private)
def handle_message_from_private(bot, message):
	if message.text == "/count":
		n, date = db.count_messages()
		fetched_diff = (datetime.datetime.now() - date).total_seconds()
		bot.send_message(
			chat_id=message.chat.id,
			text=f"Total messages: {n}, last fetched {fetched_diff} seconds ago",
			reply_to_message_id=message.id
		)


print("Launching app")
app.run()