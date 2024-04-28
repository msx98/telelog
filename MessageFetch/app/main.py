#!/usr/bin/env python3

from pyrogram import Client, filters
from pyrogram.enums import ChatType
from pyrogram.types import Message, User, Chat
from mysql_backend import MySQLBackend
from mongo_backend import MongoBackend
import datetime
from consts import *
import signal
import sys

print("Connecting to DB")
db = MySQLBackend()


print("Connecting to account")
app = Client("listener", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, phone_number=TELEGRAM_PHONE, password=TELEGRAM_PASS)


print("Defining handlers")


def signal_handler(sig, frame):
	print("Closing DB")
	db.close()
	print("Closing account")
	app.stop()
	sys.exit(0)


@app.on_message(~filters.private)
def handle_message(bot, message):
	if hasattr(message, "chat") and hasattr(message.chat, "id") and message.chat.id == DEBUG_CHAT_ID:
		print(f"Got message: {message}")
	db.add_message(message)


@app.on_message(filters.private)
def handle_message_from_private(bot, message):
	if message.text == "/count":
		n, date = db.count_messages()
		fetched_diff = (datetime.datetime.now() - date).total_seconds()
		bot.send_message(
			chat_id=message.chat.id,
			text=f"Total messages: {n}, last fetched {fetched_diff} seconds ago",
			reply_to_message_id=message.id
		)


print("Adding signal handlers")
signal.signal(signal.SIGINT, signal_handler)


print("Launching app")
app.run()
