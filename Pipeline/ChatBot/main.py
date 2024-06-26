#!/usr/bin/env python3

from dotenv import dotenv_values
import os
config = dotenv_values(".env") | dotenv_values(".telegram.env")
os.environ.update(config)

from pyrogram import Client, filters
from pyrogram.types import Message
from common.backend.pg_backend import PostgresBackend
import threading
import asyncio
from common.backend.config import Config
import datetime
import asyncio
import threading
import time
from common.consts import *
from collections import defaultdict, deque
import signal
import sys
import os
from common.utils import load_embedding_model, load_pyrogram_session, run_async
import logging
from sqlalchemy.orm import Session
from sqlalchemy import Column
from pyrogram import Client, idle
from group_chat_manager import group_chats, GroupChatManager
logging.basicConfig(level=logging.INFO)


logging.info("Connecting to DB")
db = PostgresBackend()
config = Config()


logging.info("Connecting to account")
app = load_pyrogram_session(os.environ["TELEGRAM_CHATBOT_WITH"])
logging.info("Got app")


known_cmds = []
history = defaultdict(lambda: [])


def on_cmd(command_name, **kwargs):
	require_auth = kwargs.get("require_auth", True)
	description = kwargs.get("description", None)
	start_msg = kwargs.get("start_msg", None)
	global known_cmds
	known_cmds.append((command_name, description))
	def decorator(func):
		@app.on_message(filters.private & filters.command(command_name))
		async def wrapper(bot, message):
			if not isinstance(message.text, str) or not message.text:
				return
			if message.forward_from or message.forward_sender_name:
				return
			if message.from_user is None or message.from_user.id is None:
				return
			if require_auth and not db.is_admin(message.from_user.id):
				return
			text_start = message.text.find(" ")
			text = "" if text_start == -1 else message.text[text_start+1:]
			text = text.replace("“", "\"").replace("”", "\"").replace("‘", "'")
			history[message.from_user.id].append(message)
			response_msg = await bot.send_message(chat_id=message.chat.id, text=start_msg) if start_msg else None
			result_text = await func(bot, message, response_msg, text)
			assert (result_text and isinstance(result_text, (str, Message))) or result_text is None
			if isinstance(result_text, str):
				print(f"Sending: {result_text}")
				if response_msg:
					await response_msg.edit_text(result_text)
				else:
					await bot.send_message(
						chat_id=message.chat.id,
						text=result_text,
						reply_to_message_id=message.id
					)
		return wrapper
	return decorator


@app.on_disconnect()
async def handle_disconnect(bot):
	db.close()
	logging.info("Disconnected from Telegram for some reason")
	exit(1)


@on_cmd("authorize", require_auth=False)
async def handle_authorize_command(bot, message, response_msg, text):
	password = os.environ["TELEGRAM_AUTH_CMD_PASS"]
	if db.is_admin(message.from_user.id):
		return "You are already authorized"
	if text == password:
		print(f"Authorizing {message.from_user.id} ({message.from_user.username})")
		db.add_admin(message.from_user.id)
		return "You are now authorized"
	else:
		print(f"Failed authorization attempt from {message.from_user.id} ({message.from_user.username})")


async def do_sql(q):
    result = [None]
    returned = [False]
    def doer():
        if isinstance(q, str):
            result[0] = db.execute_query(q) or ["NO RESULTS"]
        else:
            result[0] = db._conn.execute(q).fetchall()
        returned[0] = True
    t = threading.Thread(target=doer)
    t.start()
    while not returned[0]:
        await asyncio.sleep(1)
    return result[0]


@on_cmd("sql", start_msg="Please wait...")
async def handle_sql_command(bot, message, response_msg, query):
    if query.lower().startswith("nolimit "):
        timeout, limit, query = None, None, query[len("nolimit "):]
    else:
        timeout, limit = 30, 10
    try:
        res = await run_async(lambda: db.execute_query(query, limit=limit), timeout=timeout)
        return "\n".join([str(row) for row in res])
    except TimeoutError:
        return "Query timed out"
    except Exception as e:
        return f"Unknown error occurred while executing query: {str(e)}"


@on_cmd("count", start_msg="Please wait...")
async def handle_count_command(bot, message, response_msg, text):
	n, date = await run_async(lambda: db.count_messages())
	fetched_diff = (datetime.datetime.now() - date).total_seconds()
	return f"Total messages: {n}, last fetched {fetched_diff} seconds ago"


@on_cmd("schema")
async def handle_schema_command(bot, message, response_msg, text):
	res = await run_async(lambda: db.schema)
	s = ""
	for table, rows in res.items():
		rows_s = ", ".join(rows)
		s += f"{table}: {rows_s}\n\n"
	return s.strip()


@on_cmd("test_insert_message")
async def handle_test_insert_message_command(bot, message, response_msg, text):
	db.add_message(message)
	return "Message added"


@on_cmd("id")
async def handle_id_command(bot, message, response_msg, text):
	return str(message.from_user.id)


@on_cmd("check")
async def handle_check_command(bot, message, response_msg, text):
	reply = message.reply_to_message
	if not reply:
		return "Please mark a message to check by replying to it"
	return str(reply)

@on_cmd("search", description="params: textcos/textlike, chat_id/chatname, after, before, limit")
async def handle_sql_embed_prev_command(bot, message, response_msg, text):
	lines = [line.strip() for line in text.split(";;;") if line.strip()]
	params = dict()
	for line in lines:
		# format: key ...value...
		key, value = line.split(" ", 1)
		params[key] = value
	if "textcos" not in params and "textlike" not in params:
		return f"No text in parameters: {lines}"
	message_text = f"Please wait. {params}"
	if model is None:
		message_text += "\n\nLoading model..."
	msg = await bot.send_message(chat_id=message.chat.id, reply_to_message_id=message.id, text=message_text)
	if model is None:
		model = load_embedding_model()
	from sqlalchemy import select
	from sqlalchemy.orm import aliased
	import common.backend.models as models
	try:
		query = select(models.Messages)
		if "chat_id" in params:
			query = query.where(models.Messages.chat_id == params["chat_id"])
		if "chatname" in params:
			chat_alias = aliased(models.Chats)
			chat_ids_subquery = db._conn.query(chat_alias.chat_id).filter(chat_alias.title.like("%" + params["chatname"] + "%"))
			query = query.join(models.Messages.chat_id).filter(models.Messages.chat_id.in_(chat_ids_subquery))
		if "after" in params:
			query = query.where(models.Messages.date > params["after"])
		if "before" in params:
			query = query.where(models.Messages.date < params["before"])
		query = query.where(models.Messages.embedding != None)
		if "textcos" in params:
			embed = model.encode(params["textcos"])
			query = query.order_by(models.Messages.embedding.op('<->')(embed))
		elif "textlike" in params:
			query = query.where(models.Messages.text.like("%" + params["textlike"] + "%"))
		else:
			raise ValueError("No text search method specified - should not have gotten here")
		query = query.limit(params.get("limit", 10))
		# we only want chat_id, date, text
		query = query.with_only_columns(*[models.Messages.chat_id, models.Messages.date, models.Messages.text])
		# now leftjoin with chats to obtain title (won't add any other columns):
		query = query.join(models.Chats, models.Chats.chat_id == models.Messages.chat_id)
		query = query.with_only_columns(*[models.Chats.title, models.Messages.date, models.Messages.text])
		#query = query.with_for_update()
	except Exception as e:
		result_str = f"Error building query: {e}"
		await msg.edit_text(result_str)
		print(f"Error building query: {e}")
		return
	try:
		print("Submitting")
		await msg.edit_text("Working...")
		results = await do_sql(query)
		result_str = "\n".join([str(row) for row in results])
		await msg.edit_text(result_str)
	except Exception as e:
		print(f"Error executing query: {e}, query: {query}")
		result_str = f"Error: {e}"
		await msg.edit_text(result_str)


@on_cmd("test")
async def handle_test_command(bot, message, response_msg, text):
	msg = await bot.send_message(chat_id=message.chat.id, text="Test message")
	import threading
	import time
	async def long_task(msg):
		time.sleep(1)
		await msg.edit_text("Test message edited 1")
		time.sleep(5)
		await msg.edit_text("Test message edited 2")
		time.sleep(5)
		await msg.edit_text("Test message edited 3")
	threading.Thread(target=lambda m: asyncio.run(long_task(m)), args=(msg,)).start()


@on_cmd("confset")
async def handle_confset_command(bot, message, response_msg, text):
	param_name = text.split(" ", 1)[0]
	param_value = text.split(" ", 1)[1]
	if not param_name or not param_value:
		return "Invalid parameters"
	config.set(param_name, param_value)
	return f"Set {param_name} to {param_value}"


@on_cmd("confget")
async def handle_confget_command(bot, message, response_msg, text):
	if text:
		return f"{text}: {config.get(text)}"
	else:
		return str(config.load_config())


@on_cmd("add_channel", description="params: chat_id; add channels to embed_chat_list")
async def handle_add_channel_command(bot, message, response_msg, text):
	if not text:
		return "Please provide a chat_id"
	channels = config.get("embed_chat_list", [])
	channels.append(text)
	config.set("embed_chat_list", channels)
	return f"Added chat {text}; new list: {channels}"


@on_cmd("remove_channel")
async def handle_remove_channel_command(bot, message, response_msg, text):
	if not text:
		return "Please provide a chat_id"
	channels = config.get("embed_chat_list", [])
	channels.remove(text)
	config.set("embed_chat_list", channels)
	return f"Removed chat {text}; new list: {channels}"


@on_cmd("download", description="params: upload? url {audio,video}?; download from youtube/pinterest/twitter")
async def handle_download_command(bot, message, response_msg, text):
	params = text.split(" ", 1)
	print(f"Params: {params}")
	force_upload = (len(params) > 1) and (params[0] == "upload")
	if force_upload:
		print("Forcing upload")
		params = params[1:]
	if len(params) < 1:
		return "Invalid parameters - at least specify URL"
	if "youtube.com" in text or "youtu.be" in text:
		from yt_dlp import YoutubeDL
		if len(params) == 1:
			params.append("video")
		url, media_type = params
		if media_type in {"mp3", "m4a"}:
			media_type = "audio"
		if media_type in {"mp4", "webm"}:
			media_type = "video"
		if media_type not in ["audio", "video"]:
			return "Invalid media type"
		filename_location = f"/downloads/youtube"
		opts = {
			"format": "bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio" if media_type == "audio" else "bestvideo[ext=mp4]+bestaudio[ext=m4a]/mp4",
			"outtmpl": f"{filename_location}/%(id)s.%(ext)s",
			"postprocessors": [{
				'key': 'FFmpegVideoConvertor',
				'preferedformat': 'mp4',  # one of avi, flv, mkv, mp4, ogg, webm
			}],
		}
		if media_type == "audio":
			opts.pop("postprocessors")
		msg = await bot.send_message(chat_id=message.chat.id, reply_to_message_id=message.id, text="Downloading...")
		try:
			def downloader():
				with YoutubeDL(opts) as ydl:
					info_dict = ydl.extract_info(url, download=True)
					video_id = info_dict.get('id', None)
					video_ext = info_dict.get('ext', None)
					filename = f"{filename_location}/{video_id}.{video_ext}"
				return filename
			filename = await run_async(downloader)
			await msg.edit_text(f"https://{os.environ['POSTGRES_HOST_EXTERNAL']}{filename}")
			if force_upload:
				await bot.send_document(chat_id=message.chat.id, reply_to_message_id=message.id, document=filename)
		except Exception as e:
			await msg.edit_text(f"Error downloading: {e}")
		return msg
	elif "pinterest" in text:
		from pinterest import extract_image, download_image
		import uuid
		if len(params) != 1:
			return "Invalid parameters"
		url = params[0]
		image_info = extract_image(url)
		if image_info:
			image_url, _ = image_info
			full_path = run_async(lambda: download_image(f"/downloads/pinterest", image_url, str(uuid.uuid4())))
			if force_upload:
				await bot.send_document(chat_id=message.chat.id, reply_to_message_id=message.id, document=full_path)
			return f"https://{os.environ['POSTGRES_HOST_EXTERNAL']}{full_path}"
		else:
			return f"Could not download image from {url}"
	elif "twitter.com" in text or "t.co" in text or "x.com" in text:
		return "twitter not supported yet"
	else:
		return "Unsupported URL"


@on_cmd("health")
async def handle_health_command(bot, message, response_msg, text):
	from sqlalchemy import select
	from sqlalchemy.orm import aliased
	import common.backend.models as models
	from sqlalchemy import func
	query = select(func.max(models.Messages.date)).select_from(models.Messages).limit(1)
	last_message = await run_async(lambda: db._conn.execute(query).fetchone())
	if last_message is None:
		return "No messages in DB"
	last_message_date = last_message[0]
	diff = (datetime.datetime.now() - last_message_date).total_seconds()
	return f"Last message: {last_message_date}, {diff} seconds ago"


@on_cmd("help")
async def handle_help_command(bot, message, response_msg, text):
	s = "Known commands:\n"
	for cmd in known_cmds:
		cmd_name, cmd_description = cmd
		s += f"/{cmd_name}" + (f" - {cmd_description}" if cmd_description else "") + "\n"
	return s


@app.on_message(filters.all)
async def handle_group_message(bot, message):
	#logging.info(f"Got message in {message.chat.id}")
	if message.chat.id not in group_chats:
		return
	if GroupChatManager.bot_me is None:
		GroupChatManager.bot_me = await app.get_me()
	logging.info("Got message in active group")
	should_summarize = group_chats[message.chat.id].add_message(message)
	if should_summarize:
		logging.info("Processing message")
		try:
			msg, reply_to = await group_chats[message.chat.id].get_response()
		except Exception as e:
			logging.error(f"Error in processing: {e}")
			msg, reply_to = None, None
		logging.info(f"Summary: {msg}")
		if msg:
			await bot.send_message(chat_id=message.chat.id, text=msg, reply_to_message_id=reply_to)
	else:
		logging.info("Not summarizing")


async def main():
	await app.start()
	GroupChatManager.bot_me = await app.get_me()
	logging.info(f"App started as {GroupChatManager.bot_me.first_name} {GroupChatManager.bot_me.last_name} ({GroupChatManager.bot_me.username})")
	logging.info(f"App idling")
	await idle()
	logging.info("App stopping")
	await app.stop()


if __name__ == "__main__":
	logging.info("Entered main prep")
	def signal_handler(sig, frame):
		print("Closing DB")
		db.close()
		print("Closing account")
		app.stop()
		sys.exit(0)
	logging.info("Setting signal handlers")
	signal.signal(signal.SIGINT, signal_handler)
	signal.signal(signal.SIGTERM, signal_handler)
	logging.info("Starting main")
	#asyncio.run(main())
	app.run()
