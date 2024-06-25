from typing import List
from collections import deque
import os
import time
from pyrogram.types import Message
from common.utils import run_async
import typing_extensions as typing
import threading
from deep_translator import GoogleTranslator
import json
#os.chdir("Pipeline")


safety_settings=[
	{
		"category": "HARM_CATEGORY_HARASSMENT",
		"threshold": "BLOCK_NONE",
	},
	{
		"category": "HARM_CATEGORY_HATE_SPEECH",
		"threshold": "BLOCK_NONE",
	},
	{
		"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
		"threshold": "BLOCK_NONE",
	},
	{
		"category": "HARM_CATEGORY_DANGEROUS_CONTENT",
		"threshold": "BLOCK_NONE",
	},
]


import google.generativeai as genai
genai.configure(api_key=os.environ["GEMINI_API_KEY"])
model = genai.GenerativeModel("gemini-1.5-pro")


class TemplateLoader:
	def __init__(self, name: str):
		self.name = name
		try:
			with open(f"ChatBot/template_{name}.txt", "r") as f:
				self.text = f.read()
		except:
			with open(f"Pipeline/ChatBot/template_{name}.txt", "r") as f:
				self.text = f.read()
	
	def apply(self, **kwargs):
		return self.text.format(**kwargs)


class Template:
	Message = TemplateLoader("message")
	PromptParticipate = TemplateLoader("prompt_participate")
	PromptSummary = TemplateLoader("prompt_summary")


def message_to_prompt(message: Message, message_text: str = None) -> str:
	if message.from_user:
		sender_name = message.from_user.first_name + (f" {message.from_user.last_name}" if message.from_user.last_name else "")
	else:
		sender_name = "UNKNOWN"
	if message.reply_to_message and message.reply_to_message.from_user:
		responds_to_sender = message.reply_to_message.from_user.first_name + (f" {message.reply_to_message.from_user.last_name}" if message.reply_to_message.from_user.last_name else "")
	else:
		responds_to_sender = "None"
	if message.reply_to_message:
		responds_to_message_id = message.reply_to_message.id
	else:
		responds_to_message_id = "None"
	if not message_text:
		message_text = message.text
	if not message_text:
		message_text = "Message has no content (possibly media?)"
	return Template.Message.apply(message_id=message.id, message_text=message_text, sender_name=sender_name, responds_to_sender=responds_to_sender, responds_to_message_id=responds_to_message_id)


class Response(typing.TypedDict):
	should_participate: bool
	response_text: typing.Optional[str]
	reply_to_message_id: typing.Optional[int]


class GroupChatManager:
	bot_me = None
	def __init__(self, chat_id):
		self.chat_id = chat_id
		self.lock = threading.Lock()
		self.messages = deque()
		self.last_summary = None
		self.last_summary_heb = None
		self.last_summary_time = 0
		self.summary_min_interval = 10
		self.summary_job = None
		self._chat = None
		self.start_summary_job()
	
	@property
	def chat(self):
		if not self._chat:
			self._chat = model.start_chat(history=[])
		return self._chat

	@property
	def my_id(self) -> int:
		assert self.bot_me is not None
		return self.bot_me.id
	
	@property
	def my_name(self) -> str:
		assert self.bot_me is not None
		return self.bot_me.first_name + (f" {self.bot_me.last_name}" if self.bot_me.last_name else "")
	
	def add_message(self, message: Message) -> bool:
		self.messages.append(message)
		msg_threshold, time_threshold = 1, self.summary_min_interval
		if message.reply_to_message and message.reply_to_message.from_user.id == self.my_id:
			msg_threshold, time_threshold = 1, 1
		next_time = self.last_summary_time + time_threshold
		if len(self.messages) >= msg_threshold and time.time() >= next_time:
			return True
		else:
			return False
	
	def get_prompt_summary(self, messages):
		l = []
		name = self.my_name
		l.append(Template.PromptSummary.apply(name=name))
		if self.last_summary:
			l.append(self.last_summary)
		for message in messages:
			l.append(message_to_prompt(message, message_text=GoogleTranslator(target_language="en").translate(message.text)))
		return l

	def summarize(self, threshold_trigger=10, keep_after_summary=5):
		assert threshold_trigger > keep_after_summary
		if len(self.messages) < threshold_trigger:
			return
		messages = []
		with self.lock:
			while len(self.messages) > keep_after_summary:
				messages.append(self.messages.popleft())
		summary_prompt = self.get_prompt_summary(messages)
		summary = model.generate_content(summary_prompt, safety_settings=safety_settings)
		last_summary_heb = GoogleTranslator(target_language="he").translate(summary)
		with self.lock:
			self.last_summary = summary
			self.last_summary_heb = last_summary_heb
			self.last_summary_time = time.time()
	
	def start_summary_job(self):
		assert self.summary_job is None
		def f():
			while True:
				self.summarize()
				time.sleep(1)
		self.summary_job = threading.Thread(target=f)
		self.summary_job.start()
	
	def get_prompt_participate(self):
		l = []
		name = self.my_name
		l.append(Template.PromptParticipate.apply(name=name))
		with self.lock:
			if self.last_summary_heb:
				l.append(self.last_summary_heb)
			for message in self.messages:
				l.append(message_to_prompt(message))
		return l

	def parse_response(self, response):
		response = response.strip()
		d = {"summary": None, "msg": None, "reply_to": None}
		if not response:
			return d
		if "<SUMMARY>" in response:
			start = response.find("<SUMMARY>") + len("<SUMMARY>")
			end = response.find("</SUMMARY>")
			if start != -1 and end != -1:
				d["summary"] = response[start:end]
		if "<MSG" in response:
			start = response.find("<MSG")
			tags_start = start + len("<MSG")
			tags_end = response.find(">", tags_start)
			if "ID=" in response[tags_start:tags_end]:
				d["reply_to"] = int(response[tags_start:tags_end].split("ID='")[1].split("'")[0])
			msg_end = response.find("</MSG>")
			msg_start = tags_end + 1
			msg = response[msg_start:msg_end]
			d["msg"] = msg
		return d

	async def translate_async(self, query: str|List[str], target_language: str) -> str:
		if isinstance(query, str):
			query = [query]
		return await run_async(lambda: GoogleTranslator(target_language=target_language).translate_batch(query))
	
	async def get_response(self):
		response = await model.generate_content_async(
			self.get_prompt_participate(),
			safety_settings=safety_settings,
			generation_config=genai.GenerationConfig(
				response_mime_type="application/json",
				response_schema = Response,
			)
		)
		try:
			response = json.loads(response.text)
		except:
			return None, None
		if not response.get("should_participate", None):
			return None, None
		return response.get("response_text", None), response.get("reply_to_message_id", None)


group_chats = {
	chat_id: GroupChatManager(chat_id)
	for chat_id in {int(x) for x in os.environ["TELEGRAM_CHATBOT_ACTIVE_GROUPS"].split(",") if x}
}
