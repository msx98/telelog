from typing import Tuple, Dict, Any, Optional, List
from collections import OrderedDict
import pyrogram.types
from pyrogram.types import Message, User, Chat, ChatReactions, Reaction, Poll, PollOption, Dialog
import psycopg2
from psycopg2.extensions import connection as PostgresConnection
import threading
import queue
import time
import datetime
from consts import *
from base_backend import BaseBackendWithQueue, MessageQueue, media_type_dict, chat_type_dict, StoredDialog
from emoji_map import EmojiMap
import json


class TableNames:
    MESSAGES = "messages"
    CHATS = "chats"
    REACTIONS = "reactions"
    POLLS = "polls"


def create_dict_insert_query(*, table_name, values, on_conflict_keys=[], on_conflict_update_keys = []) -> Tuple[str, List]:
    keys, values = zip(*values.items())
    if on_conflict_update_keys == "*":
        on_conflict_update_keys = [k for k in keys if k not in on_conflict_keys]
    on_conflict_update_keys = [k for k in on_conflict_update_keys if (k in keys) and (k not in on_conflict_keys)]
    columns = ', '.join(keys)
    placeholders = ', '.join(['%s'] * len(keys))
    query = f"""INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"""
    if on_conflict_update_keys:
        assert on_conflict_keys
    if on_conflict_keys:
        assert on_conflict_update_keys
        on_conflict_keys_str = ', '.join(on_conflict_keys)
        if on_conflict_update_keys == "*":
            on_conflict_update_keys = [k for k in keys if k not in on_conflict_keys]
        assert on_conflict_update_keys
        query += \
            f" ON CONFLICT ({on_conflict_keys_str}) DO UPDATE SET " \
            + (', '.join([f"{key} = EXCLUDED.{key}" for key in on_conflict_update_keys]))
    return query, values


def perform_insert(cur, *, table_name, values, on_conflict_keys=[], on_conflict_update_keys = []):
    query, values = create_dict_insert_query(table_name=table_name, values=values, on_conflict_keys=on_conflict_keys, on_conflict_update_keys=on_conflict_update_keys)
    cur.execute(query, values)


def perform_insert_message(cur, message: Message):
    reactions_list: Dict[int, int] = get_reactions_list(message)
    options_list: Dict[str, int] = get_poll_options(message) if message.poll else dict()
    chat_id: int = message.chat.id
    message_id: int = message.id
    if message.outgoing:
        return
    if chat_id is None or message_id is None:
        return
    sender_id: int = message.from_user.id if message.from_user else \
        (message.sender_chat.id if message.sender_chat else None)
    text: str = get_message_text(message)
    date: datetime.datetime = message.date
    views: int = message.views
    forwards: int = message.forwards
    forward_from_chat_id: int = message.forward_from_chat.id if message.forward_from_chat else \
        (message.forward_from.id if message.forward_from else None)
    forward_from_message_id: int = message.forward_from_message_id
    reply_to_message_id: int = message.reply_to_message_id
    poll_vote_count: int = sum(options_list.values()) if options_list else None
    reactions_vote_count: int = sum(get_reactions_list(message).values())
    media_type, file_id, file_unique_id = get_message_media(message)
    perform_insert(
        cur,
        table_name=TableNames.MESSAGES,
        values=OrderedDict(
            chat_id=chat_id,
            message_id=message_id,
            sender_id=sender_id,
            text=text,
            date=date,
            views=views,
            forwards=forwards,
            forward_from_chat_id=forward_from_chat_id,
            forward_from_message_id=forward_from_message_id,
            reply_to_message_id=reply_to_message_id,
            poll_vote_count=poll_vote_count,
            reactions_vote_count=reactions_vote_count,
            media_type=media_type,
            file_id=file_id,
            file_unique_id=file_unique_id
        ),
        on_conflict_keys=["chat_id", "message_id"],
        on_conflict_update_keys="*",
    )
    for r, c in reactions_list.items():
        r_norm = c / reactions_vote_count if reactions_vote_count else 0
        perform_insert(
            cur,
            table_name=TableNames.REACTIONS,
            values=OrderedDict(
                chat_id=chat_id,
                message_id=message_id,
                reaction_id=r,
                reaction_votes_norm=r_norm,
                reaction_votes_abs=c
            ),
            on_conflict_keys=["chat_id", "message_id", "reaction_id"],
            on_conflict_update_keys="*",
        )
    for o, v in options_list.items():
        v_norm = v / poll_vote_count if poll_vote_count else 0
        perform_insert(
            cur,
            table_name=TableNames.POLLS,
            values=OrderedDict(
                chat_id=chat_id,
                message_id=message_id,
                poll_option_id=o,
                poll_option_text=o,
                poll_option_votes_norm=v_norm,
                poll_option_votes_abs=v
            ),
            on_conflict_keys=["chat_id", "message_id", "poll_option_id"],
            on_conflict_update_keys="*",
        )


def perform_insert_dialog(cur, dialog: Dialog, update_top_message_id: bool = False):
    chat: Chat = dialog.chat
    if chat is None:
        return
    chat_dict = dict(
        chat_id = chat.id,
        top_message_id = dialog.top_message.id if dialog.top_message else None,
        title = chat.title,
        first_name = chat.first_name,
        last_name = chat.last_name,
        username = chat.username,
        invite_link = chat.invite_link,
        chat_type = chat_type_dict[chat.type],
        members_count = chat.members_count,
        is_verified = chat.is_verified,
        is_restricted = chat.is_restricted,
        is_scam = chat.is_scam,
        is_fake = chat.is_fake,
        is_support = chat.is_support,
        linked_chat_id = chat.linked_chat.id if chat.linked_chat else None
    )
    for k, v in chat_dict.items():
        if v is None:
            chat_dict.pop(k)
    if "chat_id" not in chat_dict:
        return
    if not update_top_message_id:
        chat_dict.pop("top_message_id", None)
    if len(chat_dict) == 1:
        return
    perform_insert(
        cur,
        table_name=TableNames.CHATS,
        values=chat_dict,
        on_conflict_keys=["chat_id"],
        on_conflict_update_keys="*",
    )


def perform_update_top_message_id(cur, chat_id: int, top_message_id: int):
    perform_insert(
        cur,
        table_name=TableNames.CHATS,
        values=dict(
            chat_id=chat_id,
            top_message_id=top_message_id
        ),
        on_conflict_keys=["chat_id"],
        on_conflict_update_keys=["top_message_id"],
    )


def get_reactions_list(message: Message) -> Dict[int, int]:
    reactions = message.reactions
    i = 0
    while (not isinstance(reactions, list)) and hasattr(reactions, "reactions") and i < 5:
        reactions = getattr(reactions, "reactions", None)
        i += 1
    if not isinstance(reactions, list):
        return None # FIXME report error
    return {EmojiMap.to_int(r.emoji): r.count for r in reactions}


def get_poll_options(message: Message) -> Dict[str, int]:
    if not message.poll:
        return dict()
    poll: Poll = message.poll
    if not isinstance(poll.options, list):
        return dict()
    return {o.text: o.voter_count for o in poll.options}


def get_message_text(message: Message) -> str:
    if message.text:
        return message.text
    elif message.caption:
        return message.caption
    elif message.poll:
        return message.poll.question
    else:
        return None


def get_message_media(message: Message) -> Tuple[str, str]:
    """ returns (media_type, file_id) """
    if message.document:
        return "document", message.document.file_id, message.document.file_unique_id
    elif message.audio:
        return "audio", message.audio.file_id, message.audio.file_unique_id
    elif message.voice:
        return "voice", message.voice.file_id, message.voice.file_unique_id
    elif message.video:
        return "video", message.video.file_id, message.video.file_unique_id
    elif message.sticker:
        return "sticker", message.sticker.file_id, message.sticker.file_unique_id
    elif message.animation:
        return "animation", message.animation.file_id, message.animation.file_unique_id
    elif message.video_note:
        return "video_note", message.video_note.file_id, message.video_note.file_unique_id
    elif message.photo:
        return "photo", message.photo.file_id, message.photo.file_unique_id
    elif message.new_chat_photo:
        return "new_chat_photo", message.new_chat_photo.file_id, message.new_chat_photo.file_unique_id
    else:
        return None, None, None


class PostgresBackend(BaseBackendWithQueue):
    _conn: PostgresConnection

    def __init__(
        self,
        *,
        host = None,
        port = None,
        user = None,
        password = None,
        database = None,
        **kwargs,
    ):
        self._conn: PostgresConnection = psycopg2.connect(
            host = host or POSTGRES_HOST,
            port = port or POSTGRES_PORT,
            user = user or POSTGRES_USER,
            password = password or POSTGRES_PASSWORD,
            database = database or POSTGRES_DB,
        )
        super().__init__(**kwargs)
        print(f"Connected to Postgres at {host}:{port}")

    @property
    def is_connected(self):
        return (isinstance(self._conn, PostgresConnection) and self._conn.status == psycopg2.extensions.STATUS_READY)
    
    def add_channel(self, dialog: Dialog):
        with self.lock:
            cur = self._conn.cursor()
            perform_insert_dialog(cur, dialog, update_top_message_id=True)
            self._conn.commit()
            cur.close()
    
    def delete_messages(self, channel_id: int, id_min: int, id_max: int):
        with self.lock:
            cur = self._conn.cursor()
            cur.execute(f"DELETE FROM {TableNames.MESSAGES} WHERE chat_id = %s AND message_id >= %s AND message_id <= %s", (channel_id, id_min, id_max))
            self._conn.commit()
            cur.close()

    def delete_channel(self, channel_id: int):
        assert self.selected_channel is None
        with self.lock:
            cur = self._conn.cursor()
            cur.execute(f"DELETE FROM {TableNames.MESSAGES} WHERE chat_id = %s", (channel_id,))
            cur.execute(f"DELETE FROM {TableNames.CHATS} WHERE chat_id = %s", (channel_id,))
            self._conn.commit()
            cur.close()

    def get_stored_dialogs(self) -> Dict[int, StoredDialog]:
        cur = self._conn.cursor()
        cur.execute(f"""
                    SELECT chat_id, chat_name, max(message_id)
                    FROM {TableNames.MESSAGES}
                    LEFT JOIN {TableNames.CHATS} ON {TableNames.MESSAGES}.chat_id = {TableNames.CHATS}.chat_id
                    GROUP BY chat_id
                    """)
        d = dict()
        for row in cur.fetchall():
            d[row[0]] = StoredDialog(title=row[1], max_id=row[2])
        cur.close()
        self._stored_dialogs = d
        return d

    def get_stored_dialogs_fast(self) -> Dict[int, StoredDialog]:
        # Return: d[channel_id] = (channel_name, max_message_id)
        cur = self._conn.cursor()
        cur.execute(f"SELECT chat_id, chat_name, top_message_id FROM {TableNames.CHATS}")
        d = dict()
        for row in cur.fetchall():
            d[row[0]] = StoredDialog(title=row[1], max_id=row[2])
        cur.close()
        self._stored_dialogs = d
        return d

    def find_channel(self, search_str: str) -> List[Dict[str, Any]]:
        cur = self._conn.cursor()
        cur.execute(f"SELECT * FROM {TableNames.CHATS} WHERE chat_name LIKE %s", (f"%{search_str}%",))
        dialogs = cur.fetchall()
        cur.close()
        return dialogs

    def count_messages(self) -> int:
        cur = self._conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {TableNames.MESSAGES}")
        n = cur.fetchone()[0]
        cur.close()
        return n

    def count_dialogs(self) -> int:
        cur = self._conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {TableNames.CHATS}")
        n = cur.fetchone()[0]
        cur.close()
        return n

    def _add_messages(self, messages):
        #print(f"Inserting {len(messages)} messages")
        cur = self._conn.cursor()
        for message in messages:
            perform_insert_message(cur, message)
        self._conn.commit()
        cur.close()
        print(f"Inserted {len(messages)} messages")
