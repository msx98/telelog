from typing import Tuple, Dict, Any, Optional, List, Set, Iterable, Callable
from collections import OrderedDict, defaultdict
import pyrogram.types
from pyrogram.types import Message, User, Chat, ChatReactions, Reaction, Poll, PollOption, Dialog, ChatPreview, MessageEntity
from pyrogram.enums import ChatType
import sqlalchemy
from sqlalchemy import Connection, create_engine, Table, Column, Integer, String, MetaData, DateTime, Float, Boolean, ForeignKey, Enum, Text, text, select, delete, func, outerjoin
import pgvector
import common.backend.models as models
import threading
import queue
import logging
import asyncio
import time
import datetime
from common.consts import consts
from common.backend.base_backend import BaseBackendWithQueue, MessageQueue, media_type_dict, chat_type_dict, StoredDialog, clean_dict
from common.backend.emoji_map import EmojiMap
import common.backend.models as models
from common.utils import create_postgres_engine
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
import json
import os


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


def compose_insert_message_query(message: Message) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    return compose_insert_message_dict_query(clean_dict(message))


def compose_insert_message_dict_query(message: Dict) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    reactions_list: Dict[int, int] = get_reactions_list(message)
    options_list: Dict[str, int] = get_poll_options(message) if message.get("poll", None) else dict()
    chat_id: int = message.get("chat", {}).get("id", None)
    message_id: int = message.get("id", None)
    if message.get("outgoing", None):
        return
    if chat_id is None or message_id is None:
        return
    sender_id: int = message.get("from_user", {}).get("id", None) if message.get("from_user", None) else \
        (message.get("sender_chat", {}).get("id", None) if message.get("sender_chat", None) else None)
    text: str = get_message_text(message)
    date: datetime.datetime = message.get("date", None)
    views: int = message.get("views", None)
    forwards: int = message.get("forwards", None)
    forward_from_chat_id: int = message.get("forward_from_chat", {}).get("id", None) if message.get("forward_from_chat", None) else \
        (message.get("forward_from", {}).get("id", None) if message.get("forward_from", None) else None)
    forward_from_message_id: int = message.get("forward_from_message_id", None)
    reply_to_message_id: int = message.get("reply_to_message_id", None)
    poll_vote_count: int = sum(options_list.values()) if options_list else None
    reactions_vote_count: int = sum(reactions_list.values()) if reactions_list is not None else None
    media_type, file_id, file_unique_id = get_message_media(message)
    items_messages = [
        dict(
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
        )
    ]
    items_reactions = []
    if reactions_list:
        for r, c in reactions_list.items():
            r_norm = c / reactions_vote_count if reactions_vote_count else 0
            items_reactions.append(dict(
                chat_id=chat_id,
                message_id=message_id,
                reaction_id=r,
                reaction_votes_norm=r_norm,
                reaction_votes_abs=c,
            ))
    items_polls = []
    if options_list:
        for i, opt in enumerate(options_list.items()):
            o, v = opt
            v_norm = v / poll_vote_count if poll_vote_count else 0
            items_polls.append(dict(
                chat_id=chat_id,
                message_id=message_id,
                poll_option_id=i,
                poll_option_text=o,
                poll_option_votes_norm=v_norm,
                poll_option_votes_abs=v
            ))
    items_chats = [compose_insert_chat_dict_query(message["chat"])] if "chat" in message else []
    items_users = [compose_insert_user_dict_query(message["from_user"])] if "from_user" in message else []
    return items_messages, items_reactions, items_polls, items_chats, items_users


def aggregate_chat_dict_queries(chat_id_key, items_chats: List[Dict[str, Any]], available: Set[int]) -> Dict[int, Dict[str, Any]]:
    d: Dict[int, Dict[str, Any]] = defaultdict(dict)
    for item in items_chats:
        if not item:
            continue
        if chat_id_key not in item:
            continue
        if item[chat_id_key] in available:
            continue
        for k, v in item.items():
            if k not in d[item[chat_id_key]] or v is not None:
                d[item[chat_id_key]][k] = v
    return d


def compose_insert_chat_dict_query(chat: Dict) -> Dict[str, Any]:
    chat_type = None
    if "type" in chat:
        chat_type = chat["type"].lower()
    elif "chat" in chat and "type" in chat["chat"]:
        chat_type = chat["chat"]["type"].lower()
    else:
        pass
    chat_dict = dict(
        chat_id=chat.get("id", None),
        title=chat.get("title", None),
        top_message_id=None,
        next_top_message_id=None,
        first_name=chat.get("first_name", None),
        last_name=chat.get("last_name", None),
        username=chat.get("username", None),
        invite_link=chat.get("invite_link", None),
        type=chat_type,
        members_count=chat.get("members_count", None),
        is_verified=chat.get("is_verified", None),
        is_restricted=chat.get("is_restricted", None),
        is_scam=chat.get("is_scam", None),
        is_fake=chat.get("is_fake", None),
        is_support=chat.get("is_support", None),
        linked_chat_id=chat.get("linked_chat", {}).get("id", None) if chat.get("linked_chat", None) else None,
        phone_number=chat.get("phone_number", None),
    )
    if "chat_id" not in chat_dict:
        return
    return chat_dict


def compose_insert_user_dict_query(chat: Dict) -> Dict[str, Any]:
    chat_dict = dict(
        sender_id=chat.get("id", None),
        first_name=chat.get("first_name", None),
        last_name=chat.get("last_name", None),
        username=chat.get("username", None),
        is_verified=chat.get("is_verified", None),
        is_restricted=chat.get("is_restricted", None),
        is_scam=chat.get("is_scam", None),
        is_fake=chat.get("is_fake", None),
        is_support=chat.get("is_support", None),
        is_bot=chat.get("is_bot", None),
        phone_number=chat.get("phone_number", None),
    )
    if "sender_id" not in chat_dict:
        return
    return chat_dict


def get_reactions_list(message: Dict) -> Dict[int, int]:
    if not EmojiMap.emoji_map:
        EmojiMap._reload_map()
    reactions = message.get("reactions", None)
    i = 0
    if reactions is None:
        return None
    while (not isinstance(reactions, list)) and reactions.get("reactions", False) and i < 5:
        reactions = reactions.get("reactions", None)
        i += 1
    if not isinstance(reactions, list):
        return None # FIXME report error
    return {EmojiMap.to_int(r.get("emoji", r.get("custom_emoji_id", None))): r.get("count", None)
            for r in reactions}


def get_poll_options(message: Dict) -> Dict[str, int]:
    if not message.get("poll", None):
        return dict()
    poll: Dict = message["poll"]
    if not isinstance(poll["options"], list):
        return dict()
    return {o["text"]: o["voter_count"] for o in poll["options"]}


def get_message_text(message: Dict) -> str:
    if message.get("text", None):
        if isinstance(message["text"], str):
            return message["text"]
        elif message["text"].get("markdown", False):
            return message["text"]["markdown"]
    elif message.get("caption", None):
        if isinstance(message["caption"], str):
            return message["caption"]
        elif message["text"].get("caption", False):
            return message["caption"]["markdown"]
    elif message.get("poll", None):
        return message["poll"]["question"]
    else:
        return None


def get_message_media(message: Dict) -> Tuple[str, str]:
    """ returns (media_type, file_id) """
    if 'document' in message:
        return "document", message['document']['file_id'], message['document']['file_unique_id']
    elif 'audio' in message:
        return "audio", message['audio']['file_id'], message['audio']['file_unique_id']
    elif 'voice' in message:
        return "voice", message['voice']['file_id'], message['voice']['file_unique_id']
    elif 'video' in message:
        return "video", message['video']['file_id'], message['video']['file_unique_id']
    elif 'sticker' in message:
        return "sticker", message['sticker']['file_id'], message['sticker']['file_unique_id']
    elif 'animation' in message:
        return "animation", message['animation']['file_id'], message['animation']['file_unique_id']
    elif 'video_note' in message:
        return "video_note", message['video_note']['file_id'], message['video_note']['file_unique_id']
    elif 'photo' in message:
        return "photo", message['photo']['file_id'], message['photo']['file_unique_id']
    elif 'new_chat_photo' in message:
        return "new_chat_photo", message['new_chat_photo']['file_id'], message['new_chat_photo']['file_unique_id']
    else:
        return None, None, None


from sqlalchemy import inspect, UniqueConstraint, text
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


class PostgresBackend(BaseBackendWithQueue):
    _conn: Connection

    def __init__(
        self,
        name = "postgres",
        *,
        model = None,
        **kwargs,
    ):
        self._engine = create_postgres_engine()
        self._conn: Connection = self.new_connection()
        self._model = model
        super().__init__(name, **kwargs)
        logging.info(f"Connected to Postgres at {self._engine.url}")
        self._known_chats: Set[int] = set()

    def new_connection(self) -> Connection:
        return self._engine.connect()
    
    def _commit(self):
        self._conn.connection.commit()
    
    @staticmethod
    def create_db(
        *,
        host,
        port,
        user,
        password,
        database,
        schema_file = None,
    ):
        assert (schema_file is None) or (schema_file.endswith(".sql") and os.path.exists(schema_file))
        try:
            import psycopg2
            con = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database="postgres"
            )
            con.autocommit = True
            cur = con.cursor()
            cur.execute(psycopg2.sql.SQL('CREATE DATABASE {};').format(
                psycopg2.sql.Identifier(database)))
            cur.close()
            con.close()
        except:
            pass
        try:
            con = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )
            cur = con.cursor()
            if schema_file:
                with open(schema_file) as f:
                    cur.execute(f.read())
            cur.close()
            con.commit()
            con.close()
        except Exception as e:
            pass

    @property
    def is_connected(self):
        return (isinstance(self._conn, Connection) and self._conn.closed == 0)
    
    def add_channel(self, dialog: Dialog|Chat|ChatPreview|List[Dialog|Chat|ChatPreview]):
        dialog_list: List[Dialog|Chat|ChatPreview] = []
        if isinstance(dialog, (Dialog, Chat, ChatPreview)):
            dialog_list.append(dialog)
        elif isinstance(dialog, list):
            dialog_list = dialog
        else:
            raise ValueError(f"Incorrect dialog type: {type(dialog)}")
        dialog_query_list = []
        for dialog in dialog_list:
            if isinstance(dialog, Dialog):
                chat: Chat = dialog.chat
                d = compose_insert_chat_dict_query(clean_dict(chat))
            elif isinstance(dialog, Chat):
                chat: Chat = dialog
                d = compose_insert_chat_dict_query(clean_dict(chat))
            elif isinstance(dialog, ChatPreview):
                chat: ChatPreview = dialog
                d = compose_insert_chat_dict_query(clean_dict(chat))
            else:
                raise ValueError(f"Incorrect dialog type: {type(dialog)}")
            dialog_query_list.append(d)
        with Session(self._engine) as sess:
            upsert(sess, models.Chats, dialog_query_list)
            sess.flush()
            sess.commit()
    
    def set_top_message_id_to_db_max(self, channel_id: int):
        self.add_message(lambda: self._set_top_message_id_to_db_max(channel_id))
    
    def _set_top_message_id_to_db_max(self, channel_id: int):
        #logging.info(f"Waiting for queue to flush once before setting {channel_id} top_message_id to max")
        #await self.wait_for_queue_flush_batch()
        with Session(self._engine) as sess:
            stmt = (
                select(func.max(models.Messages.message_id))
                .where(models.Messages.chat_id == channel_id)
            )
            top_message_id = sess.execute(stmt).scalar()
            if top_message_id is None:
                return
        self.set_top_message_id(channel_id, top_message_id, skip_check=True)
    
    def set_top_message_id(self, channel_id: int, top_message_id: int, skip_check: bool = False):
        """
        Announce that the DB contains all available messages in range [0, top_message_id]
        """
        with Session(self._engine) as sess:
            if not skip_check:
                # first, ensure that there are no messages with message_id > top_message_id
                stmt = (
                    sess.query(models.Messages)
                    .filter(models.Messages.chat_id == channel_id)
                    .filter(models.Messages.message_id > top_message_id)
                    .limit(1)
                )
                rows = stmt.all()
                if rows:
                    row = rows[0]
                    raise RuntimeError(f"Cannot set top_message_id to {top_message_id} for chat {channel_id}: there are messages with message_id > {top_message_id}, such as {row.message_id}")
            # now we can insert
            sess.execute(
                insert(models.Chats)
                .values(dict(chat_id=channel_id, top_message_id=top_message_id))
                .on_conflict_do_update(
                    index_elements=["chat_id"],
                    set_=dict(top_message_id=top_message_id)
                )
            )
            sess.flush()
            sess.commit()

    def set_ongoing_write(self, channel_id: int, ongoing_write: bool):
        with Session(self._engine) as sess:
            sess.execute(
                insert(models.Chats)
                .values(dict(chat_id=channel_id, ongoing_write=ongoing_write))
                .on_conflict_do_update(
                    index_elements=["chat_id"],
                    set_=dict(ongoing_write=ongoing_write)
                )
            )
            sess.flush()
            sess.commit()

    def get_offsets_from_ongoing_writes_reactions(self) -> Dict[int, Optional[int]]:
        with Session(self._engine) as sess:
            stmt = (
                select(
                    models.Chats.chat_id, 
                    select(func.min(models.Reactions.message_id))
                        .where(models.Reactions.chat_id == models.Chats.chat_id)
                        .where(
                            (models.Chats.top_message_id.is_(None))
                            | (models.Reactions.message_id > models.Chats.top_message_id)
                        )
                        .label("min_message_id")
                )
                .select_from(models.Chats)
                # ongoing_write is just an indicator - not actually necessary for correctness
                # was originally here for performance, but turns out it's not necessary for that either
                #.where(models.Chats.ongoing_write == True)
            )
            rows = {row[0]: row[1] for row in sess.execute(stmt).fetchall() if row[1] is not None}
        return rows

    def get_offsets_from_ongoing_writes(self) -> Dict[int, Optional[int]]:
        with Session(self._engine) as sess:
            stmt = (
                select(
                    models.Chats.chat_id, 
                    select(func.min(models.Messages.message_id))
                        .where(models.Messages.chat_id == models.Chats.chat_id)
                        .where(
                            (models.Chats.top_message_id.is_(None))
                            | (models.Messages.message_id > models.Chats.top_message_id)
                        )
                        .label("min_message_id")
                )
                .select_from(models.Chats)
                # ongoing_write is just an indicator - not actually necessary for correctness
                # was originally here for performance, but turns out it's not necessary for that either
                #.where(models.Chats.ongoing_write == True)
            )
            rows = {row[0]: row[1] for row in sess.execute(stmt).fetchall() if row[1] is not None}
        return rows

    
    def delete_messages(self, channel_id: int, id_min: int, id_max: int):
        assert id_min <= id_max
        with self.lock:
            cur = self._conn.connection.cursor()
            cur.execute(f"DELETE FROM {TableNames.POLLS} WHERE chat_id = %s AND message_id >= %s AND message_id <= %s", (channel_id, id_min, id_max))
            cur.execute(f"DELETE FROM {TableNames.REACTIONS} WHERE chat_id = %s AND message_id >= %s AND message_id <= %s", (channel_id, id_min, id_max))
            cur.execute(f"DELETE FROM {TableNames.MESSAGES} WHERE chat_id = %s AND message_id >= %s AND message_id <= %s", (channel_id, id_min, id_max))
            self._commit()
            cur.close()

    def delete_channel(self, channel_id: int):
        assert self.selected_channel is None
        with self.lock:
            cur = self._conn.connection.cursor()
            cur.execute(f"DELETE FROM {TableNames.POLLS} WHERE chat_id = %s", (channel_id,))
            cur.execute(f"DELETE FROM {TableNames.REACTIONS} WHERE chat_id = %s", (channel_id,))
            cur.execute(f"DELETE FROM {TableNames.MESSAGES} WHERE chat_id = %s", (channel_id,))
            cur.execute(f"DELETE FROM {TableNames.CHATS} WHERE chat_id = %s", (channel_id,))
            self._commit()
            cur.close()

    def get_stored_dialogs(self) -> Dict[int, StoredDialog]:
        cur = self._conn.connection.cursor()
        cur.execute(f"""
                    SELECT {TableNames.CHATS}.chat_id, {TableNames.CHATS}.title, subquery.max_message_id
                    FROM (
                        SELECT chat_id, MAX(message_id) AS max_message_id FROM {TableNames.MESSAGES} GROUP BY chat_id
                    ) AS subquery
                    LEFT JOIN {TableNames.CHATS} ON subquery.chat_id = chats.chat_id
                    """)
        d = dict()
        for row in cur.fetchall():
            if row[0] is None:
                continue
            d[row[0]] = StoredDialog(title=row[1], max_id=row[2])
            assert d[row[0]].max_id is not None
        cur.close()
        self._stored_dialogs = d
        with Session(self._engine) as sess:
            self._known_chats |= {c.sender_id for c in sess.query(models.Users.chat_id).all()}
        return d
    
    def get_chats_with_reactions_bak(self) -> Set[int]:
        cur = self._conn.connection.cursor()
        cur.execute(f"SELECT DISTINCT chat_id FROM reactions_bak")
        d = set()
        for row in cur.fetchall():
            d.add(row[0])
        cur.close()
        return d

    def get_stored_dialogs_committed(self) -> Dict[int, StoredDialog]:
        # Return: d[channel_id] = (channel_name, max_message_id)
        cur = self._conn.connection.cursor()
        cur.execute(f"SELECT chat_id, title, top_message_id, username, invite_link FROM {TableNames.CHATS}")
        d = dict()
        for row in cur.fetchall():
            d[row[0]] = StoredDialog(title=row[1], max_id=row[2], username=row[3], invite_link=row[4])
            #assert d[row[0]].max_id is not None
        cur.close()
        self._stored_dialogs = d
        self._known_chats |= set(d.keys())
        return d

    def find_channel(self, search_str: str) -> List[Dict[str, Any]]:
        cur = self._conn.connection.cursor()
        cur.execute(f"SELECT * FROM {TableNames.CHATS} WHERE chat_name LIKE %s", (f"%{search_str}%",))
        dialogs = cur.fetchall()
        cur.close()
        return dialogs

    def count_messages(self) -> Tuple[int, datetime.datetime]:
        cur = self._conn.connection.cursor()
        cur.execute(f"SELECT COUNT(*), MAX(date) FROM {TableNames.MESSAGES}")
        res = cur.fetchone()
        cur.close()
        n, maxdate = res[0], res[1]
        return n, maxdate
    
    @property
    def schema(self) -> Dict[str, List[str]]:
        if hasattr(self, "_schema"):
            return self._schema
        results = self.execute_query("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public'", limit=None)
        schema = dict()
        for row in results:
            table_name, column_name = row
            if table_name not in schema:
                schema[table_name] = []
            schema[table_name].append(column_name)
        schema = {k:v for k,v in schema.items() if k in {
            "messages", "chats", "reactions", "polls"
        }}
        self._schema = schema
        return schema

    def execute_query(self, query: str, *, limit: int = 10) -> List[Tuple[Any]]:
        conn = self.new_connection()
        cur = conn.connection.cursor()
        try:
            cur.execute(query)
            if limit is not None:
                rows = cur.fetchmany(limit)
            else:
                rows = cur.fetchall()
        except SyntaxError as e:
            return (e.pgcode, e.pgerror)
        except Exception as e:
            return str(e)
        cur.close()
        conn.close()
        return rows

    def count_dialogs(self) -> int:
        cur = self._conn.connection.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {TableNames.CHATS}")
        n = cur.fetchone()[0]
        cur.close()
        return n

    def add_messages(self, messages: Iterable[Message|Callable]):
        if not messages:
            return
        callables = [m for m in messages if not isinstance(m, Message)] # can use callable() too I guess
        messages = [m for m in messages if isinstance(m, Message)]
        items_messages, items_reactions, items_polls, items_chats, items_users = [], [], [], [], []
        for i in range(len(messages)):
            normalized = compose_insert_message_query(messages[i])
            if not normalized:
                continue
            items_messages_add, items_reactions_add, items_polls_add, items_chats_add, items_users_add = normalized
            items_messages += items_messages_add
            items_reactions += items_reactions_add
            items_polls += items_polls_add
            items_chats += items_chats_add
            items_users += items_users_add
        items_chats_dict = aggregate_chat_dict_queries("chat_id", items_chats, self._known_chats)
        items_users_dict = aggregate_chat_dict_queries("sender_id", items_users, self._known_chats)
        logging.info(f"items_chats_dict: {len(items_chats)}->{len(items_chats_dict)}, items_users_dict: {len(items_users)}->{len(items_users_dict)}")
        new_chats = set(items_chats_dict.keys()) | set(items_users_dict.keys())
        update_count_messages, update_count_reactions, update_count_polls, update_count_chats = 0, 0, 0, 0
        with Session(self._engine) as sess:
            if items_messages:
                update_count_messages += upsert(sess, models.Messages, items_messages).rowcount
            if items_reactions:
                update_count_reactions += upsert(sess, models.Reactions, items_reactions).rowcount
            if items_polls:
                update_count_polls += upsert(sess, models.Polls, items_polls).rowcount
            if items_chats_dict:
                update_count_chats += upsert(sess, models.Chats, list(items_chats_dict.values())).rowcount
            if items_users_dict:
                update_count_chats += upsert(sess, models.Users, list(items_users_dict.values())).rowcount
            sess.flush()
            sess.commit()
        update_count = update_count_messages + update_count_reactions + update_count_polls + update_count_chats
        self._known_chats |= new_chats
        if update_count:
            logging.info(f"Inserted {update_count} items, of which: {update_count_messages} messages, {update_count_reactions} reactions, {update_count_polls} polls, {update_count_chats} chats")
        for c in callables:
            c()
        logging.info(f"Called {len(callables)} callables")
