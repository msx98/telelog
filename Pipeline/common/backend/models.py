from enum import Enum
from sqlalchemy import Column, Integer, String, Boolean, BigInteger, Float, Text, DateTime, CheckConstraint, PickleType, text, select, update, insert, PrimaryKeyConstraint, func
from sqlalchemy.dialects.postgresql import ENUM, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from pgvector.sqlalchemy import Vector

Base = declarative_base()

media_type_enum = ENUM(
    'document', 
    'audio', 
    'voice', 
    'video', 
    'sticker', 
    'animation', 
    'new_chat_photo', 
    'video_note', 
    'photo',
    name='media_type'
)

chat_type_enum = ENUM(
    'private',
    'bot',
    'group',
    'supergroup',
    'channel',
    name='chat_type'
)

class ChatType(Enum):
    private = 'private'
    bot = 'bot'
    group = 'group'
    supergroup = 'supergroup'
    channel = 'channel'

class Chats(Base):
    __tablename__ = 'chats'
    chat_id = Column(BigInteger, nullable=False, primary_key=True)
    top_message_id = Column(BigInteger)
    title = Column(Text)
    first_name = Column(Text)
    last_name = Column(Text)
    username = Column(Text)
    invite_link = Column(Text)
    type = Column(chat_type_enum)
    members_count = Column(Integer, CheckConstraint('members_count >= 0'))
    is_verified = Column(Boolean)
    is_restricted = Column(Boolean)
    is_scam = Column(Boolean)
    is_fake = Column(Boolean)
    is_support = Column(Boolean)
    linked_chat_id = Column(BigInteger)
    phone_number = Column(Text)


class Users(Base):
    __tablename__ = 'users'
    sender_id = Column(BigInteger, nullable=False, primary_key=True)
    first_name = Column(Text)
    last_name = Column(Text)
    username = Column(Text)
    is_verified = Column(Boolean)
    is_restricted = Column(Boolean)
    is_scam = Column(Boolean)
    is_fake = Column(Boolean)
    is_support = Column(Boolean)
    is_bot = Column(Boolean)
    phone_number = Column(Text)


class Messages(Base):
    __tablename__ = 'messages'
    chat_id = Column(BigInteger, primary_key=True)
    message_id = Column(BigInteger, primary_key=True)
    sender_id = Column(BigInteger)
    text = Column(Text)
    embedding = Column(Vector(768))
    date = Column(DateTime)
    views = Column(Integer, CheckConstraint('views>=0'))
    forwards = Column(Integer, CheckConstraint('forwards>=0'))
    forward_from_chat_id = Column(BigInteger)
    forward_from_message_id = Column(BigInteger)
    reply_to_message_id = Column(BigInteger)
    poll_vote_count = Column(Integer, CheckConstraint('poll_vote_count>=0'))
    reactions_vote_count = Column(Integer, CheckConstraint('reactions_vote_count>=0'))
    media_type = Column(media_type_enum)
    file_id = Column(Text)
    file_unique_id = Column(Text)
    has_poll = Column(Boolean)
    has_reactions = Column(Boolean)
    has_text = Column(Boolean)
    has_media = Column(Boolean)


class MessageEmbeddings(Base):
    __tablename__ = 'message_embeddings'

    chat_id = Column(BigInteger, primary_key=True)
    message_id = Column(BigInteger, primary_key=True)
    embedding = Column(Vector(768))


class Reactions(Base):
    __tablename__ = 'reactions'
    chat_id = Column(BigInteger, primary_key=True, nullable=False)
    message_id = Column(BigInteger, primary_key=True, nullable=False)
    reaction_id = Column(Integer, primary_key=True, nullable=False)
    reaction_votes_norm = Column(Float, CheckConstraint('reaction_votes_norm>=0'))
    reaction_votes_abs = Column(Integer, CheckConstraint('reaction_votes_abs>=0'))
    __table_args__ = (
        PrimaryKeyConstraint('chat_id', 'message_id', 'reaction_id'),
    )


class Polls(Base):
    __tablename__ = 'polls'
    chat_id = Column(BigInteger, primary_key=True, nullable=False)
    message_id = Column(BigInteger, primary_key=True, nullable=False)
    poll_option_id = Column(Integer, primary_key=True, nullable=False)
    poll_option_text = Column(String)
    poll_option_votes_norm = Column(Float, CheckConstraint('poll_option_votes_norm>=0'))
    poll_option_votes_abs = Column(Integer, CheckConstraint('poll_option_votes_abs>=0'))
    poll_option_text_embedding = Column(Vector(768))
    __table_args__ = (
        PrimaryKeyConstraint('chat_id', 'message_id', 'poll_option_id'),
    )


class Configurations(Base):
    __tablename__ = 'configurations'
    project = Column(String, primary_key=True)
    key = Column(String, primary_key=True)
    value = Column(PickleType)
    last_updated = Column(DateTime, default=func.now())


class EmojiMap(Base):
    __tablename__ = 'emoji_map'
    reaction_id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    reaction = Column(String, nullable=False, unique=True)
    is_custom = Column(Boolean, nullable=False)
