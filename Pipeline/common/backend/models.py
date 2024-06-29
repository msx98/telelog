from typing import Dict
from enum import Enum
from sqlalchemy import Column, Integer, String, Boolean, BigInteger, Float, Text, DateTime, CheckConstraint, PickleType, text, select, update, insert, PrimaryKeyConstraint, func, UniqueConstraint
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

granularity_type_enum = ENUM(
    '1m',
    '2m',
    '5m',
    '15m',
    '30m',
    '60m',
    '90m',
    '1d',
    name='granularity_type'
)

class GranularityType(Enum):
    one_minute = '1m'
    two_minutes = '2m'
    five_minutes = '5m'
    fifteen_minutes = '15m'
    thirty_minutes = '30m'
    sixty_minutes = '60m'
    ninety_minutes = '90m'
    one_day = '1d'

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
    #next_top_message_id = Column(BigInteger, CheckConstraint('next_top_message_id is null or top_message_id is null or next_top_message_id >= top_message_id'))
    #ongoing_write = Column(Boolean)
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


class StocksMeta(Base):
    __tablename__ = 'stocks_meta'
    symbol_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    symbol = Column(String, primary_key=True, unique=True)
    name = Column(String)
    sector = Column(String)
    industry = Column(String)
    country = Column(String)
    market_cap = Column(Float)
    pe_ratio = Column(Float)
    eps = Column(Float)
    dividend_yield = Column(Float)
    beta = Column(Float)
    __table_args__ = (
        CheckConstraint('market_cap >= 0'),
        CheckConstraint('pe_ratio >= 0'),
        CheckConstraint('eps >= 0'),
        CheckConstraint('dividend_yield >= 0'),
        CheckConstraint('beta >= 0'),
    )


class Stocks(Base):
    __tablename__ = "stocks"
    symbol = Column(String)
    granularity = Column(granularity_type_enum)
    date = Column(DateTime)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    adj_close = Column(Float)
    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'granularity', 'date'),
        CheckConstraint('open >= 0'),
        CheckConstraint('high >= 0'),
        CheckConstraint('low >= 0'),
        CheckConstraint('close >= 0'),
        CheckConstraint('volume >= 0'),
        CheckConstraint('adj_close >= 0'),
    )


class StocksBase(Base):
    __abstract__ = True
    symbol = Column(String)
    date = Column(DateTime)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    adj_close = Column(Float)
    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'date'),
        #UniqueConstraint('symbol', 'date'),
        CheckConstraint('open >= 0'),
        CheckConstraint('high >= 0'),
        CheckConstraint('low >= 0'),
        CheckConstraint('close >= 0'),
        CheckConstraint('volume >= 0'),
        CheckConstraint('adj_close >= 0'),
    )


class StocksGran1m(StocksBase):
    __tablename__ = 'stocks_1m'


class StocksGran2m(StocksBase):
    __tablename__ = 'stocks_2m'


class StocksGran5m(StocksBase):
    __tablename__ = 'stocks_5m'


class StocksGran15m(StocksBase):
    __tablename__ = 'stocks_15m'


class StocksGran30m(StocksBase):
    __tablename__ = 'stocks_30m'


class StocksGran60m(StocksBase):
    __tablename__ = 'stocks_60m'


class StocksGran90m(StocksBase):
    __tablename__ = 'stocks_90m'


class StocksGran1d(StocksBase):
    __tablename__ = 'stocks_1d'


StocksGran: Dict[str, StocksBase] = {
    "1m": StocksGran1m,
    "2m": StocksGran2m,
    "5m": StocksGran5m,
    "15m": StocksGran15m,
    "30m": StocksGran30m,
    "60m": StocksGran60m,
    "90m": StocksGran90m,
    "1d": StocksGran1d,
}


class StocksUnpartNosym(Base):
    __tablename__ = 'stocks_unpart_nosym'
    #symbol_id = Column(Integer, primary_key=True)
    date = Column(DateTime, primary_key=True)
    yyyymm = Column(Integer)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    adj_close = Column(Float)
    __table_args__ = (
        PrimaryKeyConstraint('date'),
        UniqueConstraint('date'),
        CheckConstraint('yyyymm >= 180001 AND yyyymm <= 999912'),
        CheckConstraint('open >= 0'),
        CheckConstraint('high >= 0'),
        CheckConstraint('low >= 0'),
        CheckConstraint('close >= 0'),
        CheckConstraint('volume >= 0'),
        CheckConstraint('adj_close >= 0'),
    )


class StocksUnpartStr(Base):
    __tablename__ = 'stocks_unpart_str'
    #symbol_id = Column(Integer, primary_key=True)
    symbol = Column(String)
    date = Column(DateTime)
    yyyymm = Column(Integer)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    adj_close = Column(Float)
    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'date'),
        UniqueConstraint('symbol', 'date'),
        CheckConstraint('yyyymm >= 180001 AND yyyymm <= 999912'),
        CheckConstraint('open >= 0'),
        CheckConstraint('high >= 0'),
        CheckConstraint('low >= 0'),
        CheckConstraint('close >= 0'),
        CheckConstraint('volume >= 0'),
        CheckConstraint('adj_close >= 0'),
    )


class StocksUnpart(Base):
    __tablename__ = 'stocks_unpart'
    symbol_id = Column(Integer)
    date = Column(DateTime)
    yyyymm = Column(Integer)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    adj_close = Column(Float)
    __table_args__ = (
        PrimaryKeyConstraint('symbol_id', 'date'),
        UniqueConstraint('symbol_id', 'date'),
        CheckConstraint('yyyymm >= 180001 AND yyyymm <= 999912'),
        CheckConstraint('open >= 0'),
        CheckConstraint('high >= 0'),
        CheckConstraint('low >= 0'),
        CheckConstraint('close >= 0'),
        CheckConstraint('volume >= 0'),
        CheckConstraint('adj_close >= 0'),
    )


class MessageChain(Base):
    __tablename__ = 'message_chain'
    chat_id = Column(BigInteger, primary_key=True)
    last_message_id = Column(BigInteger, primary_key=True)
    chain_len = Column(Integer, CheckConstraint('chain_len >= 1'))
    chain = Column(Text)
    __table_args__ = (
        PrimaryKeyConstraint('chat_id', 'last_message_id'),
        CheckConstraint('chain_len >= 1'),
    )


class MessageChainEmbeddingsHegemmav2(Base):
    __tablename__ = 'message_chain_embeddings_hegemmav2'
    chat_id = Column(BigInteger, primary_key=True)
    last_message_id = Column(BigInteger, primary_key=True)
    embedding = Column(Vector(3072), nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('chat_id', 'last_message_id'),
    )
