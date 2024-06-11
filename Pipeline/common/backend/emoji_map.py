from typing import Dict, List
from common.utils import create_postgres_engine
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
import common.backend.models as models


class EmojiMap:
    engine = create_postgres_engine()
    emoji_map: Dict[str|int, int] = dict()

    @classmethod
    def _reload_map(cls):
        with Session(cls.engine) as session:
            stmt = select(models.EmojiMap)
            result = session.execute(stmt).fetchall()
            for row in result:
                emoji = row[0].reaction
                cls.emoji_map[emoji] = row[0].reaction_id

    @classmethod
    def _save_map(cls):
        stmt = insert(models.EmojiMap).values([
            {"reaction": emoji_val, "is_custom": isinstance(emoji_val, int)}
            for emoji_val, _ in cls.emoji_map.items()
        ])
        stmt = stmt.on_conflict_do_update(
            index_elements=['reaction_id'],
            set_={"reaction": stmt.excluded.reaction, "is_custom": stmt.excluded.is_custom}
        )
        cls.conn.execute(stmt)
        cls.conn.commit()
    
    @classmethod
    def _add_emoji(cls, emoji: str|int):
        e = models.EmojiMap(reaction=str(emoji), is_custom=isinstance(emoji, int))
        with Session(cls.engine) as session:
            session.add(e)
            session.flush()
            session.refresh(e)
        assert e.reaction_id is not None
        cls.emoji_map[emoji] = e.reaction_id
    
    @classmethod
    def to_int(cls, emoji: str|int) -> int:
        if isinstance(emoji, (str, int)):
            emoji = str(emoji)
            if emoji not in cls.emoji_map:
                cls._add_emoji(emoji)
            return cls.emoji_map[emoji]
        else:
            raise ValueError(f"Unknown emoji type: {emoji} = {type(emoji)}")


EmojiMap._reload_map()
