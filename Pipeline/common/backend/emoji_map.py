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
    def _add_emoji(cls, emoji: str, is_custom: bool):
        e = models.EmojiMap(reaction=emoji, is_custom=is_custom)
        with Session(cls.engine) as session:
            stmt = (
                insert(models.EmojiMap)
                .values(reaction=e.reaction, is_custom=e.is_custom)
                .on_conflict_do_update(
                    index_elements=[models.EmojiMap.reaction],
                    set_={models.EmojiMap.is_custom: e.is_custom}
                )
                .returning(models.EmojiMap.reaction_id)
            )
            result = session.execute(stmt)
            e.reaction_id = result.fetchone()[0]
            session.flush()
            session.commit()
        assert e.reaction_id is not None
        cls.emoji_map[emoji] = e.reaction_id
    
    @classmethod
    def to_int(cls, emoji: str|int) -> int:
        if isinstance(emoji, (str, int)):
            is_custom = isinstance(emoji, int)
            emoji = str(emoji)
            if emoji not in cls.emoji_map:
                cls._add_emoji(emoji, is_custom)
            return cls.emoji_map[emoji]
        else:
            raise ValueError(f"Unknown emoji type: {emoji} = {type(emoji)}")
