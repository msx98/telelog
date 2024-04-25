from typing import Dict, List
import os


def emoji_to_int(emoji: str) -> int:
    return int.from_bytes(emoji.encode("utf-32"), "little")


def int_to_emoji(i: int) -> str:
    return i.to_bytes(8, "little").decode("utf-32")


def emoji_to_line(emoji: str|int) -> str:
    if isinstance(emoji, int): # keep int
        return f"1,{emoji}"
    elif isinstance(emoji, str): # convert to int
        assert emoji
        return "0," + (",".join(str(ord(c)) for c in emoji))
    else:
        raise ValueError(f"Unknown emoji type: {emoji} = {type(emoji)}")


def line_to_emoji(line: str) -> str|int:
    line = line.split(",")
    keep_int = int(line[0])
    if keep_int:
        assert len(line) == 2
        return int(line[1])
    return "".join(chr(int(c)) for c in line[1:])


class EmojiMap:
    path: str = "emoji_map.txt"
    emoji_map: Dict[str|int, int] = dict()
    emoji_map_inv: Dict[int, str|int] = dict()

    @classmethod
    def _reload_map(cls):
        if not os.path.exists(cls.path):
            return
        with open(cls.path, "r") as f:
            for line in f:
                emoji = line.strip()
                if not emoji:
                    continue
                emoji = line_to_emoji(emoji)
                cls.emoji_map[emoji] = len(cls.emoji_map)

    @classmethod
    def _save_map(cls):
        with open(cls.path, "w") as f:
            emoji_list = sorted(cls.emoji_map.items(), key=lambda x: x[1])
            for emoji, _ in emoji_list:
                f.write(f"{emoji_to_line(emoji)}\n")
    
    @classmethod
    def _add_emoji(cls, emoji: str|int):
        assert emoji not in cls.emoji_map
        cls.emoji_map[emoji] = len(cls.emoji_map)
        with open(cls.path, "a") as f:
            f.write(f"{emoji_to_line(emoji)}\n")
    
    @classmethod
    def to_int(cls, emoji: str|int) -> int:
        if isinstance(emoji, (str, int)):
            if emoji not in cls.emoji_map:
                cls._add_emoji(emoji)
            return cls.emoji_map[emoji]
        else:
            raise ValueError(f"Unknown emoji type: {emoji} = {type(emoji)}")
    
    @classmethod
    def from_int(cls, i: int) -> str:
        if i in cls.emoji_map_inv:
            return cls.emoji_map_inv[i]
        for emoji, j in cls.emoji_map.items():
            if j == i:
                cls.emoji_map_inv[i] = emoji
                return emoji
        raise ValueError(f"Unknown emoji index: {i}")

EmojiMap._reload_map()
