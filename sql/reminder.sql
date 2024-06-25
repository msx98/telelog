-- set all chats in this list to top_message_id:=top_message_id-1 later
WITH diff AS (
WITH t1 AS (
    SELECT chat_id, COUNT(*) AS cnt_bak
    FROM reactions_bak
    GROUP BY chat_id
), t2 AS (
    SELECT chat_id, COUNT(*) AS cnt, MAX(message_id) AS max_msg, MIN(message_id) AS min_msg
    FROM reactions
    GROUP BY chat_id
)
SELECT *
FROM t1
LEFT JOIN t2 USING (chat_id)
WHERE (cnt IS NULL) OR (cnt < cnt_bak)
)
SELECT chats.chat_id, chats.top_message_id, diff.max_msg, diff.min_msg, diff.cnt, diff.cnt_bak
FROM diff LEFT JOIN chats
USING (chat_id) WHERE chats.top_message_id IS NOT NULL;
