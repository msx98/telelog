WITH RECURSIVE message_tree AS (
    SELECT
        m.chat_id,
        m.message_id,
        m.reply_to_message_id,
        m.text,
        ARRAY[m.text] as text_chain, 1 AS cnt, ARRAY[m.message_id] AS msg_chain
    FROM messages m
    WHERE m.chat_id = -1001647036111

    UNION ALL

    SELECT
        m.chat_id,
        mt.message_id,
        m.reply_to_message_id,
        m.text,
        mt.text_chain || m.text, mt.cnt + 1, mt.msg_chain || m.message_id
    FROM messages m
    JOIN message_tree mt ON m.chat_id = mt.chat_id AND mt.reply_to_message_id = m.message_id
)
SELECT message_id, reply_to_message_id, msg_chain
FROM message_tree ORDER BY message_id;
