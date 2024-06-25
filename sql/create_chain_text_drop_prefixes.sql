WITH actual_tree AS (WITH RECURSIVE message_tree AS (
    SELECT
        m.chat_id,
        m.message_id AS first_msg_id,
        m.reply_to_message_id,
        ARRAY[m.message_id] AS msg_chain, m.message_id AS lili
    FROM messages m
    WHERE m.chat_id = -1001647036111

    UNION ALL

    SELECT
        m.chat_id,
        m.message_id,
        m.reply_to_message_id,
        m.message_id || mt.msg_chain, mt.lili
    FROM messages m
    JOIN message_tree mt ON m.chat_id = mt.chat_id AND mt.reply_to_message_id = m.message_id
)
SELECT chat_id, lili, first_msg_id, msg_chain[1] as last_msg_id, msg_chain
FROM message_tree WHERE reply_to_message_id is null) SELECT
  mt.chat_id, 
  mt.msg_chain,
  ARRAY(
    SELECT
      m.message_id
    FROM messages m
    WHERE m.chat_id = mt.chat_id
    AND m.message_id = ANY(mt.msg_chain)
    ORDER BY array_position(mt.msg_chain, m.message_id)
  ) AS text_chain
FROM actual_tree mt LEFT JOIN messages t ON t.chat_id = mt.chat_id AND t.reply_to_message_id = mt.lili WHERE t.message_id IS NULL ORDER BY msg_chain;
