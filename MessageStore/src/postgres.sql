CREATE TYPE IF NOT EXISTS chat_type AS ENUM ('private', 'bot', 'group', 'supergroup', 'channel');
CREATE TYPE IF NOT EXISTS media_type AS ENUM (
    'document', 
    'audio', 
    'voice', 
    'video', 
    'sticker', 
    'animation', 
    'new_chat_photo', 
    'video_note', 
    'photo'
);

CREATE TABLE IF NOT EXISTS chats (
    chat_id BIGINT NOT NULL,
    top_message_id BIGINT,
    title TEXT,
    first_name TEXT,
    last_name TEXT,
    username TEXT,
    invite_link TEXT,
    type chat_type,
    members_count INTEGER CHECK (members_count >= 0),
    is_verified BOOLEAN,
    is_restricted BOOLEAN,
    is_scam BOOLEAN,
    is_fake BOOLEAN,
    is_support BOOLEAN,
    linked_chat_id BIGINT,
    PRIMARY KEY (chat_id)
);

CREATE TABLE IF NOT EXISTS messages (
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    sender_id BIGINT,
    text TEXT,
    embedding vector(768),
    date TIMESTAMP,
    views INTEGER CHECK (views >= 0),
    forwards INTEGER CHECK (forwards >= 0),
    forward_from_chat_id BIGINT,
    forward_from_message_id BIGINT,
    reply_to_message_id BIGINT,
    poll_vote_count INTEGER CHECK (poll_vote_count >= 0),
    reactions_vote_count INTEGER CHECK (reactions_vote_count >= 0),
    media_type media_type,
    file_id TEXT,
    PRIMARY KEY (chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS reactions (
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    reaction_id INTEGER,
    reaction_votes_norm REAL CHECK (reaction_votes_norm >= 0),
    reaction_votes_abs INTEGER CHECK (reaction_votes_abs >= 0),
    PRIMARY KEY (chat_id, message_id, reaction_id)
);


CREATE TABLE IF NOT EXISTS polls (
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    poll_option_id INTEGER,
    poll_option_text TEXT,
    poll_option_votes_norm REAL CHECK (poll_option_votes_norm >= 0),
    poll_option_votes_abs INTEGER CHECK (poll_option_votes_norm >= 0),
    poll_option_text_embedding REAL[],
    PRIMARY KEY (chat_id, message_id, poll_option_id)
);

CREATE INDEX IF NOT EXISTS idx_chat ON messages (chat_id);
CREATE INDEX IF NOT EXISTS idx_date ON messages (
    (date_part('year', date)),
    (date_part('month', date)),
    (date_part('day', date))
);
CREATE INDEX IF NOT EXISTS idx_reaction ON reactions (reaction_id);

ALTER TABLE reactions ADD CONSTRAINT fk_reactions_message FOREIGN KEY (chat_id, message_id) REFERENCES messages (chat_id, message_id);
ALTER TABLE polls ADD CONSTRAINT fk_polls_message FOREIGN KEY (chat_id, message_id) REFERENCES messages (chat_id, message_id);
