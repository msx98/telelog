CREATE TYPE chat_type AS ENUM ('private', 'bot', 'group', 'supergroup', 'channel');
CREATE TYPE media_type AS ENUM (
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
CREATE TYPE reaction_item AS (
    reaction_id INTEGER,
    reaction_votes_norm REAL,
    reaction_votes_abs INTEGER
);
CREATE TYPE poll_option AS (
    poll_option_id INTEGER,
    poll_option_text TEXT,
    poll_option_votes_norm REAL,
    poll_option_votes_abs INTEGER,
    poll_option_text_embedding REAL[]
);

CREATE TABLE chats (
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

CREATE TABLE messages (
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    sender_id BIGINT,
    text TEXT,
    date TIMESTAMP,
    views INTEGER CHECK (views >= 0),
    forwards INTEGER CHECK (forwards >= 0),
    forward_from_chat_id BIGINT,
    forward_from_message_id BIGINT,
    reply_to_message_id BIGINT,
    poll_vote_count INTEGER CHECK (poll_vote_count >= 0),
    poll poll_option[],
    reactions_vote_count INTEGER CHECK (reactions_vote_count >= 0),
    reactions reaction_item[],
    media_type media_type,
    file_id TEXT,
    file_unique_id TEXT,
    has_poll BOOLEAN GENERATED ALWAYS AS (poll_vote_count IS NOT NULL) STORED,
    has_reactions BOOLEAN GENERATED ALWAYS AS (reactions_vote_count IS NOT NULL) STORED,
    has_text BOOLEAN GENERATED ALWAYS AS (text IS NOT NULL) STORED,
    has_media BOOLEAN GENERATED ALWAYS AS (media_type IS NOT NULL) STORED,
    PRIMARY KEY (chat_id, message_id)
);


CREATE INDEX idx_chat ON messages (chat_id);
CREATE INDEX idx_date ON messages (
    (date_part('year', date)),
    (date_part('month', date)),
    (date_part('day', date))
);
CREATE INDEX idx_reaction ON reactions (reaction_id);
