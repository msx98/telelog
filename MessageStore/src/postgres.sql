CREATE EXTENSION IF NOT EXISTS vector;
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
    phone_number TEXT,
    PRIMARY KEY (chat_id)
);

CREATE TABLE users (
    sender_id BIGINT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    username TEXT,
    is_verified BOOLEAN,
    is_restricted BOOLEAN,
    is_scam BOOLEAN,
    is_fake BOOLEAN,
    is_support BOOLEAN,
    is_bot BOOLEAN,
    phone_number TEXT,
    PRIMARY KEY (sender_id)
);

CREATE TABLE messages (
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
    file_unique_id TEXT,
    has_poll BOOLEAN GENERATED ALWAYS AS (poll_vote_count IS NOT NULL) STORED,
    has_reactions BOOLEAN GENERATED ALWAYS AS (reactions_vote_count IS NOT NULL) STORED,
    has_text BOOLEAN GENERATED ALWAYS AS (text IS NOT NULL) STORED,
    has_media BOOLEAN GENERATED ALWAYS AS (media_type IS NOT NULL) STORED,
    PRIMARY KEY (chat_id, message_id)
) PARTITION BY HASH (chat_id);

CREATE TABLE messages_0 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 0);
CREATE TABLE messages_1 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 1);
CREATE TABLE messages_2 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 2);
CREATE TABLE messages_3 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 3);
CREATE TABLE messages_4 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 4);
CREATE TABLE messages_5 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 5);
CREATE TABLE messages_6 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 6);
CREATE TABLE messages_7 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 7);
CREATE TABLE messages_8 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 8);
CREATE TABLE messages_9 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 9);
CREATE TABLE messages_10 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 10);
CREATE TABLE messages_11 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 11);
CREATE TABLE messages_12 PARTITION OF messages FOR VALUES WITH (MODULUS 13,REMAINDER 12);

CREATE TABLE message_embeddings (
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    embedding VECTOR(768),
    PRIMARY KEY (chat_id, message_id)
);

CREATE TABLE reactions (
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    reaction_id INTEGER,
    reaction_votes_norm REAL CHECK (reaction_votes_norm >= 0),
    reaction_votes_abs INTEGER CHECK (reaction_votes_abs >= 0),
    PRIMARY KEY (chat_id, message_id, reaction_id)
);


CREATE TABLE polls (
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    poll_option_id INTEGER,
    poll_option_text TEXT,
    poll_option_votes_norm REAL CHECK (poll_option_votes_norm >= 0),
    poll_option_votes_abs INTEGER CHECK (poll_option_votes_norm >= 0),
    poll_option_text_embedding vector(768),
    PRIMARY KEY (chat_id, message_id, poll_option_id)
);

CREATE TABLE emoji_map (
    reaction_id SERIAL PRIMARY KEY,
    reaction TEXT NOT NULL,
    is_custom boolean
);

CREATE TABLE configurations (
    project text not null,
    key text not null,
    value bytea,
    last_updated timestamp default current_timestamp,
    PRIMARY KEY (project, key)
) PARTITION BY HASH (project);

CREATE TABLE configurations_partition_0 PARTITION OF configurations FOR VALUES WITH (MODULUS 7, REMAINDER 0);
CREATE TABLE configurations_partition_1 PARTITION OF configurations FOR VALUES WITH (MODULUS 7, REMAINDER 1);
CREATE TABLE configurations_partition_2 PARTITION OF configurations FOR VALUES WITH (MODULUS 7, REMAINDER 2);
CREATE TABLE configurations_partition_3 PARTITION OF configurations FOR VALUES WITH (MODULUS 7, REMAINDER 3);
CREATE TABLE configurations_partition_4 PARTITION OF configurations FOR VALUES WITH (MODULUS 7, REMAINDER 4);
CREATE TABLE configurations_partition_5 PARTITION OF configurations FOR VALUES WITH (MODULUS 7, REMAINDER 5);
CREATE TABLE configurations_partition_6 PARTITION OF configurations FOR VALUES WITH (MODULUS 7, REMAINDER 6);

CREATE INDEX idx_chat ON messages (chat_id);
CREATE INDEX idx_date ON messages (
    (date_part('year', date)),
    (date_part('month', date)),
    (date_part('day', date))
);
CREATE INDEX idx_reaction ON reactions (reaction_id);

ALTER TABLE reactions ADD CONSTRAINT fk_reactions_message FOREIGN KEY (chat_id, message_id) REFERENCES messages (chat_id, message_id);
ALTER TABLE polls ADD CONSTRAINT fk_polls_message FOREIGN KEY (chat_id, message_id) REFERENCES messages (chat_id, message_id);
ALTER TABLE message_embeddings ADD CONSTRAINT fk_message_embeddings_message FOREIGN KEY (chat_id, message_id) REFERENCES messages (chat_id, message_id);

CREATE OR REPLACE FUNCTION has_arabic_text(column_value text) RETURNS boolean AS $$
DECLARE
    result boolean;
BEGIN
    SELECT column_value ~ E'[\u0600-\u06FF]' INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION arabic_pct(column_value text) RETURNS real AS $$
DECLARE
    result real;
BEGIN
    SELECT COALESCE(length(regexp_replace(column_value, E'[^\u0600-\u06FF]', '', 'g'))::real / length(column_value), 0) INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION has_english_text(column_value text) RETURNS boolean AS $$
DECLARE
    result boolean;
BEGIN
    SELECT column_value ~ E'[a-zA-Z]' INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION english_pct(column_value text) RETURNS real AS $$
DECLARE
    result real;
BEGIN
    SELECT COALESCE(length(regexp_replace(column_value, E'[^a-zA-Z]', '', 'g'))::real / length(column_value), 0) INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION has_hebrew_text(column_value text) RETURNS boolean AS $$
DECLARE
    result boolean;
BEGIN
    SELECT column_value ~ E'[\u0590-\u05FF]' INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION hebrew_pct(column_value text) RETURNS real AS $$
DECLARE
    result real;
BEGIN
    SELECT COALESCE(length(regexp_replace(column_value, E'[^\u0590-\u05FF]', '', 'g'))::real / length(column_value), 0) INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;
