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

CREATE TABLE stocks_meta (
    symbol_id SERIAL PRIMARY KEY UNIQUE,
    symbol VARCHAR NOT NULL UNIQUE,
    name TEXT,
    exchange TEXT,
    industry TEXT,
    sector TEXT,
    country TEXT,
    market_cap FLOAT CHECK (market_cap >= 0),
    pe_ratio FLOAT CHECK (pe_ratio >= 0),
    eps FLOAT CHECK (eps >= 0)
);

CREATE TABLE stocks_unpart_nosym (
    date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    yyyymm INTEGER CHECK (yyyymm >= 180001 AND yyyymm <= 999912),
    open FLOAT CHECK (open >= 0),
    high FLOAT CHECK (high >= 0),
    low FLOAT CHECK (low >= 0),
    close FLOAT CHECK (close >= 0),
    volume FLOAT CHECK (volume >= 0),
    adj_close FLOAT CHECK (adj_close >= 0),
    UNIQUE (date),
    PRIMARY KEY (date)
);

CREATE TABLE stocks_unpart_str (
    symbol VARCHAR NOT NULL,
    date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    yyyymm INTEGER CHECK (yyyymm >= 180001 AND yyyymm <= 999912),
    open FLOAT CHECK (open >= 0),
    high FLOAT CHECK (high >= 0),
    low FLOAT CHECK (low >= 0),
    close FLOAT CHECK (close >= 0),
    volume FLOAT CHECK (volume >= 0),
    adj_close FLOAT CHECK (adj_close >= 0),
    UNIQUE (symbol, date),
    PRIMARY KEY (symbol, date)
);

CREATE TABLE stocks_unpart (
    symbol_id INTEGER NOT NULL,
    date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    yyyymm INTEGER CHECK (yyyymm >= 180001 AND yyyymm <= 999912),
    open FLOAT CHECK (open >= 0),
    high FLOAT CHECK (high >= 0),
    low FLOAT CHECK (low >= 0),
    close FLOAT CHECK (close >= 0),
    volume FLOAT CHECK (volume >= 0),
    adj_close FLOAT CHECK (adj_close >= 0),
    UNIQUE (symbol_id, date),
    PRIMARY KEY (symbol_id, date)
);

CREATE OR REPLACE FUNCTION CREATE_STOCKS_TABLE(suffix text, start_year int DEFAULT 1970, end_year int DEFAULT 2040) RETURNS void AS $$
DECLARE
    year int;
    base_table_name text;
    partition_table_name text;
    create_table_statement text;
    create_partition_statement text;
BEGIN
    -- Construct the base table name
    base_table_name := quote_ident('stocks_' || suffix); 

    -- Create the main table without PARTITION BY
    create_table_statement := format('CREATE TABLE %s (
        symbol VARCHAR NOT NULL,
        date TIMESTAMP NOT NULL,
        open FLOAT CHECK (open >= 0),
        high FLOAT CHECK (high >= 0),
        low FLOAT CHECK (low >= 0),
        close FLOAT CHECK (close >= 0),
        volume FLOAT CHECK (volume >= 0),
        adj_close FLOAT CHECK (adj_close >= 0),
        UNIQUE (symbol, date),
        PRIMARY KEY (symbol, date)
    ) PARTITION BY RANGE(date)', base_table_name);

    EXECUTE create_table_statement;

    -- Loop through the years to create partitions
    FOR year IN start_year..end_year LOOP
        partition_table_name := quote_ident(format('stocks_%s_%s', suffix, year));

        create_partition_statement := format('CREATE TABLE %s PARTITION OF %s
            FOR VALUES FROM (''%s-01-01'') TO (''%s-01-01'')
        ', partition_table_name, base_table_name, year, year + 1);

        BEGIN  -- Add error handling block
            EXECUTE create_partition_statement;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error creating partition for year %: %', year, SQLERRM;
            -- You can handle the error differently here: log it, continue, etc.
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT CREATE_STOCKS_TABLE("1m");
SELECT CREATE_STOCKS_TABLE("2m");
SELECT CREATE_STOCKS_TABLE("5m");
SELECT CREATE_STOCKS_TABLE("15m");
SELECT CREATE_STOCKS_TABLE("30m");
SELECT CREATE_STOCKS_TABLE("60m");
SELECT CREATE_STOCKS_TABLE("90m");
SELECT CREATE_STOCKS_TABLE("1d");


CREATE TYPE granularity_type AS ENUM ('1m', '2m', '5m', '15m', '30m', '60m', '90m', '1d');

CREATE TABLE stocks (
    symbol VARCHAR NOT NULL,
    granularity granularity_type NOT NULL,
    date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open FLOAT CHECK (open >= 0),
    high FLOAT CHECK (high >= 0),
    low FLOAT CHECK (low >= 0),
    close FLOAT CHECK (close >= 0),
    volume FLOAT CHECK (volume >= 0),
    adj_close FLOAT CHECK (adj_close >= 0),
    PRIMARY KEY (symbol, granularity, date)
) PARTITION BY LIST (granularity);

-- Top-level partitions
DO $$
DECLARE
  granularity_item granularity_type;
BEGIN
  FOR granularity_item IN SELECT * FROM unnest(enum_range(NULL::granularity_type)) LOOP 
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS stocks_unif_%s PARTITION OF stocks FOR VALUES IN (%L) PARTITION BY RANGE (date)',
      replace(granularity_item::text, ' ', '_'),
      granularity_item
    );
  END LOOP;
END;
$$;

-- Sub-partitions creation function and procedure to call it
CREATE OR REPLACE FUNCTION create_sub_partition_if_not_exists(granularity_item granularity_type, partition_year INT) RETURNS void AS $$
BEGIN
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS stocks_unif_%s_%s_data PARTITION OF stocks_unif_%s FOR VALUES FROM (''%s-01-01'') TO (''%s-01-01'')',
    replace(granularity_item::text, ' ', '_'),
    partition_year,
    replace(granularity_item::text, ' ', '_'),
    partition_year,
    partition_year+1
  );
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
  granularity_item granularity_type;
BEGIN
  FOR granularity_item IN SELECT * FROM unnest(enum_range(NULL::granularity_type)) LOOP 
    FOR partition_year IN 1970..2040 LOOP
      PERFORM create_sub_partition_if_not_exists(granularity_item, partition_year);
    END LOOP;
  END LOOP;
END;
$$;


CREATE OR REPLACE FUNCTION stockDay(
    input_symbol text DEFAULT NULL,
    interval_identifier text DEFAULT NULL,
    first_date date DEFAULT NULL,
    last_date date DEFAULT NULL
)
RETURNS TABLE (symbol text, date date, price numeric, price_stddev numeric)
AS $$
BEGIN

    RETURN QUERY EXECUTE
    format('
        WITH t AS (
            SELECT 
                stocks.symbol::TEXT,
                stocks.date::DATE, 
                ((COALESCE(stocks.open, stocks.close) + COALESCE(stocks.close, stocks.open)) / 2)::NUMERIC AS price
            FROM 
                stocks
            WHERE 
                (%L IS NULL OR granularity = %L::granularity_type)
                AND (%L IS NULL OR stocks.symbol = %L) 
                AND (%L IS NULL OR stocks.date >= %L) 
                AND (%L IS NULL OR stocks.date <= %L) 
        )
        SELECT 
            t.symbol,
            t.date,
            AVG(t.price) AS price,
            STDDEV(t.price) AS price_stddev
        FROM t
        GROUP BY t.symbol, t.date
    ', interval_identifier, interval_identifier, input_symbol, input_symbol, first_date, first_date, last_date, last_date); 
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION stockDayOld(
    interval_identifier text,
    input_symbol text DEFAULT NULL,
    first_date date DEFAULT NULL,
    last_date date DEFAULT NULL
)
RETURNS TABLE (symbol text, date date, price numeric)
AS $$
DECLARE
    table_name text;
BEGIN
    table_name := quote_ident('stocks_' || interval_identifier);

    RETURN QUERY EXECUTE
    format('
        SELECT 
            s.symbol::TEXT,
            s.date::DATE, 
            ((COALESCE(s.open, s.close) + COALESCE(s.close, s.open)) / 2)::NUMERIC AS price
        FROM 
            %s AS s 
        WHERE 
            (%L IS NULL OR s.symbol = %L) 
            AND (%L IS NULL OR s.date >= %L) 
            AND (%L IS NULL OR s.date <= %L) 
    ', table_name, input_symbol, input_symbol, first_date, first_date, last_date, last_date); 
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION joinStocks(
    symbol1_input text,
    symbol2_input text,
    interval_identifier text DEFAULT NULL
)
RETURNS TABLE (date date, symbol1 text, symbol2 text, price1 numeric, price2 numeric) 
AS $$
BEGIN
    RETURN QUERY 
    SELECT 
        s1.date,
        s1.symbol AS symbol1,
        s2.symbol AS symbol2,
        s1.price AS price1,
        s2.price AS price2
    FROM 
        stockDay(symbol1_input, interval_identifier) AS s1
    JOIN 
        stockDay(symbol2_input, interval_identifier) AS s2
    ON 
        s1.date = s2.date;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION stockDay2(
    symbol1_input text,
    symbol2_input text
)
RETURNS TABLE (ts date, price1 numeric, price2 numeric) 
AS $$
BEGIN
    RETURN QUERY
    select date as ts, s1.price as price1, s2.price as price2
    from stockDay(symbol1_input) as s1
    inner join stockDay(symbol2_input) as s2 using (date);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION stockDay3(
    symbol1_input text,
    symbol2_input text,
    symbol3_input text
)
RETURNS TABLE (ts date, price1 numeric, price2 numeric, price3 numeric)
AS $$
BEGIN
    RETURN QUERY
    select date as ts, s1.price as price1, s2.price as price2, s3.price as price3
    from stockDay(symbol1_input) as s1
    inner join stockDay(symbol2_input) as s2 using (date)
    inner join stockDay(symbol3_input) as s3 using (date);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION stockDay4(
    symbol1_input text,
    symbol2_input text,
    symbol3_input text,
    symbol4_input text
)
RETURNS TABLE (ts date, price1 numeric, price2 numeric, price3 numeric, price4 numeric)
AS $$
BEGIN
    RETURN QUERY
    select date as ts, s1.price as price1, s2.price as price2, s3.price as price3, s4.price as price4
    from stockDay(symbol1_input) as s1
    inner join stockDay(symbol2_input) as s2 using (date)
    inner join stockDay(symbol3_input) as s3 using (date)
    inner join stockDay(symbol4_input) as s4 using (date);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION stockDay5(
    symbol1_input text,
    symbol2_input text,
    symbol3_input text,
    symbol4_input text,
    symbol5_input text
)
RETURNS TABLE (ts date, price1 numeric, price2 numeric, price3 numeric, price4 numeric, price5 numeric)
AS $$
BEGIN
    RETURN QUERY
    select date as ts, s1.price as price1, s2.price as price2, s3.price as price3, s4.price as price4, s5.price as price5
    from stockDay(symbol1_input) as s1
    inner join stockDay(symbol2_input) as s2 using (date)
    inner join stockDay(symbol3_input) as s3 using (date)
    inner join stockDay(symbol4_input) as s4 using (date)
    inner join stockDay(symbol5_input) as s5 using (date);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION joinStocksVar(
    VARIADIC symbols text[]
)
RETURNS TABLE (date date, symbol text, price numeric) 
AS $$
DECLARE
    sql_query text;
    i int;
BEGIN
    -- Construct the base SQL query
    sql_query := 'SELECT s1.date';

    -- Loop through the array of symbols to construct the SQL query dynamically
    FOR i IN 1 .. array_length(symbols, 1) LOOP
        sql_query := sql_query || format(', s%d.symbol AS symbol%d, s%d.price AS price%d', i, i, i, i);
    END LOOP;

    sql_query := sql_query || ' FROM stockDay($1) AS s1';

    -- Add join statements dynamically
    FOR i IN 2 .. array_length(symbols, 1) LOOP
        sql_query := sql_query || format(' JOIN stockDay($%d) AS s%d ON s%d.date = s1.date', i, i, i);
    END LOOP;

    -- Execute the dynamically constructed SQL query
    RETURN QUERY EXECUTE sql_query USING symbols;

END;
$$ LANGUAGE plpgsql;


--ALTER TABLE stocks ADD CONSTRAINT fk_stocks_meta FOREIGN KEY (symbol_id) REFERENCES stocks_meta (symbol_id);

CREATE TABLE chats (
    chat_id BIGINT NOT NULL,
    top_message_id BIGINT,
--    next_top_message_id BIGINT CHECK (
--        next_top_message_id is null
--        or top_message_id is null
--        or next_top_message_id >= top_message_id
--    ),
--    ongoing_write BOOLEAN NOT NULL DEFAULT FALSE,
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
    reaction TEXT NOT NULL UNIQUE,
    is_custom boolean
);

ALTER TABLE reactions ADD CONSTRAINT fk_reactions_emoji_map FOREIGN KEY (reaction_id) REFERENCES emoji_map (reaction_id);

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

CREATE OR REPLACE FUNCTION ChatStats(
    CHAT_ID_ARG BIGINT,
    N INT DEFAULT 3
)
RETURNS TABLE (
    hour INTEGER,
    activity_fraction FLOAT,
    most_active_sender_ids BIGINT[],
    most_active_names TEXT[]
)
LANGUAGE sql
AS $$
WITH hourly_activity AS (
    SELECT
       (((date_part('hour', m.date) + 3)::DECIMAL) % 24) AS hour,
        m.sender_id,
        COUNT(*) AS message_count
    FROM messages m
    WHERE m.chat_id = CHAT_ID_ARG
    GROUP BY 1, 2
),
ranked_activity AS (
    SELECT
        hour,
        sender_id,
        message_count,
        RANK() OVER (PARTITION BY hour ORDER BY message_count DESC) AS rank,
        SUM(message_count) OVER (PARTITION BY hour) as total_messages_per_hour
    FROM hourly_activity
),
top_n_activity AS (
    SELECT
        hour,
        sender_id,
        message_count,
        total_messages_per_hour,
        rank
    FROM ranked_activity
    WHERE rank <= N
    ORDER BY rank ASC
)
SELECT
    t.hour,
    COALESCE(
        (SUM(t.message_count)::FLOAT) / (t.total_messages_per_hour::FLOAT),
        0
    ) AS activity_fraction,
    ARRAY_AGG(t.sender_id ORDER BY rank ASC) AS most_active_sender_ids,
    ARRAY_AGG(u.first_name || COALESCE(' ' || u.last_name, '') ORDER BY rank ASC) AS most_active_names
FROM top_n_activity t
LEFT JOIN users u ON t.sender_id = u.sender_id
GROUP BY t.hour, t.total_messages_per_hour
ORDER BY t.hour;
$$;
