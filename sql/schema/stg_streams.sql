CREATE TABLE IF NOT EXISTS stg_streams (

    stream_sk INTEGER,
    user_sk INTEGER,
    track_sk INTEGER,
    date_sk INTEGER,
    listened_seconds INTEGER,
    play_count INTEGER,
    was_skipped BOOLEAN,
    device_type VARCHAR

);