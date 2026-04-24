CREATE OR REPLACE TABLE fact_streams (
    stream_sk        INTEGER PRIMARY KEY,
    user_sk          INTEGER,
    track_sk         INTEGER,
    date_sk          INTEGER,
    listened_seconds INTEGER,
    play_count       INTEGER,
    was_skipped      BOOLEAN,
    device_type      VARCHAR
);
