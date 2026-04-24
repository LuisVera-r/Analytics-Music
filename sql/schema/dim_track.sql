CREATE OR REPLACE TABLE dim_track (
    track_sk         INTEGER PRIMARY KEY,
    track_id         VARCHAR,
    track_name       VARCHAR,
    artist_sk        INTEGER,
    genre            VARCHAR,
    duration_seconds INTEGER,
    album            VARCHAR,
    release_date     DATE
);
