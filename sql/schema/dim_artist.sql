CREATE OR REPLACE TABLE dim_artist (
    artist_sk   INTEGER PRIMARY KEY,
    artist_id   VARCHAR,
    artist_name VARCHAR,
    country     VARCHAR,
    genre       VARCHAR
)
