SELECT
    stream_sk,
    user_sk,
    track_sk,
    date_sk,
    CAST(listened_seconds AS INTEGER)  AS listened_seconds,
    CAST(play_count AS INTEGER)        AS play_count,
    CAST(was_skipped AS BOOLEAN)       AS was_skipped,
    LOWER(TRIM(device_type))           AS device_type,
    CURRENT_TIMESTAMP                  AS stg_loaded_at
FROM {{ source('musicflow', 'fact_streams') }}
WHERE listened_seconds > 0      -- filtra streams inválidos
  AND user_sk IS NOT NULL
  AND track_sk IS NOT NULL
  AND date_sk IS NOT NULL  
