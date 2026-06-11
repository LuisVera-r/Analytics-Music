-- models/marts/mart_top_artists.sql
-- Top artistas por horas escuchadas — reporte semanal

{{ config(materialized='table') }}

WITH streams_enriched AS (
    SELECT
        s.stream_sk,
        s.listened_seconds,
        t.artist_sk,
        u.region,
        d.week,
        d.year
    FROM {{ ref('stg_streams') }} s
    JOIN {{ source('musicflow', 'dim_track')  }} t ON s.track_sk = t.track_sk
    JOIN {{ source('musicflow', 'dim_user')   }} u ON s.user_sk  = u.user_sk
    JOIN {{ source('musicflow', 'dim_date')   }} d ON s.date_sk  = d.date_sk
    WHERE u.is_current = TRUE
),

artist_hours AS (
    SELECT
        se.artist_sk,
        a.artist_name,
        a.genre,
        se.region,
        se.week,
        se.year,
        ROUND(SUM(se.listened_seconds) / 3600.0, 2) AS total_hours,
        COUNT(*)                                      AS total_streams
    FROM streams_enriched se
    JOIN {{ source('musicflow', 'dim_artist') }} a ON se.artist_sk = a.artist_sk
    GROUP BY se.artist_sk, a.artist_name, a.genre, se.region, se.week, se.year
)

SELECT
    artist_name,
    genre,
    region,
    week,
    year,
    total_hours,
    total_streams,
    DENSE_RANK() OVER (
        PARTITION BY region, week, year
        ORDER BY total_hours DESC
    ) AS ranking
FROM artist_hours
