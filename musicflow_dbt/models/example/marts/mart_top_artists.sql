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
    JOIN {{ source('musicflow', 'dim_track') }} t 
        ON s.track_sk = t.track_sk
    -- JOIN SCD-2 Point-in-Time: Unimos el stream con el usuario que era vigente EN ESE MOMENTO
    JOIN {{ source('musicflow', 'dim_user') }} u 
        ON s.user_sk = u.user_sk 
        AND s.date_sk >= u.valid_from_sk  -- Asumiendo que tienes date_sk en dim_user
        AND s.date_sk <= u.valid_to_sk    -- Si usas fechas reales, usa s.date = u.valid_from, etc.
    JOIN {{ source('musicflow', 'dim_date') }} d 
        ON s.date_sk = d.date_sk
    -- QUITAMOS EL WHERE is_current = TRUE para no distorsionar la historia regional
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
        COUNT(*) AS total_streams
    FROM streams_enriched se
    JOIN {{ source('musicflow', 'dim_artist') }} a 
        ON se.artist_sk = a.artist_sk
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
    -- Usamos DENSE_RANK() para que si dos artistas empatan en horas, tengan el mismo rank
    -- y el siguiente salte al número que corresponda (1, 2, 2, 3)
    DENSE_RANK() OVER (
        PARTITION BY region, week, year
        ORDER BY total_hours DESC
    ) AS ranking
FROM artist_hours