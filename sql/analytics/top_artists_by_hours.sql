-- Top 10 artistas por horas escuchadas por región

SELECT
    artist_name,
    region,
    total_hours,
    ranking
FROM (
    SELECT
        a.artist_name,
        u.region,
        ROUND(SUM(f.listened_seconds) / 3600.0, 2) AS total_hours,
        DENSE_RANK() OVER (
            PARTITION BY u.region
            ORDER BY SUM(f.listened_seconds) DESC
        ) AS ranking
    FROM fact_streams f
    JOIN dim_track  t ON f.track_sk  = t.track_sk
    JOIN dim_artist a ON t.artist_sk = a.artist_sk
    JOIN dim_user   u ON f.user_sk   = u.user_sk
        AND u.is_current = TRUE
    JOIN dim_date   d ON f.date_sk   = d.date_sk
    WHERE d.week = EXTRACT(WEEK FROM CURRENT_DATE)
      AND d.year = EXTRACT(YEAR FROM CURRENT_DATE)
    GROUP BY a.artist_name, u.region
) ranked
WHERE ranking <= 10
ORDER BY region, ranking;
