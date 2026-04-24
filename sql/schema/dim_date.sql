CREATE OR REPLACE TABLE dim_date (
    date_sk       INTEGER PRIMARY KEY,
    full_date     DATE,
    day           INTEGER,
    month         INTEGER,
    month_name    VARCHAR,
    quarter       INTEGER,
    year          INTEGER,
    week          INTEGER,
    day_of_week   VARCHAR,
    is_weekend    BOOLEAN,
    is_holiday    BOOLEAN
);
