CREATE OR REPLACE TABLE dim_user (
    user_sk     INTEGER PRIMARY KEY,
    user_id     VARCHAR,
    name        VARCHAR,
    country     VARCHAR,
    region      VARCHAR,
    age         INTEGER,
    gender      VARCHAR,
    signup_date DATE,
    valid_from  DATE NOT NULL,
    valid_to    DATE NOT NULL DEFAULT '9999-12-31',
    is_current  BOOLEAN NOT NULL DEFAULT TRUE
);
