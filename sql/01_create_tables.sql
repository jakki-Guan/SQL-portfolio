-- Create a dedicated schema
CREATE SCHEMA IF NOT EXISTS yelp_data;

-- =========================
-- Business
-- =========================
CREATE TABLE IF NOT EXISTS yelp_data.business (
  business_id     TEXT PRIMARY KEY,
  name            TEXT NOT NULL,
  address         TEXT,
  city            TEXT,
  state           TEXT,
  postal_code     TEXT,
  latitude        DOUBLE PRECISION,
  longitude       DOUBLE PRECISION,
  stars           NUMERIC(3,2),
  review_count    INTEGER,
  is_open         INTEGER,
  attributes      JSONB,
  categories      TEXT,
  hours           JSONB
);

CREATE INDEX IF NOT EXISTS idx_business_city_state 
    ON yelp_data.business (city, state);

CREATE INDEX IF NOT EXISTS idx_business_stars 
    ON yelp_data.business (stars DESC);

CREATE INDEX IF NOT EXISTS idx_business_categories_gin 
    ON yelp_data.business USING GIN (to_tsvector('simple', COALESCE(categories,'')));

CREATE OR REPLACE VIEW yelp_data.business_category AS
SELECT
  b.business_id,
  trim(unnest(string_to_array(b.categories, ','))) AS category
FROM yelp_data.business b
WHERE b.categories IS NOT NULL;


-- =========================
-- Users
-- =========================
-- Staging table for user (TEXT for friends/elite)
CREATE TABLE IF NOT EXISTS yelp_data.user_stage (
  user_id          TEXT,
  name             TEXT,
  review_count     INTEGER,
  yelping_since    DATE,
  friends_txt      TEXT,       -- comes from CSV
  useful           INTEGER,
  funny            INTEGER,
  cool             INTEGER,
  fans             INTEGER,
  elite_txt        TEXT,       -- comes from CSV
  average_stars    NUMERIC(3,2),
  compliment_hot   INTEGER,
  compliment_more  INTEGER,
  compliment_profile INTEGER,
  compliment_cute  INTEGER,
  compliment_list  INTEGER,
  compliment_note  INTEGER,
  compliment_plain INTEGER,
  compliment_cool  INTEGER,
  compliment_funny INTEGER,
  compliment_writer INTEGER,
  compliment_photos INTEGER
);

CREATE TABLE IF NOT EXISTS yelp_data."user" (
  user_id          TEXT PRIMARY KEY,
  name             TEXT,
  review_count     INTEGER,
  yelping_since    DATE,
  friends          TEXT [],
  useful           INTEGER,
  funny            INTEGER,
  cool             INTEGER,
  fans             INTEGER,
  elite            INTEGER [],
  average_stars    NUMERIC(3,2),
  compliment_hot       INTEGER,
  compliment_more      INTEGER,
  compliment_profile   INTEGER,
  compliment_cute      INTEGER,
  compliment_list      INTEGER,
  compliment_note      INTEGER,
  compliment_plain     INTEGER,
  compliment_cool      INTEGER,
  compliment_funny     INTEGER,
  compliment_writer    INTEGER,
  compliment_photos    INTEGER
);

CREATE INDEX IF NOT EXISTS idx_user_avg_stars 
    ON yelp_data."user"(average_stars DESC);

CREATE INDEX IF NOT EXISTS idx_user_review_count 
    ON yelp_data."user"(review_count DESC);


-- =========================
-- Reviews
-- =========================
CREATE TABLE IF NOT EXISTS yelp_data.review (
  review_id    TEXT PRIMARY KEY,
  user_id      TEXT NOT NULL REFERENCES yelp_data."user"(user_id),
  business_id  TEXT NOT NULL REFERENCES yelp_data.business(business_id),
  stars        INTEGER,
  "date"       DATE,
  "text"       TEXT,
  useful       INTEGER,
  funny        INTEGER,
  cool         INTEGER
);

CREATE INDEX IF NOT EXISTS idx_review_business_date 
    ON yelp_data.review (business_id, "date");

CREATE INDEX IF NOT EXISTS idx_review_user_date 
    ON yelp_data.review (user_id, "date");

CREATE INDEX IF NOT EXISTS idx_review_stars 
    ON yelp_data.review (stars);


-- =========================
-- Tips
-- =========================
CREATE TABLE IF NOT EXISTS yelp_data.tip (
  user_id          TEXT NOT NULL REFERENCES yelp_data."user"(user_id),
  business_id      TEXT NOT NULL REFERENCES yelp_data.business(business_id),
  "date"           DATE,
  "text"           TEXT,
  compliment_count INTEGER,
  PRIMARY KEY (user_id, business_id, "date")
);

CREATE INDEX IF NOT EXISTS idx_tip_business_date 
    ON yelp_data.tip (business_id, "date");


-- =========================
-- Checkins
-- =========================
CREATE TABLE IF NOT EXISTS yelp_data.checkin_raw (
  business_id  TEXT PRIMARY KEY REFERENCES yelp_data.business(business_id),
  "date"       TEXT
);

CREATE TABLE IF NOT EXISTS yelp_data.checkin_event (
  business_id  TEXT NOT NULL REFERENCES yelp_data.business(business_id),
  ts           TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_checkin_event_biz_ts 
    ON yelp_data.checkin_event (business_id, ts);

CREATE OR REPLACE FUNCTION yelp_data.normalize_checkins() 
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO yelp_data.checkin_event (business_id, ts)
  SELECT
    r.business_id,
    to_timestamp(trim(x), 'YYYY-MM-DD HH24:MI:SS')
  FROM yelp_data.checkin_raw r,
       unnest(string_to_array(r."date", ',')) AS x
  ON CONFLICT DO NOTHING;
END$$;
