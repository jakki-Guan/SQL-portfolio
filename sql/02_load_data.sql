-- sql/02_load_data.sql
-- Assumes CSVs are available inside the container at /data/*
-- and your schema yelp_data is already created.

BEGIN;
SET datestyle = 'ISO, YMD';
SET search_path TO yelp_data, public;

TRUNCATE TABLE
  review,
  tip,
  checkin_event,
  checkin_raw,
  user_stage,
  "user",
  business
RESTART IDENTITY CASCADE;

-- ---------- BUSINESS (use the fixed CSV) ----------
COPY business (
  business_id, name, address, city, state, postal_code,
  latitude, longitude, stars, review_count, is_open,
  attributes, categories, hours
)
FROM '/data/business_clean.csv'
WITH (FORMAT csv, HEADER true);

-- ---------- USERS ----------
-- Step 1: Load raw data into the staging table
COPY user_stage (
    user_id,
    name,
    review_count,
    yelping_since,
    friends_txt,
    useful,
    funny,
    cool,
    fans,
    elite_txt,
    average_stars,
    compliment_hot,
    compliment_more,
    compliment_profile,
    compliment_cute,
    compliment_list,
    compliment_note,
    compliment_plain,
    compliment_cool,
    compliment_funny,
    compliment_writer,
    compliment_photos
)
FROM '/data/user.csv'
WITH (FORMAT CSV, HEADER true);

-- Step 2: Transform and insert data from the staging table into the final table
INSERT INTO "user" (
    user_id,
    name,
    review_count,
    yelping_since,
    friends,
    useful,
    funny,
    cool,
    fans,
    elite,
    average_stars,
    compliment_hot,
    compliment_more,
    compliment_profile,
    compliment_cute,
    compliment_list,
    compliment_note,
    compliment_plain,
    compliment_cool,
    compliment_funny,
    compliment_writer,
    compliment_photos
)
SELECT
    user_id,
    name,
    review_count,
    yelping_since,
    string_to_array(friends_txt, ','),
    useful,
    funny,
    cool,
    fans,
    string_to_array(elite_txt, ','),
    average_stars,
    compliment_hot,
    compliment_more,
    compliment_profile,
    compliment_cute,
    compliment_list,
    compliment_note,
    compliment_plain,
    compliment_cool,
    compliment_funny,
    compliment_writer,
    compliment_photos
FROM
    user_stage;
-- ---------- REVIEWS ----------
DO
$$
BEGIN
   RAISE NOTICE 'Starting review data COPY...';
   COPY review (
       review_id,
       user_id,
       business_id,
       stars,
       date,
       text,
       useful,
       funny,
       cool
   )
   FROM '/data/review_clean.csv'
   WITH (FORMAT CSV, HEADER true);
   RAISE NOTICE 'Review data COPY completed successfully.';
EXCEPTION
   WHEN OTHERS THEN
       RAISE NOTICE 'An error occurred during review data COPY: %', SQLERRM;
       RAISE;
END
$$
LANGUAGE plpgsql;

-- ---------- TIPS ----------
COPY tip (
    user_id,
    business_id,
    text,
    date,
    compliment_count
)
FROM '/data/tip.csv'
WITH (FORMAT CSV, HEADER true);

-- ---------- CHECKINS (raw) ----------
COPY checkin_raw (business_id, date)
FROM '/data/checkin.csv'
WITH (FORMAT csv, HEADER true);

-- If you created normalize_checkins() in 01_create_tables.sql:
SELECT yelp_data.normalize_checkins();

COMMIT;

