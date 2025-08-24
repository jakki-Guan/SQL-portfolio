-- sql/load_all_data.sql
-- This script provides a comprehensive and safe way to load all Yelp data
-- by handling parent-child table dependencies and cleaning data in a single run.

BEGIN;
SET datestyle = 'ISO, YMD';
SET search_path TO yelp_data, public;

-- =========================================
-- Step 1: Truncate all tables to ensure a clean load.
-- =========================================
TRUNCATE TABLE
    review,
    tip,
    checkin_event,
    checkin_raw,
    user_stage,
    "user",
    business
RESTART IDENTITY CASCADE;


-- =========================================
-- Step 2: Load BUSINESS data
-- =========================================
DO $$
BEGIN
    RAISE NOTICE 'Starting to load business data...';
    COPY business (
        business_id,
        name,
        address,
        city,
        state,
        postal_code,
        latitude,
        longitude,
        stars,
        review_count,
        is_open,
        attributes,
        categories,
        hours
    )
    FROM '/data/business_clean.csv'
    WITH (FORMAT CSV, HEADER true);
    RAISE NOTICE 'Business data load completed.';
END $$ LANGUAGE plpgsql;


-- =========================================
-- Step 3: Load USERS data
-- =========================================
DO $$
BEGIN
    RAISE NOTICE 'Starting to load user data...';
    -- Load raw data into the staging table
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

    -- Transform and insert data from the staging table into the final table
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
    RAISE NOTICE 'User data load completed.';
END $$ LANGUAGE plpgsql;


-- =========================================
-- Step 4: Load REVIEW data safely
-- =========================================
DO $$
BEGIN
    RAISE NOTICE 'Starting safe load for the review table...';
    -- Use a temporary table to load the raw review data without constraints.
    CREATE TEMPORARY TABLE temp_review (
       review_id TEXT PRIMARY KEY,
       user_id TEXT,
       business_id TEXT,
       stars INTEGER,
       date DATE,
       text TEXT,
       useful INTEGER,
       funny INTEGER,
       cool INTEGER
    );

    -- Copy the raw review data into the temporary table.
    COPY temp_review (
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

    -- Now, insert only the valid reviews into the final 'review' table.
    -- We use an INNER JOIN to ensure both user_id and business_id exist.
    INSERT INTO review (
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
    SELECT
        tr.review_id,
        tr.user_id,
        tr.business_id,
        tr.stars,
        tr.date,
        tr.text,
        tr.useful,
        tr.funny,
        tr.cool
    FROM
        temp_review tr
    INNER JOIN
        yelp_data."user" u ON tr.user_id = u.user_id
    INNER JOIN
        yelp_data.business b ON tr.business_id = b.business_id;

    -- Clean up the temporary table.
    DROP TABLE temp_review;
    RAISE NOTICE 'Safe load for the review table completed.';
END $$ LANGUAGE plpgsql;


-- =========================================
-- Step 5: Load TIP data safely
-- =========================================
DO $$
BEGIN
    RAISE NOTICE 'Starting safe load for the tip table...';
    CREATE TEMPORARY TABLE temp_tip (
        user_id TEXT,
        business_id TEXT,
        text TEXT,
        date DATE,
        compliment_count INTEGER
    );

    -- Copy raw tip data into a temporary table.
    COPY temp_tip (
        user_id,
        business_id,
        text,
        date,
        compliment_count
    )
    FROM '/data/tip.csv'
    WITH (FORMAT CSV, HEADER true);

    -- Insert only valid tips into the final tip table.
    -- Use ON CONFLICT to ignore duplicates based on the primary key.
    INSERT INTO tip (
        user_id,
        business_id,
        text,
        date,
        compliment_count
    )
    SELECT
        tt.user_id,
        tt.business_id,
        tt.text,
        tt.date,
        tt.compliment_count
    FROM
        temp_tip tt
    INNER JOIN
        yelp_data."user" u ON tt.user_id = u.user_id
    INNER JOIN
        yelp_data.business b ON tt.business_id = b.business_id
    ON CONFLICT (user_id, business_id, date) DO NOTHING;

    -- Clean up the temporary table.
    DROP TABLE temp_tip;
    RAISE NOTICE 'Safe load for the tip table completed.';
END $$ LANGUAGE plpgsql;


-- =========================================
-- Step 6: Load CHECKIN data safely
-- =========================================
DO $$
BEGIN
    RAISE NOTICE 'Starting safe load for the checkin tables...';
    CREATE TEMPORARY TABLE temp_checkin (
        business_id TEXT,
        date TEXT
    );

    -- Load raw checkin data into a temporary table.
    COPY temp_checkin (business_id, date)
    FROM '/data/checkin.csv'
    WITH (FORMAT CSV, HEADER true);

    -- Insert only valid checkins into the raw checkin table.
    -- We check only for the business_id since that's the only foreign key.
    INSERT INTO checkin_raw (
        business_id,
        date
    )
    SELECT
        tc.business_id,
        tc.date
    FROM
        temp_checkin tc
    INNER JOIN
        yelp_data.business b ON tc.business_id = b.business_id;

    -- Clean up the temporary table.
    DROP TABLE temp_checkin;
    RAISE NOTICE 'Safe load for the checkin tables completed.';
END $$ LANGUAGE plpgsql;

-- Normalize the checkin data into the checkin_event table.
-- This requires the normalize_checkins() function to be created
-- from the 01_create_tables.sql script.
SELECT yelp_data.normalize_checkins();

COMMIT;
