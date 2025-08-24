# Data Loading Documentation – Yelp Dataset ETL

This document describes the issues encountered when loading the [Yelp Open Dataset](https://www.yelp.com/dataset) into PostgreSQL, and the solutions implemented to ensure clean, valid, and analytics-ready data.

---

## 📌 Overview
The raw Yelp dataset contains millions of records in JSON format.  
Challenges included:
- Nested JSON structures  
- Inconsistent formats  
- Duplicate and orphaned rows  
- Constraint violations (primary/foreign keys)  

To address these, I used a **staging → validation → final load** strategy with a combination of **Python preprocessing** and **PostgreSQL SQL scripts**.

---

## 🏢 Business Table
**Problem**  
- `attributes` and `hours` columns contained nested JSON objects.  
- PostgreSQL’s `COPY` command cannot directly ingest nested structures.  

**Solution**  
- Preprocessed with a Python script: `businessdatafix.py`.  
- Converted nested dictionaries into JSON strings.  
- Output saved as `business_clean.csv`, which could be ingested cleanly via `COPY`.  

---

## 👤 User Table
**Problem**  
- `friends` and `elite` columns were stored as strings representing arrays, e.g.:  
  ```text
  ["friend1", "friend2"]
- This format broke when loading into PostgreSQL `TEXT[]` array type.

**Solution**  
1. Loaded raw data into a staging table (`user_stage`) with `friends` and `elite` as text.  
2. Inserted into final `user` table using:  

   ```sql
   INSERT INTO user (user_id, friends, elite, ...)
   SELECT user_id,
          string_to_array(REPLACE(REPLACE(friends, '[', ''), ']', ''), ','),
          string_to_array(REPLACE(REPLACE(elite, '[', ''), ']', ''), ','),
          ...
   FROM user_stage;
   ```
3. Ensured valid array conversion without data loss.

---

## 📝 Review Table

**Problem**  
- `date` column had mixed formats, including invalid entries such as `"0"`.  
- Foreign key constraints (`user_id`, `business_id`) caused errors when referencing non-existent users/businesses (orphaned rows).  

**Solution**  
1. Preprocessed with Python script: `review-data-cleaner.py`.  
   - Standardized dates.  
   - Replaced invalid values.  
2. Loaded into staging table, then inserted into final `review` table with:  

   ```sql
   INSERT INTO review (review_id, user_id, business_id, ...)
   SELECT r.review_id, r.user_id, r.business_id, ...
   FROM review_stage r
   INNER JOIN user u ON r.user_id = u.user_id
   INNER JOIN business b ON r.business_id = b.business_id;
   ```
3. Ensured only valid rows with existing parent IDs were inserted.
---

## 💡 Tip & Checkin Tables

**Problem**  
- Contained duplicate entries, violating primary key constraints.  
- Included orphaned rows referencing missing `user_id` or `business_id`.  

**Solution**  
1. Loaded raw data into staging tables.  
2. Inserted into final tables with validation joins:  

   ```sql
   INSERT INTO tip (user_id, business_id, text, date, ...)
   SELECT t.user_id, t.business_id, t.text, t.date, ...
   FROM tip_stage t
   INNER JOIN user u ON t.user_id = u.user_id
   INNER JOIN business b ON t.business_id = b.business_id
   ON CONFLICT DO NOTHING;
   ```
   
3. Used `ON CONFLICT DO NOTHING` to gracefully handle duplicates.

---

## ✅ Summary of Strategies

- **Staging Tables**: Isolated raw loads before applying validation.  
- **Python Preprocessing**: Fixed nested JSON and inconsistent formats.  
- **SQL Validation Joins**: Prevented orphaned rows by enforcing referential integrity.  
- **Safe Load Patterns**: Used `ON CONFLICT` to handle duplicates without breaking pipelines.  

---

## 🎯 Outcome

- Successfully ingested **millions of Yelp records** into a clean **PostgreSQL schema**.  
- All tables enforced **primary/foreign key integrity**.  
- Dataset is now **analytics-ready** for queries, dashboards, and ML pipelines.  
