# SQL Portfolio – Yelp Dataset ETL & Analytics

This project demonstrates how to design and implement a **data pipeline** using **PostgreSQL, Python, and Docker**.  
I built a relational schema, processed raw JSON data from the [Yelp Open Dataset](https://www.yelp.com/dataset), and solved common real-world data challenges to make the dataset analytics-ready.

---

## 🚀 Key Highlights
- **ETL Pipeline**: Converted messy JSONL into clean CSV, loaded into PostgreSQL with staging → final schema strategy.  
- **Schema Design**: Six core tables (`business`, `user`, `review`, `tip`, `checkin_raw`, `checkin_event`) with enforced foreign keys and data integrity.  
- **Data Cleaning & Validation**: Used Python + SQL to handle nested JSON, inconsistent date formats, orphaned rows, and duplicates.  
- **Dockerized Environment**: PostgreSQL + pgAdmin setup for reproducible local development.  
- **Analytics-Ready**: Final schema supports exploration, dashboards, and future ML pipelines.  

---

## 🛠️ Tech Stack
- **SQL / PostgreSQL** (COPY, staging tables, ON CONFLICT, joins, arrays)  
- **Python** (pandas, JSON preprocessing, ETL scripts)  
- **Docker & Docker Compose** (database + pgAdmin setup)  

---

## 📊 Data Loading Challenges & Solutions

During ETL, I solved several issues to ensure clean and reliable data:

- **Business Table** → Nested JSON (`attributes`, `hours`) → converted to JSON strings via Python preprocessing.  
- **User Table** → Friends/elite arrays in string format → transformed with `string_to_array()` in staging → final load.  
- **Review Table** → Mixed date formats + orphaned rows → cleaned with Python and validated with INNER JOINs.  
- **Tip & Checkin Tables** → Duplicates + orphaned rows → staging + `ON CONFLICT DO NOTHING` to ensure safe loads.  

👉 See full details in [Data Loading Documentation](./docs/data_loading.md)

---

## 📂 Repo Structure
- SQL-portfolio/
  - docker-compose.yml
  - sql/  — schema & ETL SQL scripts
  - scripts/  — Python data cleaning scripts
  - docs/
    - data_loading.md
  - README.md

---

## 🌟 What I Learned
This project demonstrates my ability to:
- Build **robust ETL pipelines** from raw JSON to a normalized relational schema.  
- Ensure **data integrity** with validation, staging/final schema strategy, and safe load patterns.  
- Combine **Python + SQL** for preprocessing, cleaning, and transformation.  
- Prepare **analytics-ready datasets** for BI dashboards or ML pipelines.  

---
