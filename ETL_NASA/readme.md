📁 Project Structure
ETL_NASA
│── data
│   ├── raw
│   │   └── nasa_YYYYMMDD_HHMMSS.json     # Raw JSON from NASA API
│   ├── staged
│       ├── nasa_apod_staged.csv          # Optional staging file
│       └── nasa_cleaned.csv              # Cleaned, transformed output
│
│── scripts
│   ├── extract_nasa.py                   # Extracts NASA APOD JSON
│   ├── transform_nasa.py                 # Cleans & converts to CSV
│   └── load_nasa.py                      # Loads CSV → Supabase DB
│
│── .env                                   # API Keys (not committed)

🧠 Overview of Pipeline
1️⃣ Extract Phase – extract_nasa.py

Calls NASA APOD API using your API key.

Saves the API response into timestamped JSON under data/raw/.

Ensures directory creation automatically.

Output Example:

data/raw/nasa_20251209_135312.json

2️⃣ Transform Phase – transform_nasa.py

Reads the latest raw JSON file.

Converts it into a clean, structured DataFrame.

Creates the following fields:

date

title

explanation

media_type

img_url

inserted_at (current timestamp)

Saves cleaned data into:

data/staged/nasa_cleaned.csv

3️⃣ Load Phase – load_nasa.py

Reads the cleaned CSV file.

Converts dates and timestamps into Supabase-friendly format.

Uploads the data row-by-row into Supabase table: nasa_apod
(Batch size = 1, to avoid rate limits)

Prints progress logs during insert.

Finishes with a success message.

🔐 Environment Variables (.env)

Make sure your .env file contains:

SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_service_role_key
NASA_API_KEY=your_nasa_api_key   # (Optional if hardcoded)

🛠️ How to Run the Pipeline
Step 1 — Install Dependencies
pip install requests pandas python-dotenv supabase

Step 2 — Run Extract
python scripts/extract_nasa.py

Step 3 — Run Transform
python scripts/transform_nasa.py

Step 4 — Load to Supabase
python scripts/load_nasa.py

📊 Supabase Table Schema (Recommended)

Create a table:

Table: nasa_apod
Column	Type
id	bigint (PK)
date	date
title	text
explanation	text
media_type	text
img_url	text
inserted_at	timestamp
⭐ Features

Fully automated ETL pipeline.

Timestamped raw data archiving.

Clean transformation for structured storage.

Server-friendly batch loading.

Supabase native integration.

Easy to schedule via cron or GitHub Actions.

📌 Future Enhancements 

Add logging to a file.

Add Airflow / Prefect orchestration.

Automate daily APOD ingestion.

Support for multiple NASA APIs.

