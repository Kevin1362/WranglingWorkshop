# WranglingWorkshop (Data Engineering & EDA)

This repository contains a professional, well-commented Jupyter Notebook that:
- Connects to a free Neon.tech Postgres database
- Creates an `employees` table (required columns)
- Generates >= 500 synthetic employee records using Faker
- Injects dirty/incomplete/illogical data into ~20% of records (missing fields, invalid salary/date, non-IT titles)
- Inserts data into Postgres and loads it into Pandas for EDA
- Cleans, transforms, feature-engineers, and scales the data
- Produces 2 visualizations:
  1) Grouped bar chart: avg salary by position & start year (standard)
  2) Heatmap: avg salary by department & position (advanced join)

## Folder structure
- `notebooks/` : main notebook to run and submit
- `src/`       : reusable modules (db, generator, cleaning, features, viz)
- `outputs/`   : saved charts (optional)
- `docs/`      : PDF cover sheet for submission

## Setup & Run (Windows)
1. Create a Neon Postgres database and copy connection details.
2. Copy `.env.example` to `.env` and fill values.
3. Create and activate venv:
   ```powershell
   python -m venv venv
   venv\Scripts\activate
   ```
4. Install requirements:
   ```powershell
   pip install -r requirements.txt
   ```
5. Open `notebooks/WranglingWorkshop_Main.ipynb` and **Run All**.

## Submission
- Push this repo to GitHub as **WranglingWorkshop**
- Submit the notebook + PDF cover sheet in `docs/Workshop_Cover.pdf`
