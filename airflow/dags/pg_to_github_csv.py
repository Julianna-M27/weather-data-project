from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import os, base64, json, requests, pandas as pd
from sqlalchemy import create_engine

PG_URL   = "postgresql+psycopg2://db_user:db_password@db:5432/db"
SQL      = "SELECT * FROM dev.daily_average ORDER BY 1"
CSV_PATH = "/tmp/daily_average.csv"

GH_TOKEN  = os.getenv("GITHUB_TOKEN")
GH_REPO   = os.getenv("GITHUB_REPO")
GH_BRANCH = os.getenv("GITHUB_BRANCH", "main")
GH_PATH   = os.getenv("GITHUB_PATH", "daily_average.csv")

if not GH_TOKEN or not GH_REPO:
    raise RuntimeError("Missing GITHUB_TOKEN or GITHUB_REPO")

def export_to_csv():
    engine = create_engine(PG_URL)
    df = pd.read_sql(SQL, engine)
    df.to_csv(CSV_PATH, index=False)

def upload_to_github():
    with open(CSV_PATH, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode("utf-8")

    headers = {"Authorization": f"Bearer {GH_TOKEN}", "Accept": "application/vnd.github+json"}

    # Get current sha (if file exists)
    r = requests.get(
        f"https://api.github.com/repos/{GH_REPO}/contents/{GH_PATH}?ref={GH_BRANCH}",
        headers=headers
    )
    sha = r.json().get("sha") if r.status_code == 200 else None

    # Create or update
    payload = {
        "message": f"Update {GH_PATH} {datetime.utcnow().isoformat()}Z",
        "content": content_b64,
        "branch": GH_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    resp = requests.put(
        f"https://api.github.com/repos/{GH_REPO}/contents/{GH_PATH}",
        headers=headers,
        data=json.dumps(payload),
    )
    resp.raise_for_status()

default_args = {"start_date": datetime(2025, 9, 1)}

with DAG(
    dag_id="pg_to_github_csv_hourly",
    schedule=timedelta(hours=1),
    catchup=False,
    default_args=default_args,
    description="Export Postgres to CSV and push to GitHub for Sheets IMPORTDATA",
) as dag:
    task1 = PythonOperator(task_id="export_csv", python_callable=export_to_csv)
    task2 = PythonOperator(task_id="push_github", python_callable=upload_to_github)
    task1 >> task2
