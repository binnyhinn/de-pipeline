from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

from extract import fetch_prices
from load import load_clean, load_raw
from transform import transform_prices

default_args = {
    "owner": "binny",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="crypto_etl",
    schedule="@hourly",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args=default_args,
    tags=["crypto", "etl"],
)
def crypto_etl():
    @task
    def extract() -> dict:
        fetched_at = datetime.now(timezone.utc).isoformat()
        records = fetch_prices()
        return {"fetched_at": fetched_at, "records": records}

    @task
    def save_raw(data: dict) -> None:
        load_raw(data["records"], datetime.fromisoformat(data["fetched_at"]))

    @task
    def transform(data: dict) -> list[dict]:
        rows = transform_prices(
            data["records"], datetime.fromisoformat(data["fetched_at"])
        )
        # Convert datetimes to strings so Airflow can pass rows between tasks
        for row in rows:
            row["fetched_at"] = row["fetched_at"].isoformat()
        return rows

    @task
    def save_clean(rows: list[dict]) -> None:
        for row in rows:
            row["fetched_at"] = datetime.fromisoformat(row["fetched_at"])
        load_clean(rows)

    data = extract()
    save_raw(data)
    save_clean(transform(data))


crypto_etl()