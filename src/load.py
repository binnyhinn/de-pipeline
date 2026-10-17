import logging
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json, execute_values

logger = logging.getLogger(__name__)

CONN_ID = "warehouse"


def load_raw(records: list[dict], fetched_at: datetime) -> None:
    """Store the untouched API response in the raw layer."""
    conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw.crypto_prices (fetched_at, payload) VALUES (%s, %s)",
                (fetched_at, Json(records)),
            )
        logger.info("Loaded raw payload with %d records", len(records))
    finally:
        conn.close()


def load_clean(rows: list[dict]) -> None:
    """Insert validated rows into the clean layer (safe to re-run)."""
    if not rows:
        logger.warning("No clean rows to load")
        return

    values = [
        (
            r["coin_id"],
            r["price_usd"],
            r["market_cap_usd"],
            r["volume_24h_usd"],
            r["price_change_24h_pct"],
            r["fetched_at"],
        )
        for r in rows
    ]

    conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
    try:
        with conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO clean.crypto_prices
                    (coin_id, price_usd, market_cap_usd, volume_24h_usd,
                     price_change_24h_pct, fetched_at)
                VALUES %s
                ON CONFLICT (coin_id, fetched_at) DO NOTHING
                """,
                values,
            )
        logger.info("Loaded %d clean rows", len(values))
    finally:
        conn.close()