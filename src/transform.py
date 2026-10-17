import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def transform_prices(records: list[dict], fetched_at: datetime) -> list[dict]:
    """Clean and validate raw CoinGecko records."""
    clean = []
    seen = set()

    for record in records:
        coin_id = record.get("id")
        price = record.get("current_price")

        # Validate: must have an id and a positive price
        if not coin_id or price is None or price <= 0:
            logger.warning("Dropping invalid record: %s", record)
            continue

        # Remove duplicates within the same batch
        if coin_id in seen:
            logger.warning("Dropping duplicate: %s", coin_id)
            continue
        seen.add(coin_id)

        clean.append(
            {
                "coin_id": coin_id,
                "price_usd": price,
                "market_cap_usd": record.get("market_cap"),
                "volume_24h_usd": record.get("total_volume"),
                "price_change_24h_pct": record.get("price_change_percentage_24h"),
                "fetched_at": fetched_at,
            }
        )

    logger.info("Kept %d of %d records", len(clean), len(records))
    return clean