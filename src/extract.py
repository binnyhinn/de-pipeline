import logging

import requests

logger = logging.getLogger(__name__)

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin", "ripple", "polkadot", "litecoin"]


def fetch_prices() -> list[dict]:
    """Fetch current market data for our coins from CoinGecko."""
    params = {
        "vs_currency": "usd",
        "ids": ",".join(COINS),
        "order": "market_cap_desc",
    }
    logger.info("Fetching prices for %d coins", len(COINS))

    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    logger.info("Received %d records", len(data))
    return data