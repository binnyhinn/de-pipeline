-- Latest price for each coin
SELECT DISTINCT ON (coin_id)
       coin_id, price_usd, price_change_24h_pct, fetched_at
FROM clean.crypto_prices
ORDER BY coin_id, fetched_at DESC;

-- Biggest 24-hour movers in the most recent fetch
SELECT coin_id, price_usd, price_change_24h_pct
FROM clean.crypto_prices
WHERE fetched_at = (SELECT max(fetched_at) FROM clean.crypto_prices)
ORDER BY abs(price_change_24h_pct) DESC;

-- Average price per coin across all runs
SELECT coin_id, round(avg(price_usd), 2) AS avg_price_usd, count(*) AS samples
FROM clean.crypto_prices
GROUP BY coin_id
ORDER BY avg_price_usd DESC;

-- How many pipeline runs have been loaded
SELECT count(DISTINCT fetched_at) AS runs_loaded
FROM clean.crypto_prices;

-- Row counts in each layer
SELECT 'raw' AS layer, count(*) AS row_count FROM raw.crypto_prices
UNION ALL
SELECT 'clean', count(*) FROM clean.crypto_prices;