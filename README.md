Crypto ETL Pipeline
An automated, containerised data pipeline that ingests cryptocurrency market data from the CoinGecko API every hour, validates and cleans it, and loads it into a PostgreSQL data warehouse. Orchestrated with Apache Airflow.
Author: R. Binny Hinn 

1. Overview
Market data changes constantly, and raw API responses are not safe to analyse directly: fields can be missing, values can be invalid, and re-runs can create duplicates. This project solves that with a layered pipeline:
the raw layer keeps the exact API response for traceability and reprocessing
the clean layer holds validated, analysis-ready rows
The whole stack starts with one command (docker compose up -d) and runs unattended on an hourly schedule.
2. Architecture
                         ┌──────────────────────────────┐
                         │      Apache Airflow DAG      │
                         │        (@hourly, 3 retries)  │
                         └──────────────────────────────┘
                                       │
  CoinGecko API ──► extract ──┬──► save_raw ───────────► PostgreSQL  raw.crypto_prices
                              │
                              └──► transform ──► save_clean ──► PostgreSQL  clean.crypto_prices

Component	Purpose
extract	Calls the CoinGecko /coins/markets endpoint for 8 coins
save_raw	Stores the untouched response as JSONB
transform	Validates records, removes duplicates, maps API fields to warehouse columns
save_clean	Inserts validated rows (idempotent)

3. Tech stack

Area	Tool
Language	Python 3.12
Orchestration	Apache Airflow 2.9.3 (LocalExecutor)
Warehouse	PostgreSQL 16
Containerisation	Docker, Docker Compose
Testing	pytest
Data source	CoinGecko public API (no API key required)

4. Key features
Data ingestion
Hourly pull of price, market cap, 24h volume and 24h price change for bitcoin, ethereum, solana, cardano, dogecoin, ripple, polkadot and litecoin
30-second request timeout and HTTP error checking, so a bad response fails loudly and triggers a retry
Orchestration (Airflow)
4 tasks with explicit dependencies, defined with the TaskFlow API
Automatic retries: 3 attempts, 2 minutes apart
catchup=False to avoid backfilling every missed hour
Full run history, logs and task status in the Airflow UI
Transformations and validation
Drops records with a missing coin id
Drops records with a null, zero or negative price
Removes duplicate coins within a batch
Renames API fields to match the warehouse schema
Logs every dropped record with a warning
Warehouse design
raw schema: full API payload plus fetch timestamp
clean schema: typed columns with composite primary key (coin_id, fetched_at)
Idempotent loads using ON CONFLICT DO NOTHING, so retries and re-runs never create duplicate rows
Quality and observability
Unit tests for the transformation logic
Structured Python logging in every stage
Airflow UI for monitoring failures and retries
5. Data model
raw.crypto_prices

Column	Type	Description
id	SERIAL (PK)	Row id
fetched_at	TIMESTAMPTZ	When the data was fetched
payload	JSONB	Full, unmodified API response

clean.crypto_prices

Column	Type	Description
coin_id	TEXT	Coin identifier, e.g. bitcoin
price_usd	NUMERIC	Current price in USD
market_cap_usd	NUMERIC	Market capitalisation
volume_24h_usd	NUMERIC	24-hour trading volume
price_change_24h_pct	NUMERIC	24-hour price change in percent
fetched_at	TIMESTAMPTZ	Fetch timestamp

Primary key: (coin_id, fetched_at)
6. Project structure
de-pipeline/
├── dags/
│   └── crypto_etl.py        Airflow DAG definition
├── src/
│   ├── extract.py           API ingestion
│   ├── transform.py         cleaning and validation
│   └── load.py              PostgreSQL loaders
├── sql/
│   └── init.sql             raw and clean schemas
├── tests/
│   └── test_transform.py    unit tests
├── docker-compose.yml       full stack definition
└── README.md
7. Getting started
Prerequisites: Docker Desktop (with the engine running).
git clone https://github.com/binnyhinn/de-pipeline.git
cd de-pipeline
docker compose up -d
Open http://localhost:8080 and log in (admin / admin)
Un-pause the crypto_etl DAG using the toggle
Trigger it manually with the play button, or wait for the next hourly run
To stop everything: docker compose down (add -v to also delete the stored data).
8. Querying the warehouse
Latest prices:
docker compose exec warehouse-db psql -U warehouse -d warehouse -c \
"SELECT coin_id, price_usd, price_change_24h_pct, fetched_at
 FROM clean.crypto_prices
 ORDER BY fetched_at DESC, coin_id
 LIMIT 8;"
Row counts per layer:
docker compose exec warehouse-db psql -U warehouse -d warehouse -c \
"SELECT 'raw' AS layer, count(*) FROM raw.crypto_prices
 UNION ALL
 SELECT 'clean', count(*) FROM clean.crypto_prices;"
The warehouse is also reachable from your machine at localhost:5433 (user warehouse, database warehouse) with any SQL client.
9. Testing
docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -v

Test	What it verifies
valid record is kept and renamed	field mapping is correct
missing price is dropped	null prices are rejected
zero or negative price is dropped	invalid values are rejected
missing id is dropped	records without an identifier are rejected
duplicates are removed	the same coin is not loaded twice in a batch

10. Design decisions
Raw and clean layers: keeping the original payload means transformation logic can change later and history can be reprocessed without calling the API again.
Idempotent loads: Airflow retries tasks on failure, so every write must be safe to repeat.
Plain Python transforms: the dataset is small, so plain Python keeps the logic easy to unit test without extra dependencies.
Separate databases: Airflow's metadata and the analytics warehouse live in different Postgres instances, so pipeline internals never mix with business data.
11. Limitations
Credentials are hardcoded for local development; a production setup would use a secrets manager
The free CoinGecko tier is rate limited; the hourly schedule stays well within it
Data passes between tasks through Airflow XCom, which suits small payloads but not large datasets
Single-node LocalExecutor, not a distributed setup
No alerting yet on task failure (failures are visible in the Airflow UI only)
12. Future improvements
Email or Slack alerts on task failure
Data quality checks (for example with Great Expectations)
A dashboard on top of the clean layer
Moving to the CeleryExecutor or Kubernetes for scaling
Extending to a streaming version with Kafka
13. What this project demonstrates
Data ingestion from a public API · workflow orchestration and retry handling · data validation and deduplication · warehouse layering · idempotent loading · containerised deployment · automated testing · logging and monitoring

