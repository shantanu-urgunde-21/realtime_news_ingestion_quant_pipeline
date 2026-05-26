# Changelog

## [1.3.0] - Decoupled Event-Driven Streaming & Stateful Joins
### Added
- Added a new service `clickhouse_monitoring` to `docker-compose.yml` mapped to a new host persistent volume `clickhouse_monitoring_data` and exposed on HTTP port `8124` / Native port `9001`.
- Added a new SQL schema initialization script `src/code/infra/monitoring_schema.sql` defining the `telemetry` database with four optimized `MergeTree` tables: `pipeline_latencies` (tracking E2E processing latencies), `system_metrics` (storing CPU, Memory, Disk, and Network RX/TX stats), `kafka_metrics` (committed/latest offsets and partition lag), and `service_logs` (centralized logs from all containers).
- Added `KafkaProducer` instantiation and retry configuration (15 attempts with 2s delays) in the `__init__` constructor of `NewsIngestionService` inside `src/code/news_service/main.py`.
- Added a dedicated `publish_to_kafka` method in `NewsIngestionService` that maps bulk processed ticker results into JSON payloads containing `"symbol"`, `"weighted_avg_sentiment"`, `"news_title"`, and `"ts_ms"`, and publishes them directly to the `news_sentiment` Kafka topic.
- Added a robust `_av_timestamp_to_ms` utility parsing method in `NewsIngestionService` supporting conversions for both Alpha Vantage `YYYYMMDDTHHMMSS` and standard SQL DateTime `YYYY-MM-DD HH:MM:SS` strings to output epoch millisecond timestamps (`ts_ms`).
- Added a new `NewsSentimentSchema(pw.Schema)` mapping `/value/symbol`, `/value/weighted_avg_sentiment`, `/value/news_title`, and `/value/ts_ms` to `src/code/calc_service/schemas.py` to ingest streaming sentiments.

### Changed
- Refactored `src/code/calc_service/main.py` to configure a real-time `pw.io.kafka.read` reader subscribing to the `news_sentiment` Kafka topic using the new `NewsSentimentSchema`.
- Implemented a stateful streaming temporal `asof_join` on `symbol` and `ts_ms` (via `JoinMode.LEFT` mode) joining the rolling stock calculations (`enriched_final`) with the dynamic `news_sentiment` stream in the Pathway computation graph in `calc_service/main.py`.
- Enriched the final output selection (`final_table` stream) in `calc_service/main.py` by coalescing `weighted_avg_sentiment` (defaulting to `0.0`) and `news_title` (defaulting to `"No news available"`) and writing them upstream to the `stock_calculation_table` Kafka topic.
- Refactored the ML execution worker in `src/code/decision_service/xgboost_mdl_inf.py` to read `weighted_avg_sentiment` and `news_title` directly from the incoming `stock_calculation_table` JSON payloads via `data.get()`, rendering the inference engine completely stateless.
- Updated `src/code/stock_service/main.py` to read simulation speed dynamically from the `REPLAY_SPEEDUP` environment variable (defaulting to a lightweight `5.0x` instead of a hardcoded `30.0x`) to prevent WSL2 CPU and disk I/O resource throttling.
- Increased ClickHouse connection retry limits (`max_retries`) in `ClickHouseNewsWriter.__init__` in `src/code/news_service/main.py` from 10 to 30 attempts to accommodate slow database initialization warmup times.
- Upgraded all `depends_on` conditions from `service_started` to `service_healthy` for the `kafka` and `clickhouse` containers in `docker-compose.yml` to guarantee safe and orderly startup boot sequencing.
- Updated the primary database schema in `src/code/infra/schema.sql` to include `weighted_avg_sentiment Float32` and `news_title String` in the schemas for `kafka_input`, `final_table`, and the materialized view `mv_kafka_to_final`.

### Removed
- Removed the `clickhouse_driver.Client` import, environment connection parameters, `sentiment_cache` memory buffer dictionaries, `last_cache_update` timers, and the `update_sentiment_cache()` database periodic polling function from `src/code/decision_service/xgboost_mdl_inf.py`.
- Removed `clickhouse-driver` dependency from `src/code/decision_service/requirements.txt` to minimize container dependencies.

## [1.2.0] - Architecture Optimization
### Removed
- Removed the legacy python container `infra_init` from `docker-compose.yml` and replaced database initialization by mounting `schema.sql` directly into ClickHouse's native entrypoint initialization directory (`/docker-entrypoint-initdb.d/schema.sql`).
- Removed the obsolete `stock_timestamp` Kafka topic and terminated the background consumer threads that were parsing timestamps inside `news_service/main.py`.
- Removed the redundant individual ML training scripts `xgboost_mdl_pct_change.py` and `xgboost_mdl_big_move.py` from `src/code/decision_service/`.

### Added
- Added `train_models.py` in `decision_service` to consolidate machine learning model training into a single script that queries ClickHouse, performs out-of-sample train-test splitting, fits both percentage change regressor and big move classifier, and exports `xgb_pct_change_model.json` and `xgb_classifier_model.json` sequentially.
- Added `pw.persistence.Backend.filesystem("./data")` stateful persistence into `calc_service/main.py` mapping to a dedicated host Docker volume (`calc_data`) to prevent the loss of the 150-minute sliding GARCH/ARMA volatility calculations across container reboots.
- Added a reverse mapping encoder in the ML training logic that maps categorical ticker names to integer codes using a saved `symbol_mapping.pkl` file.

### Changed
- Refactored `get_latest_timestamp()` in `news_service/main.py` to execute direct HTTP GET calls (`SELECT MAX(timestamp)`) to the ClickHouse port `8123` to dynamically calculate search window boundaries for the news API.
- Optimized database insertion in `ClickHouseNewsWriter.bulk_insert` to serialize and merge multiple ticker sentiment rows into a newline-separated NDJSON format, posting them in a single bulk HTTP transaction using `FORMAT JSONEachRow` (increasing ingestion speed by over 90%).
- Optimized the critical quote-consumption loop in `decision_service/xgboost_mdl_inf.py` by introducing an in-memory dictionary cache `sentiment_cache` that loads ticker sentiments from `market_data.sentiment_stream` in a background interval every 5 minutes (`CACHE_TTL = 300`), completely bypassing synchronous database queries on each incoming tick.
- Updated database definitions in `schema.sql` to include `CREATE TABLE IF NOT EXISTS` guard statements, declared the `sentiment_stream` table ordered by `(cycle, symbol)`, and partitioned `final_table` dynamically by month `toYYYYMM(timestamp)` with `index_granularity = 8192` for rapid analytics execution.

## [1.1.0] - Dockerization & Stabilization
### Added
- Comprehensive `docker-compose.yml` for orchestrating Kafka (KRaft mode), ClickHouse, and Python microservices.
- `Dockerfile` definitions for all microservices.
- Exponential backoff retry loops (30 attempts) in Python services to prevent crashes while waiting for ClickHouse initialization.
- `dummy_firebase.py` mock module to allow the `backend` to run locally without real cloud credentials.
- Environment variable injection for database credentials, removing hardcoded parameters.

### Fixed
- ClickHouse port mismatch in `decision_service` (corrected `8123` to `9000` for the native TCP driver).
- Missing `logging` imports causing crashes in `backend/send_message.py`.
