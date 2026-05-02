# Changelog

## [1.2.0] - Architecture Optimization
### Removed
- `infra_init` microservice. Database initialization is now handled natively by ClickHouse's entrypoint.
- `stock_timestamp` Kafka topic and associated background consumer threads in `news_service`.
- Redundant ML training scripts (`xgboost_mdl_pct_change.py` and `xgboost_mdl_big_move.py`).

### Added
- `train_models.py` in `decision_service` to consolidate machine learning model training into a single execution.
- Pathway persistence (`pw.persistence.Backend.filesystem`) in `calc_service` to retain the 150-minute sliding window state across container restarts.
- `calc_data` persistent Docker volume for state recovery.

### Changed
- Refactored `news_service` date-range logic to use direct ClickHouse queries (`SELECT MAX(timestamp)`) instead of Kafka event tracking.
- Optimized `news_service` ClickHouse insertions to use bulk HTTP POST requests (`JSONEachRow` format) instead of individual per-ticker requests.
- Optimized `decision_service` inference loop by replacing synchronous per-message ClickHouse queries with a 5-minute in-memory sentiment cache.
- Updated `schema.sql` to use `CREATE MATERIALIZED VIEW IF NOT EXISTS` and added `symbol` to the `ORDER BY` index in `sentiment_stream`.

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
