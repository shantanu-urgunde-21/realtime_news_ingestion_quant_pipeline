# Sentinel-Stream: Rust-Accelerated Real-Time Investment Decision & Alerts Engine

Sentinel-Stream is a production-grade, event-driven quantitative trading and alerts engine. It leverages **Pathway** for stateful sliding-window calculations (including GARCH, ARMA, RSI, and MACD), **Confluent Kafka** for real-time microservices routing, and **ClickHouse** for high-throughput analytical storage, driving dual-model **XGBoost** inference to deliver high-confidence market signals.

## Quick Start

The entire pipeline is containerized and orchestrated via Docker Compose.

### Requirements
- **Docker** and **Docker Compose**
- (Optional) **Python 3.11+** (only if running training scripts locally)

### Steps to Run
1. **Clone the repository**
2. **Start the pipeline**:
   ```bash
   docker-compose up -d --build
   ```
   *This command will pull the necessary images, build the microservices, and initialize the ClickHouse schema automatically.*

3. **Verify the services**:
   ```bash
   docker-compose ps
   ```

---

## 🏗️ Architecture & Data Flow

The system consists of 7 core services working in a reactive event-driven stream:

1.  **Stock Service**: Replays historical stock data from a local CSV feed into the `stock_table` Kafka topic (simulation speed is dynamically configurable via the `REPLAY_SPEEDUP` variable).
2.  **News Service**: Fetches global news sentiment and publishes aggregated news sentiment records directly to the `news_sentiment` Kafka topic at high-frequency (1-minute cycles).
3.  **Calc Service (Pathway)**: Performs real-time windowed volatility and indicator forecasting (GARCH/ARMA, RSI, MACD). Performs an in-memory stateful **temporal `asof_join`** on `symbol` and `ts_ms` combining stock ticks with news sentiment, and emits the finished feature vector upstream to `stock_calculation_table`.
4.  **ClickHouse (OLAP Storage)**: Pipes raw calculations into analytical tables natively via Materialized Views (`mv_kafka_to_final`) for historical model training.
5.  **Decision Service (XGBoost)**: Completely **stateless and decoupled from ClickHouse**, reading pre-joined technical indicators and news sentiments directly from the incoming Kafka stream to execute immediate dual-model XGBoost inferences.
6.  **Backend (Alerts)**: Listens for high-confidence predictions on the `alert` Kafka topic and dispatches mock Firebase push notifications.
7.  **ClickHouse Telemetry (`clickhouse_monitoring`)**: A dedicated database instance (`telemetry` database) logging centralized logs, container system resources, Kafka lag metrics, and end-to-end pipeline latencies.

---

## 🛠️ Configuration & Development

### Local Testing (Dummy Mode)
By default, the pipeline is configured for easy local testing without external dependencies:
- **Firebase**: `USE_DUMMY_FIREBASE=true` enables a mock alert system.
- **Stock Simulation**: `REPLAY_SPEEDUP=5.0` replays stock price updates at a stable 5x simulation speed, and `USE_DUMMY_API=true` in `news_service` generates local simulated articles.

### Retraining the AI Models
If you have new data in ClickHouse and want to update the XGBoost models:
1. Enter the decision service container:
   ```bash
   docker-compose exec decision_service bash
   ```
2. Run the consolidated training script:
   ```bash
   python train_models.py
   ```
3. Restart the service to load new models:
   ```bash
   docker-compose restart decision_service
   ```

---

## ⚡ Performance Optimizations

This pipeline has been hardened for production-level throughput:
- **Stateful Temporal Joins in Pathway**: Displaces data-joins upstream into Pathway, executing low-latency Rust-accelerated `asof_join` calls on active memory tables.
- **Stateless Inference Decoupling**: Completely eliminates database dependency (polling threads, caches, and connections) from the critical ML path, avoiding downstream lag.
- **Microservices Health check Dependency**: Downstream microservices only start once Kafka and ClickHouse pass automated health checks, guaranteeing robust orchestration.
- **Dynamic Simulation Throttle**: Scalable tick density configuration to prevent WSL2 and local Docker Desktop performance bottlenecks.
- **Isolated Telemetry Tier**: Directs logs, metrics, and latency diagnostics to a dedicated database instance to prevent metrics queries from competing with core financial queries.

---

## 📜 Logs & Monitoring
Each service logs its activity to the shared `logs` directory. You can query centralized metrics inside the telemetry instance:
```bash
docker-compose exec clickhouse_monitoring clickhouse-client -q "SELECT * FROM telemetry.pipeline_latencies LIMIT 10"
```
