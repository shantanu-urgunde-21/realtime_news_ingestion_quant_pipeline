# 📈 Sentinel-Stream: Real-Time Quantitative Trading & Alerts Engine

![Architecture: Microservices](https://img.shields.io/badge/Architecture-Microservices-blue)
![Data Stream: Kafka](https://img.shields.io/badge/Data_Stream-Confluent_Kafka-black)
![Processing: Pathway](https://img.shields.io/badge/Processing-Pathway_(Rust)-orange)
![ML: XGBoost](https://img.shields.io/badge/ML-XGBoost-green)

**Sentinel-Stream** is a production-grade, event-driven quantitative trading and alerts engine. Designed for high-throughput and low-latency, it leverages **Pathway (Rust)** for stateful sliding-window calculations (GARCH, ARMA, RSI, MACD), **Confluent Kafka** for real-time routing, and **ClickHouse** for analytical storage—all driving a dual-model **XGBoost** inference pipeline to deliver high-confidence market signals.

### 🧰 Tech Stack & Project Composition
Based on structural analysis, the codebase is composed of **15 core Python/Code modules**, orchestrated via **Docker & Docker Compose**, and divided into distinct layers:
*   **Ingestion:** Python-based Stock & News API Services.
*   **Computation:** Rust-accelerated Pathway Calculation Service.
*   **Intelligence:** XGBoost Decision Service.
*   **Alerts:** Firebase-integrated Backend Service.
*   **Infrastructure:** ClickHouse SQL schemas and Telemetry.

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

## 🏗️ System Architecture & Data Flow

The system consists of 7 core microservices working in a reactive event-driven stream:

1.  **Stock Service**: Replays historical stock data from a local CSV feed into the `stock_table` Kafka topic (simulation speed is dynamically configurable via the `REPLAY_SPEEDUP` variable).
2.  **News Service**: Fetches global news sentiment and publishes aggregated news sentiment records directly to the `news_sentiment` Kafka topic at high-frequency.
3.  **Calc Service (Pathway)**: Performs real-time windowed volatility and indicator forecasting (GARCH/ARMA, RSI, MACD). Resolves stateful **temporal `asof_join`** operations in-memory.
4.  **ClickHouse (OLAP Storage)**: Pipes raw calculations into analytical tables natively via Materialized Views (`mv_kafka_to_final`).
5.  **Decision Service (XGBoost)**: Reads pre-joined technical indicators and news sentiments directly from the Kafka stream to execute immediate dual-model inferences.
6.  **Backend (Alerts)**: Listens for high-confidence predictions on the `alert` Kafka topic and dispatches push notifications.
7.  **ClickHouse Telemetry (`clickhouse_monitoring`)**: A dedicated database instance for centralized telemetry logging.

---

## 🔍 Deep Dive: Calculation Service

The **Calculation Service** is the analytical heart of the pipeline, leveraging Rust-backed **Pathway**. It performs continuous incremental computation rather than batch processing. 
*   **Temporal Joins:** Uses `asof_join` on `symbol` and `ts_ms` to combine asynchronous stock ticks with the latest news sentiment accurately.
*   **Windowed Metrics:** Calculates complex technical indicators (e.g., RSI, EMA, MACD, GARCH volatility) on rolling memory windows without needing external database calls.
*   **Upstream Emission:** The finished, fully-enriched feature vector is emitted directly to the `stock_calculation_table` topic.

---

## 🛡️ Engineering Excellence & Guardrails

Sentinel-Stream is built with strict defensive programming and infrastructure resilience:

*   **Stateless Inference Decoupling:** The XGBoost `decision_service` is completely decoupled from ClickHouse. It reads directly from Kafka, eliminating database polling threads, caches, and connection overhead from the critical ML path.
*   **Robust Orchestration & Failsafes:** Docker Compose enforces strict `depends_on` health checks. Downstream microservices *only* start once Kafka and ClickHouse report healthy, preventing startup race conditions and message drops.
*   **Type Safety & Schema Validation:** Data boundaries between microservices are enforced using Pydantic schemas (e.g., `QuoteSchema`, `NewsSentimentSchema`), ensuring malformed events are dropped before polluting the inference engine.
*   **Kafka-Based Retry Routing & DLQ:** Enforces dynamic Kafka-based retry topic routing (`alert_retry`) with incremental delay backoffs and a Dead Letter Queue (`alert_dlq`) for quarantine. This ensures transient network or API failures (e.g. downstream alert dispatches) do not block or crash the pipeline.
*   **Isolated Telemetry Tier:** Centralized logs, system resource metrics, and end-to-end latencies are sent to a completely dedicated `telemetry` database instance. This prevents diagnostic queries from competing with core financial queries.
*   **Dynamic Throttle Guardrails:** Simulation speed is dynamically configurable to prevent CPU starvation and buffer overflows in local Docker Desktop environments.

---

## ⚡ Performance Optimizations

*   **Stateful Memory Operations**: Displaces heavy data-joins upstream into Pathway, executing low-latency Rust-accelerated joins on active memory tables rather than expensive SQL queries.
*   **Dual-Model Concurrency**: The Decision service executes two separate XGBoost models simultaneously in memory, minimizing blocking I/O and processing lag.

---

## 🛠️ Configuration & Development

### Local Testing (Dummy Mode)
By default, the pipeline is configured for easy local testing without external dependencies:
- **Firebase**: `USE_DUMMY_FIREBASE=true` enables a mock alert system.
- **Stock Simulation**: `REPLAY_SPEEDUP=5.0` replays stock price updates at a stable 5x simulation speed.
- **News Mocking**: `USE_DUMMY_API=true` generates local simulated articles.

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

## 📜 Logs & Monitoring
Each service logs its activity to the shared `logs` directory. You can query centralized metrics inside the telemetry instance:
```bash
docker-compose exec clickhouse_monitoring clickhouse-client -q "SELECT * FROM telemetry.pipeline_latencies LIMIT 10"
```
