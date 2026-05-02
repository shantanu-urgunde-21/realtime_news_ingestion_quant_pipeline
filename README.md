# Real-Time AI Investment Decision System

An end-to-end, high-performance microservice pipeline for real-time stock analytics, sentiment analysis, and AI-driven trading alerts.

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

The system consists of 6 core services working in a reactive stream:

1.  **Stock Service**: Fetches real-time market data and publishes to Kafka.
2.  **Calc Service (Pathway)**: Performs windowed GARCH/ARMA calculations and technical indicator generation (EMA, RSI, MACD). Uses **Pathway Persistence** to survive restarts.
3.  **News Service**: Ingests global news sentiment for all active tickers. Optimized with **Bulk Inserts** to ClickHouse.
4.  **ClickHouse**: Our primary OLAP database. Schema is initialized natively on boot.
5.  **Decision Service (XGBoost)**: Consumes technical indicators and sentiment data to predict price moves. Features a **5-minute in-memory sentiment cache** to prevent database bottlenecks.
6.  **Backend (Alerts)**: Listens for high-confidence predictions and sends alerts via Firebase Cloud Messaging (FCM).

---

## 🛠️ Configuration & Development

### Local Testing (Dummy Mode)
By default, the pipeline is configured for easy local testing without external dependencies:
- **Firebase**: `USE_DUMMY_FIREBASE=true` in `docker-compose.yml` enables a mock alert system.
- **Stock API**: `USE_DUMMY_API=true` in `stock_service` environments uses simulated market data.

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
- **Bulk Ingestion**: `news_service` batches hundreds of ticker updates into single ClickHouse transactions.
- **In-Memory Caching**: `decision_service` avoids hammering the database by caching sentiment scores in RAM, updating every 5 minutes.
- **State Persistence**: `calc_service` uses a persistent Docker volume (`calc_data`) to retain its 150-minute sliding window memory across restarts.
- **Native Initialization**: Replaced legacy Python infra-init scripts with native ClickHouse entrypoint SQL initialization for 100% reliability.

---

## 📜 Logs & Monitoring
Each service logs its activity to the shared `logs` directory or can be viewed via Docker:
```bash
docker-compose logs -f [service_name]
```
