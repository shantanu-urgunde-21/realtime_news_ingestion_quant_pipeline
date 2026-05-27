# Sentinel-Stream: Local Development & Operations Guide

This guide provides detailed instructions on how to set up, run, monitor, and debug the entire Quantitative News Ingestion & Alerts pipeline locally on your Linux host.

---

## 1. Prerequisites & Host Configuration

### 💻 System Requirements
1. **Docker Engine & Compose:** Used for running the isolated backend databases and broker clusters.
2. **Python 3.12 or 3.13:** The virtual environment should be configured natively on your host machine.

### 🌐 Network Hostname Resolution
Because Kafka and ClickHouse inside Docker are configured to communicate using dedicated service hostnames, we must map them on the host. This allows your local Python processes to connect out-of-the-box with zero modifications.

Run the following command once to configure hostname resolution:
```bash
echo "127.0.0.1 kafka clickhouse clickhouse_monitoring" | sudo tee -a /etc/hosts
```

---

## 2. Automated Orchestration Script (Recommended)

A fully featured control utility `run_local.sh` is provided in the project root to manage the entire lifecycle of the hybrid stack.

### 🚀 Start the Entire Pipeline
This command maps files, boots up Docker database/broker instances, waits until they pass native health checks, and launches all 5 microservices plus the lag monitor in the correct logical dependency order:
```bash
./run_local.sh start
```

### 📊 Check Pipeline Health & Process Status
Displays all active background microservices and reports Docker container states:
```bash
./run_local.sh status
```

### 📋 Stream Real-Time Application Logs
Tail the logs of any microservice in real-time. Available log services are: `backend`, `decision_service`, `calc_service`, `news_service`, `stock_service`, and `kafka_monitor`.
```bash
# Example: Tailing the Decision Service (XGBoost ML) logs
./run_local.sh logs decision_service

# Example: Tailing the main Pathway Calculation Engine
./run_local.sh logs calc_service
```

### 🛑 Stop Everything Cleanly
Gracefully kills all running background Python microservices and stops all running Docker containers:
```bash
./run_local.sh stop
```

---

## 3. Manual Command Breakdown (Step-by-Step)

If you prefer to run and debug the services manually across separate terminal windows, use the following commands.

### Step 1: Start Databases & Brokers in Docker
Spins up only the core infrastructure (Kafka broker, main ClickHouse, and Monitoring ClickHouse):
```bash
docker compose up -d kafka clickhouse clickhouse_monitoring
```
*(Wait until `docker compose ps` shows all three services as `healthy`)*

### Step 2: Initialize the Python Environment
Create a unified Python virtual environment at the repository root and install all required quantitative, machine learning, and database client libraries:
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Upgrade pip and install all required modules
pip install --upgrade pip
pip install requests kafka-python pathway python-dotenv psutil xgboost numpy scikit-learn pandas clickhouse-connect
```

### Step 3: Run the Microservices in Separate Terminals
In each terminal window, **ensure you activate the environment** (`source venv/bin/activate`) and **set the module search path** (`export PYTHONPATH=$(pwd)/src/code` or point directly to the directory).

Run them in this exact order:

#### **Terminal 1: Backend Service (FCM Notification Mock)**
```bash
export PYTHONPATH="/home/shantanu/programming/realtime_news_ingestion_quant_pipeline/src/code"
cd src/code/backend
KAFKA_BROKER=kafka:9092 USE_DUMMY_FIREBASE=true python3 main.py
```

#### **Terminal 2: Decision Service (XGBoost ML Predictions)**
```bash
export PYTHONPATH="/home/shantanu/programming/realtime_news_ingestion_quant_pipeline/src/code"
cd src/code/decision_service
KAFKA_BROKER=kafka:9092 CLICKHOUSE_HOST=clickhouse CLICKHOUSE_PORT=9000 python3 xgboost_mdl_inf.py
```

#### **Terminal 3: Pathway Calculation Service (Stream Join Indicator compute)**
```bash
export PYTHONPATH="/home/shantanu/programming/realtime_news_ingestion_quant_pipeline/src/code"
cd src/code/calc_service
KAFKA_BROKER=kafka:9092 python3 main.py
```

#### **Terminal 4: News Service ( Headline Sentiment Ingestion)**
```bash
export PYTHONPATH="/home/shantanu/programming/realtime_news_ingestion_quant_pipeline/src/code"
cd src/code/news_service
KAFKA_BROKER=kafka:9092 USE_DUMMY_API=true python3 main.py
```

#### **Terminal 5: Stock Service (Market Feed Replay - Trigger)**
```bash
export PYTHONPATH="/home/shantanu/programming/realtime_news_ingestion_quant_pipeline/src/code"
cd src/code/stock_service
KAFKA_BROKER=kafka:9092 REPLAY_SPEEDUP=5.0 python3 main.py
```

#### **Terminal 6: Consumer Group Lag Monitor Daemon (Telemetry)**
```bash
export PYTHONPATH="/home/shantanu/programming/realtime_news_ingestion_quant_pipeline/src/code"
cd src/code/infra
KAFKA_BROKER=kafka:9092 python3 kafka_monitor.py
```

---

## 4. Troubleshooting & Operational Gotchas

### 🚨 1. FileNotFoundError (CWD Relative Paths)
* **Problem:** Running python main scripts directly from the repository root (e.g. `python3 src/code/stock_service/main.py`) causes relative asset paths like `merged_stock_popular_red.csv` or `xgb_pct_change_model.json` to fail with `FileNotFoundError`.
* **Explanation:** Python evaluates relative paths relative to the current working directory (CWD) of your terminal session, not the location of the `.py` script.
* **Solution:** Always `cd` into the respective microservice subdirectory before executing the python script (e.g., `cd src/code/stock_service && python3 main.py`). The `run_local.sh` orchestrator automates this by wrapping executions in subshell blocks `(cd <dir> && ...) &`.

### 🚨 2. ClickHouse Authentication Failed (401 Unauthorized)
* **Problem:** The `news_service` or database loaders output `ClickHouse connection returned status 401`.
* **Explanation:** The ClickHouse container has password protection enabled by default (`CLICKHOUSE_PASSWORD=password`). Standard connection URLs like `http://localhost:8123` without credentials will be rejected.
* **Solution:** Use the HTTP Basic Authentication format inside your `src/code/news_service/.env` file:
  ```env
  CLICKHOUSE_URL=http://default:password@127.0.0.1:8123
  ```

### 🚨 3. HTTP Read Timed Out (IPv6 Loopback Drop)
* **Problem:** Connection queries to `localhost:8123` consistently time out after exactly 5 seconds (`Read timed out. (read timeout=5)`).
* **Explanation:** ClickHouse binds natively to IPv4 (`0.0.0.0:8123`). However, modern Linux resolvers map the hostname `localhost` to the IPv6 loopback (`::1`) first. When Python requests try to establish a handshake over IPv6, the system silently drops the packets instead of rejecting them immediately, causing the socket to hang until the timeout is reached.
* **Solution:** Replace `localhost` with the explicit IPv4 loopback IP **`127.0.0.1`** in all `.env` connection strings and environment variables. This bypasses IPv6 lookups entirely and connects in under 1ms.

### 🚨 4. Kafka `UnknownTopicOrPartition` Error on Startup
* **Problem:** You see error logs such as: `Subscribed topic not available: news_sentiment: Broker: Unknown topic or partition` or `Message consumption error: UnknownTopicOrPartition`.
* **Explanation:** When the Calculation Service (`calc_service`) or Decision Service starts up, they immediately subscribe to their target Kafka topics (`news_sentiment` and `stock_table` / `stock_calculation_table`). If the respective producer microservices (like `stock_service` or `news_service`) have not sent their first messages yet, these topics do not yet exist on the Kafka broker, raising a transient query error.
* **Solution:** No action is needed. Kafka and Pathway are built to self-heal. As soon as the producers send their initial messages, the broker auto-creates the topics, and the consumers automatically resolve the subscription and begin streaming within seconds.

---

## 5. Quantitative ML Pipeline Insights (Warm-up & GARCH)

This section answers critical design and timing questions regarding the GARCH/ARMA models and the stateful stream join.

### ⏱️ GARCH Warm-up Time & Speedup Factors
The Calculation Service computes stateful **GARCH(1,1) Volatility** and **ARMA(1,1) Returns** forecasts over a sliding window:
* **Duration of window:** `30` periods of `5-minute` intervals = **150 minutes of simulated market data**.
* **Hop interval:** `5 minutes`.

Depending on the `REPLAY_SPEEDUP` environment variable configured in `stock_service` (default `5.0`), the real-world time required to accumulate the first complete 150-minute window of historical data varies:

| Replay Speedup Factor | Simulated Time Accumulated | Real-world Warm-up Wall-time |
| :---: | :---: | :---: |
| **1.0x** | 150 minutes | 2 hours 30 minutes |
| **5.0x** (Default) | 150 minutes | **30 minutes** |
| **10.0x** | 150 minutes | 15 minutes |
| **30.0x** | 150 minutes | 5 minutes |

> [!NOTE]
> Setting the speedup factor too high (e.g. `> 30x`) may cause processing lag or queue overflows in local Docker environments with limited CPU cores due to the high intensity of real-time GARCH fitting inside the Pathway stream. `5.0x` to `10.0x` is the recommended sweet spot.

---

### 🧠 Is GARCH required for the Decision Service?
**Yes, GARCH is strictly required.** The trained XGBoost regressors and classifiers inside `decision_service` utilize a 15-feature matrix to predict stock percentage changes and the probability of a "big move." 
Among these, the following GARCH/ARMA parameters are crucial inputs:
1. `sigma_forecast` (GARCH volatility estimate for $t+1$)
2. `arma_forecast` (ARMA return estimate for $t+1$)
3. `risk_adj_ret` (Risk-adjusted return: `arma_forecast / sigma_forecast`)

Without these features, inference would yield high-variance or incorrect predictions.

---

### ⚙️ What are other services doing during the 30-minute warm-up?
During the first 30 minutes of real-world time (before the first 150-minute simulated window fills):
1. **Stock Service:** Continuously streams historical CSV bars to Kafka (`stock_table`) at `5.0x` speed.
2. **News Service:** Asynchronously polls/ingests real-time news sentiments into Kafka (`news_sentiment`).
3. **Calculation Service (Pathway):**
   * Stateful indicators with shorter windows (e.g., **RSI** requiring 14 periods, **EMA/MACD** requiring shorter warm-ups) begin calculating and updating immediately as bars arrive.
   * Because the GARCH window is not yet complete, the service **gracefully coalesces GARCH metrics** to default baseline values:
     * `sigma_forecast` = `1e-3` (0.001 baseline volatility)
     * `arma_forecast` = `0.0`
     * `risk_adj_ret` = `0.0`
   * These default values are joined with quotes and streamed immediately to `stock_calculation_table` to prevent blocking the pipeline.
4. **Decision Service (XGBoost):** Actively consumes these partially-enriched calculation messages. It executes inference successfully using the default baseline parameters (`1e-3` / `0.0`) so the system remains alive and operational.
5. **Backend Service (Firebase Mock):** Stands ready, listening to `alert` messages to print alerts if predictions exceed the trigger thresholds (even during warm-up).
6. **Lag Monitor:** Continues logging broker and consumer group offset performance metrics into the ClickHouse tracking database.

---

## 6. Verification Checklist
1. **FCM Alerts:** The backend logs should start logging `✓ Alert sent for [SYMBOL]: mock-msg-...` whenever the Decision Service detects an absolute price calculation change of `> 2%` with high confidence.
2. **Lag Tracking:** Run the lag monitor and query the database logs in ClickHouse to verify metrics are ingested successfully:
   ```bash
   # Connect to ClickHouse client inside Docker
   docker exec -it clickhouse_monitoring clickhouse-client --query "SELECT * FROM telemetry.kafka_metrics LIMIT 10"
   ```
3. **No Auth Errors:** Centralized log flushes in the microservices logs will output `[TelemetryClient Info] Reconnected successfully...` on startup and will run silently, suppressing redundant alerts or exceptions.
