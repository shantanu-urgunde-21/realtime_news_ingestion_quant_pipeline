# Sentinel-Stream: Real-Time Investment Decision Pipeline Architecture

```text
=============================================================================================================
                                  SENTINEL-STREAM PIPELINE
=============================================================================================================

[ DATA SOURCES ]
       |
       +---> [1] Historical CSV Data (merged_stock_popular_red.csv)
       |
       +---> [2] Alpha Vantage API (Simulated High-Frequency Financial News Headlines)

-------------------------------------------------------------------------------------------------------------
[ MICROSERVICES & REAL-TIME DATA FLOW ]

  [1] CSV Data
       |
       v
 +-------------------------+          +------------------------------+
 |      STOCK SERVICE      | =======> |  Kafka Topic: 'stock_table'  | =======\
 | (Replays CSV at 5x      |          |  (Raw Price & Volume Data)   |         \
 |  simulation throttle)   |          +------------------------------+          \
 +-------------------------+                                                     \
                                                                                  \
  [2] News API                                                                     v
 +-------------------------+          +----------------------------------+   +---------------------------+
 |      NEWS SERVICE       | =======> | Kafka Topic: 'news_sentiment'    | > |       CALC SERVICE        |
 | (Runs on 1-min cycle,   |          | (Aggregated Sentiment Scores)    |   | (Pathway Stream Engine)   |
 |  FinBERT NLP sentiment) |          +----------------------------------+   | - In-Memory Indicators    |
 +-------------------------+                                                 | - Stateful temporal join  |
                                                                             |   (asof_join on symbol/ts)|
                                                                             +---------------------------+
                                                                                           |
        +----------------------------------------------------------------------------------+
        |
        v
 +-------------------------------------+       +------------------------------------------------------------+
 | Kafka Topic: 'stock_calculation...' | ====> |                    CLICKHOUSE DATABASE                     |
 | (Unified Price Ticks, Indicators,   |       | - Ingests Kafka natively via 'kafka_input' table           |
 |  and Joined Sentiment Metrics)      |       | - Materialized View ('mv_kafka_to_final') writes to:       |
 +-------------------------------------+       |   [Table: final_table] for historical backtests            |
        |                                      +------------------------------------------------------------+
        |
        v
 +-------------------------+
 |    DECISION SERVICE     |
 | (XGBoost ML Inference)  |
 | - 100% Stateless        |
 | - Zero DB Connections   |
 | - Instant forward passes|
 +-------------------------+
        |
        | (Publishes Alert IF absolute predicted change > 2% AND confidence > 50%)
        v
 +------------------------------+
 |     Kafka Topic: 'alert'     |
 +------------------------------+
        |
        v
 +-------------------------+
 |     BACKEND SERVICE     | =======> [Firebase Cloud Messaging] =======> [Mobile Devices / Users]
 | (Notification Manager)  |             (Push Notifications)
 | - Exponential Backoff   |
 +-------------------------+

-------------------------------------------------------------------------------------------------------------
[ TELEMETRY & OBSERVABILITY TIER ]

 +-----------------------------+       +------------------------------------------------------------+
 | All Services (Logs/Metrics) | ====> |             clickhouse_monitoring (isolated port 8124)     |
 | - cpu, RAM, rx/tx throughput|       |             [Table: telemetry.pipeline_latencies]          |
 | - E2E Latencies & Logs      |       |             [Table: telemetry.system_metrics]              |
 +-----------------------------+       +------------------------------------------------------------+
=============================================================================================================
```

## Architecture Details:
1. **Upstream Streaming Joins:**
   - Instead of microservices querying databases for joining historical attributes, `calc_service` performs an in-memory, stateful `asof_join` on `symbol` and `ts_ms` directly inside Pathway's Rust-accelerated engine.
2. **Stateless Decoupled Inference:**
   - The `decision_service` has **zero database dependencies**. It is a pure ML worker that consumes a complete pre-joined calculations-and-sentiment payload from Kafka, performs sub-millisecond XGBoost model evaluations, and publishes alerts.
3. **Robust Orchestration Dependencies:**
   - Microservices share automated Docker health checks, holding initialization loops until Kafka and ClickHouse pass internal connectivity checks.
4. **Dedicated Telemetry Cluster:**
   - Observability log sinks, consumer lag statistics, and end-to-end processing delays are stored in an isolated ClickHouse server (`clickhouse_monitoring` database instance) to ensure monitoring queries never compete with the main high-frequency quantitative pipeline.
