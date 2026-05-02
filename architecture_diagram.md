# Real-Time AI Investment Decision System Architecture

```text
=============================================================================================================
                                REAL-TIME AI INVESTMENT DECISION PIPELINE
=============================================================================================================

[ DATA SOURCES ]
       |
       +---> [1] Historical CSV Data (merged_stock_popular_red.csv)
       |
       +---> [2] Alpha Vantage API (Live Financial News Headlines)

-------------------------------------------------------------------------------------------------------------
[ MICROSERVICES & REAL-TIME DATA FLOW ]

  [1] CSV Data
       |
       v
 +-------------------------+          +------------------------------+          +---------------------------+
 |      STOCK SERVICE      | =======> |  Kafka Topic: 'stock_table'  | =======> |       CALC SERVICE        |
 | (Replays CSV at 30x     |          |  (Raw Price & Volume Data)   |          | (Pathway Stream Engine)   |
 |  to simulate live feed) |          +------------------------------+          | - Calculates GARCH/ARMA   |
 +-------------------------+                                                    | - 150-min sliding windows |
                                                                                +---------------------------+
                                                                                              |
        +-------------------------------------------------------------------------------------+
        |
        v
 +---------------------------------------+       +----------------------------------------------------------+
 | Kafka Topic: 'stock_calculation_... ' | ====> |                   CLICKHOUSE DATABASE                    |
 | (Enriched with Technical Indicators)  |       | [Table: kafka_input]                                     |
 +---------------------------------------+       |  - ClickHouse natively consumes the Kafka topic          |
        |                                        +----------------------------------------------------------+
        |                                                                     |
        |                                                                     v
        |                                        +----------------------------------------------------------+
        |                                        | [Materialized View: mv_kafka_to_final]                   |
        |                                        |  - Automatically triggers on new data                    |
        |                                        |  - Parses JSON and moves data to final storage           |
        |                                        +----------------------------------------------------------+
        |                                                                     |
        |                                                                     v
        |                                        +----------------------------------------------------------+
        |       +--------------------------------| [Table: final_table]                                     |
        |       |    (Queries MAX(timestamp)     |  - Long-term storage for historical analytics            |
        |       |     to sync simulated time)    +----------------------------------------------------------+
        v       v
 +-------------------------+  [2] Alpha Vantage 
 |      NEWS SERVICE       | <----- API Data     +----------------------------------------------------------+
 | (Fetches News & uses    |                     | [Table: sentiment_stream]                                |
 |  FinBERT NLP for score) | ==================> |  - News Service uses Bulk HTTP POST (JSONEachRow) to     |
 +-------------------------+   (Direct Insert)   |    insert hundreds of sentiment scores at once.          |
                                                 +----------------------------------------------------------+
                                                                              |
 +-------------------------+    (Periodic 5-min in-memory caching)            |
 |    DECISION SERVICE     | <------------------------------------------------+
 | (XGBoost ML Inference)  |
 | - Uses Cached Sentiment |
 | - Uses Kafka Indicators |
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

=============================================================================================================
```

## Architecture Details:
1. **Microservice Responsibilities:** 
   - `calc_service` handles heavy stream processing and rolling window calculations using Pathway.
   - `decision_service` combines this structured output with external sentiment to execute XGBoost inference models.
2. **Database Interactions:** 
   - The pipeline uses a hybrid insertion strategy: `news_service` uses bulk HTTP inserts into `sentiment_stream`, while `calc_service` routes data through Kafka, which is passively ingested by ClickHouse via a Kafka Engine and Materialized View into `final_table`.
3. **Data Synchronization:** 
   - `news_service` stays temporally synced by periodically querying the latest timestamp from `final_table`.
4. **Performance Caching:** 
   - `decision_service` utilizes an in-memory 5-minute cache of the `sentiment_stream` table to process thousands of inference requests per second without database bottlenecks.
