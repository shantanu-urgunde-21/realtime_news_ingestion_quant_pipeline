/* ============================================================
   MONITORING, METRICS & TELEMETRY DATABASE
   ============================================================ */
CREATE DATABASE IF NOT EXISTS telemetry;
USE telemetry;

/* ============================================================
   1. PIPELINE LATENCY METRICS  
   Tracks processing speed, network delays, and end-to-end lag
   ============================================================ */
CREATE TABLE IF NOT EXISTS pipeline_latencies (
    timestamp DateTime DEFAULT now(),
    service_name LowCardinality(String),
    symbol LowCardinality(String),
    metric_name LowCardinality(String),  -- 'ingestion_delay', 'indicator_compute_delay', 'ml_inference_delay', 'fcm_push_delay', 'e2e_latency'
    latency_ms Float64,
    cycle UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (service_name, metric_name, timestamp)
SETTINGS index_granularity = 8192;


/* ============================================================
   2. SYSTEM RESOURCE MONITORING  
   Stores telemetry of CPU, Memory, Disk usage across microservices
   ============================================================ */
CREATE TABLE IF NOT EXISTS system_metrics (
    timestamp DateTime DEFAULT now(),
    service_name LowCardinality(String),
    cpu_utilization_pct Float32,
    memory_used_mb Float32,
    memory_total_mb Float32,
    disk_used_gb Float32,
    network_rx_bytes UInt64,
    network_tx_bytes UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (service_name, timestamp)
SETTINGS index_granularity = 8192;


/* ============================================================
   3. KAFKA MESSAGE BROKER METRICS  
   Tracks consumer group lags, messages/sec, and queuing offsets
   ============================================================ */
CREATE TABLE IF NOT EXISTS kafka_metrics (
    timestamp DateTime DEFAULT now(),
    topic_name LowCardinality(String),
    consumer_group LowCardinality(String),
    partition UInt32,
    committed_offset Int64,
    latest_offset Int64,
    consumer_lag Int64,
    messages_per_sec Float32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (topic_name, consumer_group, partition, timestamp)
SETTINGS index_granularity = 8192;


/* ============================================================
   4. CENTRALIZED SERVICE LOG SINK  
   Saves logs (especially WARNING & ERROR levels) for cross-service diagnostics
   ============================================================ */
CREATE TABLE IF NOT EXISTS service_logs (
    timestamp DateTime DEFAULT now(),
    service_name LowCardinality(String),
    log_level LowCardinality(String),  -- 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'FATAL'
    message String,
    exception_details String,
    host_name String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (log_level, service_name, timestamp)
SETTINGS index_granularity = 8192;
