import os
import requests
import json
import time
import queue
import threading
import logging
from datetime import datetime

logger = logging.getLogger("telemetry_client")

class TelemetryClient:
    """
    Asynchronous, non-blocking Telemetry Client for quantitative pipeline metrics.
    Uses a thread-safe Queue and background worker thread to batch and push records
    to the clickhouse_monitoring server via its HTTP interface.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(TelemetryClient, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        
        # In Docker Compose, the monitoring server hostname is clickhouse_monitoring
        self.url = os.getenv("CLICKHOUSE_MONITORING_URL", "http://clickhouse_monitoring:8123")
        self.password = os.getenv("CLICKHOUSE_PASSWORD", "password")
        self.queue = queue.Queue()
        self.stop_event = threading.Event()
        
        # Start background worker thread
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()
        self._initialized = True
        logger.info(f"TelemetryClient initialized connecting to {self.url}")

    def log_latency(self, service_name, symbol, metric_name, latency_ms, cycle=0):
        """
        Queue a pipeline latency record to be processed asynchronously.
        
        Args:
            service_name: Name of the microservice reporting the metric
            symbol: Stock symbol associated with the calculation (e.g., 'AAPL')
            metric_name: Name of the latency gauge ('ingestion_delay', 'ml_inference_delay', etc.)
            latency_ms: Latency value in milliseconds
            cycle: Evaluation cycle number
        """
        record = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "service_name": service_name,
            "symbol": symbol,
            "metric_name": metric_name,
            "latency_ms": float(latency_ms),
            "cycle": int(cycle)
        }
        self.queue.put(("pipeline_latencies", record))

    def log_service_log(self, service_name, log_level, message, exception_details="", host_name=""):
        """
        Queue a centralized logging record to be processed asynchronously.
        """
        record = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "service_name": service_name,
            "log_level": log_level,
            "message": message,
            "exception_details": exception_details or "",
            "host_name": host_name or os.getenv("HOSTNAME", "localhost")
        }
        self.queue.put(("service_logs", record))

    def log_system_metrics(self, service_name, cpu_utilization_pct, memory_used_mb, memory_total_mb, disk_used_gb, network_rx_bytes, network_tx_bytes):
        """
        Queue a system resource utilization record to be processed asynchronously.
        """
        record = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "service_name": service_name,
            "cpu_utilization_pct": float(cpu_utilization_pct),
            "memory_used_mb": float(memory_used_mb),
            "memory_total_mb": float(memory_total_mb),
            "disk_used_gb": float(disk_used_gb),
            "network_rx_bytes": int(network_rx_bytes),
            "network_tx_bytes": int(network_tx_bytes)
        }
        self.queue.put(("system_metrics", record))

    def log_kafka_metrics(self, topic_name, consumer_group, partition, committed_offset, latest_offset, consumer_lag, messages_per_sec):
        """
        Queue a Kafka broker metric record to be processed asynchronously.
        """
        record = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "topic_name": topic_name,
            "consumer_group": consumer_group,
            "partition": int(partition),
            "committed_offset": int(committed_offset),
            "latest_offset": int(latest_offset),
            "consumer_lag": int(consumer_lag),
            "messages_per_sec": float(messages_per_sec)
        }
        self.queue.put(("kafka_metrics", record))

    def _worker(self):
        """Background worker loop to batch and write telemetry records periodically."""
        buffer = {
            "pipeline_latencies": [],
            "service_logs": [],
            "system_metrics": [],
            "kafka_metrics": []
        }
        last_flush_time = time.time()
        
        while not self.stop_event.is_set() or not self.queue.empty():
            try:
                # Poll message queue with a timeout to permit periodic flushing on idle
                try:
                    table, record = self.queue.get(timeout=1.0)
                    buffer[table].append(record)
                    self.queue.task_done()
                except queue.Empty:
                    pass

                # Flush rules: buffer size >= 20 OR idle duration >= 2.0s with entries present
                time_since_flush = time.time() - last_flush_time
                total_records = sum(len(lst) for lst in buffer.values())
                
                if total_records >= 20 or (total_records > 0 and time_since_flush >= 2.0):
                    self._flush(buffer)
                    last_flush_time = time.time()
                    
            except Exception as e:
                # Soft fallback to stderr to avoid recursive logging loops on failure
                print(f"[TelemetryClient Worker Error] Failed processing queue: {e}")
                time.sleep(1)

    def _flush(self, buffer):
        """Execute NDJSON bulk insert post requests to clickhouse_monitoring."""
        for table, records in list(buffer.items()):
            if not records:
                continue
            
            # Serialize list of objects to newline-delimited JSON (NDJSON) format
            payload = "\n".join(json.dumps(r) for r in records)
            query = f"INSERT INTO telemetry.{table} FORMAT JSONEachRow"
            
            headers = {
                "Content-Type": "application/x-ndjson"
            }
            if self.password:
                headers["X-ClickHouse-Key"] = self.password
                
            try:
                params = {"query": query}
                response = requests.post(
                    self.url,
                    params=params,
                    data=payload,
                    headers=headers,
                    timeout=5
                )
                if response.status_code == 200:
                    records.clear()  # Flush successful, clear batch buffer
                else:
                    print(f"[TelemetryClient Alert] Failed to flush to {table}: HTTP {response.status_code} - {response.text[:200]}")
            except Exception as e:
                print(f"[TelemetryClient Warning] Failed to post telemetry to ClickHouse: {e}")

    def shutdown(self):
        """Gracefully shut down background queue worker thread."""
        self.stop_event.set()
        if self.worker_thread.is_alive():
            self.worker_thread.join(timeout=3.0)


class ClickHouseLogHandler(logging.Handler):
    """
    Custom Python logging Handler that intercepts service warning and error logs,
    and publishes them asynchronously to the clickhouse_monitoring log registry.
    """
    def __init__(self, service_name, level=logging.WARNING):
        super().__init__(level)
        self.service_name = service_name
        self.telemetry = TelemetryClient()

    def emit(self, record):
        try:
            # Format log message
            msg = self.format(record)
            
            # Format exception traceback if present
            exception_details = ""
            if record.exc_info:
                if self.formatter:
                    exception_details = self.formatter.formatException(record.exc_info)
                else:
                    import traceback
                    exception_details = "".join(traceback.format_exception(*record.exc_info))
                
            self.telemetry.log_service_log(
                service_name=self.service_name,
                log_level=record.levelname,
                message=msg,
                exception_details=exception_details
            )
        except Exception as e:
            print(f"[ClickHouseLogHandler Error] Failed to emit log: {e}")

