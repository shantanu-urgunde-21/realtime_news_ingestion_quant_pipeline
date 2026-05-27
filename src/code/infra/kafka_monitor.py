import os
import sys
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

# Add parent directory to sys.path so we can import infra modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from infra.telemetry_client import TelemetryClient

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("kafka_monitor")

def run_kafka_monitor():
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    logger.info(f"Initializing Kafka Lag Monitor connecting to {kafka_broker}...")

    # Wait for Kafka to boot up if started as a container service
    max_init_retries = 10
    consumer = None
    for attempt in range(max_init_retries):
        try:
            consumer = KafkaConsumer(bootstrap_servers=kafka_broker)
            logger.info("Kafka consumer initialized successfully.")
            break
        except Exception as e:
            logger.warning(f"Failed to connect to Kafka (attempt {attempt+1}/{max_init_retries}): {e}. Retrying in 5 seconds...")
            time.sleep(5)
    
    if not consumer:
        logger.critical("Could not connect to Kafka Broker. Exiting...")
        sys.exit(1)

    telemetry = TelemetryClient()
    logger.info("TelemetryClient connected. Commencing Kafka metrics poll loop...")

    # List of topic-group pairings to track
    monitored_targets = [
        {"topic": "stock_table", "group": "pathway-group"},
        {"topic": "news_sentiment", "group": "pathway-news-group"},
        {"topic": "stock_calculation_table", "group": "math-group"},
        {"topic": "alert", "group": "math-group"}
    ]

    # Dictionary to keep track of previous commits to calculate processing velocity
    # Key: (topic, group, partition) -> Value: (offset, timestamp)
    historical_commits = {}

    while True:
        poll_start = time.time()
        
        try:
            for target in monitored_targets:
                topic = target["topic"]
                group = target["group"]
                
                # Fetch partitions for the topic
                try:
                    partitions = consumer.partitions_for_topic(topic)
                except KafkaError as ke:
                    logger.warning(f"Error fetching partitions for topic {topic}: {ke}")
                    continue

                if not partitions:
                    logger.debug(f"Topic {topic} has no partitions or does not exist yet.")
                    continue
                
                # Create a client for the specific consumer group to get committed offsets
                try:
                    group_consumer = KafkaConsumer(
                        bootstrap_servers=kafka_broker,
                        group_id=group
                    )
                except Exception as e:
                    logger.warning(f"Could not connect to group {group}: {e}")
                    continue

                # Query high watermarks (end offsets)
                tps = [TopicPartition(topic, p) for p in partitions]
                try:
                    latest_offsets = consumer.end_offsets(tps)
                except Exception as e:
                    logger.warning(f"Failed to fetch end offsets for topic {topic}: {e}")
                    group_consumer.close()
                    continue

                # Compute partition metrics
                for p in partitions:
                    tp = TopicPartition(topic, p)
                    latest_offset = latest_offsets.get(tp, 0)
                    
                    # Fetch committed offset
                    try:
                        committed_offset_val = group_consumer.committed(tp)
                    except Exception as e:
                        logger.warning(f"Failed to fetch committed offset for {topic} partition {p} group {group}: {e}")
                        committed_offset_val = None

                    # If no committed offset exists yet, treat as offset 0
                    committed_offset = committed_offset_val if committed_offset_val is not None else 0
                    consumer_lag = max(0, latest_offset - committed_offset)

                    # Calculate processing velocity (messages/sec)
                    current_time = time.time()
                    history_key = (topic, group, p)
                    messages_per_sec = 0.0

                    if history_key in historical_commits:
                        prev_offset, prev_time = historical_commits[history_key]
                        dt = current_time - prev_time
                        delta_offset = committed_offset - prev_offset
                        
                        if delta_offset >= 0 and dt > 0.01:
                            messages_per_sec = float(delta_offset / dt)
                    
                    # Update historical commits
                    # Only record committed offset if it is valid (i.e. not None)
                    if committed_offset_val is not None:
                        historical_commits[history_key] = (committed_offset, current_time)

                    # Log metric to telemetry batcher
                    logger.info(
                        f"[KAFKA TELEMETRY] Topic: {topic} | Group: {group} | Part: {p} | "
                        f"Latest: {latest_offset} | Committed: {committed_offset} | "
                        f"Lag: {consumer_lag} | Rate: {messages_per_sec:.2f} msg/s"
                    )
                    
                    telemetry.log_kafka_metrics(
                        topic_name=topic,
                        consumer_group=group,
                        partition=p,
                        committed_offset=committed_offset,
                        latest_offset=latest_offset,
                        consumer_lag=consumer_lag,
                        messages_per_sec=messages_per_sec
                    )

                group_consumer.close()
                
        except Exception as e:
            logger.error(f"Error in Kafka telemetry poll iteration: {e}", exc_info=True)

        # Sleep to achieve 15 second intervals, taking elapsed duration into account
        elapsed = time.time() - poll_start
        sleep_duration = max(1.0, 15.0 - elapsed)
        time.sleep(sleep_duration)

if __name__ == "__main__":
    try:
        run_kafka_monitor()
    except KeyboardInterrupt:
        logger.info("Kafka Lag Monitor terminated by user request.")
