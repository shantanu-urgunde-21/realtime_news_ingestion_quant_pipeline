import os
from pathlib import Path
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Service configuration
MICROSERVICE_NAME = "calc_service"

# Ensure logs directory exists
log_dir = Path("../logs")
log_dir.mkdir(parents=True, exist_ok=True)

# Configure logging for production
# Using 'a' (append) mode instead of 'w' to preserve logs across restarts
logging.basicConfig(
    level=logging.INFO,
    filename=f"../logs/{MICROSERVICE_NAME}.log",
    filemode="a",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(MICROSERVICE_NAME)
logger.info(f"Starting {MICROSERVICE_NAME} - Initializing technical analysis pipeline")

# Fetch and validate Kafka Broker
kafka_broker = os.getenv("KAFKA_BROKER")
if not kafka_broker:
    logger.error("KAFKA_BROKER environment variable not set - cannot connect to Kafka")
    raise ValueError("KAFKA_BROKER environment variable is required")

logger.info(f"Connecting to Kafka broker: {kafka_broker}")
