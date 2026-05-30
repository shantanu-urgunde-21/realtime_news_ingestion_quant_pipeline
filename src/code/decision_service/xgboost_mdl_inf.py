"""
Decision Service - Real-time Trading Alert Inference Service

This service consumes stock calculation data from Kafka, performs real-time
predictions using trained XGBoost models, and sends alerts to Kafka when
significant price movements are predicted.

Input: Kafka topic 'stock_calculation_table' with technical indicators
Output: Kafka topic 'alert' with trading alerts (when conditions are met)
"""
import json
import time
from kafka import KafkaConsumer, KafkaProducer
import xgboost as xgb
import numpy as np
import pickle
import pandas as pd
from sklearn.preprocessing import StandardScaler
from pathlib import Path
import logging
import os

# Service configuration
MICROSERVICE_NAME = "decision_service"

# Ensure logs directory exists
log_dir = Path("../logs")
log_dir.mkdir(parents=True, exist_ok=True)

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    filename=f"../logs/{MICROSERVICE_NAME}.log",
    filemode="a",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(MICROSERVICE_NAME)
logger.info("Starting Decision Service - Real-time inference pipeline")

# Initialize Observability Telemetry
from infra.telemetry_client import TelemetryClient, ClickHouseLogHandler
telemetry = TelemetryClient()

# Set up ClickHouse Centralized Log Harvesting (Warning/Error/Fatal)
ch_handler = ClickHouseLogHandler(service_name=MICROSERVICE_NAME)
ch_handler.setFormatter(logging.Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(ch_handler)
logger.info("Centralized ClickHouse logging telemetry registered successfully")

# Start Container System Resource Telemetry Daemon
try:
    from infra.system_daemon import start_system_daemon
    start_system_daemon(MICROSERVICE_NAME)
    logger.info("System Resource Telemetry Daemon started successfully")
except Exception as e:
    logger.warning(f"Failed to start System Resource Telemetry Daemon: {e}")


# ============================================================================
# Model Loading
# ============================================================================
# Load trained XGBoost models for inference
logger.info("Loading trained XGBoost models")

# Load percentage change regression model
model_pct_file = "xgb_pct_change_model.json"
if not Path(model_pct_file).exists():
    logger.error(f"Model file not found: {model_pct_file}")
    raise FileNotFoundError(f"Model file not found: {model_pct_file}")

try:
    model_pct = xgb.XGBRegressor()
    model_pct.load_model(model_pct_file)
    logger.info(f"Loaded percentage change regression model from {model_pct_file}")
except Exception as e:
    logger.error(f"Failed to load regression model: {str(e)}", exc_info=True)
    raise

# Load big move classification model
model_class_file = "xgb_classifier_model.json"
if not Path(model_class_file).exists():
    logger.error(f"Model file not found: {model_class_file}")
    raise FileNotFoundError(f"Model file not found: {model_class_file}")

try:
    model_class = xgb.XGBClassifier()
    model_class.load_model(model_class_file)
    logger.info(f"Loaded big move classifier model from {model_class_file}")
except Exception as e:
    logger.error(f"Failed to load classifier model: {str(e)}", exc_info=True)
    raise

# ClickHouse Connection removed for stateless inference decoupling

# ============================================================================
# Symbol Mapping
# ============================================================================
# Load symbol mapping for encoding stock symbols to integer codes
symbol_mapping_file = "symbol_mapping.pkl"
if not Path(symbol_mapping_file).exists():
    logger.error(f"Symbol mapping file not found: {symbol_mapping_file}")
    raise FileNotFoundError(f"Symbol mapping file not found: {symbol_mapping_file}")

with open(symbol_mapping_file, "rb") as f:
    symbol_mapping = pickle.load(f)
logger.info(f"Loaded symbol mapping with {len(symbol_mapping)} symbols")

# Invert mapping: {"AAPL":0, "GOOGL":1, ...} for encoding
symbol_to_code = {v: k for k, v in symbol_mapping.items()}

# ============================================================================
# Kafka Configuration
# ============================================================================
# Consumer: Read stock calculation data from Kafka
logger.info("Configuring Kafka consumer for topic 'stock_calculation_table'")
kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
max_retries = 30
retry_delay = 2
consumer = None

for attempt in range(max_retries):
    try:
        consumer = KafkaConsumer(
            "stock_calculation_table",
            bootstrap_servers=kafka_broker,
            group_id="math-group",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        logger.info("Kafka consumer configured successfully")
        break
    except Exception as e:
        if attempt < max_retries - 1:
            logger.warning(f"Kafka not available (attempt {attempt+1}/{max_retries}): {str(e)}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
        else:
            logger.error(f"Failed to connect to Kafka after {max_retries} attempts: {str(e)}", exc_info=True)
            raise

# Producer: Send alerts to Kafka
logger.info("Configuring Kafka producer for topic 'alert'")
producer = None
for attempt in range(max_retries):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("Kafka producer configured successfully")
        break
    except Exception as e:
        if attempt < max_retries - 1:
            logger.warning(f"Kafka not available for producer (attempt {attempt+1}/{max_retries}): {str(e)}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
        else:
            logger.error(f"Failed to connect Kafka producer after {max_retries} attempts: {str(e)}", exc_info=True)
            raise

logger.info("Waiting for messages from Kafka...")
print("Waiting for messages...")

# ============================================================================
# Main Processing Loop
# ============================================================================
# Continuously consume stock calculation data, make predictions, and send alerts
logger.info("Starting main processing loop")

try:
    for msg in consumer:
        data = msg.value
        logger.debug(f"Received message: {json.dumps(data, indent=2)}")
        print("Received: ", data)
        
        # Clean and validate symbol
        data["symbol"] = data["symbol"].strip()
        current_symbol = data["symbol"]
        
        if not current_symbol:
            logger.warning("Received message with empty symbol, skipping")
            continue

        # Measure Ingestion & Indicator Computation latency
        birth_time_ms = float(data.get("ts_ms", 0.0))
        if birth_time_ms > 0:
            ingestion_delay = (time.time() * 1000) - birth_time_ms
            telemetry.log_latency(
                service_name=MICROSERVICE_NAME,
                symbol=current_symbol,
                metric_name="ingestion_delay",
                latency_ms=ingestion_delay,
                cycle=0
            )

        # Encode symbol to integer code (as used during training)
        symbol_encoded = symbol_to_code.get(current_symbol, -1)

        if symbol_encoded == -1:
            logger.warning(f"Unknown symbol: {current_symbol}, skipping (not in training data)")
            print(f"Unknown symbol: {current_symbol}, skipping")
            continue

        # ====================================================================
        # Fetch News Sentiment Data directly from Kafka Payload
        # ====================================================================
        weighted_sentiment = float(data.get("weighted_avg_sentiment", 0.0))
        news_titles = data.get("news_title", "No news available")
        logger.debug(f"Retrieved streaming sentiment {weighted_sentiment:.4f} for {current_symbol}")

        # ====================================================================
        # Feature Engineering
        # ====================================================================
        # Build feature vector in EXACT training order
        # Features: symbol, ts_ms, close, sigma_forecast, arma_forecast,
        #           ema_trend_filter_up/down, long_term_bias_trend_up/down,
        #           macd_signal, risk_adj_ret, long_signal, short_signal,
        #           rsi_timing, weighted_avg_sentiment
        try:
            features = np.array([[
                symbol_encoded,
                float(data["ts_ms"]),
                float(data["close"]),
                float(data["sigma_forecast"]),
                float(data["arma_forecast"]),
                int(data["ema_trend_filter_trend_up"]),
                int(data["ema_trend_filter_trend_down"]),
                int(data["long_term_bias_trend_up"]),
                int(data["long_term_bias_trend_down"]),
                float(data["macd_signal"]),
                float(data["risk_adj_ret"]),
                int(data["long_signal"]),
                int(data["short_signal"]),
                float(data["rsi_timing"]),
                weighted_sentiment
            ]])
        except KeyError as e:
            logger.error(f"Missing required field in data for {current_symbol}: {str(e)}")
            continue
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid data type for {current_symbol}: {str(e)}")
            continue

        # ====================================================================
        # Model Predictions
        # ====================================================================
        inference_start = time.time()
        
        # Predict percentage change (regression)
        try:
            y_pred_pct = model_pct.predict(features)
            predicted_change = y_pred_pct[0]
            logger.info(f"Prediction for {current_symbol}: {predicted_change:.4f}% change")
            print(f"Prediction: {predicted_change:.4f}%")
        except Exception as e:
            logger.error(f"Prediction failed for {current_symbol}: {str(e)}", exc_info=True)
            continue

        # Predict probability of big move (classification)
        try:
            y_pred_prob = model_class.predict_proba(features)[:, 1]
            big_move_prob = y_pred_prob[0]
            logger.info(f"Big move probability for {current_symbol}: {big_move_prob:.4f}")
            print(f"Probability: {big_move_prob:.4f}")
        except Exception as e:
            logger.error(f"Probability prediction failed for {current_symbol}: {str(e)}", exc_info=True)
            continue

        inference_end = time.time()
        inference_delay = (inference_end - inference_start) * 1000
        telemetry.log_latency(
            service_name=MICROSERVICE_NAME,
            symbol=current_symbol,
            metric_name="ml_inference_delay",
            latency_ms=inference_delay,
            cycle=0
        )
        
        if birth_time_ms > 0:
            e2e_delay = (inference_end * 1000) - birth_time_ms
            telemetry.log_latency(
                service_name=MICROSERVICE_NAME,
                symbol=current_symbol,
                metric_name="ml_e2e_delay",
                latency_ms=e2e_delay,
                cycle=0
            )

        # ====================================================================
        # Alert Generation
        # ====================================================================
        # Send alert if: predicted change > 2% or < -2% AND big move probability > 50%
        alert_threshold_pct = 2.0
        alert_threshold_prob = 0.5
        
        if (abs(predicted_change) > alert_threshold_pct) and (big_move_prob > alert_threshold_prob):
            import uuid
            alert_id = str(uuid.uuid4())
            
            record = {
                "symbol": current_symbol,
                "Predicted_change": float(predicted_change),
                "News": news_titles,
                "Sentiment Score": weighted_sentiment,
                "close": float(data["close"]),
                "sigma_forecast": float(data["sigma_forecast"]),
                "ema_filter_trend_up": int(data.get("ema_trend_filter_trend_up", 0)),
                "ema_filter_trend_down": int(data.get("ema_trend_filter_trend_down", 0)),
            }

            try:
                producer.send("alert", value=record)
                producer.flush()
                
                # Log alert to telemetry for business metrics
                telemetry.log_alert(
                    symbol=current_symbol,
                    predicted_change_pct=float(predicted_change),
                    prediction_confidence=float(big_move_prob),
                    sentiment_score=float(weighted_sentiment),
                    rsi_signal=int(data.get("rsi_timing", 0)),
                    model_name="xgb_pct_change_classifier",
                    ml_inference_latency_ms=float(ml_inference_latency),
                    triggered_by="price_and_confidence_threshold",
                    alert_id=alert_id
                )
                
                logger.info(
                    f"Alert sent for {current_symbol}: "
                    f"predicted_change={predicted_change:.4f}%, "
                    f"big_move_prob={big_move_prob:.4f}, "
                    f"alert_id={alert_id}"
                )
                print(f"Alert sent for {current_symbol}")
            except Exception as e:
                logger.error(f"Failed to send alert for {current_symbol}: {str(e)}", exc_info=True)

except KeyboardInterrupt:
    logger.info("Processing interrupted by user")
except Exception as e:
    logger.error(f"Fatal error in processing loop: {str(e)}", exc_info=True)
    raise
finally:
    if consumer:
        consumer.close()
        logger.info("Kafka consumer closed")
    if producer:
        producer.close()
        logger.info("Kafka producer closed")