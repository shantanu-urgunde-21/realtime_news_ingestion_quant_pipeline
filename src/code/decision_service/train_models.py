"""
Decision Service - Consolidated XGBoost Model Training

This script trains both the percentage change regressor and the big move classifier.
It fetches the dataset from ClickHouse once, prepares the features, and trains
both models sequentially, saving them as JSON files for inference.
"""
from clickhouse_driver import Client
import pickle
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score, classification_report, confusion_matrix
import xgboost as xgb
from pathlib import Path
import logging
import os

# Service configuration
MICROSERVICE_NAME = "decision_service"

# Ensure logs directory exists
log_dir = Path("../logs")
log_dir.mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    filename=f"../logs/{MICROSERVICE_NAME}_training.log",
    filemode="a",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(MICROSERVICE_NAME)
logger.info("Starting Consolidated XGBoost Model Training")

# ============================================================================
# ClickHouse Connection
# ============================================================================
logger.info("Connecting to ClickHouse database")
try:
    ch_host = os.getenv("CLICKHOUSE_HOST", "localhost")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 9000))
    ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    client = Client(host=ch_host, port=ch_port, password=ch_password)
    client.execute("SELECT 1")
    logger.info("Connected to ClickHouse successfully")
except Exception as e:
    logger.error(f"Failed to connect to ClickHouse: {str(e)}", exc_info=True)
    raise

# ============================================================================
# Data Extraction from ClickHouse
# ============================================================================
logger.info("Querying stock technical indicators and sentiment data")
query_stock = """
    SELECT symbol, timestamp, ts_ms, close, sigma_forecast, arma_forecast,
           ema_trend_filter_trend_up, ema_trend_filter_trend_down,
           long_term_bias_trend_up, long_term_bias_trend_down,
           macd_signal, risk_adj_ret, long_signal, short_signal,
           rsi_timing, pct_change
    FROM final_table
    WHERE ts_ms IN (
        SELECT DISTINCT ts_ms 
        FROM final_table 
        ORDER BY ts_ms DESC 
        LIMIT 1000
    )
    ORDER BY ts_ms DESC, symbol
"""

query_news = """
    SELECT symbol, news_titles, sentiment_scores, relevance_scores, weighted_avg_sentiment
    FROM market_data.sentiment_stream
    WHERE cycle = (SELECT MAX(cycle) FROM market_data.sentiment_stream)
    ORDER BY symbol
"""

try:
    data_stock = client.execute(query_stock)
    data_news = client.execute(query_news)
    logger.info(f"Retrieved {len(data_stock)} stock records and {len(data_news)} news records")
except Exception as e:
    logger.error(f"Failed to query data: {str(e)}", exc_info=True)
    raise

# ============================================================================
# Data Preparation
# ============================================================================
columns_stock = ['symbol','timestamp','ts_ms','close','sigma_forecast','arma_forecast',
           'ema_trend_filter_up','ema_trend_filter_down',
           'long_term_bias_trend_up','long_term_bias_trend_down',
           'macd_signal','risk_adj_ret','long_signal','short_signal',
           'rsi_timing','pct_change']

columns_news = ['symbol', 'news_titles', 'sentiment_scores', 'relevance_scores', 'weighted_avg_sentiment']

df_stock = pd.DataFrame(data_stock, columns=columns_stock)
df_news = pd.DataFrame(data_news, columns=columns_news)

# Merge stock and news data
cumm_df = df_stock.merge(
    df_news[['symbol', 'weighted_avg_sentiment']],
    on='symbol',
    how='left'
)
logger.info(f"Merged dataset: {len(cumm_df)} rows")

# Load symbol mapping
symbol_mapping_file = "symbol_mapping.pkl"
if not Path(symbol_mapping_file).exists():
    raise FileNotFoundError(f"Symbol mapping file not found: {symbol_mapping_file}")

with open(symbol_mapping_file, "rb") as f:
    symbol_mapping = pickle.load(f)

# Create reverse mapping and encode symbols
reverse_mapping = {v: k for k, v in symbol_mapping.items()}
cumm_df['symbol'] = cumm_df['symbol'].map(reverse_mapping)
cumm_df['symbol'] = cumm_df['symbol'].astype(int)

# Fill missing sentiment scores
cumm_df['weighted_avg_sentiment'] = cumm_df['weighted_avg_sentiment'].fillna(0)
cumm_df = cumm_df.drop(columns=['timestamp'])

# Create classification target (>2% move)
cumm_df['big_move'] = (cumm_df['pct_change'] > 2).astype(int)

# ============================================================================
# Train-Test Split
# ============================================================================
X = cumm_df.drop(columns=['pct_change', 'big_move'])
y_pct = cumm_df['pct_change']
y_class = cumm_df['big_move']

# Split for regression
X_train_pct, X_test_pct, y_train_pct, y_test_pct = train_test_split(X, y_pct, test_size=0.2, shuffle=False)
# Split for classification
X_train_cls, X_test_cls, y_train_cls, y_test_cls = train_test_split(X, y_class, test_size=0.2, shuffle=False)

logger.info("=" * 60)
logger.info("TRAINING REGRESSION MODEL (Percentage Change)")
# ============================================================================
# Train Regression Model
# ============================================================================
model_pct = xgb.XGBRegressor(
    objective='reg:squarederror',
    n_estimators=800,
    learning_rate=0.01,
    max_depth=8,
    min_child_weight=3,
    subsample=0.75,
    colsample_bytree=0.75,
    reg_lambda=1.0,
    reg_alpha=0.3,
    gamma=0.1
)

model_pct.fit(X_train_pct, y_train_pct)
preds_pct = model_pct.predict(X_test_pct)

logger.info(f"MSE: {mean_squared_error(y_test_pct, preds_pct):.6f}")
logger.info(f"R2 Score: {r2_score(y_test_pct, preds_pct):.4f}")
model_pct.save_model("xgb_pct_change_model.json")
logger.info("Regression model saved.")

logger.info("=" * 60)
logger.info("TRAINING CLASSIFICATION MODEL (Big Move)")
# ============================================================================
# Train Classification Model
# ============================================================================
# Calculate scale_pos_weight for imbalance
pos_weight = 1.0
if len(y_train_cls[y_train_cls==1]) > 0:
    pos_weight = len(y_train_cls[y_train_cls==0]) / len(y_train_cls[y_train_cls==1])

model_cls = xgb.XGBClassifier(
    scale_pos_weight=pos_weight,
    n_estimators=900,
    learning_rate=0.02,
    max_depth=7,
    subsample=0.8,
    colsample_bytree=0.8,
)

model_cls.fit(X_train_cls, y_train_cls)
preds_cls = model_cls.predict(X_test_cls)

logger.info(f"Accuracy: {accuracy_score(y_test_cls, preds_cls):.4f}")
logger.info(f"Classification Report:\n{classification_report(y_test_cls, preds_cls)}")
model_cls.save_model("xgb_classifier_model.json")
logger.info("Classification model saved.")

logger.info("=" * 60)
logger.info("All models trained and saved successfully.")
