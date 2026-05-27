"""
Calculation Service - Stock Market Technical Analysis Microservice

This service performs real-time technical and sentiment analysis on stock market data:
- GARCH volatility forecasting
- ARMA return forecasting
- RSI (Relative Strength Index) calculations
- EMA (Exponential Moving Average) calculations
- MACD (Moving Average Convergence Divergence) signal generation
- Trading signal generation based on multiple indicators
- Real-time temporal stream joins (LEFT asof_join) combining quotes with news sentiment

Input: Kafka topics 'stock_table' (quotes) and 'news_sentiment' (aggregated news sentiment)
Output: Kafka topic 'stock_calculation_table' with enriched technical indicators and joined sentiments
"""

import pathway as pw
from config import logger, kafka_broker
from schemas import QuoteSchema, NewsSentimentSchema
from indicators import (
    calculate_all_metrics,
    rsi_with_signal,
    calculate_ema_macd_signal,
    get_prev_histogram
)

# ============================================================================
# KAFKA INPUT: Read stock quote data from Kafka topic
# ============================================================================
logger.info("Reading from Kafka topic 'stock_table'...")

try:
    quotes = pw.io.kafka.read(
        rdkafka_settings={
            "bootstrap.servers": kafka_broker,
            "group.id": "pathway-group",
            "auto.offset.reset": "latest",
        },
        topic="stock_table",
        format="json",
        schema=QuoteSchema,
        json_field_paths={
            "symbol": "/value/symbol",
            "open": "/value/open",
            "high": "/value/high",
            "low": "/value/low",
            "close": "/value/close",
            "volume": "/value/volume",
            "timestamp": "/value/timestamp",
            "ts_ms": "/value/ts_ms"
        }
    )
    logger.info("Successfully configured Kafka reader for topic 'stock_table'")
except Exception as e:
    logger.error(f"Failed to configure Kafka reader: {str(e)}", exc_info=True)
    raise

# ============================================================================
# KAFKA INPUT: Read news sentiment data from Kafka topic
# ============================================================================
logger.info("Reading from Kafka topic 'news_sentiment'...")

try:
    news_sentiment = pw.io.kafka.read(
        rdkafka_settings={
            "bootstrap.servers": kafka_broker,
            "group.id": "pathway-news-group",
            "auto.offset.reset": "latest",
        },
        topic="news_sentiment",
        format="json",
        schema=NewsSentimentSchema,
        json_field_paths={
            "symbol": "/value/symbol",
            "weighted_avg_sentiment": "/value/weighted_avg_sentiment",
            "news_title": "/value/news_title",
            "ts_ms": "/value/ts_ms"
        }
    )
    logger.info("Successfully configured Kafka reader for topic 'news_sentiment'")
except Exception as e:
    logger.error(f"Failed to configure Kafka reader for topic 'news_sentiment': {str(e)}", exc_info=True)
    raise

# Prepare price tuples for temporal windowing operations
logger.debug("Preparing price tuples for temporal operations")
quotes = quotes.with_columns(
    ts_price_tuple=pw.make_tuple(quotes.ts_ms, quotes.close)
)

# ============================================================================
# PIPELINE STEP 1: GARCH Volatility Forecasting + ARMA Return Forecasting
# ============================================================================
logger.info("Creating sliding window for GARCH volatility and ARMA return forecasting")
windowed_combined = quotes.windowby(
    quotes.ts_ms,
    window=pw.temporal.sliding(
        hop=5 * 60_000,            # 5 minutes
        duration=30 * 5 * 60_000,  # 30 periods = 150 minutes
    ),
    instance=quotes.symbol,
    behavior=pw.temporal.common_behavior(cutoff=35 * 5 * 60_000),  # 175 minutes max delay
).reduce(
    symbol=pw.this._pw_instance,
    price_close_tuples=pw.reducers.sorted_tuple(pw.make_tuple(pw.this.ts_ms, pw.this.close)),
    cnt=pw.reducers.count(),
    start_ts=pw.this._pw_window_start,
    end_ts=pw.this._pw_window_end,
)

logger.info("Applying GARCH volatility and ARMA return forecasting calculations")
windowed_combined = windowed_combined.with_columns(
    metrics=calculate_all_metrics(windowed_combined.price_close_tuples)
).with_columns(
    prev_close=pw.this.metrics[0],
    ret=pw.this.metrics[1],
    sigma_forecast=pw.this.metrics[2],
    arma_forecast=pw.this.metrics[3],
    sigma_t=pw.this.metrics[4],
    resid=pw.this.metrics[5],
).select(
    symbol=pw.this.symbol,
    prev_close=pw.this.prev_close,
    ret=pw.this.ret,
    sigma_forecast=pw.this.sigma_forecast,
    arma_forecast=pw.this.arma_forecast,
    sigma_t=pw.this.sigma_t,
    resid=pw.this.resid,
    end_ts=pw.this.end_ts,
)

logger.info("Joining GARCH/ARMA metrics back to original quotes using asof_join")
enriched_with_garch_join = quotes.asof_join(
    windowed_combined,
    quotes.ts_ms,
    windowed_combined.end_ts,
    quotes.symbol == windowed_combined.symbol,
    how=pw.JoinMode.LEFT,
)

enriched_with_garch = enriched_with_garch_join.select(
    symbol=quotes.symbol,
    open=quotes.open,
    high=quotes.high,
    low=quotes.low,
    volume=quotes.volume,
    timestamp=quotes.timestamp,
    ts_ms=quotes.ts_ms,
    close=quotes.close,
    prev_close=pw.coalesce(windowed_combined.prev_close, 0.0),
    ret=pw.coalesce(windowed_combined.ret, 0.0),
    resid=pw.coalesce(windowed_combined.resid, 0.0),
    sigma_t=pw.coalesce(windowed_combined.sigma_t, 1e-3),
    sigma_forecast=pw.coalesce(windowed_combined.sigma_forecast, 1e-3),
    arma_forecast=pw.coalesce(windowed_combined.arma_forecast, 0.0),
)

# ============================================================================
# PIPELINE STEP 2: RSI (Relative Strength Index) Calculation + Signal Generation
# ============================================================================
logger.info("Creating sliding window for RSI calculation and signal generation")
window_rsi_combined = enriched_with_garch.windowby(
    enriched_with_garch.ts_ms,
    window=pw.temporal.sliding(
        hop=5 * 60_000,            # 5 minutes
        duration=17 * 5 * 60_000,  # 17 periods = 85 minutes (14 for RSI + 3 for signal)
    ),
    instance=enriched_with_garch.symbol,
    behavior=pw.temporal.common_behavior(cutoff=20 * 5 * 60_000),  # 100 minutes max delay
).reduce(
    symbol=pw.this._pw_instance,
    price_tuples=pw.reducers.sorted_tuple(pw.make_tuple(pw.this.ts_ms, pw.this.close)),
    end_ts=pw.this._pw_window_end,
)

window_rsi_combined = window_rsi_combined.with_columns(
    rsi_result=rsi_with_signal(window_rsi_combined.price_tuples)
).with_columns(
    rsi_timing=pw.this.rsi_result[1]
).select(
    symbol=pw.this.symbol,
    rsi_timing=pw.this.rsi_timing,
    end_ts=pw.this.end_ts,
)

logger.debug("Joining RSI signals back to enriched quotes")
enriched_with_garch_rsi = enriched_with_garch.asof_join(
    window_rsi_combined,
    enriched_with_garch.ts_ms,
    window_rsi_combined.end_ts,
    enriched_with_garch.symbol == window_rsi_combined.symbol,
    how=pw.JoinMode.LEFT,
)

enriched_with_garch = enriched_with_garch_rsi.select(
    symbol=enriched_with_garch.symbol,
    open=enriched_with_garch.open,
    high=enriched_with_garch.high,
    low=enriched_with_garch.low,
    volume=enriched_with_garch.volume,
    timestamp=enriched_with_garch.timestamp,
    ts_ms=enriched_with_garch.ts_ms,
    close=enriched_with_garch.close,
    ret=enriched_with_garch.ret,
    resid=enriched_with_garch.resid,
    sigma_t=enriched_with_garch.sigma_t,
    sigma_forecast=enriched_with_garch.sigma_forecast,
    arma_forecast=enriched_with_garch.arma_forecast,
    rsi_timing=pw.coalesce(window_rsi_combined.rsi_timing, 0),
    prev_close=enriched_with_garch.prev_close,
)

# ============================================================================
# PIPELINE STEP 3: EMA (Exponential Moving Average) + MACD Calculation
# ============================================================================
logger.info("Creating sliding window for EMA and MACD calculation")
windowed_ema_all = enriched_with_garch.windowby(
    enriched_with_garch.ts_ms,
    window=pw.temporal.sliding(
        hop=5 * 60_000,
        duration=210 * 60_000,     # 210 minutes (42 periods) - buffer for EMA_200
    ),
    instance=enriched_with_garch.symbol,
    behavior=pw.temporal.common_behavior(cutoff=250 * 60_000),      # 250 minutes max delay
).reduce(
    symbol=pw.this._pw_instance,
    prices=pw.reducers.sorted_tuple(pw.make_tuple(pw.this.ts_ms, pw.this.close)),
    end_ts=pw.this._pw_window_end,
)

windowed_ema_all = windowed_ema_all.with_columns(
    ema_result=calculate_ema_macd_signal(windowed_ema_all.prices)
).with_columns(
    ema_12=pw.this.ema_result[0],
    ema_26=pw.this.ema_result[1],
    ema_20=pw.this.ema_result[2],
    ema_50=pw.this.ema_result[3],
    ema_200=pw.this.ema_result[4],
    macd=pw.this.ema_result[5],
    signal=pw.this.ema_result[6],
).select(
    symbol=pw.this.symbol,
    ema_12=pw.this.ema_12,
    ema_26=pw.this.ema_26,
    ema_20=pw.this.ema_20,
    ema_50=pw.this.ema_50,
    ema_200=pw.this.ema_200,
    macd=pw.this.macd,
    signal=pw.this.signal,
    end_ts=pw.this.end_ts,
)

logger.debug("Joining EMA/MACD metrics back to enriched quotes")
enriched_final_join = enriched_with_garch.asof_join(
    windowed_ema_all,
    enriched_with_garch.ts_ms,
    windowed_ema_all.end_ts,
    enriched_with_garch.symbol == windowed_ema_all.symbol,
    how=pw.JoinMode.LEFT,
)

enriched_final = enriched_final_join.select(
    symbol=enriched_with_garch.symbol,
    timestamp=enriched_with_garch.timestamp,
    ts_ms=enriched_with_garch.ts_ms,
    open=enriched_with_garch.open,
    high=enriched_with_garch.high,
    low=enriched_with_garch.low,
    close=enriched_with_garch.close,
    volume=enriched_with_garch.volume,
    ret=enriched_with_garch.ret,
    resid=enriched_with_garch.resid,
    sigma_t=enriched_with_garch.sigma_t,
    sigma_forecast=enriched_with_garch.sigma_forecast,
    arma_forecast=enriched_with_garch.arma_forecast,
    rsi_timing=enriched_with_garch.rsi_timing,
    prev_close=enriched_with_garch.prev_close,
    ema_12=pw.coalesce(windowed_ema_all.ema_12, 0.0),
    ema_26=pw.coalesce(windowed_ema_all.ema_26, 0.0),
    ema_20=pw.coalesce(windowed_ema_all.ema_20, 0.0),
    ema_50=pw.coalesce(windowed_ema_all.ema_50, 0.0),
    ema_200=pw.coalesce(windowed_ema_all.ema_200, 0.0),
    macd=pw.coalesce(windowed_ema_all.macd, 0.0),
    signal=pw.coalesce(windowed_ema_all.signal, 0.0),
)
logger.info("EMA/MACD enrichment completed")

# ============================================================================
# PIPELINE STEP 4: Derived Metrics & MACD Histogram Tracking
# ============================================================================
logger.info("Calculating derived trading metrics and filters")
enriched_final = enriched_final.with_columns(
    histogram=enriched_final.macd - enriched_final.signal,
    ema_trend_filter_trend_up=enriched_final.ema_20 > enriched_final.ema_50,
    ema_trend_filter_trend_down=enriched_final.ema_20 < enriched_final.ema_50,
    long_term_bias_trend_up=enriched_final.close > enriched_final.ema_200,
    long_term_bias_trend_down=enriched_final.close < enriched_final.ema_200,
    risk_adj_ret=pw.if_else(
        enriched_final.sigma_forecast > 0,
        enriched_final.arma_forecast / enriched_final.sigma_forecast,
        0.0
    ),
    long_signal=(enriched_final.arma_forecast > 0),
    short_signal=(enriched_final.arma_forecast < 0),
    pct_change=pw.if_else(
        (enriched_final.prev_close > 0),
        ((enriched_final.close - enriched_final.prev_close) / enriched_final.prev_close) * 100,
        0.0
    )
)

logger.info("Creating window for MACD histogram tracking")
windowed_histogram = enriched_final.windowby(
    enriched_final.ts_ms,
    window=pw.temporal.sliding(
        hop=5 * 60_000,
        duration=10 * 60_000,      # 10 minutes (2 periods)
    ),
    instance=enriched_final.symbol,
    behavior=pw.temporal.common_behavior(cutoff=15 * 60_000),      # 15 minutes max delay
).reduce(
    symbol=pw.this._pw_instance,
    histogram_values=pw.reducers.sorted_tuple(pw.make_tuple(pw.this.ts_ms, pw.this.histogram)),
    end_ts=pw.this._pw_window_end,
)

windowed_histogram = windowed_histogram.with_columns(
    histogram_prev=get_prev_histogram(windowed_histogram.histogram_values),
).select(
    symbol=pw.this.symbol,
    histogram_prev=pw.this.histogram_prev,
    end_ts=pw.this.end_ts,
)

logger.debug("Joining previous histogram values back to enriched quotes")
enriched_final_hist = enriched_final.asof_join(
    windowed_histogram,
    enriched_final.ts_ms,
    windowed_histogram.end_ts,
    enriched_final.symbol == windowed_histogram.symbol,
    how=pw.JoinMode.LEFT,
)

enriched_final = enriched_final_hist.select(
    *enriched_final,
    histogram_prev=windowed_histogram.histogram_prev,
)

# ============================================================================
# PIPELINE STEP 5: MACD Signal Classification & Final Output Table
# ============================================================================
logger.info("Classifying MACD trading signals based on histogram")
enriched_final = enriched_final.with_columns(
    histogram_growing=pw.if_else(
        pw.this.histogram_prev.is_not_none(),
        pw.this.histogram > pw.this.histogram_prev,
        False
    ),
    histogram_shrinking=pw.if_else(
        pw.this.histogram_prev.is_not_none(),
        pw.this.histogram < pw.this.histogram_prev,
        False
    ),
)

enriched_final = enriched_final.with_columns(
    macd_signal=pw.if_else(
        (pw.this.histogram > 0) & pw.this.histogram_growing,
        2,  # Strong Buy: positive histogram and increasing momentum
        pw.if_else(
            (pw.this.histogram > 0) & pw.this.histogram_shrinking,
            1,  # Weak Buy: positive histogram but decreasing momentum
            pw.if_else(
                (pw.this.histogram < 0) & pw.this.histogram_shrinking,
                -2,  # Strong Sell: negative histogram and increasing negative momentum
                pw.if_else(
                    (pw.this.histogram < 0) & pw.this.histogram_growing,
                    -1,  # Weak Sell: negative histogram but decreasing negative momentum
                    0  # No signal: neutral or insufficient data
                )
            )
        )
    )
)
logger.info("MACD signal classification completed")

logger.info("Performing real-time, stateful asof_join on symbol and ts_ms with news sentiment")
enriched_with_sentiment = enriched_final.asof_join(
    news_sentiment,
    enriched_final.ts_ms,
    news_sentiment.ts_ms,
    enriched_final.symbol == news_sentiment.symbol,
    how=pw.JoinMode.LEFT,
)

logger.info("Preparing final output table with selected columns including sentiment metrics")
final_table = enriched_with_sentiment.select(
    symbol=enriched_final.symbol,
    timestamp=enriched_final.timestamp,
    ts_ms=enriched_final.ts_ms,
    close=enriched_final.close,
    sigma_forecast=enriched_final.sigma_forecast,
    arma_forecast=enriched_final.arma_forecast,
    ema_trend_filter_trend_up=enriched_final.ema_trend_filter_trend_up,
    ema_trend_filter_trend_down=enriched_final.ema_trend_filter_trend_down,
    long_term_bias_trend_up=enriched_final.long_term_bias_trend_up,
    long_term_bias_trend_down=enriched_final.long_term_bias_trend_down,
    macd_signal=enriched_final.macd_signal,
    risk_adj_ret=enriched_final.risk_adj_ret,
    long_signal=enriched_final.long_signal,
    short_signal=enriched_final.short_signal,
    rsi_timing=enriched_final.rsi_timing,
    pct_change=enriched_final.pct_change,
    weighted_avg_sentiment=pw.coalesce(news_sentiment.weighted_avg_sentiment, 0.0),
    news_title=pw.coalesce(news_sentiment.news_title, "No news available")
)

# ============================================================================
# KAFKA OUTPUT: Write enriched metrics back to stock_calculation_table
# ============================================================================
try:
    logger.info(f"Configuring Kafka writer for topic 'stock_calculation_table' (broker: {kafka_broker})")
    pw.io.kafka.write(
        final_table,
        rdkafka_settings={
            "bootstrap.servers": kafka_broker,
        },
        topic_name="stock_calculation_table",
        format="json",
    )
    logger.info("Kafka writer configured for 'stock_calculation_table' topic")
except Exception as e:
    logger.error(f"Failed to configure Kafka writer for 'stock_calculation_table': {str(e)}", exc_info=True)
    raise

# ============================================================================
# START PIPELINE: Boot computation graph with disk-persisted state
# ============================================================================
logger.info("Starting Pathway computation pipeline...")
try:
    try:
        backend = pw.persistence.Backend.filesystem("./data")
        try:
            # Wrap in Config to match newer Pathway API requirements
            config = pw.persistence.Config(backend)
            pw.run(persistence_config=config)
        except TypeError:
            try:
                pw.run(persistence_backend=backend)
            except TypeError:
                pw.run(persistence_config=backend)
    except Exception as e:
        logger.warning(f"Persistence not supported or failed: {e}. Running without persistence.")
        pw.run()
except KeyboardInterrupt:
    logger.info("Pipeline interrupted by user")
    raise
except Exception as e:
    logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
    raise