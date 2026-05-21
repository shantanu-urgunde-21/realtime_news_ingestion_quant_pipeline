import pathway as pw
import math
from config import logger

# ============================================================================
# GARCH + ARMA Calculation UDF
# ============================================================================
@pw.udf
def calculate_all_metrics(price_tuples: tuple,
                          omega: float = 1e-6,
                          alpha: float = 0.05,
                          beta: float = 0.93,
                          phi: float = 0.6,
                          theta: float = 0.3,
                          init_sigma: float = 1e-3) -> tuple[float, float, float, float, float, float]:
    """
    Calculate previous close, returns, GARCH volatility forecast, and ARMA return forecast.
    
    This function combines multiple calculations to optimize performance:
    - Previous period's closing price
    - Current period's log return
    - GARCH(1,1) volatility forecast (sigma_forecast)
    - ARMA(1,1) return forecast
    - Current volatility estimate (sigma_t)
    - Current residual (actual return - ARMA forecast)
    
    Args:
        price_tuples: List of (timestamp_ms, price) tuples, sorted by timestamp
        omega: GARCH long-term variance parameter (default: 1e-6)
        alpha: GARCH weight for recent squared residuals (default: 0.05)
        beta: GARCH weight for previous variance (default: 0.93)
        phi: ARMA autoregressive coefficient (default: 0.6)
        theta: ARMA moving average coefficient (default: 0.3)
        init_sigma: Initial volatility estimate (default: 1e-3)
    
    Returns:
        Tuple of (prev_close, current_ret, sigma_forecast, arma_forecast, sigma_t, resid):
        - prev_close: Previous period's closing price
        - current_ret: Current period's log return
        - sigma_forecast: Forecasted volatility for next period
        - arma_forecast: Forecasted return for next period
        - sigma_t: Current period's volatility estimate
        - resid: Current period's residual (actual - forecasted return)
    
    Note:
        Returns default values if insufficient data is available.
        GARCH model: sigma^2(t+1) = omega + alpha * resid^2(t) + beta * sigma^2(t)
        ARMA model: ret(t+1) = phi * ret(t) + theta * resid(t)
    """
    if not price_tuples or len(price_tuples) == 0:
        logger.warning("calculate_all_metrics: Empty price_tuples, returning default values")
        return (0.0, 0.0, init_sigma, 0.0, init_sigma, 0.0)

    # Extract prices from tuples (ignore timestamps for calculation)
    prices = [p for (_, p) in price_tuples]

    # Get previous period's closing price (second-to-last price)
    # If only one price available, use it as prev_close
    prev_close = prices[-2] if len(prices) >= 2 else prices[-1]

    # Calculate current period's log return: log(price_t / price_{t-1})
    # Log returns are preferred for financial modeling due to time-additivity
    if len(prices) >= 2 and prices[-2] > 0 and prices[-1] > 0:
        current_ret = math.log(prices[-1] / prices[-2])
    else:
        current_ret = 0.0
        if len(prices) >= 2:
            logger.warning(f"calculate_all_metrics: Invalid prices for return calculation: {prices[-2]}, {prices[-1]}")

    # Need at least 2 prices to calculate returns
    if len(prices) < 2:
        logger.warning("calculate_all_metrics: Insufficient prices for GARCH calculation")
        return (prev_close, current_ret, init_sigma, 0.0, init_sigma, 0.0)

    # Calculate log returns for all price pairs in the window
    rets = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0 and prices[i] > 0:
            rets.append(math.log(prices[i] / prices[i - 1]))
        else:
            rets.append(0.0)
            logger.warning(f"calculate_all_metrics: Invalid price pair at index {i}: {prices[i-1]}, {prices[i]}")

    # Initialize GARCH model with sample variance
    # Use mean return and variance of historical returns as starting point
    mean_ret = sum(rets) / len(rets) if rets else 0.0
    var = sum((x - mean_ret) ** 2 for x in rets) / len(rets) if rets else init_sigma * init_sigma
    last_sigma2 = var if var > 0 else init_sigma * init_sigma

    # Initialize ARMA model state
    prev_ret = rets[0] if rets else 0.0
    last_resid = 0.0

    # Iterate through returns to update GARCH and ARMA models
    # This implements the recursive GARCH(1,1) and ARMA(1,1) updates
    for i in range(1, len(rets)):
        cur_ret = rets[i]
        
        # ARMA forecast: ret(t) = phi * ret(t-1) + theta * resid(t-1)
        pred = phi * prev_ret + theta * last_resid
        
        # Residual: actual return - ARMA forecast
        resid = cur_ret - pred
        
        # GARCH variance update: sigma^2(t) = omega + alpha * resid^2(t-1) + beta * sigma^2(t-1)
        sigma2 = omega + alpha * (last_resid ** 2) + beta * last_sigma2
        
        # Update state for next iteration
        last_sigma2 = sigma2
        last_resid = resid
        prev_ret = cur_ret

    # Forecast next period's volatility and return
    sigma_forecast2 = omega + alpha * (last_resid ** 2) + beta * last_sigma2
    arma_forecast = phi * prev_ret + theta * last_resid

    # Return all metrics (convert variance to standard deviation)
    return (prev_close, current_ret, math.sqrt(sigma_forecast2), arma_forecast, math.sqrt(last_sigma2), last_resid)


# ============================================================================
# RSI (Relative Strength Index) Calculation + Signal Generation
# ============================================================================
@pw.udf
def rsi_with_signal(price_tuples: tuple) -> tuple[float, int]:
    """
    Calculate RSI (Relative Strength Index) and generate trading signals.
    
    RSI is a momentum oscillator that measures the speed and magnitude of price changes.
    Values range from 0 to 100:
    - RSI > 70: Overbought (potential sell signal)
    - RSI < 30: Oversold (potential buy signal)
    - RSI = 50: Neutral
    
    Trading Signals:
    - rsi_timing = 2: Strong buy signal (rising RSI from oversold < 40)
    - rsi_timing = -2: Strong sell signal (falling RSI from overbought > 70)
    - rsi_timing = 0: No signal
    
    Args:
        price_tuples: List of (timestamp_ms, price) tuples, sorted by timestamp
    
    Returns:
        Tuple of (rsi, rsi_timing):
        - rsi: Current RSI value (0-100)
        - rsi_timing: Trading signal (-2, -1, 0, 1, 2)
    """
    if len(price_tuples) < 14:
        logger.debug(f"rsi_with_signal: Insufficient data ({len(price_tuples)} < 14), returning neutral RSI")
        return (50.0, 0)

    prices = [p for (_, p) in price_tuples]

    # Calculate price changes (gains and losses) for RSI calculation
    gains = []
    losses = []
    for i in range(1, len(prices)):
        delta = prices[i] - prices[i - 1]
        gains.append(delta if delta > 0 else 0.0)
        losses.append(-delta if delta < 0 else 0.0)

    # Use last 14 periods for RSI calculation (standard period)
    recent_gains = gains[-14:] if len(gains) >= 14 else gains
    recent_losses = losses[-14:] if len(losses) >= 14 else losses

    # Calculate average gain and average loss
    avg_gain = sum(recent_gains) / len(recent_gains) if recent_gains else 0.0
    avg_loss = sum(recent_losses) / len(recent_losses) if recent_losses else 0.0

    # Calculate RSI: RSI = 100 - (100 / (1 + RS))
    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    # Calculate RSI signal based on trend and divergence patterns
    # Need last 3 RSI values to detect patterns
    rsi_history = []
    for end_idx in range(max(14, len(prices) - 2), len(prices) + 1):
        if end_idx < 14:
            continue
        start_idx = max(0, end_idx - 14)
        window_gains = gains[start_idx:end_idx]
        window_losses = losses[start_idx:end_idx]

        if window_gains and window_losses:
            w_avg_gain = sum(window_gains) / len(window_gains)
            w_avg_loss = sum(window_losses) / len(window_losses)
            if w_avg_loss == 0:
                rsi_history.append(100.0)
            else:
                w_rs = w_avg_gain / w_avg_loss
                rsi_history.append(100 - (100 / (1 + w_rs)))

    # Generate trading signals based on RSI patterns
    rsi_timing = 0
    if len(rsi_history) >= 3:
        r0, r1, r2 = rsi_history[-1], rsi_history[-2], rsi_history[-3]
        
        # Detect rising trend
        rising = (r0 > r1) and (r1 > r2)
        
        # Buy signal: Rising RSI from oversold levels (< 40)
        reversal_buy = min(rsi_history) < 40
        
        # Sell signal: Falling RSI from overbought levels (> 70)
        reversal_sell = (max(r0, r1, r2) > 70) and (r2 < r1 < r0)

        if rising and reversal_buy:
            rsi_timing = 2
        elif reversal_sell:
            rsi_timing = -2

    return (rsi, rsi_timing)


# ============================================================================
# EMA (Exponential Moving Average) + MACD Calculation
# ============================================================================
@pw.udf
def calculate_ema_macd_signal(price_tuples: tuple) -> tuple[float, float, float, float, float, float, float]:
    """
    Calculate multiple EMAs, MACD, and MACD signal line together.
    
    EMAs calculated:
    - EMA_12: 12-period EMA (fast)
    - EMA_26: 26-period EMA (slow, for MACD)
    - EMA_20: 20-period EMA (short-term trend)
    - EMA_50: 50-period EMA (medium-term trend)
    - EMA_200: 200-period EMA (long-term trend)
    
    MACD (Moving Average Convergence Divergence):
    - MACD = EMA_12 - EMA_26
    - Signal = 9-period EMA of MACD
    
    Args:
        price_tuples: List of (timestamp_ms, price) tuples, sorted by timestamp
    
    Returns:
        Tuple of (ema_12, ema_26, ema_20, ema_50, ema_200, macd, signal)
    """
    prices = [p for (_, p) in price_tuples]

    if len(prices) == 0:
        logger.warning("calculate_ema_macd_signal: Empty price_tuples, returning default values")
        return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    def calc_ema(prices, n, alpha):
        if len(prices) == 0:
            return 0.0
        # Initialize with Simple Moving Average (SMA) for first n periods
        sma_len = min(len(prices), n)
        ema = sum(prices[:sma_len]) / sma_len
        # Apply EMA formula for remaining periods
        for i in range(sma_len, len(prices)):
            ema = alpha * prices[i] + (1 - alpha) * ema
        return ema

    # Calculate all required EMAs
    ema_12 = calc_ema(prices, 12, 2 / 13)
    ema_26 = calc_ema(prices, 26, 2 / 27)
    ema_20 = calc_ema(prices, 20, 2 / 21)
    ema_50 = calc_ema(prices, 50, 2 / 51)
    ema_200 = calc_ema(prices, 200, 2 / 201)

    # Calculate MACD line: difference between fast and slow EMA
    macd = ema_12 - ema_26

    # Calculate MACD signal line (9-period EMA of MACD)
    macd_history = []
    for end_idx in range(26, len(prices) + 1):
        window_prices = prices[:end_idx]
        w_ema12 = calc_ema(window_prices, 12, 2 / 13)
        w_ema26 = calc_ema(window_prices, 26, 2 / 27)
        macd_history.append(w_ema12 - w_ema26)

    # Signal line is 9-period EMA of MACD values
    signal = calc_ema(macd_history, 9, 2 / 10) if macd_history else 0.0

    return (ema_12, ema_26, ema_20, ema_50, ema_200, macd, signal)


# ============================================================================
# MACD Histogram Tracking UDF
# ============================================================================
@pw.udf
def get_prev_histogram(values: tuple) -> float | None:
    """
    Extract previous period's histogram value from sorted tuple.
    
    Args:
        values: Sorted tuple of (timestamp_ms, histogram_value) pairs
    
    Returns:
        Previous histogram value (second-to-last) or None if insufficient data
    """
    if len(values) < 2:
        return None
    return values[-2][1]  # Return histogram value from second-to-last entry
