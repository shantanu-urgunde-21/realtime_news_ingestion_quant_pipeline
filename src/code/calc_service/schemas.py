import pathway as pw

class QuoteSchema(pw.Schema):
    """
    Schema for stock quote data from Kafka.
    
    Attributes:
        timestamp: Human-readable timestamp string
        symbol: Stock ticker symbol (e.g., 'AAPL')
        open: Opening price for the period
        high: Highest price during the period
        low: Lowest price during the period
        close: Closing price for the period
        volume: Trading volume (number of shares)
        ts_ms: Timestamp in milliseconds (used for temporal operations)
    """
    timestamp: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    ts_ms: int


class NewsSentimentSchema(pw.Schema):
    """
    Schema for news sentiment data from Kafka.
    """
    symbol: str
    weighted_avg_sentiment: float
    news_title: str
    ts_ms: int

