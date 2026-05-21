"""
Dummy Alpha Vantage API - Local Testing without Real API Calls

This module provides mock implementations of Alpha Vantage API functions
for local development and testing without requiring actual API keys.
"""
import json
import logging
import random
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# Dummy news data based on popular stocks with correct Alpha Vantage structure
DUMMY_NEWS_DATA = {
    "status": "ok",
    "results": [
        {
            "title": "Tech Giant Reports Strong Q1 Earnings",
            "description": "Company beats expectations with solid revenue growth in cloud services",
            "source": "Financial Times",
            "url": "https://dummy-news.example.com/1",
            "image_url": "https://dummy-news.example.com/img1.jpg",
            "time_published": "20260430T143000",
            "authors": ["Jane Analyst"],
            "overall_sentiment_score": 0.75,
            "overall_sentiment_label": "positive",
            "ticker_sentiment": [
                {
                    "ticker": "AAPL",
                    "relevance_score": 0.8,
                    "ticker_sentiment_score": 0.7
                },
                {
                    "ticker": "MSFT",
                    "relevance_score": 0.6,
                    "ticker_sentiment_score": 0.65
                }
            ]
        },
        {
            "title": "Market Volatility Concerns Investors",
            "description": "Analysts express concerns about rising interest rates impacting tech stocks",
            "source": "Bloomberg",
            "url": "https://dummy-news.example.com/2",
            "image_url": "https://dummy-news.example.com/img2.jpg",
            "time_published": "20260429T101500",
            "authors": ["John Smith"],
            "overall_sentiment_score": -0.45,
            "overall_sentiment_label": "negative",
            "ticker_sentiment": [
                {
                    "ticker": "GOOGL",
                    "relevance_score": 0.7,
                    "ticker_sentiment_score": -0.5
                },
                {
                    "ticker": "AMZN",
                    "relevance_score": 0.65,
                    "ticker_sentiment_score": -0.4
                }
            ]
        },
        {
            "title": "New Product Launch Receives Positive Reception",
            "description": "Initial market response to new innovative product offering is very strong",
            "source": "Reuters",
            "url": "https://dummy-news.example.com/3",
            "image_url": "https://dummy-news.example.com/img3.jpg",
            "time_published": "20260428T094500",
            "authors": ["Sarah Tech"],
            "overall_sentiment_score": 0.85,
            "overall_sentiment_label": "positive",
            "ticker_sentiment": [
                {
                    "ticker": "NVDA",
                    "relevance_score": 0.9,
                    "ticker_sentiment_score": 0.85
                },
                {
                    "ticker": "TSLA",
                    "relevance_score": 0.75,
                    "ticker_sentiment_score": 0.8
                }
            ]
        }
    ]
}


def get_news(topics, time_from, time_to, limit=50, sort="LATEST"):
    """
    Dummy implementation of Alpha Vantage News API.
    Returns synthetic news data instead of real API calls.
    
    Args:
        topics: List of topics to search (e.g., ["technology", "finance"])
        time_from: Start time for news search
        time_to: End time for news search
        limit: Maximum number of results
        sort: Sort order (LATEST, EARLIEST, RELEVANCE)
        
    Returns:
        dict: Mock API response with news articles
    """
    logger.info(f"[DUMMY API] Fetching news for topics: {topics}")
    logger.info(f"[DUMMY API] Time range: {time_from} to {time_to}")
    
    # Return synthetic data with some randomization
    results = []
    for i, article in enumerate(DUMMY_NEWS_DATA["results"][:limit]):
        synthetic_article = article.copy()
        # Add randomization to sentiment scores
        synthetic_article["overall_sentiment_score"] += random.uniform(-0.1, 0.1)
        synthetic_article["overall_sentiment_score"] = max(-1, min(1, synthetic_article["overall_sentiment_score"]))
        
        # Randomize ticker sentiment scores as well
        if "ticker_sentiment" in synthetic_article:
            synthetic_article["ticker_sentiment"] = []
            for ticker_info in article["ticker_sentiment"]:
                ticker_copy = ticker_info.copy()
                ticker_copy["ticker_sentiment_score"] += random.uniform(-0.1, 0.1)
                ticker_copy["ticker_sentiment_score"] = max(-1, min(1, ticker_copy["ticker_sentiment_score"]))
                ticker_copy["relevance_score"] += random.uniform(-0.05, 0.05)
                ticker_copy["relevance_score"] = max(0, min(1, ticker_copy["relevance_score"]))
                synthetic_article["ticker_sentiment"].append(ticker_copy)
        
        results.append(synthetic_article)
    
    return {
        "status": "ok",
        "results": results,
        "note": "[DUMMY DATA] Using synthetic news for local testing"
    }


def get_ticker_sentiment(ticker, limit=50, sort="LATEST"):
    """
    Dummy implementation for getting sentiment data for a specific ticker.
    
    Args:
        ticker: Stock ticker symbol
        limit: Maximum results
        sort: Sort order
        
    Returns:
        dict: Mock sentiment data for the ticker
    """
    logger.info(f"[DUMMY API] Fetching sentiment for ticker: {ticker}")
    
    base_sentiment = random.uniform(-0.3, 0.8)
    
    return {
        "status": "ok",
        "ticker": ticker,
        "sentiment_score": base_sentiment,
        "sentiment_label": "positive" if base_sentiment > 0 else "negative",
        "relevance_score": random.uniform(0.5, 1.0),
        "results": DUMMY_NEWS_DATA["results"][:limit],
        "note": "[DUMMY DATA] Using synthetic sentiment data for local testing"
    }
