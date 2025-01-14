from sqlalchemy import text
from datetime import datetime, timedelta
import yfinance as yf
import asyncio
from services.db_service import get_db_engine
from consumer.ticker_yfetch_table_consumer import fetch_ticker_details
async def fetch_ticker_details(ticker_symbol):
    """
    Fetch ticker details and historical price data from Yahoo Finance.
    """
    ticker = yf.Ticker(ticker_symbol)
    
    # Fetch ticker details
    details = {
        "symbol": ticker_symbol,
        "name": ticker.info.get("longName", "Unknown"),
        "last_updated": datetime.fromtimestamp(ticker.info.get("regularMarketTime", datetime.now().timestamp()))
    }

    # Fetch 5-year historical price data
    historical_data = ticker.history(period="5y").reset_index()
    price_data = []
    for _, row in historical_data.iterrows():
        price_data.append({
            "date": row["Date"],
            "open": row["Open"],
            "high": row["High"],
            "low": row["Low"],
            "close": row["Close"],
            "volume": row["Volume"]
        })
    return details, price_data

