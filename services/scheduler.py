from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import text
from datetime import datetime, timedelta
import yfinance as yf
import asyncio
from services.db_service import get_db_engine
from consumer.ticker_yfetch_table_consumer import fetch_ticker_details
async def update_ticker(ticker_id, symbol, last_updated):
    try:
        # Fetch updated data from Yahoo Finance
        details, historical_data = await fetch_ticker_details(symbol)

        # Insert or update historical data in the database
        await insert_historical_data(ticker_id, historical_data)

        # Update `last_updated` field in the database
        await update_last_updated(ticker_id, datetime.now())
        print(f"Successfully updated ticker: {symbol}")

    except Exception as e:
        # Log the error
        log_error(f"Error updating ticker {symbol}: {str(e)}")
        # Mark the ticker as pending for retry
        await mark_ticker_as_pending(ticker_id)

async def run_scheduler():
    while True:
        try:
            # Fetch all tickers to update
            tickers = await get_all_tickers()
            for ticker in tickers:
                # Skip if already up-to-date
                if ticker["last_updated"].date() == datetime.now().date():
                    continue

                # Update each ticker
                await update_ticker(ticker["ticker_id"], ticker["symbol"], ticker["last_updated"])

        except Exception as e:
            log_error(f"Scheduler encountered an error: {str(e)}")

        # Run the scheduler daily at 6 PM
        now = datetime.now()
        next_run = (datetime.combine(now.date(), datetime.min.time()) + timedelta(days=1, hours=18))
        delay = (next_run - now).total_seconds()
        print(f"Scheduler sleeping until next run at {next_run}")
        await asyncio.sleep(delay)

# Start the scheduler
if __name__ == "__main__":
    asyncio.run(run_scheduler())
