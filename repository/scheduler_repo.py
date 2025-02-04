from sqlalchemy import text
from datetime import datetime, timedelta
import yfinance as yf
import asyncio
from services.db_service import get_db_engine
from consumer.ticker_yfetch_table_consumer import fetch_ticker_details
async def get_all_tickers():
    #print("pro")
    engine=get_db_engine()
    
    today_date = datetime.now().date()
    #print(today_date)
    with engine.connect() as connection:
        query = text("SELECT * FROM [yahoo].[dbo].[stock_daily] WHERE last_updated < :today")
        result = connection.execute(query, {"today": today_date}).fetchall()
        #print("pro")
    return(result)
async def fetch_ticker_details(ticker_symbol,last_updated):
    """
    Fetch ticker details and historical price data from Yahoo Finance.
    """
    ticker = yf.Ticker(ticker_symbol)
    # Fetch 5-year historical price data
    historical_data = ticker.history(start=last_updated, period="max").reset_index()
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
    return price_data
async def insert_historical_data(ticker_id, historical_data):
    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            # Insert historical data
            print("kate")
            for record in historical_data:
                print("inside")
                query = text("""
                    MERGE INTO [yahoo].[dbo].[stock_master] AS target
                    USING (SELECT :ticker_id AS ticker_id, :date AS date, :open AS [open], :high AS [high], :low AS [low], :close AS [close], :volume AS [volume]) AS source
                    ON target.ticker_id = source.ticker_id AND target.date = source.date
                    WHEN MATCHED THEN
                        UPDATE SET
                            target.[open] = source.[open],
                            target.[high] = source.[high],
                            target.[low] = source.[low],
                            target.[close] = source.[close],
                            target.[volume] = source.[volume]
                    WHEN NOT MATCHED THEN
                        INSERT (ticker_id, date, [open], [high], [low], [close], [volume])
                        VALUES (source.ticker_id, source.date, source.[open], source.[high], source.[low], source.[close], source.[volume]);

                """)
                connection.execute(query, {"ticker_id": ticker_id, **record})
                connection.commit()
            #Update the `last_updated` field after successful insertion
            update_query = text("UPDATE [yahoo].[dbo].[Ticker_t] SET last_updated = :last_updated WHERE ticker_id = :ticker_id")
            await connection.execute(update_query, {"last_updated": datetime.now(), "ticker_id": ticker_id})
            connection.commit()
        print(f"Inserted historical data and updated last_updated for ticker_id {ticker_id}.")
    except Exception as e:
        print(f"Error inserting historical data for ticker_id {ticker_id}: {e}")
        raise e
async def main():
    tickers = await fetch_ticker_details("aapl",datetime.now().date())
    print(tickers)
    k=await insert_historical_data(14,tickers)

# Run the main function
asyncio.run(main())