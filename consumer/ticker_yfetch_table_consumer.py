import aio_pika
import json
from sqlalchemy import text
from services.db_service import get_db_engine
from services.rabbitmq_service import send_response_to_queue
from datetime import datetime
import yfinance as yf
import asyncio

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
    historical_data = ticker.history(period="max").reset_index()
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

async def insert_ticker_data(ticker_details, price_data):
    """
    Insert ticker details and price history into the database asynchronously.
    """
    # Define your SQL queries
    query_ticker = text("""
        INSERT INTO [yahoo].[dbo].[stock_daily] (symbol, name, last_updated)
        OUTPUT inserted.ticker_id
        VALUES (:symbol, :name, :last_updated)
    """)

    query_price = text("""
        INSERT INTO [yahoo].[dbo].[stock_master] (ticker_id, [date], [open], [high], [low], [close], [volume])
        VALUES (:ticker_id, :date, :open, :high, :low, :close, :volume)
    """)

    engine = get_db_engine()

    try:
        # Check if the ticker already exists (optional part, can be removed)
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT COUNT(*) FROM [yahoo].[dbo].[stock_daily] WHERE symbol = :symbol"),
                {"symbol": ticker_details["symbol"]}
            ).fetchone()
            
            if result[0] > 0:
                print(f"Ticker {ticker_details['symbol']} already exists in the database.")
                return  # Exit if the ticker already exists

            # Ensure datetime fields are in a compatible format
            #ticker_details["created_at"] = datetime.now()  # Add current timestamp for created_at
            ticker_details["last_updated"] = ticker_details["last_updated"].strftime("%Y-%m-%d %H:%M:%S")
            #ticker_details["created_at"] = ticker_details["created_at"].strftime("%Y-%m-%d %H:%M:%S")

            # Log the ticker details
            print(f"Inserting ticker details for symbol: {ticker_details['symbol']}, Price data length: {len(price_data)}")
            print(f"Ticker details: {ticker_details}")

            # Insert ticker details into the database
            result=connection.execute(query_ticker, ticker_details)
            ticker_id = result.fetchone()[0]  # Fetch the first row and get the ticker_id
            print(f"Generated ticker_id: {ticker_id}")

            # Insert price data into the database
            print(f"Inserting price data: {len(price_data)}")
            for price in price_data:
                price["ticker_id"] = ticker_id
                price["date"] = price["date"].strftime("%Y-%m-%d %H:%M:%S")  # Ensure date is formatted for SQL

                # Log each price insert
                connection.execute(query_price, price)

            # If using SQLAlchemy with a connection, commit is not strictly necessary, unless in a transaction.
            connection.commit()  # Explicitly commit the transaction

    except Exception as e:
        print(f"Error inserting data for {ticker_details['symbol']}: {e}")
async def process_message(message: aio_pika.IncomingMessage):
    """
    Asynchronous callback to process a message from RabbitMQ.
    """
    async with message.process():
        message_data = json.loads(message.body)
        params = message_data.get("params")
        response_queue = message_data.get("response_queue")
        correlation_id = message_data.get("correlation_id")

        try:
            # Fetch ticker details and historical data
            ticker_symbol = params.get("ticker_symbol")
            ticker_details, price_data = await fetch_ticker_details(ticker_symbol)
            # Insert data into the database
            await insert_ticker_data(ticker_details, price_data)
            # Prepare response
            response = {"status": "success", "message": f"Ticker {ticker_symbol} processed successfully"}
            await send_response_to_queue(response_queue, correlation_id, response)

        except Exception as e:
            error_message = f"Error processing message: {e}"
            await send_response_to_queue(response_queue, correlation_id, {"status": "error", "message": error_message})

async def start_consumer():
    """
    Asynchronous RabbitMQ consumer.
    """
    connection = await aio_pika.connect_robust("amqp://localhost/")  # Connect to RabbitMQ
    async with connection:
        channel = await connection.channel()  # Create a channel
        queue = await channel.declare_queue("ticker_yfetch_queue", durable=True)  # Declare queue
        await queue.consume(process_message)  # Start consuming messages
        await asyncio.Future()  # Keep the consumer running

if __name__ == "__main__":
    asyncio.run(start_consumer())  # Start the async consumer
