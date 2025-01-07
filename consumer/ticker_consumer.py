import aio_pika
import json
from sqlalchemy import text
from services.db_service import get_db_engine
from services.rabbitmq_service import send_response_to_queue
from datetime import datetime
import asyncio

async def execute_query(params):
    """
    Execute a pre-defined query based on the input parameters asynchronously.
    """
    query = """
        SELECT [ticker_id], [symbol], [name], [last_updated], [created_at]
        FROM [yahoo].[dbo].[Ticker_t]
        WHERE [ticker_id] = :ticker_id
    """
    engine = get_db_engine()
    async with engine.connect() as connection:
        result = await connection.execute(text(query), params)
        users = [
            {key: (value.isoformat() if isinstance(value, datetime) else value) for key, value in zip(result.keys(), row)}
            for row in result
        ]
        return users

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
            # Execute the query asynchronously
            result = await execute_query(params)

            # Send the response back to the response queue
            await send_response_to_queue(response_queue, correlation_id, result)
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
        queue = await channel.declare_queue("ticker_queue", durable=True)  # Declare queue
        await queue.consume(process_message)  # Start consuming messages
        await asyncio.Future()  # Keep the consumer running

if __name__ == "__main__":
    asyncio.run(start_consumer())  # Start the async consumer
