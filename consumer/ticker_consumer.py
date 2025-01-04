import pika
import json
from sqlalchemy import text
from services.db_service import get_db_engine
from services.rabbitmq_service import send_response_to_queue
from datetime import datetime

def execute_query(params):
    """
    Execute a pre-defined query based on the input parameters.
    """
    query = """
        SELECT [ticker_id], [symbol], [name], [last_updated], [created_at]
        FROM [yahoo].[dbo].[Ticker_t]
        WHERE [ticker_id] = :ticker_id
    """
    engine = get_db_engine()
    with engine.connect() as connection:
        print("katespade")
        result = connection.execute(text(query), params)
        users=[
            {key: (value.isoformat() if isinstance(value, datetime) else value) for key, value in zip(result.keys(), row)}
            for row in result
        ]
        print(users)
        return users


def process_message(ch, method, properties, body):
    """
    Callback to process a message from RabbitMQ.
    """
    message = json.loads(body)
    params = message.get("params")
    response_queue = message.get("response_queue")
    correlation_id = message.get("correlation_id")
    print(message)
    try:
        result = execute_query(params)

        # Send response back to the response queue
        send_response_to_queue(response_queue, correlation_id, result)
    except Exception as e:
        print(f"Error processing message: {e}")
    
    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_consumer():
    """
    Start RabbitMQ consumer.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue="ticker_queue",durable=True)

    channel.basic_consume(queue="ticker_queue", on_message_callback=process_message)

    print("Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    start_consumer()
