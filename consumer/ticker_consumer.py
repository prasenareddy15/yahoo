import pika
import json
from services.db_service import get_db_engine

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')

def process_message(ch, method, properties, body):
    """
    Callback to process a message from RabbitMQ.
    """
    message = json.loads(body)
    query = message.get("query")
    params = message.get("params")
    response_queue = message.get("response_queue")

    # Process the query
    try:
        result = execute_query(query, params)  # Execute DB query
        send_response_to_queue(response_queue, result)  # Send result to the response queue
    except Exception as e:
        print(f"Error processing message: {e}")
    
    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

def send_response_to_queue(queue_name, result):
    """
    Send the result to the response queue.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(result),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make message persistent
        )
    )
    connection.close()

def start_consumer():
    """
    Start the RabbitMQ consumer for the 'ticker_queue'.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue='ticker_queue', durable=True)

    channel.basic_consume(queue='ticker_queue', on_message_callback=process_message)
    print("Waiting for messages...")
    channel.start_consuming()
