import pika
import json
from services.db_service import get_db_engine

def execute_query(query, params):
    """
    Execute the query on the database and return the results.
    """
    engine = get_db_engine()
    with engine.connect() as connection:
        result = connection.execute(query, **params)
        return [dict(row) for row in result]

def send_response_to_queue(response_queue, result):
    """
    Send the query results to the specified response queue.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=response_queue,durable=True)
    channel.basic_publish(exchange='', routing_key=response_queue, body=json.dumps(result))
    connection.close()

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

# RabbitMQ consumer setup
def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='ticker_queue',durable=True)

    channel.basic_consume(queue='ticker_queue', on_message_callback=process_message)

    print("Waiting for messages...")
    channel.start_consuming()
