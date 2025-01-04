import pika
import json
from services.db_service import get_db_engine

#controller rabbitmq_queue_communicators
def send_to_queue(queue_name, message):
    """
    Send message to RabbitMQ queue.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)  # Ensure durable queue
    print(queue_name)
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(message)
    )
    connection.close()

def wait_for_response(queue_name,correlation_id):
    """
    Wait for a response from RabbitMQ based on the correlation ID.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name,durable=True)

    response = None

    def callback(ch, method, properties, body):
        nonlocal response
        body = json.loads(body)
        if body.get("correlation_id") == correlation_id:
            response = body.get("data")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            channel.stop_consuming()

    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    print("Waiting for response...")
    channel.start_consuming()

    return response

#consumer responce rabbit_mq queue
def send_response_to_queue(response_queue, correlation_id, data):
    """
    Send the query results to the specified response queue.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=response_queue,durable=True)

    response_message = {"correlation_id": correlation_id, "data": data}

    channel.basic_publish(
        exchange='',
        routing_key=response_queue,
        body=json.dumps(response_message)
    )
    connection.close()
