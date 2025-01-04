from flask_restx import Resource
import json
import pika
import uuid
from models.ticker_model import get_ticker_model

def init_ticker_routes(api):
    """
    Initialize Ticker routes and attach to Swagger API.
    """
    ticker_model = get_ticker_model(api)

    @api.route('/ticker/<int:ticker_id>')
    class Ticker(Resource):
        @api.marshal_with(ticker_model)
        def get(self, ticker_id):
            """
            Send ticker details to RabbitMQ and wait for the response.
            """
            # Validate input
            if not ticker_id:
                return {"message": "ticker_id is required"}, 400

            # Create a unique correlation ID
            correlation_id = str(uuid.uuid4())

            # Create message
            message = {
                "params": {"ticker_id": ticker_id},  # Only send parameters
                "response_queue": "response_queue",
                "correlation_id": correlation_id
            }

            # Send message to RabbitMQ
            send_to_queue("ticker_queue", message)

            # Wait for response from RabbitMQ
            response = wait_for_response(correlation_id)

            return response, 200

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

def wait_for_response(correlation_id):
    """
    Wait for a response from RabbitMQ based on the correlation ID.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue="response_queue",durable=True)

    response = None

    def callback(ch, method, properties, body):
        nonlocal response
        body = json.loads(body)
        if body.get("correlation_id") == correlation_id:
            response = body.get("data")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            channel.stop_consuming()

    channel.basic_consume(queue="response_queue", on_message_callback=callback)

    print("Waiting for response...")
    channel.start_consuming()

    return response