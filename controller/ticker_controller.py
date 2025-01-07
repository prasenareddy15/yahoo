from flask_restx import Resource
import json
import pika
import uuid
from models.ticker_model import get_ticker_model
from services.rabbitmq_service import send_to_queue,wait_for_response

def init_ticker_routes(api):
    """
    Initialize Ticker routes and attach to Swagger API.
    """
    ticker_model = get_ticker_model(api)

    @api.route('/ticker/<int:ticker_id>')
    class Ticker(Resource):
        @api.marshal_with(ticker_model)
        async def get(self, ticker_id):
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
            await send_to_queue("ticker_queue", message)

            # Wait for response from RabbitMQ
            response = await wait_for_response("response_queue",correlation_id)

            return response, 200

