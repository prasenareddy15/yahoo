from flask_restx import Resource
import json
import pika
import uuid
import asyncio
from models.ticker_model import ticker_yfetch_model
from services.rabbitmq_service import send_to_queue, wait_for_response

def ticker_yfetch_routes(api):
    """
    Initialize Ticker routes and attach to Swagger API.
    """
    ticker_model = ticker_yfetch_model(api)

    @api.route('/ticker_yfetch/<string:ticker_symbol>')
    class Tickeryfetch(Resource):
        @api.marshal_with(ticker_model)
        def get(self, ticker_symbol):
            """
            Send ticker details to RabbitMQ and wait for the response.
            """
            # Validate input
            if not ticker_symbol:
                return {"message": "ticker_symbol is required"}, 400

            # Create a unique correlation ID
            correlation_id = str(uuid.uuid4())

            # Create message
            message = {
                "params": {"ticker_symbol": ticker_symbol},  # Only send parameters
                "response_queue": "ticker_yfetch_response_queue",
                "correlation_id": correlation_id
            }

            # Send message to RabbitMQ
            print(ticker_symbol, "symbol")
            loop = asyncio.new_event_loop()  # Create a new event loop
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.send_and_wait(message, correlation_id))

        async def send_and_wait(self, message, correlation_id):
            """
            This method sends the message and waits for the response asynchronously.
            """
            # Send message to RabbitMQ
            await send_to_queue("ticker_yfetch_queue", message)  # Await async function

            # Wait for response from RabbitMQ
            response = await wait_for_response("ticker_yfetch_response_queue", correlation_id)  # Await async function

            return response  # Return the response after waiting

