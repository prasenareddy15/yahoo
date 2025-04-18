from flask_restx import Resource
import json
import pika
import uuid
from models.ticker_model import get_ticker_model
from services.rabbitmq_service import send_to_queue, wait_for_response
import asyncio

def init_ticker_routes(api):
    """
    Initialize Ticker routes and attach to Swagger API.
    """
    ticker_model = get_ticker_model(api)

    @api.route('/ticker/<int:ticker_id>')
    class Ticker(Resource):
        @api.marshal_with(ticker_model)
        def get(self, ticker_id):
            if not ticker_id:
                return {"message": "ticker_id is required"}, 400

            correlation_id = str(uuid.uuid4())
            message = {
                "params": {"ticker_id": ticker_id},
                "response_queue": "response_queue",
                "correlation_id": correlation_id
            }

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            response = loop.run_until_complete(self.send_and_wait(message, correlation_id))
            loop.close()
            print(response)
            if(response):
                return(response[0])
            return {"message": "No data found"}, 404

        async def send_and_wait(self, message, correlation_id):
            await send_to_queue("ticker_queue", message)
            return await wait_for_response("response_queue", correlation_id,timeout=5)
