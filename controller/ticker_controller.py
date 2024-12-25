from flask_restx import Resource
from flask import request
import os
print(os.getcwd(),"apple")
from services.rabbitmq_service import send_to_queue
from models.ticker_model import get_ticker_model
#from services.db_service import get_db_engine
def init_ticker_routes(api):
    """
    Initialize Ticker routes and attach to Swagger API.
    """
    ticker_model = get_ticker_model(api)

    @api.route('/ticker/<int:ticker_id>')
    @api.response(404, 'Ticker not found')
    class Ticker(Resource):
        @api.marshal_with(ticker_model)
        def get(self, ticker_id):
            """
            Fetch ticker details by ticker_id.
            """
            message = {
                "query": """
                    SELECT [ticker_id], [symbol], [name], [last_updated], [created_at]
                    FROM [yahoo].[dbo].[Ticker_t]
                    WHERE [ticker_id] = :ticker_id
                """,
                "params": {"ticker_id": ticker_id},
                "response_queue": "response_queue"
            }
            send_to_queue("ticker_queue", message)  # Send query to RabbitMQ

            return {"status": "Request sent to process ticker details"}, 200
