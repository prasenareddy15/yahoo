from flask_restx import fields

def get_ticker_model(api):
    """
    Define the Swagger model for Ticker.
    """
    return api.model('Ticker', {
        'ticker_id': fields.Integer(required=True, description='The Ticker ID'),
        'symbol': fields.String(required=True, description='The ticker symbol'),
        'name': fields.String(required=True, description='The ticker name'),
        'last_updated': fields.DateTime(description='Last updated timestamp'),
        'created_at': fields.DateTime(description='Creation timestamp')
    })
def ticker_yfetch_model(api):
    """
    Define the response model for the API.
    """
    return api.model('ticker_yfetch', {
        'status': fields.String(required=True, description='The status of the operation (success or error)'),
        'message': fields.String(required=True, description='Message with additional information about the operation')
    })
