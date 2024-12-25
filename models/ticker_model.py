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
