#pip install flask_restx
from flask_restx import Api
def configure_swagger(app):
    api = Api(app, version='1.0', title='Ticker API', description='A simple Ticker API')
    return api
