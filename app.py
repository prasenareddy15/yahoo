from flask import Flask
from services.swagger import configure_swagger
from controller.ticker_controller import init_ticker_routes

app = Flask(__name__)

# Configure Swagger
api = configure_swagger(app)

# Initialize routes
init_ticker_routes(api)

if __name__ == '__main__':
    app.run(debug=True)
