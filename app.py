import threading
import asyncio
from flask import Flask
from services.swagger import configure_swagger
from controller.ticker_controller import init_ticker_routes
from controller.ticker_yfetch_table_controller import ticker_yfetch_routes
from consumer.ticker_consumer import start_consumer as ticker_consumer
from consumer.ticker_yfetch_table_consumer import start_consumer as ticker_yfetch_table_consumer

app = Flask(__name__)

# Configure Swagger
api = configure_swagger(app)

# Initialize routes
init_ticker_routes(api)
ticker_yfetch_routes(api)

def start_background_tasks():
    """
    Start asynchronous tasks in a separate thread.
    """
    loop = asyncio.new_event_loop()  # Create a new event loop
    asyncio.set_event_loop(loop)  # Set the event loop for this thread
    loop.run_until_complete(start_consumers())  # Run both consumers

def start_consumers():
    """
    Start both consumers asynchronously.
    """
    consumer_tasks = [
        ticker_consumer(),  # Start ticker consumer
        ticker_yfetch_table_consumer()  # Start second consumer
    ]
    return asyncio.gather(*consumer_tasks)

def run_flask_app():
    """
    Run the Flask app in the main thread.
    """
    app.run(debug=True, use_reloader=False)  # Avoid reloader to prevent interference

if __name__ == '__main__':
    # Start background tasks for consumers in a separate thread
    consumer_thread = threading.Thread(target=start_background_tasks)
    consumer_thread.daemon = True  # Make sure it exits when the main thread exits
    consumer_thread.start()

    # Run the Flask app in the main thread
    run_flask_app()
