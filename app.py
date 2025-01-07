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

# from flask import Flask
# from services.swagger import configure_swagger
# from controller.ticker_controller import init_ticker_routes
# from controller.ticker_yfetch_table_controller import ticker_yfetch_routes
# from consumer.ticker_consumer import start_consumer as ticker_consumer
# from consumer.ticker_yfetch_table_consumer import start_consumer as ticker_yfetch_table_consumer  # Second consumer
# import threading
# import asyncio

# app = Flask(__name__)

# # Configure Swagger
# api = configure_swagger(app)

# # Initialize routes
# init_ticker_routes(api)
# ticker_yfetch_routes(api)

# # Run asynchronous consumers in a background thread
# def start_background_tasks():
#     """
#     Run asynchronous tasks in a background thread.
#     """
#     loop = asyncio.new_event_loop()  # Create a new event loop for the thread
#     threading.Thread(target=run_async_loop, args=(loop,)).start()  # Start event loop in a new thread

# def run_async_loop(loop):
#     """
#     Run the async event loop in the background.
#     """
#     asyncio.set_event_loop(loop)
#     loop.run_until_complete(start_consumers())  # Run consumers asynchronously

# def start_consumers():
#     """
#     Start all consumers asynchronously.
#     """
#     # Start both consumers
#     consumer_tasks = [
#         ticker_consumer(),
#         ticker_yfetch_table_consumer()
#     ]
#     # Await completion of both consumer tasks
#     return asyncio.gather(*consumer_tasks)

# if __name__ == '__main__':
#     # Start background tasks in a separate thread
#     start_background_tasks()

#     # Run the Flask app
#     app.run(debug=True, use_reloader=False)  # use_reloader=False to avoid restarting the Flask app


# # import asyncio
# # from flask import Flask
# # from services.swagger import configure_swagger
# # from controller.ticker_controller import init_ticker_routes
# # from controller.ticker_yfetch_table_controller import ticker_yfetch_routes
# # from consumer.ticker_consumer import start_consumer as ticker_consumer
# # from consumer.ticker_yfetch_table_consumer import start_consumer as ticker_yfetch_table_consumer  # Second consumer
# # import threading

# # app = Flask(__name__)

# # # Configure Swagger
# # api = configure_swagger(app)

# # # Initialize routes
# # init_ticker_routes(api)
# # ticker_yfetch_routes(api)

# # def start_consumers_in_thread():
# #     """
# #     Run consumers in a separate thread to avoid blocking the Flask app.
# #     """
# #     loop = asyncio.new_event_loop()
# #     asyncio.set_event_loop(loop)
# #     loop.run_until_complete(start_consumers())

# # def start_consumers():
# #     """
# #     Start both consumers asynchronously.
# #     """
# #     asyncio.gather(
# #         ticker_consumer(),  # Start the ticker consumer
# #         ticker_yfetch_table_consumer()  # Start the second consumer
# #     )

# # if __name__ == '__main__':
# #     # Start consumers in a separate thread
# #     consumer_thread = threading.Thread(target=start_consumers_in_thread)
# #     consumer_thread.start()

# #     # Run the Flask app
# #     app.run(debug=True, use_reloader=False)  # use_reloader=False to avoid restarting the Flask app
