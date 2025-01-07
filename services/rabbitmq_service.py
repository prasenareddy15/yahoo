import aio_pika
import json
import asyncio

async def send_to_queue(queue_name, message):
    """
    Send message to RabbitMQ queue asynchronously.
    """
    connection = await aio_pika.connect_robust("amqp://localhost/")  # Asynchronous connection
    async with connection:
        channel = await connection.channel()  # Create channel
        await channel.declare_queue(name=queue_name, durable=True)  # Declare queue
        print(f"Sending to queue: {queue_name}")
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode()),
            routing_key=queue_name
        )
import aio_pika
import json
import asyncio

async def wait_for_response(queue_name, correlation_id, timeout=10):
    """
    Wait for a response from RabbitMQ based on the correlation ID asynchronously.
    
    Parameters:
    - queue_name: The name of the RabbitMQ queue to listen for responses.
    - correlation_id: The correlation ID to match against.
    - timeout: The timeout in seconds to wait for the response (default 10 seconds).
    """
    connection = await aio_pika.connect_robust("amqp://localhost/")  # Asynchronous connection
    async with connection:
        try:
            channel = await connection.channel()  # Create a channel
            queue = await channel.declare_queue(queue_name, durable=True)  # Declare queue

            response = None
            timeout_time = asyncio.get_event_loop().time() + timeout  # Set timeout

            # Define the callback to handle the response
            async def callback(message: aio_pika.IncomingMessage):
                nonlocal response
                async with message.process():
                    body = json.loads(message.body)
                    if body.get("correlation_id") == correlation_id:
                        response = body.get("data")
                        print(f"Received response: {response}")
                        await message.ack()  # Acknowledge the message

            # Start consuming messages from the declared queue
            await queue.consume(callback, no_ack=False)
            print("Waiting for response...")

            # Wait for response asynchronously with timeout
            while response is None:
                if asyncio.get_event_loop().time() > timeout_time:
                    print("Timeout reached without receiving a response.")
                    return None  # Timeout occurred
                await asyncio.sleep(1)  # Wait for a response

            return response

        except Exception as e:
            print(f"An error occurred while consuming: {e}")
            return None  # Return None if an error occurs

async def rrwait_for_response(queue_name, correlation_id):
    """
    Wait for a response from RabbitMQ based on the correlation ID asynchronously.
    """
    connection = await aio_pika.connect_robust("amqp://localhost/")  # Asynchronous connection
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(queue_name, durable=True)

        response = None

        # Define the callback to handle the response
        async def callback(message: aio_pika.IncomingMessage):
            nonlocal response
            async with message.process():
                body = json.loads(message.body)
                if body.get("correlation_id") == correlation_id:
                    response = body.get("data")
                    print(f"Received response: {response}")
                    await message.ack()

        # Start consuming messages
        await channel.consume(callback, queue=queue_name, no_ack=False)
        print("Waiting for response...")
        # Wait for response asynchronously
        while response is None:
            await asyncio.sleep(1)

        return response

async def sswait_for_response(queue_name, correlation_id):
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")  # Ensure you're using aio_pika
    async with connection:
        channel = await connection.channel()  # Create a channel using aio_pika
        queue = await channel.declare_queue(queue_name, durable=True)
        
        # Define a callback to handle messages
        async def callback(message: aio_pika.IncomingMessage):
            async with message.process():
                if message.correlation_id == correlation_id:
                    print("Received message:", message.body)
        
        # Start consuming messages
        await queue.consume(callback)
        print("Waiting for response...")
        await asyncio.Future()  # This keeps the consumer running

loop = asyncio.get_event_loop()
loop.run_until_complete(wait_for_response("ticker_yfetch_response_queue", correlation_id="9831f309-2ced-481c-84fd-db9167fe5c15"))

# Consumer response rabbit_mq queue
async def send_response_to_queue(response_queue, correlation_id, data):
    """
    Send the query results to the specified response queue asynchronously.
    """
    connection = await aio_pika.connect_robust("amqp://localhost/")  # Asynchronous connection
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(response_queue, durable=True)

        response_message = {"correlation_id": correlation_id, "data": data}

        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(response_message).encode()),
            routing_key=response_queue
        )
