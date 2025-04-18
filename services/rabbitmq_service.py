import aio_pika
import json
import asyncio

async def send_to_queue(queue_name, message):
    """
    Send message to RabbitMQ queue asynchronously.
    """
    print("send_to_queue",queue_name,message)
    connection = await aio_pika.connect_robust("amqp://localhost/")  # Asynchronous connection
    async with connection:
        channel = await connection.channel()  # Create channel
        await channel.declare_queue(name=queue_name, durable=True)  # Declare queue
        print(f"Sending to queue: {queue_name}")
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode()),
            routing_key=queue_name
        )


async def wait_for_response(queue_name, correlation_id, timeout=5):
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()

    queue = await channel.declare_queue(queue_name, durable=True)

    future = asyncio.get_event_loop().create_future()

    async def on_message(message: aio_pika.IncomingMessage):
        async with message.process():
            try:
                body = message.body.decode()
                print("Received message:", body)
                import json
                data = json.loads(body)

                if data.get("correlation_id") == correlation_id:
                    future.set_result(data["data"])
            except Exception as e:
                print(f"Error in message callback: {e}")
                future.set_result({"status": "error", "message": str(e)})

    await queue.consume(on_message)

    try:
        return await asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        return {"status": "error", "message": "Timeout waiting for response"}
    finally:
        await connection.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(wait_for_response("ticker_yfetch_response_queue", correlation_id="9831f309-2ced-481c-84fd-db9167fe5c15"))

# Consumer response rabbit_mq queue
async def send_response_to_queue(response_queue, correlation_id, data):
    """
    Send the query results to the specified response queue asynchronously.
    """
    connection = await aio_pika.connect_robust("amqp://localhost/")  # Create connection
    try:
        channel = await connection.channel()
        await channel.declare_queue(response_queue, durable=True)

        response_message = {
            "correlation_id": correlation_id,
            "data": data
        }

        print("*******************************response message", response_queue, response_message)

        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(response_message).encode()),
            routing_key=response_queue
        )
    finally:
        await connection.close()
