import asyncio
import aio_pika
import json

async def publish_message():
    connection = await aio_pika.connect_robust("amqp://user:bitnami@localhost/")
    async with connection:
        channel = await connection.channel()

        await channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps({"message": "Hello, RabbitMQ!"}).encode()),
            routing_key="file_queue"
        )
        print("Message published successfully!")

if __name__ == "__main__":
    asyncio.run(publish_message())
