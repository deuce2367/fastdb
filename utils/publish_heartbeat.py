import asyncio
import aio_pika

async def main():
    connection = await aio_pika.connect_robust(
        "amqp://user:bitnami@localhost/",
        heartbeat=60  # Heartbeat interval in seconds
    )

    async with connection:
        channel = await connection.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(body=b"Hello RabbitMQ!"),
            routing_key="test_queue"
        )
        print("Message published successfully.")

if __name__ == "__main__":
    asyncio.run(main())
