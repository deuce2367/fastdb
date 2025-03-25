import asyncio
import pika
import pika.adapters.asyncio_connection

class AsyncRabbitMQPublisher:
    """
    Asynchronous RabbitMQ Publisher that sends messages to a specified exchange and routing key.
    """

    def __init__(self, amqp_url, exchange='', routing_key='job_queue'):
        self.amqp_url = amqp_url
        self.exchange = exchange
        self.routing_key = routing_key
        self.connection = None
        self.channel = None

    async def connect(self):
        """
        Establishes an asynchronous connection to RabbitMQ.
        """
        loop = asyncio.get_running_loop()
        self.connection = await pika.adapters.asyncio_connection.AsyncioConnection.create(
            pika.URLParameters(self.amqp_url), loop=loop
        )
        self.channel = await self.connection.channel()
        print("Connected to RabbitMQ")

    async def publish_message(self, message: str):
        """
        Publishes a message asynchronously.

        Parameters:
        -----------
        message : str
            The message to be published.
        """
        if not self.channel:
            raise ConnectionError("RabbitMQ channel is not established.")

        await self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            body=message.encode('utf-8')
        )
        print(f"Message published: {message}")

    async def close(self):
        """
        Gracefully closes the RabbitMQ connection.
        """
        if self.connection:
            await self.connection.close()
            print("RabbitMQ connection closed")

async def main():
    """
    Main function to demonstrate the asynchronous RabbitMQ publisher.
    """
    publisher = AsyncRabbitMQPublisher(
        amqp_url='amqp://user:bitnami@localhost:5672/'
    )

    try:
        await publisher.connect()
        await publisher.publish_message("Hello, RabbitMQ!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await publisher.close()

if __name__ == '__main__':
    asyncio.run(main())
