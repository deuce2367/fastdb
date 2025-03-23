import asyncio
import pika
import pika.adapters.asyncio_connection
import logging

logging.basicConfig(level=logging.INFO)

RABBITMQ_URL = 'amqp://user:bitnami@localhost:5672/'
QUEUE_NAME = 'test_queue'

def on_message_received(ch, method, properties, body):
    logging.info(f"Received message: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

async def start_rabbitmq_consumer():
    while True:
        try:
            connection_params = pika.URLParameters(RABBITMQ_URL)
            connection = await pika.adapters.asyncio_connection.AsyncioConnection.create(connection_params)
            channel = await connection.channel()
            await channel.queue_declare(queue=QUEUE_NAME)
            await channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message_received)
            logging.info("[*] Waiting for messages...")
            await asyncio.Future()  # Keeps the consumer running
        except Exception as e:
            logging.error(f"RabbitMQ connection error: {e}")
            await asyncio.sleep(5)  # Retry after delay

if __name__ == "__main__":
    asyncio.run(start_rabbitmq_consumer()
