import asyncio
import pika
import pika.adapters.asyncio_connection
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import FileResponse
from prometheus_client import Counter, Gauge, generate_latest
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timedelta
import os
import time
import logging

# Application logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a handler that prints to the console
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


# FastAPI app setup with Swagger docs enabled
app = FastAPI(title="File Download Service", description="A FastAPI service for managing file downloads and logging", version="1.0")
logging.info("FastAPI APP created, Swagger enabled at /docs URL")

# Prometheus Metrics
DOWNLOAD_COUNTER = Counter("file_downloads", "Count of file downloads", ["filename"])
ERROR_COUNTER = Counter("errors", "Count of errors", ["type"])
RABBITMQ_MESSAGE_COUNTER = Counter("rabbitmq_messages", "Count of RabbitMQ messages received")

# Database setup
DATABASE_URL = "sqlite:///./downloads.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class DownloadLog(Base):
    __tablename__ = "download_logs"
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    status = Column(String(50))
    ip_address = Column(String(50))
    user_agent = Column(String(255))
    file_size = Column(Integer)
    transfer_time = Column(Float)
    throughput_mbps = Column(Float)
    file_age_seconds = Column(Float)

class FilenameRegistry(Base):
    __tablename__ = "filenames"
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), unique=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

STATIC_DIR = "./static"

# RabbitMQ Configuration
RABBITMQ_URL = 'amqp://user:bitnami@localhost:5672/'
QUEUE_NAME = 'file_queue'

def on_message_received(ch, method, properties, body):
    try:
        logging.info(f"***** Received message from RabbitMQ: {body.decode()}")
        print(f"***** Received message from RabbitMQ: {body.decode()}")
        RABBITMQ_MESSAGE_COUNTER.inc()

        db = SessionLocal()
        filename = body.decode().strip()
        existing_entry = db.query(FilenameRegistry).filter(FilenameRegistry.filename == filename).first()
        if not existing_entry:
            new_entry = FilenameRegistry(filename=filename)
            db.add(new_entry)
            db.commit()
        db.close()
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"Error processing message: {e}")
        ERROR_COUNTER.labels(type="rabbitmq_message_processing").inc()
        ch.basic_nack(delivery_tag=method.delivery_tag)

def on_connection_open(conn):
    global connection
    connection = conn
    connection.channel(on_open_callback=on_channel_open)
    logging.info(f"RMQ connection opened...")

def on_channel_open(ch):
    global channel
    channel = ch
    # Declare the queue explicitly with durable and non-exclusive options
    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,  # Ensure the queue survives server restarts
        exclusive=False,  # Make the queue available to all connections
        auto_delete=False  # Prevent automatic deletion of the queue
    )
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message_received)
    logging.info(f"[*] Channel opened, waiting for RMQ messages on {QUEUE_NAME}...")

async def start_rabbitmq_consumer():
    while True:
        try:
            connection_params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.adapters.asyncio_connection.AsyncioConnection(connection_params)
            connection.add_on_open_callback(on_connection_open)
            await asyncio.Future()  # Keeps the consumer running
            logging.info(f"RMQ connection awaited...")
        except Exception as e:
            logging.error(f"RabbitMQ connection error: {e}")
            await asyncio.sleep(5)  # Retry after delay


@app.on_event("startup")
async def startup():
    """Start RabbitMQ consumer when FastAPI app starts."""
    global consumer_task
    consumer_task = asyncio.create_task(start_rabbitmq_consumer())
    logging.info("RabbitMQ consumer started.")


@app.on_event("shutdown")
async def shutdown():
    """Gracefully shut down the RabbitMQ connection and consumer."""
    global connection, channel, consumer_task

    logging.info("FastAPI shutdown initiated.")

    # Cancel the consumer task
    logging.info("task cleanup")
    if consumer_task:
        logging.info("cancel consumer_task")
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            logging.info("RabbitMQ consumer task canceled.")

    # Close the RabbitMQ channel and connection
    logging.info("channel cleanup")
    if channel:
        try:
            channel.close()
            logging.info("RabbitMQ channel closed.")
        except Exception as e:
            logging.error(f"Error closing RabbitMQ channel: {e}")

    logging.info("connection cleanup")
    if connection:
        try:
            connection.close()
            logging.info("RabbitMQ connection closed.")
        except Exception as e:
            logging.error(f"Error closing RabbitMQ connection: {e}")

    logging.info("RMQ shutdown complete")


@app.get("/download/{filename}")
def download_file(filename: str, request: Request):
    """
    Allows a client to download a file, if it is available.  Downloads are logged.  If file is not available
    let the user know if it is a file we had at one point that has since been deleted.

    Args:
        filename (str): the name of the requested file

    Returns:
        FileResponse (the file, if it is available)

    Raises:
        HttpException: if the file is not available

    """
    db = SessionLocal()
    file_path = os.path.join(STATIC_DIR, filename)
    ip_address = request.client.host
    user_agent = request.headers.get("user-agent", "unknown")

    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        file_mod_time = os.path.getmtime(file_path)
        file_age_seconds = time.time() - file_mod_time
        start_time = time.time()

        response = FileResponse(file_path, filename=filename)

        transfer_time = time.time() - start_time
        throughput_mbps = (file_size / (1024 * 1024)) / transfer_time if transfer_time > 0 else 0

        db.add(DownloadLog(
            filename=filename,
            status='downloaded',
            ip_address=ip_address,
            user_agent=user_agent,
            file_size=file_size,
            transfer_time=transfer_time,
            throughput_mbps=throughput_mbps,
            file_age_seconds=file_age_seconds
        ))
        DOWNLOAD_COUNTER.labels(filename=filename).inc()
        db.commit()
        db.close()
        return response
    else:
        history = db.query(DownloadLog).filter(DownloadLog.filename == filename).all()
        db.add(DownloadLog(filename=filename, status='not found', ip_address=ip_address, user_agent=user_agent))
        db.commit()
        db.close()

        if history:
            raise HTTPException(status_code=404, detail=f"File '{filename}' not found, but was previously downloaded.")
        else:
            raise HTTPException(status_code=404, detail=f"File '{filename}' not found.")

@app.get("/metrics")
async def metrics():
    return generate_latest()

