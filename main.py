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

# FastAPI app setup with Swagger docs enabled
app = FastAPI(title="File Download Service", description="A FastAPI service for managing file downloads and logging", version="1.0")

# Prometheus Metrics
DOWNLOAD_COUNTER = Counter("file_downloads", "Count of file downloads", ["filename"])
ERROR_COUNTER = Counter("errors", "Count of errors", ["type"])
RABBITMQ_MESSAGE_COUNTER = Counter("rabbitmq_messages", "Count of RabbitMQ messages received")

# ---------------------------------------------------------------------------------------
# Database setup
#
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
# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

STATIC_DIR = "./static"

# RabbitMQ Configuration
RABBITMQ_URL = 'amqp://user:bitnami@localhost:5672/'
QUEUE_NAME = 'file_queue'

def on_message_received(ch, method, properties, body):
    try:
        print(f"Received message from RabbitMQ: {body.decode()}")
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

async def start_rabbitmq_consumer():
    while True:
        try:
            loop = asyncio.get_event_loop()
            connection = await pika.adapters.asyncio_connection.AsyncioConnection.create(
                pika.ConnectionParameters('localhost'),
                loop=loop
            )
            channel = await connection.channel()
            await channel.queue_declare(queue=QUEUE_NAME)
            await channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message_received)
            print("[*] Waiting for messages from RabbitMQ...")
            await asyncio.Future()  # Keeps the consumer alive
        except Exception as e:
            logging.error(f"RabbitMQ connection error: {e}")
            ERROR_COUNTER.labels(type="rabbitmq_connection").inc()
            await asyncio.sleep(5)  # Retry after a brief delay

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_rabbitmq_consumer())

@app.get("/download/{filename}")
def download_file(filename: str, request: Request):
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

