from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timedelta
import os
import time

# FastAPI app setup with Swagger docs enabled
app = FastAPI(title="File Download Service", description="A FastAPI service for managing file downloads and logging", version="1.0")

# Database setup
#DATABASE_URL = "mysql+pymysql://user:password@localhost/downloads_db"
DATABASE_URL = "sqlite:///./downloads.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define database models
class DownloadLog(Base):
    __tablename__ = "download_logs"
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), index=True)  # Specify length for VARCHAR
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)  # Added index
    status = Column(String(50))  # Specify length for VARCHAR
    ip_address = Column(String(50))  # Store IP address
    user_agent = Column(String(255))  # Store User-Agent
    file_size = Column(Integer)  # Store file size in bytes
    transfer_time = Column(Float)  # Store transfer time in seconds
    throughput_mbps = Column(Float)  # Store throughput in megabytes per second
    file_age_seconds = Column(Float)  # Store file age in seconds

class FilenameRegistry(Base):
    __tablename__ = "filenames"
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), unique=True, index=True)  # Register file names
    created_at = Column(DateTime, default=datetime.utcnow)

# Create tables
Base.metadata.create_all(bind=engine)

# Directory for static files
STATIC_DIR = "./static"

# Function to delete old records
def cleanup_old_logs():
    db = SessionLocal()
    expiry_time = datetime.utcnow() - timedelta(days=30)  # Retain logs for 30 days
    db.query(DownloadLog).filter(DownloadLog.timestamp < expiry_time).delete()
    db.commit()
    db.close()

@app.get("/download/{filename}")
def download_file(filename: str, request: Request):
    db = SessionLocal()
    file_path = os.path.join(STATIC_DIR, filename)
    ip_address = request.client.host
    user_agent = request.headers.get("user-agent", "unknown")

    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        file_mod_time = os.path.getmtime(file_path)
        file_age_seconds = time.time() - file_mod_time  # Compute file age
        start_time = time.time()
        
        response = FileResponse(file_path, filename=filename)
        
        transfer_time = time.time() - start_time
        throughput_mbps = (file_size / (1024 * 1024)) / transfer_time if transfer_time > 0 else 0
        
        # Log successful download
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
        db.commit()
        db.close()
        return response
    else:
        # Check database for past download history
        history = db.query(DownloadLog).filter(DownloadLog.filename == filename).all()
        db.add(DownloadLog(filename=filename, status='not found', ip_address=ip_address, user_agent=user_agent))
        db.commit()
        db.close()
        
        if history:
            raise HTTPException(status_code=404, detail=f"File '{filename}' not found, but was previously downloaded.")
        else:
            raise HTTPException(status_code=404, detail=f"File '{filename}' not found.")

@app.get("/status")
def file_status(filename: str = Query(..., description="Filename to check")):
    """Check if a file has been downloaded before."""
    db = SessionLocal()
    history = db.query(DownloadLog).filter(DownloadLog.filename == filename).all()
    db.close()
    
    if history:
        return {"filename": filename, "status": "Previously downloaded", "download_count": len(history)}
    else:
        return {"filename": filename, "status": "No history found"}

@app.post("/filename")
def register_filename(filename: str = Query(..., description="Filename to register")):
    """Register a new filename in the database."""
    db = SessionLocal()
    existing_entry = db.query(FilenameRegistry).filter(FilenameRegistry.filename == filename).first()
    if existing_entry:
        db.close()
        raise HTTPException(status_code=400, detail="Filename already registered")
    
    new_entry = FilenameRegistry(filename=filename)
    db.add(new_entry)
    db.commit()
    db.close()
    return {"message": "Filename registered successfully", "filename": filename}
