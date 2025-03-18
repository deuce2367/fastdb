# Use the official Python base image
FROM python:3.12-slim

ARG user=fastapi

COPY .bash_profile /etc/skel/

RUN echo 'source ~/.bash_profile' >> /etc/skel/.bashrc && useradd -m ${user} && mkdir -p /app /opt/venv && chown ${user} /app /opt/venv 

RUN apt-get update && apt-get install -y sqlite-utils python3-mysqldb python3-pymysql vim procps apt-utils

USER ${user}

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN python3 -m venv $VIRTUAL_ENV --system-site-packages

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file to the working directory
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy the application code to the working directory
COPY . .

# Expose the port on which the application will run
EXPOSE 8080

# Run the FastAPI application using uvicorn server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]
