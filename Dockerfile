# Use the python-slim-buster as the base image
FROM python:3.10-slim-buster

ENV AWS_STS_REGIONAL_ENDPOINTS="regional"

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Create MUST directories
RUN mkdir -p /app/uploads /app/logs /app/secrets

# Install PIP
RUN python -m ensurepip --upgrade

# Install Python dependencies
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt

# Define the executable
ENTRYPOINT [ "python3" ]

# File to be run
CMD [ "main.py"]
