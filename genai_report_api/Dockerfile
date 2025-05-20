# Use an official Python runtime as a parent image - Python 3.11
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

VOLUME /app



# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
# Using --no-cache-dir for smaller image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application script and supporting text files into the container
COPY app.py .
COPY prompt.txt .
COPY system_instruction.txt .

# Cloud Run will set the PORT environment variable. Uvicorn will listen on this port.
# The EXPOSE directive is good practice but Cloud Run primarily uses the PORT env var.
# EXPOSE 8080 # Default PORT value Cloud Run often uses

# Command to run Uvicorn server.
# It will listen on the port specified by the PORT environment variable (defaulting to 8080 if not set).
# Using sh -c allows environment variable expansion.    
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1"]