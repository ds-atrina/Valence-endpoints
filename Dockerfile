# Use a slim Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install ffmpeg for audio processing (used by pydub)
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements file and pre-download dependencies into wheelhouse
COPY requirements.txt .
RUN pip download --dest=/wheelhouse -r requirements.txt

# Install dependencies from wheelhouse (offline install)
RUN pip install --no-cache-dir --find-links=/wheelhouse -r requirements.txt

# Copy the actual application code
COPY . .

# Expose FastAPI default port
EXPOSE 8001

# Run the app with Uvicorn; replace `main` with your actual filename (without .py)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
