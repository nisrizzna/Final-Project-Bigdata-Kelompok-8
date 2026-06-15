FROM python:3.11-slim

WORKDIR /app

# Install dependencies sistem
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies (khusus Flask — bukan Spark)
COPY requirements-flask.txt .
RUN pip install --no-cache-dir -r requirements-flask.txt

# Copy kode dashboard
COPY . .

# Buat folder data
RUN mkdir -p data

EXPOSE 5000

CMD ["python", "app.py"]
