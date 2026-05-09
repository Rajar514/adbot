FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Persistent data folder (mount volume here on Fly/Koyeb)
RUN mkdir -p /data
ENV DATA_DIR=/data

# Tiny HTTP healthcheck server runs alongside bot (so Koyeb/Fly think it's "web")
EXPOSE 8080

CMD ["python", "bot.py"]
