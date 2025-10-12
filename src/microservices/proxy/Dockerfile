
# Lightweight Python image
FROM python:3.11-slim

WORKDIR /app

# System deps (optional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# App
COPY main.py ./

# Env defaults (can be overridden by docker-compose)
ENV PORT=8000 \
    MONOLITH_URL=http://monolith:8080 \
    MOVIES_SERVICE_URL=http://movies-service:8081 \
    EVENTS_SERVICE_URL=http://events-service:8082 \
    GRADUAL_MIGRATION=true \
    MOVIES_MIGRATION_PERCENT=50 \
    UPSTREAM_TIMEOUT=10

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
