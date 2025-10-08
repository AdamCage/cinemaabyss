
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py ./
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092     KAFKA_GROUP_ID=events-svc     TOPIC_MOVIE=cinema.events.movie     TOPIC_USER=cinema.events.user     TOPIC_PAYMENT=cinema.events.payment     LOG_LEVEL=INFO
EXPOSE 8082
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8082"]
