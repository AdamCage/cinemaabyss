
import asyncio, json, logging, os
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

APP_NAME = "events-service-min"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_MOVIE = os.getenv("TOPIC_MOVIE", "cinema.events.movie")
TOPIC_USER = os.getenv("TOPIC_USER", "cinema.events.user")
TOPIC_PAYMENT = os.getenv("TOPIC_PAYMENT", "cinema.events.payment")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "events-svc")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(APP_NAME)

app = FastAPI(title=APP_NAME)
producer: Optional[AIOKafkaProducer] = None
consumer_task: Optional[asyncio.Task] = None
shutdown_event = asyncio.Event()

class MovieEvent(BaseModel):
    movie_id: int | str
    title: str
    action: str
    user_id: int | str

class UserEvent(BaseModel):
    user_id: int | str
    username: str
    action: str
    timestamp: str

class PaymentEvent(BaseModel):
    payment_id: int | str
    user_id: int | str
    amount: float
    status: str
    timestamp: str
    method_type: str

async def get_producer() -> AIOKafkaProducer:
    global producer
    if producer is None:
        p = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, acks="all")
        try:
            await p.start()
            producer = p
            log.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP_SERVERS)
        except Exception as e:
            log.error("Kafka producer connect failed: %s", e)
            raise HTTPException(status_code=503, detail="Kafka is not ready")
    return producer

async def consumer_loop():
    while not shutdown_event.is_set():
        consumer = AIOKafkaConsumer(
            TOPIC_MOVIE, TOPIC_USER, TOPIC_PAYMENT,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
            key_deserializer=lambda v: v.decode("utf-8") if v else None,
        )
        try:
            await consumer.start()
            log.info("Kafka consumer connected (%s)", KAFKA_BOOTSTRAP_SERVERS)
            while not shutdown_event.is_set():
                msg = await consumer.getone()
                log.info("[CONSUMED] topic=%s key=%s value=%s", msg.topic, msg.key, msg.value)
        except Exception as e:
            log.warning("Consumer disconnected, retry in 2s: %s", e)
            await asyncio.sleep(2.0)
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass

@app.on_event("startup")
async def on_startup():
    global consumer_task
    consumer_task = asyncio.create_task(consumer_loop())
    log.info("Service started")

@app.on_event("shutdown")
async def on_shutdown():
    shutdown_event.set()
    if consumer_task:
        try:
            await asyncio.wait_for(consumer_task, timeout=5.0)
        except Exception:
            pass
    if producer:
        try:
            await producer.stop()
        except Exception:
            pass
    log.info("Service stopped")

@app.get("/api/events/health")
async def health():
    return {"status": True, "service": APP_NAME}

@app.post("/api/events/movie", status_code=201)
async def movie(evt: MovieEvent):
    p = await get_producer()
    meta = await p.send_and_wait(TOPIC_MOVIE, json.dumps({"type":"movie", **evt.model_dump()}).encode(), key=str(evt.movie_id).encode())
    return {"status": "success", "topic": meta.topic, "offset": meta.offset}

@app.post("/api/events/user", status_code=201)
async def user(evt: UserEvent):
    p = await get_producer()
    meta = await p.send_and_wait(TOPIC_USER, json.dumps({"type":"user", **evt.model_dump()}).encode(), key=str(evt.user_id).encode())
    return {"status": "success", "topic": meta.topic, "offset": meta.offset}

@app.post("/api/events/payment", status_code=201)
async def payment(evt: PaymentEvent):
    p = await get_producer()
    meta = await p.send_and_wait(TOPIC_PAYMENT, json.dumps({"type":"payment", **evt.model_dump()}).encode(), key=str(evt.payment_id).encode())
    return {"status": "success", "topic": meta.topic, "offset": meta.offset}
