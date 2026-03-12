from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.src.kafka_consumer import KafkaConsumerService
from fastapi.middleware.cors import CORSMiddleware

consumer_service = KafkaConsumerService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await consumer_service.start()

    yield

    await consumer_service.stop()


app = FastAPI(lifespan=lifespan)

origins = [
    "http://localhost",
    "http://localhost:8000",
    "https://multi-tenant-saas-fastapi-logging.onrender.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "ok"}
