import asyncio
import ssl
import orjson
from aiokafka import AIOKafkaConsumer

from app.src.audit_log import audit_logs
from app.src.config import env
from app.src.kafka_ssl_files_generator import generate_kafka_connection_files

KAFKA_BOOTSTRAP = env.aiven_kafka_bootstrap
TOPIC = env.aiven_kafka_topic

generate_kafka_connection_files(env)

def create_ssl_context():
    ssl_context = ssl.create_default_context(cafile="ca.pem")
    ssl_context.load_cert_chain(
        certfile="service.cert",
        keyfile="service.key"
    )
    return ssl_context


async def consume():

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        security_protocol="SSL",
        ssl_context=create_ssl_context(),
        value_deserializer=orjson.loads,

        group_id="debug-consumer",
        auto_offset_reset="earliest",  # read old messages
    )

    await consumer.start()

    try:
        async for msg in consumer:
            # print("Received message:")
            # print(f"Topic: {msg.topic}")
            # print(f"Partition: {msg.partition}")
            # print(f"Offset: {msg.offset}")
            # print(f"Key: {msg.key}")
            # print(f"Value: {msg.value}")
            # print("------")
            
            payload = msg.value

            await audit_logs(
                action=payload.get("action"),
                resource_type=payload.get("resource_type"),
                resource_id=payload.get("resource_id"),
                status=payload.get("status", "success"),
                actor_user_id=payload.get("actor_user_id"),
                organization_id=payload.get("organization_id"),
                meta_data=payload.get("meta_data"),
                ip_address=payload.get("ip_address"),
                user_agent=payload.get("user_agent"),
                endpoint=payload.get("endpoint"),
            )

    finally:
        await consumer.stop()


asyncio.run(consume())