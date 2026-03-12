import asyncio
import ssl
import orjson

from app.src.logger import logger
from aiokafka import AIOKafkaConsumer
from app.src.config import env
from app.src.audit_log import audit_logs
from app.src.kafka_ssl_files_generator import generate_kafka_connection_files

KAFKA_BOOTSTRAP = env.aiven_kafka_bootstrap
TOPIC = env.aiven_kafka_topic

generate_kafka_connection_files(env)


def create_ssl_context():
    ssl_context = ssl.create_default_context(cafile="ca.pem")
    ssl_context.load_cert_chain(certfile="service.cert", keyfile="service.key")
    return ssl_context


class KafkaConsumerService:
    def __init__(self):
        self.task = None
        self.running = False

    async def start(self):
        self.running = True
        self.task = asyncio.create_task(self._consumer_loop())

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()

    async def _consumer_loop(self):
        """
        Keeps reconnecting to Kafka if connection drops
        """
        while self.running:
            consumer = None
            try:
                logger.info("Connecting to Kafka...")

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

                logger.info("Kafka consumer started")

                async for msg in consumer:
                    await self.process_message(msg)

            except Exception as e:
                logger.error(f"Kafka consumer crashed: {e}")

            finally:
                if consumer:
                    await consumer.stop()

            # backoff before reconnect
            logger.info("Reconnecting to Kafka in 5 seconds...")
            await asyncio.sleep(5)

    async def process_message(self, msg):
        try:
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

        except Exception as e:
            logger.error(f"Message processing failed: {e}")
