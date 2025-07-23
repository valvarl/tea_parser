"""
Консюмер Kafka: читает product_raw, достаёт описание (заглушка),
апдей́тит Mongo, при желании публикует дальше (product_with_desc).
"""

import asyncio, random, orjson, logging
from typing import Dict, Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from motor.motor_asyncio import AsyncIOMotorClient

KAFKA_BOOT   = "localhost:9092"
IN_TOPIC     = "product_raw"
OUT_TOPIC    = "product_with_desc"     # можно не отправлять — покажу, как
MONGO_URL    = "mongodb://localhost:27017"
DB_NAME      = "tea_db"

logger = logging.getLogger("enricher")

client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]


async def enrich(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Заглушка: ждём 0.3–0.8 с и вставляем фиктивное описание.
    В реальном коде откроете браузер/requests → парс html.
    """
    await asyncio.sleep(random.uniform(0.3, 0.8))
    data["description"] = f"Описание для SKU {data['sku_id'] or data['id']}"
    return data


async def worker() -> None:
    cons = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_BOOT,
        group_id="enricher-grp",
        enable_auto_commit=False,            # ручной commit!
        value_deserializer=orjson.loads,
        auto_offset_reset="earliest",
    )
    prod = AIOKafkaProducer(                   # если нужен fan-out
        bootstrap_servers=KAFKA_BOOT,
        value_serializer=orjson.dumps,
        enable_idempotence=True,
    )

    await cons.start(); await prod.start()
    try:
        async for msg in cons:
            try:
                enriched = await enrich(msg.value)

                # upsert в Mongo
                key = {"ozon_id": enriched.get("ozon_id")} if enriched.get("ozon_id") else {"id": enriched["id"]}
                await db.tea_products.update_one(key, {"$set": enriched})

                # перекидываем дальше по пайплайну (опционально)
                await prod.send_and_wait(OUT_TOPIC, enriched)

                # подтверждаем offset ТОЛЬКО после успешного апдейта/send
                await cons.commit()
                logger.info("✓ enriched %s", key)
            except Exception as e:
                logger.exception("process error: %s", e)
                # offset не закоммичен → сообщение придёт снова
    finally:
        await cons.stop(); await prod.stop()

if __name__ == "__main__":
    asyncio.run(worker())