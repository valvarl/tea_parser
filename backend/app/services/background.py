from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.db.mongo import db
from app.models.task import ScrapingTask

logger = logging.getLogger(__name__)

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPIC_INDEXER_CMD = os.getenv("TOPIC_INDEXER_CMD", "indexer_cmd")
TOPIC_INDEXER_STATUS = os.getenv("TOPIC_INDEXER_STATUS", "indexer_status")
TOPIC_ENRICHER_CMD = os.getenv("TOPIC_ENRICHER_CMD", "enricher_cmd")
TOPIC_ENRICHER_STATUS = os.getenv("TOPIC_ENRICHER_STATUS", "enricher_status")

ENRICHER_BATCH_SIZE = max(1, int(os.getenv("ENRICHER_BATCH_SIZE", "50")))
STATUS_TIMEOUT_SEC = int(os.getenv("STATUS_TIMEOUT_SEC", "300"))
INDEXER_MAX_DEFERS = max(0, int(os.getenv("INDEXER_MAX_DEFERS", "3")))


# ========== time/json/hash utils ==========

def _now_ts() -> int:
    return int(time.time())

def _dumps(x: Any) -> bytes:
    return json.dumps(x, ensure_ascii=False).encode("utf-8")

def _loads(x: bytes) -> Any:
    return json.loads(x.decode("utf-8"))

def _stable_hash(s: str) -> str:
    import hashlib
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


# ========== generic helpers ==========

def chunked(seq: Iterable[Any], size: int) -> Iterable[List[Any]]:
    buf: List[Any] = []
    for s in seq:
        buf.append(s)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def batched(seq: List[int], n: int) -> Iterable[List[int]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]

def squash_sku(obj: Any, key: str = "sku") -> Any:
    if isinstance(obj, dict):
        if len(obj) == 1 and key in obj:
            return obj[key]
        return {k: squash_sku(v, key) for k, v in obj.items()}
    if isinstance(obj, list):
        return [squash_sku(x, key) for x in obj]
    if isinstance(obj, tuple):
        return tuple(squash_sku(x, key) for x in obj)
    return obj


# ========== mongo helpers (collections flow) ==========

async def prepare_collection(parent_sku: str) -> Optional[Dict[str, Any]]:
    doc = await db.candidates.find_one(
        {"sku": parent_sku},
        {
            "collections": {"sku": 1},
            "aspects": {"aspectKey": 1, "aspectName": 1, "variants": {"sku": 1}},
            "other_offers": {"sku": 1},
        },
    )
    if doc is None:
        return None
    return squash_sku(doc)

async def aggregate_collections_for_task(task_id: str) -> Dict[str, List[Dict[str, Any]]]:
    out = {"collections": [], "aspects": [], "other_offers": []}
    cursor = db.enrich_batches.find({"task_id": task_id}, {"skus": 1})
    parent_skus: set[str] = set()
    async for c in cursor:
        parent_skus |= set(c.get("skus", []))

    for parent in parent_skus:
        prepared = await prepare_collection(parent)
        if not prepared:
            continue

        for group in prepared.get("collections", []):
            skus = sorted({int(s) for s in (group if isinstance(group, list) else [])})
            if skus:
                out["collections"].append(
                    {"type": "collection", "skus": skus, "come_from": int(parent)}
                )

        for asp in prepared.get("aspects", []):
            skus = sorted({int(s) for s in (asp.get("variants", []) or [])})
            if skus:
                out["aspects"].append(
                    {
                        "type": "aspect",
                        "aspect_key": asp.get("aspectKey"),
                        "aspect_name": asp.get("aspectName"),
                        "skus": skus,
                        "come_from": int(parent),
                    }
                )

        for off in prepared.get("other_offers", []):
            skus = sorted({int(s) for s in (off if isinstance(off, list) else [off])})
            if skus:
                out["other_offers"].append(
                    {"type": "other_offer", "skus": skus, "come_from": int(parent)}
                )

    return out

def build_collection_hash(item: Dict[str, Any]) -> str:
    t = item["type"]
    if t == "collection":
        sig = f"collection:{','.join(map(str, item['skus']))}"
    elif t == "aspect":
        sig = f"aspect:{item.get('aspect_key') or ''}:{item.get('aspect_name') or ''}:{','.join(map(str, item['skus']))}"
    else:
        sig = f"other_offer:{','.join(map(str, item['skus']))}"
    return _stable_hash(sig)

def to_collection_doc(item: Dict[str, Any], task_id: str) -> Dict[str, Any]:
    return {
        "task_id": task_id,
        "status": "queued",
        "come_from": item["come_from"],
        "type": item["type"],
        "aspect_key": item.get("aspect_key"),
        "aspect_name": item.get("aspect_name"),
        "collection_hash": build_collection_hash(item),
        "skus": [{"sku": s, "status": "queued"} for s in item["skus"]],
        "created_at": _now_ts(),
        "updated_at": _now_ts(),
    }

async def insert_new_collections(task_id: str, grouped: Dict[str, List[Dict[str, Any]]]) -> Tuple[int, List[int]]:
    created_count = 0
    all_skus: set[int] = set()
    now = _now_ts()

    for bucket in ("collections", "aspects", "other_offers"):
        for item in grouped.get(bucket, []):
            doc = to_collection_doc(item, task_id)
            all_skus.update(x["sku"] for x in doc["skus"])

            # avoid conflict: do not include 'updated_at' in $setOnInsert
            on_insert = {k: v for k, v in doc.items() if k != "updated_at"}

            res = await db.collections.update_one(
                {"collection_hash": doc["collection_hash"]},
                {
                    "$setOnInsert": on_insert,
                    "$set": {"updated_at": now},
                },
                upsert=True,
            )
            if res.upserted_id is not None:
                created_count += 1

    return created_count, sorted(all_skus)

# ========== mongo helpers (tasks) ==========

async def patch_task(task_id: str, patch: Dict[str, Any]) -> None:
    patch["updated_at"] = _now_ts()
    await db.scraping_tasks.update_one({"id": task_id}, {"$set": patch})


# ========== kafka helpers ==========

async def new_producer() -> AIOKafkaProducer:
    prod = AIOKafkaProducer(bootstrap_servers=BOOT, value_serializer=_dumps, enable_idempotence=True)
    await prod.start()
    return prod

def new_consumer(topic: str, group: str, *, offset: str = "latest") -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOT,
        group_id=group,
        value_deserializer=_loads,
        auto_offset_reset=offset,
        enable_auto_commit=True,
    )


# ========== indexer/enricher roundtrips for collections follow-up ==========

async def send_to_indexer_and_wait(task_id: str, skus: List[int], *, batch_size: int = 200, timeout_sec: int = 600) -> None:
    if not skus:
        return

    prod = await new_producer()
    cons = new_consumer(TOPIC_INDEXER_STATUS, f"bg-indexer-{task_id}", offset="latest")
    await cons.start()
    try:
        batch_ids = []
        for b_id, batch in enumerate(batched(skus, batch_size), start=1):
            await prod.send_and_wait(
                TOPIC_INDEXER_CMD,
                {
                    "cmd": "add_collection_members",
                    "task_id": task_id,
                    "batch_id": b_id,
                    "skus": batch,
                    "trigger": "collections_followup",
                },
            )
            batch_ids.append(b_id)

        deadline = time.time() + timeout_sec
        done_batches: set[int] = set()
        while time.time() < deadline:
            msg = await cons.getone()
            st = msg.value or {}
            if st.get("task_id") != task_id:
                continue
            if st.get("status") == "ok" and st.get("batch_id"):
                done_batches.add(int(st["batch_id"]))
                if len(done_batches) == len(batch_ids):
                    break
            if st.get("status") == "task_done" and st.get("done") is True:
                break
        else:
            raise TimeoutError("indexer deadline exceeded")
    finally:
        await cons.stop()
        await prod.stop()

async def send_to_enricher_and_wait(task_id: str, skus: List[int], *, batch_size: int = 100, timeout_sec: int = 900) -> None:
    if not skus:
        return

    prod = await new_producer()
    cons = new_consumer(TOPIC_ENRICHER_STATUS, f"bg-enricher-{task_id}", offset="latest")
    await cons.start()
    try:
        batch_ids = []
        for b_id, batch in enumerate(batched(skus, batch_size), start=1):
            # explicit audit entry for enricher_worker
            await db.enrich_batches.insert_one(
                {
                    "task_id": task_id,
                    "batch_id": b_id,
                    "skus": batch,
                    "status": "in_progress",
                    "created_at": _now_ts(),
                    "updated_at": _now_ts(),
                    "source": "collections_followup",
                }
            )
            await prod.send_and_wait(
                TOPIC_ENRICHER_CMD,
                {
                    "cmd": "enrich_skus",
                    "task_id": task_id,
                    "batch_id": b_id,
                    "skus": batch,
                    "trigger": "collections_followup",
                },
            )
            batch_ids.append(b_id)

        deadline = time.time() + timeout_sec
        done_batches: set[int] = set()
        while time.time() < deadline:
            msg = await cons.getone()
            st = msg.value or {}
            if st.get("task_id") != task_id:
                continue
            if st.get("status") == "ok" and st.get("batch_id"):
                done_batches.add(int(st["batch_id"]))
                if len(done_batches) == len(batch_ids):
                    break
            if st.get("status") == "task_done" and st.get("done") is True:
                break
        else:
            raise TimeoutError("enricher deadline exceeded")
    finally:
        await cons.stop()
        await prod.stop()


# ========== main scraping orchestration (search flow) ==========

async def scrape_tea_products_task(
    search_term: str,
    task_id: str,
    *,
    category_id: str = "9373",
    max_pages: int = 3,
) -> None:
    await patch_task(
        task_id,
        {
            "status": "queued",
            "search_term": search_term,
            "category_id": category_id,
            "max_pages": max_pages,
            "retry_attempt": 0,
        },
    )

    prod_cmd = await new_producer()
    await prod_cmd.send_and_wait(
        TOPIC_INDEXER_CMD,
        {"task_id": task_id, "search_term": search_term, "category_id": category_id, "max_pages": max_pages},
    )
    await prod_cmd.stop()
    logger.info("indexer command sent task=%s q=%r cat=%s pages=%s", task_id, search_term, category_id, max_pages)

    c_idx = new_consumer(TOPIC_INDEXER_STATUS, f"indexer-monitor-{uuid.uuid4()}", offset="latest")
    c_enr = new_consumer(TOPIC_ENRICHER_STATUS, f"enricher-monitor-{uuid.uuid4()}", offset="latest")
    await asyncio.gather(c_idx.start(), c_enr.start())

    prod_enr = await new_producer()

    idx_done = False
    enr_done = False
    deferred_retry_task: Optional[asyncio.Task] = None

    async def watch_indexer() -> None:
        nonlocal idx_done, deferred_retry_task
        while True:
            try:
                msg = await asyncio.wait_for(c_idx.getone(), timeout=STATUS_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                logger.error("indexer status timeout > %ss", STATUS_TIMEOUT_SEC)
                break

            st: Dict[str, Any] = msg.value or {}
            if st.get("task_id") != task_id:
                continue

            await patch_task(task_id, {"indexer": st})

            if st.get("status") == "ok":
                skus = list(st.get("batch_data", {}).get("skus", []) or [])
                if skus:
                    for i, chunk in enumerate(chunked(skus, ENRICHER_BATCH_SIZE), start=1):
                        await prod_enr.send_and_wait(
                            TOPIC_ENRICHER_CMD,
                            {"cmd": "enrich_skus", "task_id": task_id, "skus": chunk},
                        )
                        logger.info(
                            "forwarded to enricher batch=%s part=%s size=%s",
                            st.get("batch_id"),
                            i,
                            len(chunk),
                        )

            if st.get("status") == "deferred":
                doc = await db.scraping_tasks.find_one({"id": task_id}, {"retry_attempt": 1})
                attempt = int((doc or {}).get("retry_attempt", 0)) + 1
                next_retry_at = int(st.get("next_retry_at") or (_now_ts() + 600))
                await patch_task(
                    task_id,
                    {
                        "status": "deferred",
                        "next_retry_at": next_retry_at,
                        "retry_attempt": attempt,
                        "deferred_reason": st.get("reason"),
                    },
                )
                logger.warning(
                    "indexer deferred task=%s attempt=%s next_retry_at=%s reason=%s",
                    task_id,
                    attempt,
                    next_retry_at,
                    st.get("reason"),
                )
                if attempt <= INDEXER_MAX_DEFERS:
                    deferred_retry_task = asyncio.create_task(
                        schedule_indexer_retry(
                            task_id=task_id,
                            search_term=search_term,
                            category_id=category_id,
                            max_pages=max_pages,
                            next_retry_at=next_retry_at,
                            attempt_no=attempt,
                        )
                    )
                else:
                    logger.error("indexer max defers exceeded task=%s", task_id)
                idx_done = True
                break

            if st.get("status") == "task_done" and st.get("done") is True:
                await prod_enr.send_and_wait(TOPIC_ENRICHER_CMD, {"cmd": "enrich_skus", "task_id": task_id})
                logger.info("finalize signal sent to enricher task=%s", task_id)
                idx_done = True
                break

            if st.get("status") == "failed":
                idx_done = True
                break

    async def watch_enricher() -> None:
        nonlocal enr_done
        while True:
            try:
                msg = await asyncio.wait_for(c_enr.getone(), timeout=STATUS_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                logger.error("enricher status timeout > %ss", STATUS_TIMEOUT_SEC)
                break

            st: Dict[str, Any] = msg.value or {}
            if st.get("task_id") != task_id:
                continue

            await patch_task(task_id, {"enricher": st})

            if st.get("status") == "task_done" and st.get("done") is True:
                enr_done = True
                break

            if st.get("status") == "failed":
                enr_done = True
                break

    try:
        await asyncio.gather(watch_indexer(), watch_enricher())
    finally:
        await asyncio.gather(c_idx.stop(), c_enr.stop(), prod_enr.stop())

    final_status = "completed" if idx_done and enr_done else "failed"
    await patch_task(task_id, {"status": final_status, "completed_at": _now_ts()})
    logger.info("scraping task %s finished %s", task_id, final_status)

    # kick off collections follow-up as a separate task
    asyncio.create_task(create_and_run_collections_task(task_id))


# ========== retry scheduling for deferred indexer ==========

async def schedule_indexer_retry(
    task_id: str,
    search_term: str,
    category_id: str,
    max_pages: int,
    next_retry_at: int,
    attempt_no: int,
) -> None:
    delay = max(0, next_retry_at - _now_ts())
    if delay > 0:
        await asyncio.sleep(delay)

    prod = await new_producer()
    try:
        await prod.send_and_wait(
            TOPIC_INDEXER_CMD,
            {
                "task_id": task_id,
                "search_term": search_term,
                "category_id": category_id,
                "max_pages": max_pages,
                "retry_attempt": attempt_no,
            },
        )
        await patch_task(task_id, {"status": "requeued", "requeued_at": _now_ts(), "retry_attempt": attempt_no})
        logger.info("requeued indexer task=%s attempt=%s", task_id, attempt_no)
    finally:
        await prod.stop()


# ========== collections follow-up orchestration ==========

async def create_and_run_collections_task(parent_task_id: str) -> str:
    collections_task = ScrapingTask(search_term=f"collections-from:{parent_task_id}").dict()
    collections_task.update(
        {
            "parent_task_id": parent_task_id,
            "task_type": "collections",
            "status": "started",
            "created_at": _now_ts(),
            "updated_at": _now_ts(),
            "trigger": "auto",
        }
    )
    await db.scraping_tasks.insert_one(collections_task)
    new_task_id = collections_task["id"]

    try:
        grouped = await aggregate_collections_for_task(parent_task_id)
        created_cnt, all_skus = await insert_new_collections(new_task_id, grouped)

        await db.scraping_tasks.update_one(
            {"id": new_task_id},
            {
                "$set": {
                    "meta": {"created_collections": created_cnt, "skus_to_process": len(all_skus)},
                    "updated_at": _now_ts(),
                }
            },
        )

        await send_to_indexer_and_wait(new_task_id, all_skus)
        await send_to_enricher_and_wait(new_task_id, all_skus)

        await db.collections.update_many(
            {"task_id": new_task_id, "status": "queued"},
            {"$set": {"status": "processed", "updated_at": _now_ts()}},
        )
        await db.scraping_tasks.update_one(
            {"id": new_task_id},
            {"$set": {"status": "finished", "finished_at": _now_ts(), "updated_at": _now_ts()}},
        )
    except Exception as e:
        logger.exception("collections follow-up failed: %s", e)
        await db.scraping_tasks.update_one(
            {"id": new_task_id},
            {"$set": {"status": "failed", "error": str(e), "updated_at": _now_ts()}},
        )

    return new_task_id
