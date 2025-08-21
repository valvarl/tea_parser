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
from app.models.task import BaseTask, TaskStatus

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

def _now_dt() -> datetime:
    return datetime.utcnow()

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
            "_id": 0,
            "collections.sku": 1,
            "aspects.aspectKey": 1,
            "aspects.aspectName": 1,
            "aspects.variants.sku": 1,
            "other_offers.sku": 1,
        },
    )
    if doc is None:
        return None
    return squash_sku(doc)

def _as_str_sku(x: Any) -> str:
    # поддерживает '123', 123, {'sku': '123', ...}
    if isinstance(x, dict) and "sku" in x:
        return str(x["sku"])
    return str(x)

def _sku_sort_key(s: str) -> tuple[int, int | str]:
    ss = s.strip()
    return (0, int(ss)) if ss.isdigit() else (1, ss)

def _normalize_skus(seq: Iterable[Any]) -> list[str]:
    # вытянули sku, убрали пустые, дедуп, отсортировали
    uniq = { _as_str_sku(v).strip() for v in (seq or []) }
    uniq.discard("")
    return sorted(uniq, key=_sku_sort_key)

def _normalize_collection_groups(value: Any) -> list[list[str]]:
    """
    Превращает вход в список групп, где каждая группа — список sku.
    Поддерживает:
      - список словарей/строк/чисел -> одна группа
      - список списков (внутри — dict/str/int) -> несколько групп
    """
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        # если внутри есть подсписки — трактуем как группы
        if any(isinstance(x, (list, tuple, set)) for x in value):
            groups: list[list[str]] = []
            for grp in value:
                groups.append(_normalize_skus(grp))
            return [g for g in groups if g]
        # иначе это "плоский" список элементов -> одна группа
        return [_normalize_skus(value)]
    # на всякий случай: одиночное значение -> одна группа
    return [_normalize_skus([value])]

async def aggregate_collections_for_task(task_id: str) -> Dict[str, List[Dict[str, Any]]]:
    out = {"collections": [], "aspects": [], "other_offers": []}

    cursor = db.enrich_batches.find({"task_id": task_id}, {"skus": 1})
    parent_skus: set[str] = set()
    async for c in cursor:
        parent_skus |= {str(s) for s in c.get("skus", [])}

    for parent in parent_skus:
        prepared = await prepare_collection(parent)
        if not prepared:
            continue
        
        for asp in prepared.get("aspects", []) or []:
            skus = _normalize_skus((asp or {}).get("variants", []))
            if skus:
                out["aspects"].append(
                    {
                        "type": "aspect",
                        "aspect_key": asp.get("aspectKey"),
                        "aspect_name": asp.get("aspectName"),
                        "skus": skus,
                        "come_from": parent,
                    }
                )

        for skus in _normalize_collection_groups(prepared.get("collections")):
            if skus:
                out["collections"].append(
                    {"type": "collection", "skus": skus, "come_from": parent}
                )

        other_skus = _normalize_skus(prepared.get("other_offers", []))
        if other_skus:
            out["other_offers"].append(
                {"type": "other_offer", "skus": other_skus, "come_from": parent}
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

async def insert_new_collections(task_id: str, grouped: Dict[str, List[Dict[str, Any]]]) -> Tuple[int, List[str]]:
    created_count = 0
    all_skus: set[str] = set()
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

    return created_count, sorted(all_skus, key=_sku_sort_key)

# ========== mongo helpers (tasks) ==========

async def patch_task(task_id: str, patch: Dict[str, Any]) -> None:
    await db.scraping_tasks.update_one(
        {"id": task_id},
        {
            "$set": patch,
            "$currentDate": {"updated_at": True},
        },
    )

async def push_status(task_id: str, to_status: TaskStatus, *, reason: Optional[str] = None) -> None:
    doc = await db.scraping_tasks.find_one({"id": task_id}, {"status": 1})
    from_status = (doc or {}).get("status")
    await db.scraping_tasks.update_one(
        {"id": task_id},
        {
            "$set": {"status": to_status},
            "$push": {
                "status_history": {
                    "from_status": from_status,
                    "to_status": to_status,
                    "at": _now_dt(),
                    "reason": reason,
                }
            },
            "$currentDate": {"updated_at": True},
        },
    )

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

async def send_to_indexer_and_wait(task_id: str, skus: List[str], *, batch_size: int = 200, timeout_sec: int = 600) -> None:
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

async def send_to_enricher_and_wait(task_id: str, skus: List[str], *, batch_size: int = 100, timeout_sec: int = 900) -> None:
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
    # pending -> queued
    await push_status(task_id, TaskStatus.queued)
    await patch_task(
        task_id,
        {
            "params.search_term": search_term,
            "params.category_id": category_id,
            "params.max_pages": max_pages,
            "attempt.current": 0,
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

            # сохраняем «как есть» во временное поле workers.indexer.raw + быстрые счетчики в stats
            norm_stats = {}
            if st.get("status") == "task_done":
                norm_stats = {
                    "scraped_products": int(st.get("scraped_products", 0)),
                    "failed_products": int(st.get("failed_products", 0)),
                    "inserted_products": int(st.get("inserted_products", 0)),
                    "updated_products": int(st.get("updated_products", 0)),
                    "total_products": int(st.get("total_products", 0)),
                }

            await patch_task(task_id, {
                "workers.indexer.status": st.get("status"),
                "workers.indexer.stats": norm_stats,
            })

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
                
                # аккумулируем агрегаты
                inc_stats = {
                    "stats.products_indexed": int(st.get("batch_data", {}).get("inserted", 0)) + int(st.get("batch_data", {}).get("updated", 0)),
                    "stats.new_products": int(st.get("batch_data", {}).get("inserted", 0)),
                    "stats.pages_indexed": int(st.get("batch_data", {}).get("pages", 0)),
                }
                await db.scraping_tasks.update_one(
                    {"id": task_id},
                    {"$inc": inc_stats, "$currentDate": {"updated_at": True}},
                )

            if st.get("status") == "deferred":
                doc = await db.scraping_tasks.find_one({"id": task_id}, {"retry_attempt": 1})
                attempt = int((doc or {}).get("retry_attempt", 0)) + 1
                next_retry_at = int(st.get("next_retry_at") or (_now_ts() + 600))
                await patch_task(
                    task_id,
                    {
                        "status": TaskStatus.deferred,
                        "attempt.current": attempt,
                        "deferred_reason": st.get("reason"),
                        "next_retry_at": next_retry_at,
                    },
                )
                await push_status(task_id, TaskStatus.deferred, reason=st.get("reason"))
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
                await patch_task(task_id, {"error_message": st.get("error")})
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

            norm_stats = {}
            if st.get("status") == "task_done":
                norm_stats = {
                    "skus_enriched": int(st.get("enriched_products", st.get("enriched", 0))),
                    "failed": int(st.get("failed_products", st.get("failed", 0))),
                }

            await patch_task(task_id, {
                "workers.enricher.status": st.get("status"),
                "workers.enricher.stats": norm_stats,
            })

            if st.get("status") == "task_done" and st.get("done") is True:
                enr_done = True
                break

            if st.get("status") == "failed":
                await patch_task(task_id, {"error_message": st.get("error")})
                enr_done = True
                break

    try:
        await asyncio.gather(watch_indexer(), watch_enricher())
    finally:
        await asyncio.gather(c_idx.stop(), c_enr.stop(), prod_enr.stop())

    final_status = TaskStatus.finished if idx_done and enr_done else TaskStatus.failed
    await patch_task(task_id, {"finished_at": _now_dt()})
    await push_status(task_id, final_status)
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
    # подтянуть pipeline_id из родителя
    parent = await db.scraping_tasks.find_one({"id": parent_task_id}, {"pipeline_id": 1})
    pipeline_id = (parent or {}).get("pipeline_id") or parent_task_id

    collections = BaseTask(
        task_type="collections",
        status=TaskStatus.queued,  # совместимость
        params={
            "source_task_id": parent_task_id,
            "max_pages": 3,
            "trigger": "auto",
        },
        parent_task_id=parent_task_id,
        pipeline_id=pipeline_id,
    ).model_dump(exclude_none=True)
    collections["_id"] = collections["id"]

    # нормализуем стартовый статус
    collections["status_history"] = [
        {"from_status": None, "to_status": TaskStatus.queued, "at": _now_dt(), "reason": "auto-followup"}
    ]
    await db.scraping_tasks.insert_one(collections)
    new_task_id = collections["id"]

    # привяжем follow-up к outputs родителя
    await db.scraping_tasks.update_one(
        {"id": parent_task_id},
        {"$push": {"outputs.next_tasks": {"task_type": "collections", "task_id": new_task_id}},
         "$currentDate": {"updated_at": True}}
    )

    try:
        # queued -> running
        await push_status(new_task_id, TaskStatus.running)

        grouped = await aggregate_collections_for_task(parent_task_id)
        created_cnt, all_skus = await insert_new_collections(new_task_id, grouped)

        await patch_task(new_task_id, {
            "workers.collector.status": "task_done",
            "workers.collector.stats": {
                "created_collections": created_cnt,
                "skus_discovered": len(all_skus),
            },
            "stats.created_collections": created_cnt,
            "stats.skus_to_process": len(all_skus),
        })

        await db.scraping_tasks.update_one(
            {"id": new_task_id},
            {
                "$set": {"stats.created_collections": created_cnt, "stats.skus_to_process": len(all_skus)},
                "$currentDate": {"updated_at": True},
            },
        )

        await send_to_indexer_and_wait(new_task_id, all_skus)
        await patch_task(new_task_id, {
            "workers.indexer.status": "task_done",
            "workers.indexer.stats.skus_sent": len(all_skus)
        })
        
        await send_to_enricher_and_wait(new_task_id, all_skus)
        await patch_task(new_task_id, {
            "workers.enricher.status": "task_done",
            "workers.enricher.stats.skus_enriched": len(all_skus)  # или фактическое число из ответа
        })

        await db.collections.update_many(
            {"task_id": new_task_id, "status": "queued"},
            {"$set": {"status": "processed"}, "$currentDate": {"updated_at": True}},
        )

        await patch_task(new_task_id, {"finished_at": _now_dt()})
        await push_status(new_task_id, TaskStatus.finished)
    except Exception as e:
        logger.exception("collections follow-up failed: %s", e)
        await patch_task(new_task_id, {"error_message": str(e)})
        await push_status(new_task_id, TaskStatus.failed)

    return new_task_id
