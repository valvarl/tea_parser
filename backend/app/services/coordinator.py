# coordinator.py
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


# ========= time/json/hash utils =========

def _now_dt() -> datetime:
    return datetime.utcnow()

def _now_ts() -> int:
    return int(time.time())

def _dumps(x: Any) -> bytes:
    return json.dumps(x, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

def _loads(x: bytes) -> Any:
    return json.loads(x.decode("utf-8"))

def _stable_hash(s: str) -> str:
    import hashlib
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


# ========= generic helpers =========

def chunked(seq: Iterable[Any], size: int) -> Iterable[List[Any]]:
    buf: List[Any] = []
    for s in seq:
        buf.append(s)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def batched(seq: List[int | str], n: int) -> Iterable[List[int | str]]:
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


# ========= mongo helpers (collections flow) =========

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
    if isinstance(x, dict) and "sku" in x:
        return str(x["sku"])
    return str(x)

def _sku_sort_key(s: str) -> tuple[int, int | str]:
    ss = s.strip()
    return (0, int(ss)) if ss.isdigit() else (1, ss)

def _normalize_skus(seq: Iterable[Any]) -> list[str]:
    uniq = {_as_str_sku(v).strip() for v in (seq or [])}
    uniq.discard("")
    return sorted(uniq, key=_sku_sort_key)

def _normalize_collection_groups(value: Any) -> list[list[str]]:
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        if any(isinstance(x, (list, tuple, set)) for x in value):
            groups: list[list[str]] = []
            for grp in value:
                groups.append(_normalize_skus(grp))
            return [g for g in groups if g]
        return [_normalize_skus(value)]
    return [_normalize_skus([value])]

async def aggregate_collections_for_task(task_id: str) -> Dict[str, List[Dict[str, Any]]]:
    out = {"collections": [], "aspects": [], "other_offers": []}
    cursor = db.enrich_batches.find({"task_id": task_id}, {"skus": 1})
    parent_skus: set[str] = set()
    async for c in cursor:
        parent_skus |= {str(s) for s in c.get("skus", [])}

    for parent in sorted(parent_skus, key=_sku_sort_key):
        prepared = await prepare_collection(parent)
        if not prepared:
            continue

        for asp in (prepared.get("aspects") or []):
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
                out["collections"].append({"type": "collection", "skus": skus, "come_from": parent})

        other_skus = _normalize_skus(prepared.get("other_offers", []))
        if other_skus:
            out["other_offers"].append({"type": "other_offer", "skus": other_skus, "come_from": parent})

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

def build_processing_order(grouped: Dict[str, List[Dict[str, Any]]]) -> List[str]:
    ordered: List[str] = []
    seen: set[str] = set()
    for bucket in ("aspects", "collections", "other_offers"):
        for item in grouped.get(bucket, []) or []:
            for s in item.get("skus", []):
                if s not in seen:
                    seen.add(s)
                    ordered.append(s)
    return ordered

async def insert_new_collections(task_id: str, grouped: Dict[str, List[Dict[str, Any]]]) -> Tuple[int, List[str]]:
    created_count = 0
    now = _now_ts()
    for bucket in ("collections", "aspects", "other_offers"):
        for item in grouped.get(bucket, []):
            doc = to_collection_doc(item, task_id)
            on_insert = {k: v for k, v in doc.items() if k != "updated_at"}
            res = await db.collections.update_one(
                {"collection_hash": doc["collection_hash"]},
                {"$setOnInsert": on_insert, "$set": {"updated_at": now}},
                upsert=True,
            )
            if res.upserted_id is not None:
                created_count += 1
    processing_order = build_processing_order(grouped)
    return created_count, processing_order


# ========= mongo helpers (tasks/events/progress) =========

async def patch_task(task_id: str, patch: Dict[str, Any]) -> None:
    await db.scraping_tasks.update_one(
        {"id": task_id},
        {"$set": patch, "$currentDate": {"updated_at": True}},
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

def _dedup_key(worker: str, payload: Dict[str, Any]) -> str:
    # Stable key that ignores volatile fields like ts and cumulative counters drift.
    base = {
        "worker": worker,
        "task_id": payload.get("task_id"),
        "status": payload.get("status"),
        "batch_id": payload.get("batch_id"),
    }
    if payload.get("status") == "deferred":
        base["next_retry_at"] = payload.get("next_retry_at")
    if payload.get("status") == "failed":
        base["error"] = payload.get("error") or payload.get("error_message")
    return _stable_hash(json.dumps(base, sort_keys=True, separators=(",", ":")))

async def record_worker_event(task_id: str, worker: str, payload: Dict[str, Any]) -> bool:
    ev_hash = _dedup_key(worker, payload)
    res = await db.worker_events.update_one(
        {"task_id": task_id, "worker": worker, "event_hash": ev_hash},
        {
            "$setOnInsert": {
                "task_id": task_id,
                "worker": worker,
                "event_hash": ev_hash,
                "payload": payload,
                "ts": _now_ts(),
            }
        },
        upsert=True,
    )
    return res.upserted_id is not None

async def mark_forwarded_once(task_id: str, worker: str, batch_id: int | str) -> bool:
    res = await db.worker_progress.update_one(
        {"task_id": task_id, "worker": worker, "batch_id": str(batch_id)},
        {
            "$setOnInsert": {
                "task_id": task_id,
                "worker": worker,
                "batch_id": str(batch_id),
                "ts": _now_ts(),
            }
        },
        upsert=True,
    )
    return res.upserted_id is not None


# ========= kafka helpers =========

async def new_producer() -> AIOKafkaProducer:
    prod = AIOKafkaProducer(bootstrap_servers=BOOT, value_serializer=_dumps, enable_idempotence=True)
    await prod.start()
    return prod

def new_consumer(topic: str, group: str, *, offset: str = "latest", manual_commit: bool = True) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOT,
        group_id=group,
        value_deserializer=_loads,
        auto_offset_reset=offset,
        enable_auto_commit=not manual_commit,  # manual commit by default for monitors
    )

# ========= coordinator =========

class Coordinator:
    def __init__(self) -> None:
        self._producer: Optional[AIOKafkaProducer] = None
        self._tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        await self._ensure_indexes()
        self._producer = await new_producer()
        await self.resume_inflight()

    async def stop(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        self._tasks.clear()
        if self._producer:
            await self._producer.stop()
            self._producer = None

    async def run_scrape_task(
        self,
        *,
        search_term: str,
        task_id: str,
        category_id: str = "9373",
        max_pages: int = 3,
    ) -> None:
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

        idx_group = self._stable_group("idx", task_id)
        enr_group = self._stable_group("enr", task_id)
        c_idx = new_consumer(TOPIC_INDEXER_STATUS, idx_group, offset="latest", manual_commit=True)
        c_enr = new_consumer(TOPIC_ENRICHER_STATUS, enr_group, offset="latest", manual_commit=True)
        await asyncio.gather(c_idx.start(), c_enr.start())

        # send command after consumers are ready to avoid missing early statuses
        await self._send_indexer_cmd(
            {
                "task_id": task_id,
                "search_term": search_term,
                "category_id": category_id,
                "max_pages": max_pages,
            }
        )
        logger.info("indexer command sent task=%s q=%r cat=%s pages=%s", task_id, search_term, category_id, max_pages)

        watcher = asyncio.create_task(self._watch_both(task_id, search_term, category_id, max_pages, c_idx, c_enr))
        self._tasks.add(watcher)
        watcher.add_done_callback(self._tasks.discard)

    async def resume_inflight(self) -> None:
        cur = db.scraping_tasks.find(
            {"status": {"$in": [TaskStatus.running, TaskStatus.queued, TaskStatus.deferred]}},
            {"id": 1, "params.search_term": 1, "params.category_id": 1, "params.max_pages": 1, "task_type": 1,
             "workers.indexer.status": 1, "workers.enricher.status": 1, "next_retry_at": 1, "attempt.current": 1}
        )
        async for t in cur:
            task_id = t["id"]
            task_type = t.get("task_type") or "scrape"

            if task_type == "collections":
                watcher = asyncio.create_task(self._resume_collections_task(task_id))
                self._tasks.add(watcher)
                watcher.add_done_callback(self._tasks.discard)
                continue

            search_term = ((t.get("params") or {}).get("search_term")) or ""
            category_id = ((t.get("params") or {}).get("category_id")) or "9373"
            max_pages = int(((t.get("params") or {}).get("max_pages")) or 3)

            idx_group = self._stable_group("idx", task_id)
            enr_group = self._stable_group("enr", task_id)
            c_idx = new_consumer(TOPIC_INDEXER_STATUS, idx_group, offset="latest", manual_commit=True)
            c_enr = new_consumer(TOPIC_ENRICHER_STATUS, enr_group, offset="latest", manual_commit=True)
            await asyncio.gather(c_idx.start(), c_enr.start())

            if not ((t.get("workers") or {}).get("indexer") or {}) and search_term:
                await self._send_indexer_cmd(
                    {"task_id": task_id, "search_term": search_term, "category_id": category_id, "max_pages": max_pages}
                )

            if t.get("status") == TaskStatus.deferred:
                next_retry_at = int(t.get("next_retry_at") or 0)
                attempt = int(((t.get("attempt") or {}).get("current")) or 0)
                asyncio.create_task(
                    self._schedule_indexer_retry(
                        task_id=task_id,
                        search_term=search_term,
                        category_id=category_id,
                        max_pages=max_pages,
                        next_retry_at=next_retry_at,
                        attempt_no=attempt + 1,
                    )
                )

            watcher = asyncio.create_task(self._watch_both(task_id, search_term, category_id, max_pages, c_idx, c_enr))
            self._tasks.add(watcher)
            watcher.add_done_callback(self._tasks.discard)

    # ----- core watchers -----

    async def _watch_both(self, task_id: str, search_term: str, category_id: str, max_pages: int,
                          c_idx: AIOKafkaConsumer, c_enr: AIOKafkaConsumer) -> None:
        prod_enr = await new_producer()
        idx_done = False
        enr_done = False
        start_ts = _now_ts()

        async def watch_indexer() -> None:
            nonlocal idx_done
            while True:
                try:
                    msg = await asyncio.wait_for(c_idx.getone(), timeout=STATUS_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    await patch_task(task_id, {"workers.indexer.status": "stalled"})
                    logger.warning("indexer stalled: no status for >%ss", STATUS_TIMEOUT_SEC)
                    continue

                st: Dict[str, Any] = msg.value or {}
                if st.get("task_id") != task_id:
                    await c_idx.commit();  # commit unrelated anyway; group is task-specific
                    continue
                if st.get("ts") and isinstance(st["ts"], int) and st["ts"] + 5 < start_ts:
                    await c_idx.commit();  # ignore old, but still move offset
                    continue
                if not await record_worker_event(task_id, "indexer", st):
                    await c_idx.commit()
                    continue

                if st.get("status") == "ok":
                    skus = list(st.get("batch_data", {}).get("skus", []) or [])
                    batch_id = st.get("batch_id")
                    if skus and batch_id is not None:
                        if await mark_forwarded_once(task_id, "indexer->enricher", batch_id):
                            for i, chunk in enumerate(chunked(skus, ENRICHER_BATCH_SIZE), start=1):
                                await prod_enr.send_and_wait(
                                    TOPIC_ENRICHER_CMD,
                                    {
                                        "cmd": "enrich_skus",
                                        "task_id": task_id,
                                        "skus": chunk,
                                        "trigger": "indexer_forward",
                                    },
                                )
                                logger.info("forwarded to enricher batch=%s part=%s size=%s", batch_id, i, len(chunk))
                        inc_stats = {
                            "stats.products_indexed": int(st.get("batch_data", {}).get("inserted", 0))
                            + int(st.get("batch_data", {}).get("updated", 0)),
                            "stats.new_products": int(st.get("batch_data", {}).get("inserted", 0)),
                            "stats.pages_indexed": int(st.get("batch_data", {}).get("pages", 0)),
                        }
                        await db.scraping_tasks.update_one({"id": task_id}, {"$inc": inc_stats, "$currentDate": {"updated_at": True}})

                if st.get("status") == "deferred":
                    attempt = await self._bump_attempt(task_id)
                    next_retry_at = int(st.get("next_retry_at") or (_now_ts() + 600))
                    await patch_task(task_id, {
                        "status": TaskStatus.deferred,
                        "deferred_reason": st.get("reason"),
                        "next_retry_at": next_retry_at,
                        "attempt.current": attempt,
                    })
                    await push_status(task_id, TaskStatus.deferred, reason=st.get("reason"))
                    logger.warning("indexer deferred task=%s attempt=%s next_retry_at=%s reason=%s", task_id, attempt, next_retry_at, st.get("reason"))
                    if attempt <= INDEXER_MAX_DEFERS:
                        asyncio.create_task(self._schedule_indexer_retry(
                            task_id=task_id, search_term=search_term, category_id=category_id,
                            max_pages=max_pages, next_retry_at=next_retry_at, attempt_no=attempt
                        ))
                    else:
                        logger.error("indexer max defers exceeded task=%s", task_id)
                    idx_done = True
                    await c_idx.commit()
                    return

                if st.get("status") == "task_done" and st.get("done") is True:
                    await prod_enr.send_and_wait(
                        TOPIC_ENRICHER_CMD, {"cmd": "enrich_skus", "task_id": task_id, "trigger": "finalize_from_indexer"}
                    )
                    logger.info("finalize signal sent to enricher task=%s", task_id)
                    idx_done = True
                    await c_idx.commit()
                    return

                if st.get("status") == "failed":
                    err = st.get("error") or st.get("error_message")
                    await patch_task(task_id, {"error_message": err})
                    idx_done = True
                    await c_idx.commit()
                    return

                norm_stats = {}
                if st.get("status") == "task_done":
                    norm_stats = {
                        "scraped_products": int(st.get("scraped_products", 0)),
                        "failed_products": int(st.get("failed_products", 0)),
                        "inserted_products": int(st.get("inserted_products", 0)),
                        "updated_products": int(st.get("updated_products", 0)),
                        "total_products": int(st.get("total_products", 0)),
                    }
                await patch_task(task_id, {"workers.indexer.status": st.get("status"), "workers.indexer.stats": norm_stats})
                await c_idx.commit()

        async def watch_enricher() -> None:
            nonlocal enr_done
            while True:
                try:
                    msg = await asyncio.wait_for(c_enr.getone(), timeout=STATUS_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    await patch_task(task_id, {"workers.enricher.status": "stalled"})
                    logger.warning("enricher stalled: no status for >%ss", STATUS_TIMEOUT_SEC)
                    continue

                st: Dict[str, Any] = msg.value or {}
                if st.get("task_id") != task_id:
                    await c_enr.commit()
                    continue
                if st.get("ts") and isinstance(st["ts"], int) and st["ts"] + 5 < start_ts:
                    await c_enr.commit()
                    continue
                if not await record_worker_event(task_id, "enricher", st):
                    await c_enr.commit()
                    continue

                if st.get("status") == "ok":
                    scraped = int(st.get("scraped_products", 0))
                    failed = int(st.get("failed_products", 0))
                    await db.scraping_tasks.update_one(
                        {"id": task_id},
                        {"$inc": {"stats.skus_enriched": scraped, "stats.skus_failed": failed}, "$currentDate": {"updated_at": True}},
                    )

                if st.get("status") == "task_done" and st.get("done") is True:
                    enr_done = True
                    await c_enr.commit()
                    return

                if st.get("status") == "failed":
                    err = st.get("error") or st.get("error_message")
                    await patch_task(task_id, {"error_message": err})
                    enr_done = True
                    await c_enr.commit()
                    return

                norm_stats = {}
                if st.get("status") == "task_done":
                    norm_stats = {
                        "skus_enriched": int(st.get("enriched_products", st.get("enriched", 0))),
                        "failed": int(st.get("failed_products", st.get("failed", 0))),
                    }
                await patch_task(task_id, {"workers.enricher.status": st.get("status"), "workers.enricher.stats": norm_stats})
                await c_enr.commit()

        try:
            await asyncio.gather(watch_indexer(), watch_enricher())
        finally:
            await asyncio.gather(c_idx.stop(), c_enr.stop(), prod_enr.stop())

        final_status = TaskStatus.finished if idx_done and enr_done else TaskStatus.failed
        await patch_task(task_id, {"finished_at": _now_dt()})
        await push_status(task_id, final_status)
        logger.info("scraping task %s finished %s", task_id, final_status)

        if final_status == TaskStatus.finished:
            asyncio.create_task(self.create_and_run_collections_task(task_id))

    # ----- scheduling / retries -----

    async def _schedule_indexer_retry(
        self,
        *,
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
        await self._send_indexer_cmd(
            {
                "task_id": task_id,
                "search_term": search_term,
                "category_id": category_id,
                "max_pages": max_pages,
                "retry_attempt": attempt_no,
            }
        )
        await patch_task(task_id, {"status": "requeued", "requeued_at": _now_ts(), "retry_attempt": attempt_no})
        logger.info("requeued indexer task=%s attempt=%s", task_id, attempt_no)

    async def _bump_attempt(self, task_id: str) -> int:
        doc = await db.scraping_tasks.find_one({"id": task_id}, {"attempt.current": 1})
        attempt = int(((doc or {}).get("attempt") or {}).get("current") or 0) + 1
        await patch_task(task_id, {"attempt.current": attempt})
        return attempt

    # ----- commands -----

    async def _send_indexer_cmd(self, payload: Dict[str, Any]) -> None:
        if not self._producer:
            self._producer = await new_producer()
        await self._producer.send_and_wait(TOPIC_INDEXER_CMD, payload)

    async def _send_enricher_cmd(self, payload: Dict[str, Any]) -> None:
        if not self._producer:
            self._producer = await new_producer()
        await self._producer.send_and_wait(TOPIC_ENRICHER_CMD, payload)

    # ----- collections follow-up -----

    async def create_and_run_collections_task(self, parent_task_id: str) -> str:
        parent = await db.scraping_tasks.find_one({"id": parent_task_id}, {"pipeline_id": 1})
        pipeline_id = (parent or {}).get("pipeline_id") or parent_task_id

        collections = BaseTask(
            task_type="collections",
            status=TaskStatus.queued,
            params={"source_task_id": parent_task_id, "max_pages": 3, "trigger": "auto"},
            parent_task_id=parent_task_id,
            pipeline_id=pipeline_id,
        ).model_dump(exclude_none=True)
        collections["_id"] = collections["id"]
        collections["status_history"] = [
            {"from_status": None, "to_status": TaskStatus.queued, "at": _now_dt(), "reason": "auto-followup"}
        ]
        await db.scraping_tasks.insert_one(collections)
        new_task_id = collections["id"]

        await db.scraping_tasks.update_one(
            {"id": parent_task_id},
            {"$push": {"outputs.next_tasks": {"task_type": "collections", "task_id": new_task_id}},
             "$currentDate": {"updated_at": True}},
        )

        try:
            await push_status(new_task_id, TaskStatus.running)

            grouped = await aggregate_collections_for_task(parent_task_id)
            created_cnt, all_skus = await insert_new_collections(new_task_id, grouped)

            await patch_task(
                new_task_id,
                {
                    "workers.collector.status": "task_done",
                    "workers.collector.stats": {"created_collections": created_cnt, "skus_discovered": len(all_skus)},
                    "stats.created_collections": created_cnt,
                    "stats.skus_to_process": len(all_skus),
                },
            )

            await self._send_indexer_and_wait(new_task_id, all_skus)
            await patch_task(new_task_id, {"workers.indexer.status": "task_done", "workers.indexer.stats.skus_sent": len(all_skus)})

            await self._send_enricher_and_wait(new_task_id, all_skus)
            await patch_task(new_task_id, {"workers.enricher.status": "task_done", "workers.enricher.stats.skus_enriched": len(all_skus)})

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

    async def _resume_collections_task(self, task_id: str) -> None:
        doc = await db.scraping_tasks.find_one({"id": task_id}, {"workers": 1, "parent_task_id": 1})
        grouped = await aggregate_collections_for_task((doc or {}).get("parent_task_id") or task_id)
        _, all_skus = await insert_new_collections(task_id, grouped)

        if ((doc or {}).get("workers", {}).get("indexer") or {}).get("status") != "task_done":
            await self._send_indexer_and_wait(task_id, all_skus)
        if ((doc or {}).get("workers", {}).get("enricher") or {}).get("status") != "task_done":
            await self._send_enricher_and_wait(task_id, all_skus)

        await db.collections.update_many(
            {"task_id": task_id, "status": "queued"},
            {"$set": {"status": "processed"}, "$currentDate": {"updated_at": True}},
        )

    # ----- blocking waits for collections follow-up -----

    async def _send_indexer_and_wait(self, task_id: str, skus: List[str], *, batch_size: int = 200, timeout_sec: int = 600) -> None:
        if not skus:
            return
        prod = await new_producer()
        cons = new_consumer(TOPIC_INDEXER_STATUS, self._stable_group("idx-col", task_id), offset="latest")
        await cons.start()
        try:
            batch_ids = []
            for b_id, batch in enumerate(batched(skus, batch_size), start=1):
                if await mark_forwarded_once(task_id, "collections->indexer", b_id):
                    await prod.send_and_wait(
                        TOPIC_INDEXER_CMD,
                        {"cmd": "add_collection_members", "task_id": task_id, "batch_id": b_id, "skus": batch, "trigger": "collections_followup"},
                    )
                batch_ids.append(b_id)

            deadline = time.time() + timeout_sec
            done_batches: set[int] = set()
            while time.time() < deadline:
                msg = await cons.getone()
                st = msg.value or {}
                if st.get("task_id") != task_id:
                    continue
                if not await record_worker_event(task_id, "indexer", st):
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

    async def _send_enricher_and_wait(self, task_id: str, skus: List[str], *, batch_size: int = 100, timeout_sec: int = 900) -> None:
        if not skus:
            return
        prod = await new_producer()
        cons = new_consumer(TOPIC_ENRICHER_STATUS, self._stable_group("enr-col", task_id), offset="latest")
        await cons.start()
        try:
            batch_ids = []
            for b_id, batch in enumerate(batched(skus, batch_size), start=1):
                if await mark_forwarded_once(task_id, "collections->enricher", b_id):
                    await db.enrich_batches.update_one(
                        {"task_id": task_id, "batch_id": b_id},
                        {
                            "$setOnInsert": {
                                "task_id": task_id,
                                "batch_id": b_id,
                                "skus": batch,
                                "status": "in_progress",
                                "created_at": _now_ts(),
                                "updated_at": _now_ts(),
                                "source": "collections_followup",
                            }
                        },
                        upsert=True,
                    )
                    await prod.send_and_wait(
                        TOPIC_ENRICHER_CMD,
                        {"cmd": "enrich_skus", "task_id": task_id, "batch_id": b_id, "skus": batch, "trigger": "collections_followup"},
                    )
                batch_ids.append(b_id)

            deadline = time.time() + timeout_sec
            done_batches: set[int] = set()
            while time.time() < deadline:
                msg = await cons.getone()
                st = msg.value or {}
                if st.get("task_id") != task_id:
                    continue
                if not await record_worker_event(task_id, "enricher", st):
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

    # ----- utils -----

    def _stable_group(self, prefix: str, task_id: str) -> str:
        return f"{prefix}-monitor-{task_id}"

    async def _ensure_indexes(self) -> None:
        try:
            await db.worker_events.create_index(
                [("task_id", 1), ("worker", 1), ("event_hash", 1)],
                unique=True,
                name="uniq_worker_event",
            )
            await db.worker_progress.create_index(
                [("task_id", 1), ("worker", 1), ("batch_id", 1)],
                unique=True,
                name="uniq_worker_progress",
            )
            await db.enrich_batches.create_index(
                [("task_id", 1), ("batch_id", 1)],
                unique=True,
                partialFilterExpression={"batch_id": {"$exists": True}},
                name="uniq_enrich_batch_id",
            )
            await db.enrich_batches.create_index(
                [("task_id", 1), ("hash", 1)],
                unique=True,
                partialFilterExpression={"hash": {"$exists": True}},
                name="uniq_enrich_batch_hash",
            )
        except Exception as e:
            logger.warning("ensure_indexes failed: %s", e)
