from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.core.logging import configure_logging, get_logger
from app.db.mongo import db
from app.models.task import BaseTask, TaskStatus

# ---- configuration ----
BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPIC_INDEXER_CMD = os.getenv("TOPIC_INDEXER_CMD", "indexer_cmd")
TOPIC_INDEXER_STATUS = os.getenv("TOPIC_INDEXER_STATUS", "indexer_status")
TOPIC_ENRICHER_CMD = os.getenv("TOPIC_ENRICHER_CMD", "enricher_cmd")
TOPIC_ENRICHER_STATUS = os.getenv("TOPIC_ENRICHER_STATUS", "enricher_status")

ENRICHER_BATCH_SIZE = max(1, int(os.getenv("ENRICHER_BATCH_SIZE", "50")))
STATUS_TIMEOUT_SEC = int(os.getenv("STATUS_TIMEOUT_SEC", "300"))
HEARTBEAT_DEADLINE_SEC = int(os.getenv("HEARTBEAT_DEADLINE_SEC", "900"))
HEARTBEAT_CHECK_INTERVAL = int(os.getenv("HEARTBEAT_CHECK_INTERVAL", "60"))

RETRY_BACKOFF_BASE = int(os.getenv("RETRY_BACKOFF_BASE", "60"))
RETRY_BACKOFF_MAX = int(os.getenv("RETRY_BACKOFF_MAX", "3600"))

FORWARD_THROTTLE_DELAY_MS = int(os.getenv("FORWARD_THROTTLE_DELAY_MS", "0"))
MAX_RUNNING_TASKS = int(os.getenv("MAX_RUNNING_TASKS", "0"))

configure_logging(service="coordinator", worker="coordinator")
logger = get_logger("coordinator")


# ========= utils =========

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

def _sleep_ms(ms: int) -> asyncio.Future:
    return asyncio.sleep(max(0, ms) / 1000.0)

def _build_task_result(stats_doc: Dict[str, Any], finished_at: datetime) -> Dict[str, Any]:
    stats = (stats_doc or {}).get("stats", {}) or {}
    started_at = (stats_doc or {}).get("started_at")
    duration_sec = None
    if started_at:
        try:
            duration_sec = int((finished_at - started_at).total_seconds())
        except Exception:
            pass
    return {
        "indexer": {
            "indexed": int(((stats.get("index") or {}).get("indexed")) or 0),
            "inserted": int(((stats.get("index") or {}).get("inserted")) or 0),
            "updated": int(((stats.get("index") or {}).get("updated")) or 0),
            "pages": int(((stats.get("index") or {}).get("pages")) or 0),
        },
        "enricher": {
            "processed": int(((stats.get("enrich") or {}).get("processed")) or 0),
            "inserted": int(((stats.get("enrich") or {}).get("inserted")) or 0),
            "updated": int(((stats.get("enrich") or {}).get("updated")) or 0),
            "reviews_saved": int(((stats.get("enrich") or {}).get("reviews_saved")) or 0),
            "dlq": int(((stats.get("enrich") or {}).get("dlq")) or 0),
        },
        "forwarded_to_enricher": int(((stats.get("forwarded") or {}).get("to_enricher")) or 0),
        "collections_followup_total": int(((stats.get("collections") or {}).get("skus_to_process")) or 0),
        "duration_total_sec": duration_sec,
        "started_at": started_at,
        "finished_at": finished_at,
    }

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

def batched(seq: List[str], n: int) -> Iterable[List[str]]:
    # ensure string SKUs
    seq = [str(x).strip() for x in seq if str(x).strip()]
    for i in range(0, len(seq), n):
        yield seq[i : i + n]

def squash_sku(obj: Any, key: str = "sku") -> Any:
    if isinstance(obj, dict):
        if len(obj) == 1 and key in obj:
            return str(obj[key])
        return {k: squash_sku(v, key) for k, v in obj.items()}
    if isinstance(obj, list):
        return [squash_sku(x, key) for x in obj]
    if isinstance(obj, tuple):
        return tuple(squash_sku(x, key) for x in obj)
    return obj


# ========= mongo helpers (collections flow) =========

async def prepare_collection(parent_sku: str) -> Optional[Dict[str, Any]]:
    doc = await db.candidates.find_one(
        {"sku": str(parent_sku)},
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
    """Collection document (no embedded SKUs)."""
    now = _now_dt()
    total = len(item["skus"])
    return {
        "task_id": task_id,
        "collection_hash": build_collection_hash(item),
        "type": item["type"],
        "come_from": item["come_from"],
        "aspect_key": item.get("aspect_key"),
        "aspect_name": item.get("aspect_name"),
        "status": "queued",  # becomes 'processed' only when queued_count == 0
        "counts": {"total": total, "queued": total, "processed": 0, "dlq": 0},
        "created_at": now,
        "updated_at": now,
    }

def build_processing_order(grouped: Dict[str, List[Dict[str, Any]]]) -> List[str]:
    ordered: List[str] = []
    seen: set[str] = set()
    for bucket in ("aspects", "collections", "other_offers"):
        for item in grouped.get(bucket, []) or []:
            for s in item.get("skus", []):
                s = str(s).strip()
                if s and s not in seen:
                    seen.add(s)
                    ordered.append(s)
    return ordered

async def _insert_collection_members(task_id: str, collection_hash: str, skus: List[str]) -> None:
    """Idempotent upsert of members (queued by default)."""
    now = _now_dt()
    ops = []
    for raw in skus:
        s = str(raw).strip()
        if not s:
            continue
        ops.append(
            pymongo.UpdateOne(
                {"task_id": task_id, "collection_hash": collection_hash, "sku": s},
                {"$setOnInsert": {"task_id": task_id, "collection_hash": collection_hash, "sku": s,
                                  "status": "queued", "created_at": now}, "$set": {"updated_at": now}},
                upsert=True,
            )
        )
    if ops:
        from pymongo import errors as pme
        try:
            await db.collection_members.bulk_write(ops, ordered=False)
        except pme.BulkWriteError as e:
            logger.warning("collection_members bulk upsert partial", extra={"event": "bulk_members_partial", "error": str(e)})

async def _sync_collection_counts(task_id: str, hashes: List[str]) -> None:
    """Recalculate counts from collection_members and finalize collections if queued==0."""
    if not hashes:
        return
    pipeline = [
        {"$match": {"task_id": task_id, "collection_hash": {"$in": hashes}}},
        {"$group": {
            "_id": "$collection_hash",
            "total": {"$sum": 1},
            "queued": {"$sum": {"$cond": [{"$eq": ["$status", "queued"]}, 1, 0]}},
            "processed": {"$sum": {"$cond": [{"$eq": ["$status", "processed"]}, 1, 0]}},
            "dlq": {"$sum": {"$cond": [{"$eq": ["$status", "dlq"]}, 1, 0]}},
        }},
    ]
    async for g in db.collection_members.aggregate(pipeline):
        ch = g["_id"]
        queued = int(g.get("queued", 0))
        patch = {
            "counts.total": int(g.get("total", 0)),
            "counts.queued": queued,
            "counts.processed": int(g.get("processed", 0)),
            "counts.dlq": int(g.get("dlq", 0)),
            "updated_at": _now_dt(),
        }
        if queued == 0:
            patch["status"] = "processed"
        await db.collections.update_one({"task_id": task_id, "collection_hash": ch}, {"$set": patch})

async def insert_new_collections(task_id: str, grouped: Dict[str, List[Dict[str, Any]]]) -> Tuple[int, List[str]]:
    """Create/extend collections and populate collection_members (queued)."""
    from pymongo import UpdateOne
    created_count = 0
    now = _now_dt()
    touched_hashes: set[str] = set()

    # Upsert collection documents (no embedded SKUs)
    for bucket in ("collections", "aspects", "other_offers"):
        for item in grouped.get(bucket, []):
            doc = to_collection_doc(item, task_id)
            filt = {"task_id": task_id, "collection_hash": doc["collection_hash"]}
            res = await db.collections.update_one(
                filt,
                {"$setOnInsert": doc, "$set": {"updated_at": now}},
                upsert=True,
            )
            if res.upserted_id is not None:
                created_count += 1
            touched_hashes.add(doc["collection_hash"])
            # Upsert members for this collection
            await _insert_collection_members(task_id, doc["collection_hash"], item["skus"])

    # Recalculate counts from collection_members and finalize if applicable
    await _sync_collection_counts(task_id, list(touched_hashes))
    processing_order = build_processing_order(grouped)
    return created_count, processing_order


# ========= task helpers =========

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
    base = {
        "worker": worker,
        "task_id": payload.get("task_id"),
        "status": payload.get("status"),
        "batch_id": payload.get("batch_id"),
        "trigger": payload.get("trigger"),
    }
    if payload.get("status") == "deferred":
        base["next_retry_at"] = payload.get("next_retry_at")
        base["reason_code"] = payload.get("reason_code")
    if payload.get("status") == "failed":
        base["error"] = payload.get("error") or payload.get("error_message")
        base["reason_code"] = payload.get("reason_code")
    return _stable_hash(json.dumps(base, sort_keys=True, separators=(",", ":")))

async def record_worker_event(task_id: str, worker: str, payload: Dict[str, Any]) -> str:
    ev_hash = _dedup_key(worker, payload)
    ts_int = int(payload.get("ts") or _now_ts())
    ts_dt = datetime.utcfromtimestamp(ts_int)
    await db.worker_events.update_one(
        {"task_id": task_id, "worker": worker, "event_hash": ev_hash},
        {
            "$setOnInsert": {
                "task_id": task_id,
                "worker": worker,
                "event_hash": ev_hash,
                "payload": payload,
                "metrics_applied": False,
                "ts": ts_int,      # wire-level int
                "ts_dt": ts_dt,    # ISODate for TTL
            }
        },
        upsert=True,
    )
    await db.scraping_tasks.update_one(
        {"id": task_id},
        {"$max": {"last_event_ts": ts_int}, "$currentDate": {"updated_at": True}},
    )
    return ev_hash

async def acquire_event_for_metrics(task_id: str, worker: str, ev_hash: str) -> bool:
    res = await db.worker_events.find_one_and_update(
        {"task_id": task_id, "worker": worker, "event_hash": ev_hash, "metrics_applied": {"$ne": True}},
        {"$set": {"metrics_applied": True}},
        return_document=False,
    )
    return res is not None

async def mark_forwarded_once(task_id: str, worker: str, batch_id: int | str) -> bool:
    res = await db.worker_progress.update_one(
        {"task_id": task_id, "worker": worker, "batch_id": str(batch_id)},
        {
            "$setOnInsert": {
                "task_id": task_id,
                "worker": worker,
                "batch_id": str(batch_id),
                "ts": _now_dt(),  # ISODate
            }
        },
        upsert=True,
    )
    return res.upserted_id is not None


# ========= kafka =========

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
        enable_auto_commit=not manual_commit,
    )


# ========= coordinator =========

class Coordinator:
    def __init__(self) -> None:
        self._producer: Optional[AIOKafkaProducer] = None
        self._tasks: set[asyncio.Task] = set()
        self._hb_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        await self._ensure_indexes()
        self._producer = await new_producer()
        self._hb_task = asyncio.create_task(self._heartbeat_monitor())
        self._tasks.add(self._hb_task)
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
        if MAX_RUNNING_TASKS > 0:
            running = await db.scraping_tasks.count_documents({"status": TaskStatus.running})
            if running >= MAX_RUNNING_TASKS:
                await patch_task(task_id, {"error_message": "coordinator_overloaded"})
                await push_status(task_id, TaskStatus.failed, reason="coordinator_overloaded")
                logger.error("rejecting task due to overload", extra={"event": "reject_task", "task_id": task_id})
                return

        await push_status(task_id, TaskStatus.queued)
        await patch_task(
            task_id,
            {
                "params.search_term": search_term,
                "params.category_id": category_id,
                "params.max_pages": max_pages,
                "stats": {
                    "index": {"indexed": 0, "inserted": 0, "updated": 0, "pages": 0},
                    "enrich": {"processed": 0, "inserted": 0, "updated": 0, "reviews_saved": 0, "dlq": 0},
                    "collections": {"created_collections": 0, "skus_to_process": 0},
                    "forwarded": {"to_enricher": 0, "from_collections": 0},
                },
                "started_at": _now_dt(),
            },
        )
        await push_status(task_id, TaskStatus.running)
        await patch_task(task_id, {"last_event_ts": _now_ts()})

        idx_group = self._stable_group("idx", task_id)
        enr_group = self._stable_group("enr", task_id)
        c_idx = new_consumer(TOPIC_INDEXER_STATUS, idx_group, offset="latest", manual_commit=True)
        c_enr = new_consumer(TOPIC_ENRICHER_STATUS, enr_group, offset="latest", manual_commit=True)
        await asyncio.gather(c_idx.start(), c_enr.start())

        await self._send_indexer_cmd(
            {
                "task_id": task_id,
                "search_term": search_term,
                "category_id": category_id,
                "max_pages": max_pages,
                "trigger": "search",
            }
        )
        logger.info("indexer command sent", extra={"event": "send_cmd", "task_id": task_id, "trigger": "search"})

        watcher = asyncio.create_task(self._watch_both(task_id, c_idx, c_enr))
        self._tasks.add(watcher)
        watcher.add_done_callback(self._tasks.discard)

    async def resume_inflight(self) -> None:
        cur = db.scraping_tasks.find(
            {"status": {"$in": [TaskStatus.running, TaskStatus.queued, TaskStatus.deferred]}},
            {"id": 1, "params.search_term": 1, "params.category_id": 1, "params.max_pages": 1, "task_type": 1,
             "workers.indexer.status": 1, "workers.enricher.status": 1, "next_retry_at": 1}
        )
        async for t in cur:
            task_id = t["id"]
            task_type = t.get("task_type") or "scrape"

            if (t.get("status") or None) != TaskStatus.running:
                await push_status(task_id, TaskStatus.running)
                await patch_task(task_id, {"last_event_ts": _now_ts()})

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
                    {"task_id": task_id, "search_term": search_term, "category_id": category_id, "max_pages": max_pages, "trigger": "resume"}
                )

            watcher = asyncio.create_task(self._watch_both(task_id, c_idx, c_enr))
            self._tasks.add(watcher)
            watcher.add_done_callback(self._tasks.discard)

    async def _watch_both(self, task_id: str, c_idx: AIOKafkaConsumer, c_enr: AIOKafkaConsumer) -> None:
        prod_enr = await new_producer()
        idx_done = False
        enr_done = False

        async def _update_last_event(ts: Optional[int]) -> None:
            if ts:
                await db.scraping_tasks.update_one(
                    {"id": task_id},
                    {"$max": {"last_event_ts": int(ts)}, "$currentDate": {"updated_at": True}},
                )

        async def watch_indexer() -> None:
            nonlocal idx_done
            while True:
                try:
                    msg = await asyncio.wait_for(c_idx.getone(), timeout=STATUS_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    await patch_task(task_id, {"workers.indexer.status": "stalled"})
                    logger.warning("indexer stalled", extra={"event": "stalled", "task_id": task_id})
                    continue

                st: Dict[str, Any] = msg.value or {}
                ts = int(st.get("ts") or _now_ts())
                await _update_last_event(ts)
                try:
                    if st.get("task_id") != task_id:
                        await c_idx.commit()
                        continue

                    ev_hash = await record_worker_event(task_id, "indexer", st)

                    if st.get("status") == "running":
                        await patch_task(task_id, {"workers.indexer.status": "running", "workers.indexer.started_at": _now_dt()})
                        logger.info("indexer running", extra={"event": "running", "task_id": task_id})

                    if st.get("status") == "ok":
                        bd = st.get("batch_data") or {}
                        to_forward = list(bd.get("new_skus") or bd.get("skus") or [])
                        batch_id = st.get("batch_id")

                        if to_forward and batch_id is not None:
                            if await mark_forwarded_once(task_id, "indexer->enricher", batch_id):
                                for chunk in chunked(to_forward, ENRICHER_BATCH_SIZE):
                                    await prod_enr.send_and_wait(
                                        TOPIC_ENRICHER_CMD,
                                        {
                                            "cmd": "enrich_skus",
                                            "task_id": task_id,
                                            "skus": [str(x).strip() for x in chunk],
                                            "trigger": "indexer_forward",
                                        },
                                    )
                                    await db.scraping_tasks.update_one(
                                        {"id": task_id},
                                        {"$inc": {"stats.forwarded.to_enricher": len(chunk)}, "$currentDate": {"updated_at": True}},
                                    )
                                    if FORWARD_THROTTLE_DELAY_MS > 0:
                                        await _sleep_ms(FORWARD_THROTTLE_DELAY_MS)

                        if await acquire_event_for_metrics(task_id, "indexer", ev_hash):
                            ins = int(bd.get("inserted", 0))
                            upd = int(bd.get("updated", bd.get("matched", 0)))
                            pgs = int(bd.get("pages", 0))
                            await db.scraping_tasks.update_one(
                                {"id": task_id},
                                {
                                    "$inc": {
                                        "stats.index.indexed": ins + upd,
                                        "stats.index.inserted": ins,
                                        "stats.index.updated": upd,
                                        "stats.index.pages": pgs,
                                    },
                                    "$currentDate": {"updated_at": True},
                                },
                            )

                    if st.get("status") == "retrying":
                        await patch_task(task_id, {"workers.indexer.status": "retrying"})
                        logger.warning(
                            "indexer retrying",
                            extra={"event": "retrying", "task_id": task_id, "attempt": st.get("attempt"), "next_retry_in_sec": st.get("next_retry_in_sec")},
                        )

                    if st.get("status") == "task_done" and st.get("done") is True:
                        await prod_enr.send_and_wait(
                            TOPIC_ENRICHER_CMD, {"cmd": "enrich_skus", "task_id": task_id, "trigger": "finalize_from_indexer"}
                        )
                        await patch_task(task_id, {"workers.indexer.finished_at": _now_dt()})
                        logger.info("indexer done", extra={"event": "task_done", "task_id": task_id})
                        idx_done = True
                        await c_idx.commit()
                        return

                    if st.get("status") == "failed":
                        err = st.get("error") or st.get("error_message")
                        await patch_task(
                            task_id,
                            {
                                "error_message": err,
                                "workers.indexer.last_error": err,
                                "workers.indexer.last_error_at": _now_dt(),
                                "workers.indexer.finished_at": _now_dt(),
                            },
                        )
                        logger.error("indexer failed", extra={"event": "failed", "task_id": task_id, "reason_code": st.get("reason_code")})
                        idx_done = True
                        await c_idx.commit()
                        return

                    await patch_task(task_id, {"workers.indexer.status": st.get("status")})
                finally:
                    await c_idx.commit()

        async def watch_enricher() -> None:
            nonlocal enr_done
            while True:
                try:
                    msg = await asyncio.wait_for(c_enr.getone(), timeout=STATUS_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    await patch_task(task_id, {"workers.enricher.status": "stalled"})
                    logger.warning("enricher stalled", extra={"event": "stalled", "task_id": task_id})
                    continue

                st: Dict[str, Any] = msg.value or {}
                ts = int(st.get("ts") or _now_ts())
                await _update_last_event(ts)
                try:
                    if st.get("task_id") != task_id:
                        await c_enr.commit()
                        continue

                    ev_hash = await record_worker_event(task_id, "enricher", st)

                    if st.get("status") == "running":
                        await patch_task(task_id, {"workers.enricher.status": "running", "workers.enricher.started_at": _now_dt()})
                        logger.info("enricher running", extra={"event": "running", "task_id": task_id})

                    if st.get("status") == "ok":
                        if await acquire_event_for_metrics(task_id, "enricher", ev_hash):
                            bd = st.get("batch_data") or {}
                            inc = {
                                "stats.enrich.processed": int(bd.get("processed", 0)),
                                "stats.enrich.inserted": int(bd.get("inserted", 0)),
                                "stats.enrich.updated": int(bd.get("updated", 0)),
                            }
                            if "reviews_saved" in bd:
                                inc["stats.enrich.reviews_saved"] = int(bd.get("reviews_saved", 0))
                            if "dlq" in bd:
                                inc["stats.enrich.dlq"] = int(bd.get("dlq", 0))
                            await db.scraping_tasks.update_one({"id": task_id}, {"$inc": inc, "$currentDate": {"updated_at": True}})

                        # Per-batch collection members update
                        bd = st.get("batch_data") or {}
                        processed_skus = [str(s).strip() for s in (bd.get("skus") or []) if str(s).strip()]
                        dlq_skus: List[str] = []
                        b_id = st.get("batch_id")
                        if b_id is not None:
                            dlq_docs = await db.enrich_dlq.find(
                                {"task_id": task_id, "batch_id": int(b_id)},
                                {"sku": 1, "_id": 0}
                            ).to_list(None)
                            dlq_skus = [str(d.get("sku")).strip() for d in dlq_docs if d.get("sku")]

                        await self._update_collections_per_skus(
                            task_id=task_id,
                            processed_skus=processed_skus,
                            dlq_skus=dlq_skus,
                        )

                    if st.get("status") == "task_done" and st.get("done") is True:
                        await patch_task(task_id, {"workers.enricher.finished_at": _now_dt()})
                        logger.info("enricher done", extra={"event": "task_done", "task_id": task_id})
                        enr_done = True
                        await c_enr.commit()
                        return

                    if st.get("status") == "failed":
                        err = st.get("error") or st.get("error_message")
                        await patch_task(
                            task_id,
                            {
                                "error_message": err,
                                "workers.enricher.last_error": err,
                                "workers.enricher.last_error_at": _now_dt(),
                                "workers.enricher.finished_at": _now_dt(),
                            },
                        )
                        logger.error("enricher failed", extra={"event": "failed", "task_id": task_id, "reason_code": st.get("reason_code")})
                        enr_done = True
                        await c_enr.commit()
                        return

                    await patch_task(task_id, {"workers.enricher.status": st.get("status")})
                finally:
                    await c_enr.commit()

        try:
            await asyncio.gather(watch_indexer(), watch_enricher())
        finally:
            await asyncio.gather(c_idx.stop(), c_enr.stop(), prod_enr.stop())

        final_status = TaskStatus.finished if idx_done and enr_done else TaskStatus.failed
        finished_at = _now_dt()
        await patch_task(task_id, {"finished_at": finished_at})
        await push_status(task_id, final_status)

        stats_doc = await db.scraping_tasks.find_one({"id": task_id}, {"stats": 1, "started_at": 1})
        result = _build_task_result(stats_doc, finished_at)
        await db.scraping_tasks.update_one({"id": task_id}, {"$set": {"result": result}, "$currentDate": {"updated_at": True}})

        logger.info("task finalized", extra={"event": "final", "task_id": task_id, "status": final_status})

        if final_status == TaskStatus.finished:
            asyncio.create_task(self.create_and_run_collections_task(task_id))

    # ----- heartbeat -----

    async def _heartbeat_monitor(self) -> None:
        try:
            while True:
                now_ts = _now_ts()
                deadline = now_ts - HEARTBEAT_DEADLINE_SEC
                cur = db.scraping_tasks.find(
                    {"status": TaskStatus.running, "last_event_ts": {"$lt": deadline}},
                    {"id": 1, "last_event_ts": 1},
                )
                async for t in cur:
                    task_id = t["id"]
                    await patch_task(
                        task_id,
                        {
                            "error_message": "inactivity_timeout",
                            "workers.indexer.status": "stalled",
                            "workers.enricher.status": "stalled",
                            "workers.indexer.last_error": "inactivity_timeout",
                            "workers.enricher.last_error": "inactivity_timeout",
                            "workers.indexer.last_error_at": _now_dt(),
                            "workers.enricher.last_error_at": _now_dt(),
                        },
                    )
                    await push_status(task_id, TaskStatus.failed, reason="inactivity_timeout")
                    await db.scraping_tasks.update_one({"id": task_id}, {"$set": {"finished_at": _now_dt()}, "$currentDate": {"updated_at": True}})
                    logger.error("heartbeat timeout", extra={"event": "heartbeat_timeout", "task_id": task_id})
                await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL)
        except asyncio.CancelledError:
            return

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
            await db.scraping_tasks.update_one({"id": new_task_id},
                {"$set": {
                    "last_event_ts": _now_ts(),
                    "stats": {
                        "index": {"indexed": 0, "inserted": 0, "updated": 0, "pages": 0},
                        "enrich": {"processed": 0, "inserted": 0, "updated": 0, "reviews_saved": 0, "dlq": 0},
                        "collections": {"created_collections": 0, "skus_to_process": 0},
                        "forwarded": {"to_enricher": 0, "from_collections": 0},
                    },
                    "started_at": _now_dt(),
                }, "$currentDate": {"updated_at": True}})

            grouped = await aggregate_collections_for_task(parent_task_id)
            created_cnt, all_skus = await insert_new_collections(new_task_id, grouped)

            await patch_task(
                new_task_id,
                {
                    "workers.collector.status": "task_done",
                    "stats.collections.created_collections": created_cnt,
                    "stats.collections.skus_to_process": len(all_skus),
                },
            )

            if all_skus:
                await db.scraping_tasks.update_one(
                    {"id": new_task_id},
                    {"$inc": {"stats.forwarded.from_collections": len(all_skus)}, "$currentDate": {"updated_at": True}},
                )

            # 1) Indexer upserts collection SKUs and returns 'new_skus' per batch.
            new_skus = await self._send_indexer_and_wait(new_task_id, all_skus)
            await patch_task(new_task_id, {"workers.indexer.status": "task_done"})

            # 2) Enrich ONLY new_skus to avoid re-enrichment.
            await self._send_enricher_and_wait(new_task_id, new_skus)
            await patch_task(new_task_id, {"workers.enricher.status": "task_done"})

            finished_at = _now_dt()
            await patch_task(new_task_id, {"finished_at": finished_at})
            await push_status(new_task_id, TaskStatus.finished)

            stats_doc = await db.scraping_tasks.find_one({"id": new_task_id}, {"stats": 1, "started_at": 1})
            result = _build_task_result(stats_doc, finished_at)
            await db.scraping_tasks.update_one({"id": new_task_id}, {"$set": {"result": result}, "$currentDate": {"updated_at": True}})

        except Exception as e:
            logger.exception("collections follow-up failed", extra={"event": "failed", "task_id": new_task_id})
            await patch_task(new_task_id, {"error_message": str(e)})
            await push_status(new_task_id, TaskStatus.failed)

        return new_task_id

    async def _resume_collections_task(self, task_id: str) -> None:
        doc = await db.scraping_tasks.find_one({"id": task_id}, {"status": 1, "workers": 1, "parent_task_id": 1, "started_at": 1, "stats": 1})
        if (doc or {}).get("status") != TaskStatus.running:
            await push_status(task_id, TaskStatus.running)
            await patch_task(task_id, {"last_event_ts": _now_ts()})

        grouped = await aggregate_collections_for_task((doc or {}).get("parent_task_id") or task_id)
        _, all_skus = await insert_new_collections(task_id, grouped)

        if all_skus:
            await db.scraping_tasks.update_one(
                {"id": task_id},
                {"$inc": {"stats.forwarded.from_collections": len(all_skus)}, "$currentDate": {"updated_at": True}},
            )

        try:
            if ((doc or {}).get("workers", {}).get("indexer") or {}).get("status") != "task_done":
                new_skus = await self._send_indexer_and_wait(task_id, all_skus)
            else:
                new_skus = []

            if ((doc or {}).get("workers", {}).get("enricher") or {}).get("status") != "task_done":
                await self._send_enricher_and_wait(task_id, new_skus)
        except TimeoutError as e:
            await patch_task(task_id, {
                "status": TaskStatus.deferred,
                "deferred_reason": "enricher_wait_timeout",
                "next_retry_at": _now_ts() + 300,
                "error_message": str(e),
            })
            await push_status(task_id, TaskStatus.deferred, reason="enricher_wait_timeout")
            asyncio.create_task(self._resume_collections_task(task_id))
            return

        await patch_task(task_id, {
            "workers.indexer.status": "task_done",
            "workers.enricher.status": "task_done",
            "workers.indexer.finished_at": _now_dt(),
            "workers.enricher.finished_at": _now_dt(),
        })
        finished_at = _now_dt()
        await patch_task(task_id, {"finished_at": finished_at})
        await push_status(task_id, TaskStatus.finished)

        stats_doc = await db.scraping_tasks.find_one({"id": task_id}, {"stats": 1, "started_at": 1})
        result = _build_task_result(stats_doc, finished_at)
        await db.scraping_tasks.update_one({"id": task_id}, {"$set": {"result": result}, "$currentDate": {"updated_at": True}})

    # ----- blocking waits for collections follow-up -----

    async def _wait_for_batches(
        self,
        *,
        task_id: str,
        status_consumer: AIOKafkaConsumer,
        worker: str,
        timeout_sec: int,
        expected_batches: Optional[int] = None,
        collect_new_index_skus: bool = False,
        update_collections_per_batch: bool = True,
    ) -> List[str]:
        """
        Waits for batches from a given worker. When 'collect_new_index_skus' is True and worker='indexer',
        aggregates 'new_skus' across batches and returns a unique, normalized list.
        """
        last_progress = time.time()
        done_batches: set[int] = set()
        new_skus_acc: List[str] = []

        while True:
            remaining = timeout_sec - (time.time() - last_progress)
            if remaining <= 0:
                raise TimeoutError(f"{worker} wait inactivity timeout; done={len(done_batches)}")
            msg = await asyncio.wait_for(status_consumer.getone(), timeout=remaining)
            st = msg.value or {}
            try:
                if st.get("task_id") != task_id:
                    continue
                ev_hash = await record_worker_event(task_id, worker, st)
                ts = int(st.get("ts") or _now_ts())
                await db.scraping_tasks.update_one(
                    {"id": task_id},
                    {"$max": {"last_event_ts": ts}, "$currentDate": {"updated_at": True}},
                )

                if st.get("status") == "ok":
                    bd = st.get("batch_data") or {}
                    if await acquire_event_for_metrics(task_id, worker, ev_hash):
                        if worker == "indexer":
                            ins = int(bd.get("inserted", 0))
                            upd = int(bd.get("updated", bd.get("matched", 0)))
                            pgs = int(bd.get("pages", 0))
                            await db.scraping_tasks.update_one(
                                {"id": task_id},
                                {"$inc": {
                                    "stats.index.indexed": ins + upd,
                                    "stats.index.inserted": ins,
                                    "stats.index.updated": upd,
                                    "stats.index.pages": pgs,
                                }, "$currentDate": {"updated_at": True}},
                            )
                            if collect_new_index_skus:
                                ns = _normalize_skus(bd.get("new_skus") or [])
                                if ns:
                                    new_skus_acc.extend(ns)
                        elif worker == "enricher":
                            inc = {
                                "stats.enrich.processed": int(bd.get("processed", 0)),
                                "stats.enrich.inserted": int(bd.get("inserted", 0)),
                                "stats.enrich.updated": int(bd.get("updated", 0)),
                            }
                            if "reviews_saved" in bd:
                                inc["stats.enrich.reviews_saved"] = int(bd.get("reviews_saved", 0))
                            if "dlq" in bd:
                                inc["stats.enrich.dlq"] = int(bd.get("dlq", 0))
                            await db.scraping_tasks.update_one({"id": task_id}, {"$inc": inc, "$currentDate": {"updated_at": True}})

                    if worker == "enricher" and update_collections_per_batch:
                        processed_skus = [str(s).strip() for s in (bd.get("skus") or []) if str(s).strip()]
                        dlq_skus: List[str] = []
                        b_id = st.get("batch_id")
                        if b_id is not None:
                            dlq_docs = await db.enrich_dlq.find(
                                {"task_id": task_id, "batch_id": int(b_id)},
                                {"sku": 1, "_id": 0}
                            ).to_list(None)
                            dlq_skus = [str(d.get("sku")).strip() for d in dlq_docs if d.get("sku")]

                        await self._update_collections_per_skus(
                            task_id=task_id,
                            processed_skus=processed_skus,
                            dlq_skus=dlq_skus,
                        )

                if st.get("status") == "ok" and st.get("batch_id"):
                    done_batches.add(int(st["batch_id"]))
                    last_progress = time.time()
                    if expected_batches is not None and len(done_batches) >= expected_batches:
                        break

                if st.get("status") == "task_done" and st.get("done") is True:
                    break
            finally:
                await status_consumer.commit()

        if collect_new_index_skus:
            return _normalize_skus(new_skus_acc)
        return []

    async def _send_indexer_and_wait(self, task_id: str, skus: List[str], *, batch_size: int = 200, timeout_sec: int = 600) -> List[str]:
        if not skus:
            return []
        cons = new_consumer(TOPIC_INDEXER_STATUS, self._stable_group("idx-col", task_id), offset="latest", manual_commit=True)
        await cons.start()
        prod = await new_producer()
        try:
            batch_ids = []
            for b_id, batch in enumerate(batched(skus, batch_size), start=1):
                await prod.send_and_wait(
                    TOPIC_INDEXER_CMD,
                    {"cmd": "add_collection_members", "task_id": task_id, "batch_id": b_id, "skus": batch, "trigger": "collections_followup"},
                )
                batch_ids.append(b_id)
            new_skus = await self._wait_for_batches(
                task_id=task_id,
                status_consumer=cons,
                worker="indexer",
                timeout_sec=timeout_sec,
                expected_batches=len(batch_ids),
                collect_new_index_skus=True,
            )
            return new_skus
        finally:
            await cons.stop()
            await prod.stop()

    async def _send_enricher_and_wait(self, task_id: str, skus: List[str], *, batch_size: int = 100, timeout_sec: int = 900) -> None:
        if not skus:
            return
        cons = new_consumer(TOPIC_ENRICHER_STATUS, self._stable_group("enr-col", task_id), offset="latest", manual_commit=True)
        await cons.start()
        prod = await new_producer()
        try:
            batch_ids = []
            for b_id, batch in enumerate(batched(skus, batch_size), start=1):
                await db.enrich_batches.update_one(
                    {"task_id": task_id, "batch_id": b_id},
                    {
                        "$setOnInsert": {
                            "task_id": task_id,
                            "batch_id": b_id,
                            "skus": batch,
                            "status": "in_progress",
                            "created_at": _now_dt(),
                            "updated_at": _now_dt(),
                            "source": "collections_followup",
                        }
                    },
                    upsert=True,
                )
                await prod.send_and_wait(
                    TOPIC_ENRICHER_CMD,
                    {"cmd": "enrich_skus", "task_id": task_id, "batch_id": b_id, "skus": batch, "trigger": "collections_followup"},
                )
                await db.scraping_tasks.update_one(
                    {"id": task_id},
                    {"$inc": {"stats.forwarded.to_enricher": len(batch)}, "$currentDate": {"updated_at": True}},
                )
                batch_ids.append(b_id)

            await self._wait_for_batches(
                task_id=task_id,
                status_consumer=cons,
                worker="enricher",
                timeout_sec=timeout_sec,
                expected_batches=len(batch_ids),
            )
        finally:
            await cons.stop()
            await prod.stop()

    # ----- utils -----

    async def _update_collections_per_skus(self, *, task_id: str, processed_skus: List[str], dlq_skus: List[str]) -> None:
        """Mark members by SKU and refresh collections' counters; finalize when queued==0."""
        processed_skus = [s for s in {str(x).strip() for x in processed_skus} if s]
        dlq_skus = [s for s in {str(x).strip() for x in dlq_skus} if s]

        now = _now_dt()
        if processed_skus:
            await db.collection_members.update_many(
                {"task_id": task_id, "sku": {"$in": processed_skus}, "status": "queued"},
                {"$set": {"status": "processed", "updated_at": now}},
            )
        if dlq_skus:
            await db.collection_members.update_many(
                {"task_id": task_id, "sku": {"$in": dlq_skus}, "status": "queued"},
                {"$set": {"status": "dlq", "updated_at": now}},
            )

        affected = set()
        if processed_skus:
            ch1 = await db.collection_members.distinct(
                "collection_hash", {"task_id": task_id, "sku": {"$in": processed_skus}}
            )
            affected.update(ch1)
        if dlq_skus:
            ch2 = await db.collection_members.distinct(
                "collection_hash", {"task_id": task_id, "sku": {"$in": dlq_skus}}
            )
            affected.update(ch2)

        if affected:
            await _sync_collection_counts(task_id, [str(h) for h in affected])

    def _stable_group(self, prefix: str, task_id: str) -> str:
        return f"{prefix}-monitor-{task_id}"

    async def _ensure_indexes(self) -> None:
        try:
            await db.worker_events.create_index(
                [("task_id", 1), ("worker", 1), ("event_hash", 1)],
                unique=True,
                name="uniq_worker_event",
            )
            await db.worker_events.create_index([("ts_dt", 1)], expireAfterSeconds=14 * 24 * 3600, name="ttl_worker_events")

            await db.worker_progress.create_index(
                [("task_id", 1), ("worker", 1), ("batch_id", 1)],
                unique=True,
                name="uniq_worker_progress",
            )
            await db.worker_progress.create_index([("ts", 1)], expireAfterSeconds=60 * 24 * 3600, name="ttl_worker_progress")

            # collections: no embedded skus; unique per task
            await db.collections.create_index(
                [("task_id", 1), ("collection_hash", 1)],
                unique=True,
                name="uniq_collection_per_task",
            )
            await db.collections.create_index([("status", 1), ("updated_at", 1)], name="ix_collection_status_updated")

            # collection_members: unique per (task, collection, sku); also lookup helpers
            await db.collection_members.create_index(
                [("task_id", 1), ("collection_hash", 1), ("sku", 1)],
                unique=True,
                name="uniq_member",
            )
            await db.collection_members.create_index([("task_id", 1), ("sku", 1)], name="ix_member_task_sku")
            await db.collection_members.create_index([("task_id", 1), ("collection_hash", 1), ("status", 1)], name="ix_member_task_hash_status")
            await db.collection_members.create_index([("updated_at", 1)], name="ix_member_updated_at")

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
            await db.enrich_dlq.create_index([("task_id", 1), ("sku", 1)], name="ix_dlq_task_sku")
            await db.enrich_dlq.create_index([("updated_at", 1)], expireAfterSeconds=90 * 24 * 3600, name="ttl_enrich_dlq")
            await db.scraping_tasks.create_index([("status", 1), ("updated_at", 1)], name="ix_task_status_updated")
        except Exception as e:
            logger.warning("ensure_indexes failed", extra={"event": "ensure_indexes_failed", "error": str(e)})
