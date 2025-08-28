# coordinator_dag.py  (v2.0 stream-aware with edges_ex + first-batch start + adapters)
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Literal
from enum import Enum

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# ─────────────────────────── App DB (motor) ───────────────────────────
# Replace this with your actual motor db:
# from app.db.mongo import db
class _Fake:
    def __getattr__(self, k):  # type: ignore
        raise RuntimeError("Replace _Fake db with your motor db. from app.db.mongo import db")
db = _Fake()  # REPLACE in your project

# ─────────────────────────── Config ───────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
WORKER_TYPES = [s.strip() for s in os.getenv("WORKER_TYPES", "indexer,enricher,grouper,analyzer").split(",") if s.strip()]

TOPIC_CMD_FMT = os.getenv("TOPIC_CMD_FMT", "cmd.{type}.v1")
TOPIC_STATUS_FMT = os.getenv("TOPIC_STATUS_FMT", "status.{type}.v1")
TOPIC_WORKER_ANNOUNCE = os.getenv("TOPIC_WORKER_ANNOUNCE", "workers.announce.v1")
TOPIC_QUERY = os.getenv("TOPIC_QUERY", "query.tasks.v1")
TOPIC_REPLY = os.getenv("TOPIC_REPLY", "reply.tasks.v1")

HEARTBEAT_SOFT_SEC = int(os.getenv("HB_SOFT_SEC", "300"))
HEARTBEAT_HARD_SEC = int(os.getenv("HB_HARD_SEC", "3600"))
LEASE_TTL_SEC = int(os.getenv("LEASE_TTL_SEC", "45"))
DISCOVERY_WINDOW_SEC = int(os.getenv("DISCOVERY_WINDOW_SEC", "8"))
CANCEL_GRACE_SEC = int(os.getenv("CANCEL_GRACE_SEC", "30"))
SCHEDULER_TICK_SEC = float(os.getenv("SCHEDULER_TICK_SEC", "1.0"))
FINALIZER_TICK_SEC = float(os.getenv("FINALIZER_TICK_SEC", "5.0"))
HB_MONITOR_TICK_SEC = float(os.getenv("HB_MONITOR_TICK_SEC", "10.0"))

MAX_GLOBAL_RUNNING = int(os.getenv("MAX_GLOBAL_RUNNING", "0"))  # 0 = unlimited
MAX_TYPE_CONCURRENCY = int(os.getenv("MAX_TYPE_CONCURRENCY", "0"))

# Outbox:
OUTBOX_DISPATCH_TICK_SEC = float(os.getenv("OUTBOX_DISPATCH_TICK_SEC", "0.25"))
OUTBOX_MAX_RETRY = int(os.getenv("OUTBOX_MAX_RETRY", "12"))
OUTBOX_BACKOFF_MIN_MS = int(os.getenv("OUTBOX_BACKOFF_MIN_MS", "250"))
OUTBOX_BACKOFF_MAX_MS = int(os.getenv("OUTBOX_BACKOFF_MAX_MS", "60000"))

# Streaming:
STREAM_CHILD_LAUNCH_DEDUP_SEC = int(os.getenv("STREAM_CHILD_LAUNCH_DEDUP_SEC", "60"))
ARTIFACT_TTL_DAYS = int(os.getenv("ARTIFACT_TTL_DAYS", "14"))

# ─────────────────────────── Helpers ───────────────────────────
def now_dt() -> datetime:
    return datetime.now(timezone.utc)

def now_ts() -> int:
    return int(time.time())

def stable_hash(payload: Any) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(data.encode("utf-8")).hexdigest()

def dumps(x: Any) -> bytes:
    return json.dumps(x, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

def loads(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))

def _jitter_ms(base_ms: int) -> int:
    # simple ±20% jitter
    import random
    delta = int(base_ms * 0.2)
    return max(0, base_ms + random.randint(-delta, +delta))

# ─────────────────────────── Protocol models ───────────────────────────
class MsgType(str, Enum):
    cmd = "cmd"
    event = "event"
    heartbeat = "heartbeat"
    query = "query"
    reply = "reply"

class Role(str, Enum):
    coordinator = "coordinator"
    worker = "worker"

class RunState(str, Enum):
    queued = "queued"
    running = "running"
    deferred = "deferred"
    cancelling = "cancelling"
    finished = "finished"
    failed = "failed"

class EventKind(str, Enum):
    WORKER_ONLINE = "WORKER_ONLINE"
    WORKER_OFFLINE = "WORKER_OFFLINE"
    TASK_ACCEPTED = "TASK_ACCEPTED"
    TASK_HEARTBEAT = "TASK_HEARTBEAT"
    BATCH_CLAIMED = "BATCH_CLAIMED"
    BATCH_OK = "BATCH_OK"
    BATCH_FAILED = "BATCH_FAILED"
    TASK_DONE = "TASK_DONE"
    TASK_FAILED = "TASK_FAILED"
    CANCELLED = "CANCELLED"

class CommandKind(str, Enum):
    TASK_START = "TASK_START"
    TASK_CANCEL = "TASK_CANCEL"
    TASK_PAUSE = "TASK_PAUSE"
    TASK_RESUME = "TASK_RESUME"

class QueryKind(str, Enum):
    TASK_DISCOVER = "TASK_DISCOVER"

class ReplyKind(str, Enum):
    TASK_SNAPSHOT = "TASK_SNAPSHOT"

class Envelope(BaseModel):
    v: int = 1
    msg_type: MsgType
    role: Role
    corr_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    dedup_id: str
    task_id: str
    node_id: str
    step_type: str
    attempt_epoch: int
    ts: int = Field(default_factory=now_ts)
    payload: Dict[str, Any] = Field(default_factory=dict)
    target_worker_id: Optional[str] = None

class CmdTaskStart(BaseModel):
    cmd: Literal[CommandKind.TASK_START]
    input_ref: Optional[Dict[str, Any]] = None
    input_inline: Optional[Dict[str, Any]] = None
    batching: Optional[Dict[str, Any]] = None
    cancel_token: str

class CmdTaskCancel(BaseModel):
    cmd: Literal[CommandKind.TASK_CANCEL]
    reason: str
    cancel_token: str

class EvTaskAccepted(BaseModel):
    kind: Literal[EventKind.TASK_ACCEPTED]
    worker_id: str
    lease_id: str
    lease_deadline_ts: int

class EvHeartbeat(BaseModel):
    kind: Literal[EventKind.TASK_HEARTBEAT]
    worker_id: str
    lease_id: str
    lease_deadline_ts: int

class EvBatchOk(BaseModel):
    kind: Literal[EventKind.BATCH_OK]
    worker_id: str
    batch_id: Optional[int] = None
    metrics: Dict[str, Any] = Field(default_factory=dict)
    artifacts_ref: Optional[Dict[str, Any]] = None

class EvBatchFailed(BaseModel):
    kind: Literal[EventKind.BATCH_FAILED]
    worker_id: str
    batch_id: Optional[int] = None
    reason_code: str
    permanent: bool = False
    error: Optional[str] = None
    artifacts_ref: Optional[Dict[str, Any]] = None

class EvTaskDone(BaseModel):
    kind: Literal[EventKind.TASK_DONE]
    worker_id: str
    metrics: Dict[str, Any] = Field(default_factory=dict)
    artifacts_ref: Optional[Dict[str, Any]] = None

class EvTaskFailed(BaseModel):
    kind: Literal[EventKind.TASK_FAILED]
    worker_id: str
    reason_code: str
    permanent: bool = False
    error: Optional[str] = None

class EvCancelled(BaseModel):
    kind: Literal[EventKind.CANCELLED]
    worker_id: str
    reason: str

class QTaskDiscover(BaseModel):
    query: Literal[QueryKind.TASK_DISCOVER]
    want_epoch: int

class RTaskSnapshot(BaseModel):
    reply: Literal[ReplyKind.TASK_SNAPSHOT]
    worker_id: Optional[str] = None
    run_state: Literal["running", "idle", "finishing", "cancelling"]
    attempt_epoch: int
    lease: Optional[Dict[str, Any]] = None
    progress: Optional[Dict[str, Any]] = None
    artifacts: Optional[Dict[str, Any]] = None

# ─────────────────────────── DAG storage models ─────────────────────────
class RetryPolicy(BaseModel):
    max: int = 2
    backoff_sec: int = 300
    permanent_on: List[str] = Field(default_factory=lambda: ["bad_input", "schema_mismatch"])

class DagNode(BaseModel):
    node_id: str
    type: str
    status: RunState = RunState.queued
    attempt_epoch: int = 0
    lease: Dict[str, Any] = Field(default_factory=dict)
    depends_on: List[str] = Field(default_factory=list)
    fan_in: str = "all"  # "all" | "any" | "count:n"
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    routing: Dict[str, List[str]] = Field(default_factory=lambda: {"on_success": [], "on_failure": []})
    io: Dict[str, Any] = Field(default_factory=dict)
    stats: Dict[str, Any] = Field(default_factory=dict)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    last_event_ts: int = 0
    next_retry_at: Optional[int] = None
    streaming: Dict[str, Any] = Field(default_factory=dict)  # e.g. {"started_on_first_batch": True}

class TaskDoc(BaseModel):
    id: str
    pipeline_id: str
    status: RunState = RunState.queued
    params: Dict[str, Any] = Field(default_factory=dict)
    graph: Dict[str, Any] = Field(default_factory=lambda: {"nodes": [], "edges": [], "edges_ex": []})
    result: Optional[Dict[str, Any]] = None
    status_history: List[Dict[str, Any]] = Field(default_factory=list)
    coordinator: Dict[str, Any] = Field(default_factory=lambda: {"liveness": {"state": "ok"}})
    next_retry_at: Optional[int] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    last_event_ts: int = 0

# ─────────────────────────── Adapter Registry (coordinator_fn) ─────────
class AdapterError(Exception):
    pass

class CoordinatorAdapters:
    """
    Универсальные функции, которые координатор может вызвать сам (в coordinator_fn нодах)
    для склейки/агрегации/резки. Здесь нет IO к S3 — работаем через коллекцию artifacts.
    """
    @staticmethod
    async def merge_generic(task_id: str, from_nodes: List[str], target: Dict[str, Any]) -> Dict[str, Any]:
        """
        Собирает все complete/partial артефакты из from_nodes, помечает target как complete.
        Идемпотентно: ключ target {'depot','key'} перезаписывается (upsert).
        """
        try:
            shards: List[Dict[str, Any]] = []
            cur = db.artifacts.find({"task_id": task_id, "node_id": {"$in": from_nodes}})
            async for a in cur:
                shards.append({"node_id": a.get("node_id"), "shard_id": a.get("shard_id"), "status": a.get("status"), "meta": a.get("meta", {})})
            # Запишем агрегированный артефакт
            await db.artifacts.update_one(
                {"task_id": task_id, "node_id": target.get("node_id", "coordinator"), "key": target.get("key")},
                {"$set": {"status": "complete", "meta": {"merged_from": from_nodes, "shards": len(shards)}, "updated_at": now_dt()},
                 "$setOnInsert": {"task_id": task_id, "node_id": target.get("node_id", "coordinator"), "attempt_epoch": 0}},
                upsert=True
            )
            return {"ok": True, "count": len(shards)}
        except Exception as e:
            raise AdapterError(f"merge_generic failed: {e}") from e

    @staticmethod
    async def noop(_: str, **__) -> Dict[str, Any]:
        return {"ok": True}

ADAPTERS: Dict[str, Any] = {
    "merge.generic": CoordinatorAdapters.merge_generic,
    "noop": CoordinatorAdapters.noop,
}

# ─────────────────────────── Kafka bus ─────────────────────────────────
class KafkaBus:
    def __init__(self, bootstrap: str) -> None:
        self.bootstrap = bootstrap
        self._producer: Optional[AIOKafkaProducer] = None
        self._consumers: List[AIOKafkaConsumer] = []
        self._replies: Dict[str, List[Envelope]] = {}
        self._reply_events: Dict[str, asyncio.Event] = {}

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap, value_serializer=dumps, enable_idempotence=True)
        await self._producer.start()

    async def stop(self) -> None:
        for c in self._consumers:
            try:
                await c.stop()
            except Exception:
                pass
        self._consumers.clear()
        if self._producer:
            await self._producer.stop()
        self._producer = None

    def _topic_cmd(self, step_type: str) -> str:
        return TOPIC_CMD_FMT.format(type=step_type)

    def _topic_status(self, step_type: str) -> str:
        return TOPIC_STATUS_FMT.format(type=step_type)

    async def new_consumer(self, topics: List[str], group_id: str, *, manual_commit: bool = True) -> AIOKafkaConsumer:
        c = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap,
            group_id=group_id,
            value_deserializer=loads,
            enable_auto_commit=not manual_commit,
            auto_offset_reset="latest",
        )
        await c.start()
        self._consumers.append(c)
        return c

    # Raw send used by OutboxDispatcher only
    async def _raw_send(self, topic: str, key: bytes, env: Envelope) -> None:
        assert self._producer is not None
        await self._producer.send_and_wait(topic, env.model_dump(mode="json"), key=key)

    # Replies correlator
    def register_reply(self, corr_id: str) -> asyncio.Event:
        ev = asyncio.Event()
        self._replies[corr_id] = []
        self._reply_events[corr_id] = ev
        return ev

    def push_reply(self, corr_id: str, env: Envelope) -> None:
        bucket = self._replies.get(corr_id)
        if bucket is not None:
            bucket.append(env)

    def collect_replies(self, corr_id: str) -> List[Envelope]:
        envs = self._replies.pop(corr_id, [])
        self._reply_events.pop(corr_id, None)
        return envs

# ─────────────────────────── Outbox dispatcher ─────────────────────────
class OutboxDispatcher:
    """
    Exactly-once для нашей логики: At-least-once outbox + idempotent Kafka producer + consumer-side dedup_id.
    """
    def __init__(self, bus: KafkaBus) -> None:
        self.bus = bus
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try: await self._task
            except Exception: pass

    async def _loop(self) -> None:
        try:
            while self._running:
                now = now_ts()
                cur = db.outbox.find(
                    {"state": {"$in": ["pending", "retry"]}, "next_attempt_at": {"$lte": now}},
                    {"_id": 1, "topic": 1, "key": 1, "envelope": 1, "attempts": 1}
                ).sort([("next_attempt_at", 1)]).limit(200)
                any_sent = False
                async for ob in cur:
                    try:
                        env = Envelope.model_validate(ob["envelope"])
                        await self.bus._raw_send(ob["topic"], ob["key"].encode("utf-8"), env)
                        await db.outbox.update_one({"_id": ob["_id"]},
                            {"$set": {"state": "sent", "sent_at": now_dt(), "updated_at": now_dt()}})
                        any_sent = True
                    except Exception as e:
                        attempts = int(ob.get("attempts", 0)) + 1
                        if attempts >= OUTBOX_MAX_RETRY:
                            await db.outbox.update_one({"_id": ob["_id"]},
                                {"$set": {"state": "failed", "last_error": str(e), "updated_at": now_dt()}})
                        else:
                            backoff_ms = min(OUTBOX_BACKOFF_MAX_MS, max(OUTBOX_BACKOFF_MIN_MS, (2 ** attempts) * 100))
                            backoff_ms = _jitter_ms(backoff_ms)
                            await db.outbox.update_one({"_id": ob["_id"]},
                                {"$set": {"state": "retry",
                                          "attempts": attempts,
                                          "last_error": str(e),
                                          "next_attempt_at": now + (backoff_ms // 1000),
                                          "updated_at": now_dt()}})
                await asyncio.sleep(0 if any_sent else OUTBOX_DISPATCH_TICK_SEC)
        except asyncio.CancelledError:
            return

    async def enqueue(self, *, topic: str, key: str, env: Envelope) -> None:
        fp = stable_hash({"topic": topic, "key": key, "dedup_id": env.dedup_id})
        doc = {
            "fp": fp,
            "topic": topic,
            "key": key,
            "envelope": env.model_dump(mode="json"),
            "state": "pending",
            "attempts": 0,
            "next_attempt_at": now_ts(),
            "created_at": now_dt(),
            "updated_at": now_dt(),
        }
        try:
            await db.outbox.insert_one(doc)
        except Exception:
            # duplicate fp: ok
            pass

# ─────────────────────────── Coordinator core ──────────────────────────
class Coordinator:
    def __init__(self) -> None:
        self.bus = KafkaBus(KAFKA_BOOTSTRAP)
        self.outbox = OutboxDispatcher(self.bus)
        self._tasks: set[asyncio.Task] = set()
        self._running = False

        self._announce_consumer: Optional[AIOKafkaConsumer] = None
        self._status_consumers: Dict[str, AIOKafkaConsumer] = {}
        self._query_reply_consumer: Optional[AIOKafkaConsumer] = None

    # ── Lifecycle ───────────────────────────────────────────────────────
    async def start(self) -> None:
        await self._ensure_indexes()
        await self.bus.start()
        await self._start_consumers()
        await self.outbox.start()
        self._running = True
        self._spawn(self._scheduler_loop())
        self._spawn(self._heartbeat_monitor())
        self._spawn(self._finalizer_loop())
        self._spawn(self._resume_inflight())

    async def stop(self) -> None:
        self._running = False
        for t in list(self._tasks):
            t.cancel()
        self._tasks.clear()
        await self.outbox.stop()
        await self.bus.stop()

    def _spawn(self, coro):  # type: ignore
        t = asyncio.create_task(coro)
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    # ── Consumers ───────────────────────────────────────────────────────
    async def _start_consumers(self) -> None:
        # worker announce
        self._announce_consumer = await self.bus.new_consumer([TOPIC_WORKER_ANNOUNCE], group_id="coord.announce", manual_commit=True)
        self._spawn(self._run_announce_consumer(self._announce_consumer))

        # worker status per type
        for t in WORKER_TYPES:
            topic = self.bus._topic_status(t)
            c = await self.bus.new_consumer([topic], group_id=f"coord.status.{t}", manual_commit=True)
            self._status_consumers[t] = c
            self._spawn(self._run_status_consumer(t, c))

        # replies (for TASK_DISCOVER snapshots)
        self._query_reply_consumer = await self.bus.new_consumer([TOPIC_REPLY], group_id="coord.reply", manual_commit=True)
        self._spawn(self._run_reply_consumer(self._query_reply_consumer))

    async def _run_reply_consumer(self, c: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await c.getone()
                env = Envelope.model_validate(msg.value)
                if env.msg_type == MsgType.reply and env.role == Role.worker:
                    self.bus.push_reply(env.corr_id, env)
                await c.commit()
        except asyncio.CancelledError:
            return

    async def _run_announce_consumer(self, c: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await c.getone()
                env = Envelope.model_validate(msg.value)
                payload = env.payload
                kind = payload.get("kind")
                try:
                    if kind == EventKind.WORKER_ONLINE:
                        await db.worker_registry.update_one(
                            {"worker_id": payload["worker_id"]},
                            {"$set": {
                                "worker_id": payload["worker_id"],
                                "type": payload.get("type"),
                                "capabilities": payload.get("capabilities"),
                                "version": payload.get("version"),
                                "status": "online",
                                "last_seen": now_dt(),
                                "capacity": payload.get("capacity", {}),
                            }},
                            upsert=True
                        )
                    elif kind == EventKind.WORKER_OFFLINE:
                        await db.worker_registry.update_one(
                            {"worker_id": payload["worker_id"]},
                            {"$set": {"status": "offline", "last_seen": now_dt()}}
                        )
                    else:
                        await db.worker_registry.update_one(
                            {"worker_id": payload.get("worker_id")},
                            {"$set": {"last_seen": now_dt()}},
                            upsert=True
                        )
                finally:
                    await c.commit()
        except asyncio.CancelledError:
            return

    async def _run_status_consumer(self, step_type: str, c: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await c.getone()
                try:
                    env = Envelope.model_validate(msg.value)
                except Exception:
                    # некорректное сообщение — пропускаем
                    await c.commit()
                    continue

                # Dedup worker event by event_hash
                try:
                    await self._record_worker_event(env)
                except Exception:
                    # не блокируем поток статусов
                    pass

                # Fencing: ignore stale epoch
                try:
                    tdoc = await db.tasks.find_one({"id": env.task_id}, {"graph": 1, "status": 1})
                    if not tdoc:
                        await c.commit()
                        continue
                    node = self._get_node(tdoc, env.node_id)
                    if not node or env.attempt_epoch != int(node.get("attempt_epoch", 0)):
                        await c.commit()
                        continue
                except Exception:
                    await c.commit()
                    continue

                # apply last_event_ts
                try:
                    await db.tasks.update_one({"id": env.task_id},
                        {"$max": {"last_event_ts": env.ts}, "$currentDate": {"updated_at": True}})
                except Exception:
                    pass

                kind = env.payload.get("kind")
                try:
                    if kind == EventKind.TASK_ACCEPTED:
                        await self._on_task_accepted(env)
                    elif kind == EventKind.TASK_HEARTBEAT:
                        await self._on_task_heartbeat(env)
                    elif kind == EventKind.BATCH_OK:
                        await self._on_batch_ok(env)
                    elif kind == EventKind.BATCH_FAILED:
                        await self._on_batch_failed(env)
                    elif kind == EventKind.TASK_DONE:
                        await self._on_task_done(env)
                    elif kind == EventKind.TASK_FAILED:
                        await self._on_task_failed(env)
                    elif kind == EventKind.CANCELLED:
                        await self._on_cancelled(env)
                except Exception as e:
                    # Любая ошибка в обработчике — не падаем, продолжаем поток сообщений
                    raise e
                finally:
                    try:
                        await c.commit()
                    except Exception:
                        pass
        except asyncio.CancelledError:
            return

    # ── DAG utils ───────────────────────────────────────────────────────
    def _get_node(self, task_doc: Dict[str, Any], node_id: str) -> Optional[Dict[str, Any]]:
        for n in (task_doc.get("graph", {}).get("nodes") or []):
            if n.get("node_id") == node_id:
                return n
        return None

    def _children_of(self, task_doc: Dict[str, Any], node_id: str) -> List[str]:
        g = task_doc.get("graph", {})
        edges = g.get("edges") or []
        out = [dst for (src, dst) in edges if src == node_id]
        # include edges_ex if present
        for ex in (g.get("edges_ex") or []):
            if ex.get("from") == node_id:
                out.append(ex.get("to"))
        # dedup
        return list(dict.fromkeys(out))

    def _edges_ex_from(self, task_doc: Dict[str, Any], node_id: str) -> List[Dict[str, Any]]:
        return [ex for ex in (task_doc.get("graph", {}).get("edges_ex") or []) if ex.get("from") == node_id]

    # ── Task API ────────────────────────────────────────────────────────
    async def create_task(self, *, params: Dict[str, Any], graph: Dict[str, Any]) -> str:
        task_id = str(uuid.uuid4())
        # normalize graph keys
        graph.setdefault("nodes", [])
        graph.setdefault("edges", [])
        graph.setdefault("edges_ex", [])
        doc = TaskDoc(
            id=task_id,
            pipeline_id=task_id,
            status=RunState.queued,
            params=params,
            graph=graph,
            status_history=[{"from": None, "to": RunState.queued, "at": now_dt()}],
            started_at=now_dt(),
            last_event_ts=now_ts(),
        ).model_dump(mode="json")
        await db.tasks.insert_one(doc)
        return task_id

    # ── Scheduler / fan-out & fan-in (count:n) + start_when:first_batch ─
    async def _scheduler_loop(self) -> None:
        try:
            while True:
                await self._schedule_ready_nodes()
                await asyncio.sleep(SCHEDULER_TICK_SEC)
        except asyncio.CancelledError:
            return

    def _fan_in_satisfied(self, task_doc: Dict[str, Any], node: Dict[str, Any]) -> bool:
        deps = node.get("depends_on") or []
        if not deps:
            return True
        st: Dict[str, RunState] = {}
        for d in deps:
            nd = self._get_node(task_doc, d)
            rs = self._to_runstate((nd or {}).get("status"))
            st[d] = rs or RunState.queued  # по умолчанию queued
        fan_in = (node.get("fan_in") or "all").lower()
        if fan_in == "all":
            return all(s == RunState.finished for s in st.values())
        if fan_in == "any":
            return any(s == RunState.finished for s in st.values())
        if fan_in.startswith("count:"):
            try:
                k = int(fan_in.split(":", 1)[1])
            except Exception:
                k = len(deps)
            return sum(1 for s in st.values() if s == RunState.finished) >= k
        return False

    async def _artifacts_exist_from_parents(self, task_id: str, parents: List[str]) -> bool:
        if not parents:
            return False
        try:
            doc = await db.artifacts.find_one({"task_id": task_id, "node_id": {"$in": parents}})
            return bool(doc)
        except Exception:
            return False

    def _node_ready_time(self, node: Dict[str, Any]) -> bool:
        nx = node.get("next_retry_at")
        return not (nx and now_ts() < int(nx))

    async def _node_ready(self, task_doc, node) -> bool:
        status = self._to_runstate(node.get("status"))
        if status not in (RunState.queued, RunState.deferred):
            return False

        fan_in = (node.get("fan_in") or "all").lower()
        io = (node.get("io") or {})
        start_when = (io.get("start_when") or "").lower()

        # ⬇️ FIX: start_when=first_batch должен работать независимо от fan_in
        # (как только появился partial/complete от ЛЮБОГО предка из from_nodes/depends_on)
        if start_when == "first_batch":
            return await self._first_batch_available(task_doc, node)

        # Обычная логика ожидания завершения родителей
        deps = node.get("depends_on") or []
        dep_states = [
            self._to_runstate((self._get_node(task_doc, d) or {}).get("status"))
            for d in deps
        ]

        if fan_in == "all":
            return all(s == RunState.finished for s in dep_states)
        if fan_in == "any":
            return any(s == RunState.finished for s in dep_states)
        if fan_in.startswith("count:"):
            try:
                k = int(fan_in.split(":", 1)[1])
            except Exception:
                k = len(deps)
            return sum(1 for s in dep_states if s == RunState.finished) >= k

        return False

    def _to_runstate(self, v):
        if isinstance(v, RunState): return v
        if isinstance(v, str):
            try: return RunState(v)
            except Exception: return None
        return None
    
    async def _first_batch_available(self, task_doc, node) -> bool:
        io = (node.get("io") or {})
        inp = (io.get("input_inline") or {})
        args = (inp.get("input_args") or {})
        from_nodes = args.get("from_nodes") or (node.get("depends_on") or [])
        if not from_nodes:
            return False
        # ищем любой артефакт от любого из предков с partial/complete
        q = {
            "task_id": task_doc["id"],
            "node_id": {"$in": from_nodes},
            "status": {"$in": ["partial", "complete"]},
        }
        return (await db.artifacts.find_one(q)) is not None

    async def _schedule_ready_nodes(self) -> None:
        cur = db.tasks.find(
            {"status": {"$in": [RunState.running, RunState.queued, RunState.deferred]}},
            {"id": 1, "graph": 1, "status": 1}
        )
        async for t in cur:
            if self._to_runstate(t.get("status")) == RunState.queued:
                await db.tasks.update_one({"id": t["id"]}, {"$set": {"status": RunState.running}})
            for n in (t.get("graph", {}).get("nodes") or []):
                if n.get("type") == "coordinator_fn":
                    if await self._node_ready(t, n):
                        await self._run_coordinator_fn(t, n)
                    continue
                if await self._node_ready(t, n):
                    await self._preflight_and_maybe_start(t, n)

    async def _run_coordinator_fn(self, task_doc: Dict[str, Any], node: Dict[str, Any]) -> None:
        # safe run
        try:
            await db.tasks.update_one({"id": task_doc["id"], "graph.nodes.node_id": node["node_id"]},
                                      {"$set": {"graph.nodes.$.status": RunState.running,
                                                "graph.nodes.$.started_at": now_dt(),
                                                "graph.nodes.$.attempt_epoch": int(node.get("attempt_epoch", 0)) + 1}})
            io = node.get("io", {}) or {}
            fn_name = io.get("fn", "noop")
            fn_args = io.get("fn_args", {}) or {}
            fn = ADAPTERS.get(fn_name)
            if not fn:
                raise AdapterError(f"adapter '{fn_name}' not registered")
            # Подготовим target: подставим node_id для явного владельца результата
            if isinstance(fn_args, dict) and "target" in fn_args and isinstance(fn_args["target"], dict):
                fn_args["target"].setdefault("node_id", node["node_id"])
            await fn(task_doc["id"], **fn_args)
            await db.tasks.update_one({"id": task_doc["id"], "graph.nodes.node_id": node["node_id"]},
                                      {"$set": {"graph.nodes.$.status": RunState.finished,
                                                "graph.nodes.$.finished_at": now_dt(),
                                                "graph.nodes.$.last_event_ts": now_ts()}})
        except Exception as e:
            # Координаторные функции не «ломают» всю задачу: переводим в deferred с backoff
            backoff = int(((node.get("retry_policy") or {}).get("backoff_sec") or 300))
            await db.tasks.update_one({"id": task_doc["id"], "graph.nodes.node_id": node["node_id"]},
                                      {"$set": {"graph.nodes.$.status": RunState.deferred,
                                                "graph.nodes.$.next_retry_at": now_ts() + backoff,
                                                "graph.nodes.$.last_error": str(e)}})

    # ── Preflight + Outbox enqueue ──────────────────────────────────────
    async def _enqueue_cmd(self, env: Envelope) -> None:
        topic = self.bus._topic_cmd(env.step_type)
        key = f"{env.task_id}:{env.node_id}"
        await self.outbox.enqueue(topic=topic, key=key, env=env)

    async def _enqueue_query(self, env: Envelope) -> None:
        key = env.task_id
        await self.outbox.enqueue(topic=TOPIC_QUERY, key=key, env=env)

    async def _preflight_and_maybe_start(self, task_doc: Dict[str, Any], node: Dict[str, Any]) -> None:
        task_id = task_doc["id"]
        node_id = node["node_id"]
        new_epoch = int(node.get("attempt_epoch", 0)) + 1

        # Discover active/complete to avoid duplicate starts
        discover_env = Envelope(
            msg_type=MsgType.query,
            role=Role.coordinator,
            dedup_id=stable_hash({"query": str(QueryKind.TASK_DISCOVER), "task_id": task_id, "node_id": node_id, "epoch": new_epoch}),
            task_id=task_id,
            node_id=node_id,
            step_type=node["type"],
            attempt_epoch=new_epoch,
            payload=QTaskDiscover(query=QueryKind.TASK_DISCOVER, want_epoch=new_epoch).model_dump()
        )
        _ = self.bus.register_reply(discover_env.corr_id)
        await self._enqueue_query(discover_env)

        try:
            await asyncio.sleep(DISCOVERY_WINDOW_SEC)
        except asyncio.TimeoutError:
            pass
        replies = self.bus.collect_replies(discover_env.corr_id)

        # If active with older epoch or finishing → set running
        active = [r for r in replies if (r.payload.get("run_state") in ("running", "finishing")
                                         and r.payload.get("attempt_epoch") == node.get("attempt_epoch"))]
        if active:
            await db.tasks.update_one({"id": task_id, "graph.nodes.node_id": node_id},
                                      {"$set": {"graph.nodes.$.status": RunState.running}})
            return

        # If complete artifacts → mark finished
        complete = any((r.payload.get("artifacts") or {}).get("complete") for r in replies)
        if complete:
            await db.tasks.update_one({"id": task_id, "graph.nodes.node_id": node_id},
                                      {"$set": {"graph.nodes.$.status": RunState.finished,
                                                "graph.nodes.$.finished_at": now_dt(),
                                                "graph.nodes.$.attempt_epoch": new_epoch}})
            return

        # Start node
        await db.tasks.update_one({"id": task_id, "graph.nodes.node_id": node_id},
                                  {"$set": {"graph.nodes.$.status": RunState.running,
                                            "graph.nodes.$.started_at": now_dt(),
                                            "graph.nodes.$.attempt_epoch": new_epoch}})
        cancel_token = str(uuid.uuid4())
        cmd = CmdTaskStart(cmd=CommandKind.TASK_START,
                           input_ref=node.get("io", {}).get("input_ref"),
                           input_inline=node.get("io", {}).get("input_inline"),
                           batching=node.get("io", {}).get("batching"),
                           cancel_token=cancel_token)
        env = Envelope(
            msg_type=MsgType.cmd,
            role=Role.coordinator,
            dedup_id=stable_hash({"cmd": str(CommandKind.TASK_START), "task_id": task_id, "node_id": node_id, "epoch": new_epoch}),
            task_id=task_id,
            node_id=node_id,
            step_type=node["type"],
            attempt_epoch=new_epoch,
            payload=cmd.model_dump()
        )
        await self._enqueue_cmd(env)

    # ── Event handlers ──────────────────────────────────────────────────
    async def _record_worker_event(self, env: Envelope) -> None:
        evh = stable_hash({"dedup_id": env.dedup_id, "ts": env.ts})
        # upsert вместо insert_one — чтобы дубликат envelope не создавал вторую строку
        try:
            await db.worker_events.update_one(
                {"task_id": env.task_id, "node_id": env.node_id, "event_hash": evh},
                {"$setOnInsert": {
                    "task_id": env.task_id,
                    "node_id": env.node_id,
                    "step_type": env.step_type,
                    "event_hash": evh,
                    "payload": env.payload,
                    "ts": env.ts,
                    "ts_dt": datetime.fromtimestamp(env.ts, tz=timezone.utc),
                    "attempt_epoch": env.attempt_epoch,
                    "created_at": now_dt(),
                    "metrics_applied": False,
                }},
                upsert=True,
            )
        except Exception:
            # не блокируем поток
            pass
        await db.tasks.update_one({"id": env.task_id}, {"$max": {"last_event_ts": env.ts}, "$currentDate": {"updated_at": True}})

    async def _on_task_accepted(self, env: Envelope) -> None:
        p = EvTaskAccepted.model_validate(env.payload)
        await db.tasks.update_one({"id": env.task_id, "graph.nodes.node_id": env.node_id},
            {"$set": {"graph.nodes.$.lease.worker_id": p.worker_id,
                      "graph.nodes.$.lease.lease_id": p.lease_id,
                      "graph.nodes.$.lease.deadline_ts": p.lease_deadline_ts}})

    async def _on_task_heartbeat(self, env: Envelope) -> None:
        p = EvHeartbeat.model_validate(env.payload)
        await db.tasks.update_one({"id": env.task_id, "graph.nodes.node_id": env.node_id},
            {"$set": {"graph.nodes.$.lease.worker_id": p.worker_id,
                      "graph.nodes.$.lease.lease_id": p.lease_id,
                      "graph.nodes.$.lease.deadline_ts": p.lease_deadline_ts},
             "$max": {"graph.nodes.$.last_event_ts": env.ts}})

    async def _apply_metrics_once(self, env: Envelope, metrics: Dict[str, int]) -> None:
        res = await db.worker_events.find_one_and_update(
            {"task_id": env.task_id, "node_id": env.node_id, "event_hash": stable_hash({"dedup_id": env.dedup_id, "ts": env.ts}),
             "metrics_applied": {"$ne": True}},
            {"$set": {"metrics_applied": True}}
        )
        if res and metrics:
            inc: Dict[str, int] = {}
            for k, v in (metrics or {}).items():
                try:
                    # допускаем числа и числовые строки
                    if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().lstrip("-").isdigit()):
                        inc[f"graph.nodes.$.stats.{k}"] = int(v)
                except Exception:
                    continue
            if inc:
                await db.tasks.update_one(
                    {"id": env.task_id, "graph.nodes.node_id": env.node_id},
                    {"$inc": inc, "$currentDate": {"updated_at": True}}
                )

    async def _persist_artifact_partial(self, env: Envelope, ref: Dict[str, Any], meta: Dict[str, Any]) -> None:
        """
        Идемпотентная запись частичного артефакта. Любые ошибки здесь не должны
        обрушать обработку событий — они приводят к deferred состояния/ретраю downstream.
        """
        try:
            shard_id = ref.get("shard_id")
            if not shard_id:
                # Защита координатора: если шард без id — пометим событие и уйдём
                await db.worker_events.update_one(
                    {"task_id": env.task_id, "node_id": env.node_id, "event_hash": stable_hash({"dedup_id": env.dedup_id, "ts": env.ts})},
                    {"$set": {"artifact_error": "missing_shard_id"}}
                )
                return
            await db.artifacts.update_one(
                {"task_id": env.task_id, "node_id": env.node_id, "shard_id": shard_id},
                {"$setOnInsert": {"task_id": env.task_id, "node_id": env.node_id,
                                  "attempt_epoch": env.attempt_epoch, "status": "partial",
                                  "meta": meta or {}, "payload": None, "worker_id": env.payload.get("worker_id"),
                                  "created_at": now_dt()},
                 "$set": {"updated_at": now_dt()}},
                upsert=True
            )
        except Exception as e:
            # Не роняем поток: просто зафиксируем ошибку
            await db.worker_events.update_one(
                {"task_id": env.task_id, "node_id": env.node_id, "event_hash": stable_hash({"dedup_id": env.dedup_id, "ts": env.ts})},
                {"$set": {"artifact_error": str(e)}}
            )

    async def _maybe_start_children_on_first_batch(self, parent_task: Dict[str, Any], parent_node_id: str) -> None:
        """
        На первый батч родителя пытаемся стартовать детей, которые ожидают асинхронный запуск:
        - edges_ex: mode=async, trigger=on_batch
        - либо у ребёнка io.start_when=first_batch и он зависит от parent
        Стартуем атомарно с флагом 'streaming.started_on_first_batch', чтобы не триггерить повторно.
        """
        task_id = parent_task["id"]
        graph = parent_task.get("graph", {}) or {}
        nodes = graph.get("nodes") or []
        edges_ex = self._edges_ex_from(parent_task, parent_node_id)

        # Кандидаты-дети по edges/edges_ex
        direct_children = self._children_of(parent_task, parent_node_id)
        if not direct_children:
            return

        for child_id in direct_children:
            child = self._get_node(parent_task, child_id)
            if not child:
                continue

            # Применим правила:
            rule_async = any(ex for ex in edges_ex if ex.get("to") == child_id and ex.get("mode") == "async" and ex.get("trigger") == "on_batch")
            sw = (child.get("io", {}) or {}).get("start_when", "ready")
            wants_first_batch = (sw == "first_batch")

            if not (rule_async or wants_first_batch):
                continue

            # Попытаемся атомарно пометить, что мы запускаем этого ребёнка "по первому батчу"
            # Запустим только если он ещё в queued/deferred и не запускался ранним способом.
            q = {"id": task_id, "graph.nodes": {"$elemMatch": {"node_id": child_id,
                                                                "status": {"$in": [RunState.queued, RunState.deferred]},
                                                                "streaming.started_on_first_batch": {"$ne": True}}}}
            res = await db.tasks.find_one_and_update(
                q,
                {"$set": {"graph.nodes.$.streaming.started_on_first_batch": True,
                          "graph.nodes.$.next_retry_at": 0}},
            )
            if res:
                # scheduler подберёт через _node_ready (first_batch) → но чтобы ускорить,
                # можно прямо сейчас инициировать preflight if ready:
                fresh = await db.tasks.find_one({"id": task_id}, {"id": 1, "graph": 1})
                ch_node = self._get_node(fresh, child_id) if fresh else None
                if fresh and ch_node:
                    if rule_async:
                        await self._preflight_and_maybe_start(fresh, ch_node)
                    else:
                        if await self._node_ready(fresh, ch_node):
                            await self._preflight_and_maybe_start(fresh, ch_node)

    async def _on_batch_ok(self, env: Envelope) -> None:
        p = EvBatchOk.model_validate(env.payload)
        await self._apply_metrics_once(env, p.metrics or {})

        # Идемпотентно фиксируем partial артефакт (шард)
        if p.artifacts_ref:
            await self._persist_artifact_partial(env, p.artifacts_ref, p.metrics or {})

        # Попробуем «ранний старт» downstream на первый батч
        try:
            tdoc = await db.tasks.find_one({"id": env.task_id}, {"id": 1, "graph": 1})
            if tdoc:
                await self._maybe_start_children_on_first_batch(tdoc, env.node_id)
        except Exception:
            # Не блокируем обработку батчей из-за ошибок триггера
            pass

    async def _on_batch_failed(self, env: Envelope) -> None:
        p = EvBatchFailed.model_validate(env.payload)
        await self._apply_metrics_once(env, {})
        # Помечаем эпоху как «ошибочную» и переводим ноду в deferred (ретрай)
        try:
            tdoc = await db.tasks.find_one({"id": env.task_id}, {"graph": 1})
            node = self._get_node(tdoc, env.node_id) if tdoc else None
            policy = ((node or {}).get("retry_policy") or {})
            backoff = int(policy.get("backoff_sec", 300))
            await db.tasks.update_one(
                {"id": env.task_id, "graph.nodes.node_id": env.node_id},
                {"$set": {
                    "graph.nodes.$.status": RunState.deferred,
                    "graph.nodes.$.next_retry_at": now_ts() + backoff,
                    "graph.nodes.$.lease.epoch_failed": env.attempt_epoch,
                    "graph.nodes.$.last_error": p.reason_code,
                }}
            )
        except Exception:
            pass
        if p.artifacts_ref:
            # Можно пометить артефакт как "failed" для шардового мониторинга (без влияния на DAG)
            try:
                shard_id = p.artifacts_ref.get("shard_id")
                if shard_id:
                    await db.artifacts.update_one(
                        {"task_id": env.task_id, "node_id": env.node_id, "shard_id": shard_id},
                        {"$set": {"status": "failed", "updated_at": now_dt(), "error": p.reason_code}},
                        upsert=True
                    )
            except Exception:
                pass

    async def _on_task_done(self, env: Envelope) -> None:
        p = EvTaskDone.model_validate(env.payload)
        # Если в текущей эпохе уже фиксировалась ошибка — игнорируем TASK_DONE
        try:
            tdoc = await db.tasks.find_one({"id": env.task_id}, {"graph": 1})
            node = self._get_node(tdoc, env.node_id) if tdoc else None
            lease = (node or {}).get("lease") or {}
            if int(lease.get("epoch_failed", -1)) == int(env.attempt_epoch):
                return
        except Exception:
            pass
        await self._apply_metrics_once(env, p.metrics or {})
        if p.artifacts_ref:
            # помечаем результат ноды как complete
            try:
                await db.artifacts.update_one(
                    {"task_id": env.task_id, "node_id": env.node_id},
                    {"$set": {"status": "complete", "meta": p.metrics or {}, "updated_at": now_dt()},
                     "$setOnInsert": {"task_id": env.task_id, "node_id": env.node_id, "attempt_epoch": env.attempt_epoch}},
                    upsert=True
                )
            except Exception:
                pass
        await db.tasks.update_one({"id": env.task_id, "graph.nodes.node_id": env.node_id},
                                  {"$set": {"graph.nodes.$.status": RunState.finished,
                                            "graph.nodes.$.finished_at": now_dt()}})

    async def _on_task_failed(self, env: Envelope) -> None:
        p = EvTaskFailed.model_validate(env.payload)
        if p.permanent:
            await self._cascade_cancel(env.task_id, reason=f"hard_fail:{p.reason_code}")
            await db.tasks.update_one({"id": env.task_id}, {"$set": {"status": RunState.failed, "finished_at": now_dt()}})
        else:
            td = await db.tasks.find_one({"id": env.task_id}, {"graph": 1})
            node = self._get_node(td, env.node_id) if td else None
            backoff = int(((node or {}).get("retry_policy") or {}).get("backoff_sec", 300))
            await db.tasks.update_one({"id": env.task_id, "graph.nodes.node_id": env.node_id},
                                      {"$set": {"graph.nodes.$.status": RunState.deferred,
                                                "graph.nodes.$.next_retry_at": now_ts() + backoff,
                                                "graph.nodes.$.last_error": p.reason_code}})

    async def _on_cancelled(self, env: Envelope) -> None:
        await db.tasks.update_one({"id": env.task_id, "graph.nodes.node_id": env.node_id},
                                  {"$set": {"graph.nodes.$.status": RunState.deferred}})

    # ── Heartbeat monitor ───────────────────────────────────────────────
    async def _heartbeat_monitor(self) -> None:
        try:
            while True:
                cur = db.tasks.find({"status": {"$in": [RunState.running, RunState.deferred]}},
                                    {"id": 1, "last_event_ts": 1})
                async for t in cur:
                    last = int(t.get("last_event_ts") or 0)
                    dt = now_ts() - last
                    if dt >= HEARTBEAT_HARD_SEC:
                        await db.tasks.update_one({"id": t["id"]},
                                                  {"$set": {"status": RunState.failed, "finished_at": now_dt(),
                                                            "coordinator.liveness.state": "dead"}})
                    elif dt >= HEARTBEAT_SOFT_SEC:
                        await db.tasks.update_one({"id": t["id"]},
                                                  {"$set": {"status": RunState.deferred,
                                                            "coordinator.liveness.state": "suspected",
                                                            "coordinator.liveness.suspected_at": now_dt(),
                                                            "next_retry_at": now_ts() + 60}})
                await asyncio.sleep(HB_MONITOR_TICK_SEC)
        except asyncio.CancelledError:
            return

    # ── Finalizer (fan-out handling via DAG) ────────────────────────────
    async def _finalizer_loop(self) -> None:
        try:
            while True:
                await self._finalize_nodes_and_tasks()
                await asyncio.sleep(FINALIZER_TICK_SEC)
        except asyncio.CancelledError:
            return

    async def _finalize_nodes_and_tasks(self) -> None:
        cur = db.tasks.find({"status": {"$in": [RunState.running, RunState.deferred]}}, {"id": 1, "graph": 1})
        async for t in cur:
            nodes = t.get("graph", {}).get("nodes") or []
            if nodes and all(self._to_runstate(n.get("status")) == RunState.finished for n in nodes):
                result = {"nodes": [{"node_id": n["node_id"], "stats": n.get("stats", {})} for n in nodes]}
                await db.tasks.update_one({"id": t["id"]},
                    {"$set": {"status": RunState.finished, "finished_at": now_dt(), "result": result}})
                continue

    # ── Cascade cancel (uses Outbox) ────────────────────────────────────
    async def _cascade_cancel(self, task_id: str, *, reason: str) -> None:
        doc = await db.tasks.find_one({"id": task_id}, {"graph": 1})
        if not doc: return
        for n in (doc.get("graph", {}).get("nodes") or []):
            if n.get("status") in [RunState.running, RunState.deferred, RunState.queued]:
                cancel = CmdTaskCancel(cmd=CommandKind.TASK_CANCEL, reason=reason, cancel_token=str(uuid.uuid4()))
                env = Envelope(
                    msg_type=MsgType.cmd,
                    role=Role.coordinator,
                    dedup_id=stable_hash({"cmd": str(CommandKind.TASK_CANCEL), "task_id": task_id, "node_id": n["node_id"], "epoch": int(n.get("attempt_epoch", 0))}),
                    task_id=task_id,
                    node_id=n["node_id"],
                    step_type=n["type"],
                    attempt_epoch=int(n.get("attempt_epoch", 0)),
                    payload=cancel.model_dump()
                )
                await self._enqueue_cmd(env)
                await db.tasks.update_one({"id": task_id, "graph.nodes.node_id": n["node_id"]},
                                          {"$set": {"graph.nodes.$.status": RunState.cancelling}})
        await asyncio.sleep(CANCEL_GRACE_SEC)

    # ── Resume inflight ─────────────────────────────────────────────────
    async def _resume_inflight(self) -> None:
        cur = db.tasks.find({"status": {"$in": [RunState.running, RunState.deferred, RunState.queued]}} ,
                            {"id": 1, "graph": 1, "status": 1})
        async for _ in cur:
            # scheduler will adopt/start as needed
            pass

    # ── Indexes ─────────────────────────────────────────────────────────
    async def _ensure_indexes(self) -> None:
        try:
            await db.tasks.create_index([("status", 1), ("updated_at", 1)], name="ix_task_status_updated")
            await db.worker_events.create_index([("task_id", 1), ("node_id", 1), ("event_hash", 1)], unique=True, name="uniq_worker_event")
            await db.worker_events.create_index([("ts_dt", 1)], name="ttl_worker_events", expireAfterSeconds=14*24*3600)
            await db.worker_registry.create_index([("worker_id", 1)], unique=True, name="uniq_worker")
            await db.artifacts.create_index([("task_id", 1), ("node_id", 1)], name="ix_artifacts_task_node")
            await db.artifacts.create_index([("task_id", 1), ("node_id", 1), ("shard_id", 1)], unique=True, sparse=True, name="uniq_artifact_shard")
            await db.outbox.create_index([("fp", 1)], unique=True, name="uniq_outbox_fp")
            await db.outbox.create_index([("state", 1), ("next_attempt_at", 1)], name="ix_outbox_state_next")
        except Exception:
            pass

# ─────────────────────────── Minimal FastAPI ───────────────────────────
# app = FastAPI(title="Universal DAG Coordinator (Stream-aware, Outbox, fan-in/out)")

# COORD = Coordinator()

# @app.on_event("startup")
# async def _startup() -> None:
#     await COORD.start()

# @app.on_event("shutdown")
# async def _shutdown() -> None:
#     await COORD.stop()

# class CreateTaskBody(BaseModel):
#     params: Dict[str, Any] = Field(default_factory=dict)
#     graph: Dict[str, Any]

# @app.post("/tasks")
# async def create_task(b: CreateTaskBody) -> Dict[str, Any]:
#     task_id = await COORD.create_task(params=b.params, graph=b.graph)
#     return {"task_id": task_id}

# @app.get("/tasks/{task_id}")
# async def get_task(task_id: str) -> Dict[str, Any]:
#     t = await db.tasks.find_one({"id": task_id})
#     if not t:
#         raise HTTPException(status_code=404, detail="task not found")
#     t["_id"] = str(t["_id"])
#     return t
