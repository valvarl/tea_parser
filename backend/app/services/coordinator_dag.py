# coordinator_dag.py
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Literal

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, RootModel
from enum import Enum

# ─────────────────────────── App DB (motor) ───────────────────────────
# Ожидается, что в проекте есть модуль с motor.AsyncIOMotorDatabase
# from app.db.mongo import db
# Для шаблона используем "псевдо-объект" db, ожидается реальный импорт в интеграции
class _Fake:
    def __getattr__(self, k):  # type: ignore
        raise RuntimeError("Replace _Fake db with your motor db. from app.db.mongo import db")
db = _Fake()  # REPLACE in your project

# ─────────────────────────── Config ───────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
WORKER_TYPES = [s.strip() for s in os.getenv("WORKER_TYPES", "indexer,enricher").split(",") if s.strip()]

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
    # optional routing key to target particular worker
    target_worker_id: Optional[str] = None

# Payloads
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
    metrics: Dict[str, int] = Field(default_factory=dict)
    artifacts_ref: Optional[Dict[str, Any]] = None  # shard artifacts

class EvBatchFailed(BaseModel):
    kind: Literal[EventKind.BATCH_FAILED]
    worker_id: str
    batch_id: Optional[int] = None
    reason_code: str
    permanent: bool = False
    error: Optional[str] = None

class EvTaskDone(BaseModel):
    kind: Literal[EventKind.TASK_DONE]
    worker_id: str
    metrics: Dict[str, int] = Field(default_factory=dict)
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

# ─────────────────────────── Task/DAG storage models ───────────────────────────
class RetryPolicy(BaseModel):
    max: int = 2
    backoff_sec: int = 300
    permanent_on: List[str] = Field(default_factory=lambda: ["bad_input", "schema_mismatch"])

class Lease(BaseModel):
    worker_id: Optional[str] = None
    lease_id: Optional[str] = None
    deadline_ts: Optional[int] = None

class DagNode(BaseModel):
    node_id: str
    type: str  # "indexer" | "enricher" | "coordinator_fn" | ...
    status: RunState = RunState.queued
    attempt_epoch: int = 0
    lease: Lease = Field(default_factory=Lease)
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

class TaskDoc(BaseModel):
    id: str
    pipeline_id: str
    status: RunState = RunState.queued
    params: Dict[str, Any] = Field(default_factory=dict)
    graph: Dict[str, Any] = Field(default_factory=lambda: {"nodes": [], "edges": []})
    result: Optional[Dict[str, Any]] = None
    status_history: List[Dict[str, Any]] = Field(default_factory=list)
    coordinator: Dict[str, Any] = Field(default_factory=lambda: {"liveness": {"state": "ok"}})
    next_retry_at: Optional[int] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    last_event_ts: int = 0

# ─────────────────────────── Kafka bus ───────────────────────────
class KafkaBus:
    def __init__(self, bootstrap: str) -> None:
        self.bootstrap = bootstrap
        self._producer: Optional[AIOKafkaProducer] = None
        self._consumers: List[AIOKafkaConsumer] = []

        # in-memory correlator for replies
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

    async def send_cmd(self, env: Envelope) -> None:
        assert self._producer is not None
        topic = self._topic_cmd(env.step_type)
        key = f"{env.task_id}:{env.node_id}".encode("utf-8")
        await self._producer.send_and_wait(topic, env.model_dump(mode="json"), key=key)

    async def send_query(self, env: Envelope) -> None:
        assert self._producer is not None
        key = env.task_id.encode("utf-8")
        await self._producer.send_and_wait(TOPIC_QUERY, env.model_dump(mode="json"), key=key)

    async def send_cancel_broadcast(self, envs: List[Envelope]) -> None:
        for env in envs:
            await self.send_cmd(env)

    # Replies correlator (for TASK_DISCOVER)
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

# ─────────────────────────── Coordinator core ───────────────────────────
class Coordinator:
    def __init__(self) -> None:
        self.bus = KafkaBus(KAFKA_BOOTSTRAP)
        self._tasks: set[asyncio.Task] = set()
        self._running = False

        # consumers
        self._announce_consumer: Optional[AIOKafkaConsumer] = None
        self._status_consumers: Dict[str, AIOKafkaConsumer] = {}
        self._query_reply_consumer: Optional[AIOKafkaConsumer] = None

    # ── Lifecycle ───────────────────────────────────────────────────────
    async def start(self) -> None:
        await self._ensure_indexes()
        await self.bus.start()
        await self._start_consumers()
        self._running = True
        # background monitors
        self._spawn(self._scheduler_loop())
        self._spawn(self._heartbeat_monitor())
        self._spawn(self._finalizer_loop())
        # resume inflight
        self._spawn(self._resume_inflight())

    async def stop(self) -> None:
        self._running = False
        for t in list(self._tasks):
            t.cancel()
        self._tasks.clear()
        await self.bus.stop()

    def _spawn(self, coro: asyncio.coroutine) -> None:  # type: ignore
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

        # queries to workers — публикуем через bus.send_query()

    async def _run_reply_consumer(self, c: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await c.getone()
                env = Envelope.model_validate(msg.value)
                if env.msg_type == MsgType.reply and env.role == Role.worker:
                    # Attach reply to correlator
                    self.bus.push_reply(env.corr_id, env)
                await c.commit()
        except asyncio.CancelledError:
            return

    async def _run_announce_consumer(self, c: AIOKafkaConsumer) -> None:
        """
        WORKER_ONLINE/OFFLINE and heartbeats (optional)
        """
        try:
            while True:
                msg = await c.getone()
                env = Envelope.model_validate(msg.value)
                payload = env.payload
                kind = payload.get("kind")
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
                    # other announce heartbeat variants
                    await db.worker_registry.update_one(
                        {"worker_id": payload.get("worker_id")},
                        {"$set": {"last_seen": now_dt()}},
                        upsert=True
                    )
                await c.commit()
        except asyncio.CancelledError:
            return

    async def _run_status_consumer(self, step_type: str, c: AIOKafkaConsumer) -> None:
        """
        Унифицированная обработка событий от воркеров данного типа.
        """
        try:
            while True:
                msg = await c.getone()
                env = Envelope.model_validate(msg.value)
                # Dedup worker event by event_hash
                await self._record_worker_event(env)

                # Fencing: ignore stale epoch
                tdoc = await db.tasks.find_one({"id": env.task_id}, {"graph": 1, "status": 1})
                if not tdoc:
                    await c.commit()
                    continue
                node = self._get_node(tdoc, env.node_id)
                if not node or env.attempt_epoch != int(node.get("attempt_epoch", 0)):
                    await c.commit()
                    continue

                # Update last_event_ts
                await db.tasks.update_one({"id": env.task_id, "graph.nodes.node_id": env.node_id},
                    {"$max": {"last_event_ts": env.ts, "graph.nodes.$.last_event_ts": env.ts},
                     "$currentDate": {"updated_at": True}})

                kind = env.payload.get("kind")
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

                await c.commit()
        except asyncio.CancelledError:
            return

    # ── CRUD helpers for tasks/nodes ────────────────────────────────────
    def _get_node(self, task_doc: Dict[str, Any], node_id: str) -> Optional[Dict[str, Any]]:
        for n in (task_doc.get("graph", {}).get("nodes") or []):
            if n.get("node_id") == node_id:
                return n
        return None

    async def create_task(self, *, params: Dict[str, Any], graph: Dict[str, Any]) -> str:
        task_id = str(uuid.uuid4())
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

    # ── Scheduler / DAG routing ─────────────────────────────────────────
    async def _scheduler_loop(self) -> None:
        try:
            while True:
                await self._schedule_ready_nodes()
                await asyncio.sleep(SCHEDULER_TICK_SEC)
        except asyncio.CancelledError:
            return

    async def _schedule_ready_nodes(self) -> None:
        # Pick tasks with queued/deferred nodes whose dependencies satisfied and backoff expired
        cur = db.tasks.find(
            {"status": {"$in": [RunState.running, RunState.queued, RunState.deferred]}},
            {"id": 1, "graph": 1, "status": 1, "last_event_ts": 1}
        )
        async for t in cur:
            nodes: List[Dict[str, Any]] = t.get("graph", {}).get("nodes") or []
            # ensure task is running
            if t.get("status") == RunState.queued:
                await db.tasks.update_one({"id": t["id"]}, {"$set": {"status": RunState.running}})

            for n in nodes:
                if n.get("type") == "coordinator_fn":
                    # Выполнить локально (пример: постпроцессинг)
                    if self._node_ready(t, n):
                        await self._run_coordinator_fn(t, n)
                        # после выполнения — переход к детям (финализатор + инварианты тоже поймут)
                    continue

                if self._node_ready(t, n):
                    await self._preflight_and_maybe_start(t, n)

    def _node_ready(self, task_doc: Dict[str, Any], node: Dict[str, Any]) -> bool:
        status = node.get("status")
        if status not in [RunState.queued, RunState.deferred]:
            return False
        # dependency check
        deps = node.get("depends_on") or []
        if deps:
            nodes = task_doc.get("graph", {}).get("nodes") or []
            st = {d: self._get_node(task_doc, d).get("status") for d in deps}
            fan_in = (node.get("fan_in") or "all").lower()
            if fan_in == "all":
                if not all(s == RunState.finished for s in st.values()):
                    return False
            elif fan_in == "any":
                if not any(s == RunState.finished for s in st.values()):
                    return False
            elif fan_in.startswith("count:"):
                try:
                    k = int(fan_in.split(":", 1)[1])
                except Exception:
                    k = len(deps)
                if sum(1 for s in st.values() if s == RunState.finished) < k:
                    return False

        # backoff / retry window
        nx = node.get("next_retry_at")
        if nx and now_ts() < int(nx):
            return False

        return True

    async def _run_coordinator_fn(self, task_doc: Dict[str, Any], node: Dict[str, Any]) -> None:
        # Пример: postproc, который превращает артефакты родителя в вход для ребёнка
        await db.tasks.update_one({"id": task_doc["id"], "graph.nodes.node_id": node["node_id"]},
                                  {"$set": {"graph.nodes.$.status": RunState.running,
                                            "graph.nodes.$.started_at": now_dt(),
                                            "graph.nodes.$.attempt_epoch": int(node.get("attempt_epoch", 0)) + 1}})
        # Здесь ваша бизнес-логика: прочитать artifacts родителей и записать свой artifacts
        # В шаблоне просто делаем "пустой" успешный проход
        await asyncio.sleep(0)  # yield
        await db.tasks.update_one({"id": task_doc["id"], "graph.nodes.node_id": node["node_id"]},
                                  {"$set": {"graph.nodes.$.status": RunState.finished,
                                            "graph.nodes.$.finished_at": now_dt(),
                                            "graph.nodes.$.last_event_ts": now_ts()}})

    async def _preflight_and_maybe_start(self, task_doc: Dict[str, Any], node: Dict[str, Any]) -> None:
        """
        1) TASK_DISCOVER broadcast
        2) Решение: ADOPT или CANCEL_OLD_AND_RESTART или start
        """
        task_id = task_doc["id"]
        node_id = node["node_id"]
        new_epoch = int(node.get("attempt_epoch", 0)) + 1

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
        wait_ev = self.bus.register_reply(discover_env.corr_id)
        await self.bus.send_query(discover_env)

        # Подождать окно DISCOVERY_WINDOW_SEC и собрать ответы
        try:
            await asyncio.wait_for(asyncio.sleep(DISCOVERY_WINDOW_SEC), timeout=DISCOVERY_WINDOW_SEC)
        except asyncio.TimeoutError:
            pass
        replies = self.bus.collect_replies(discover_env.corr_id)

        # Проверить, есть ли ранний владелец
        active = [r for r in replies if (r.payload.get("run_state") in ("running", "finishing") and r.payload.get("attempt_epoch") == node.get("attempt_epoch"))]
        if active:
            # ADOPT: оставляем за текущим воркером; только убедимся, что таск помечен running
            await db.tasks.update_one({"id": task_id, "graph.nodes.node_id": node_id},
                                      {"$set": {"graph.nodes.$.status": RunState.running}})
            return

        # Если есть complete artifacts из прошлого — можно пропустить
        complete = any((r.payload.get("artifacts") or {}).get("complete") for r in replies)
        if complete:
            await db.tasks.update_one({"id": task_id, "graph.nodes.node_id": node_id},
                                      {"$set": {"graph.nodes.$.status": RunState.finished,
                                                "graph.nodes.$.finished_at": now_dt(),
                                                "graph.nodes.$.attempt_epoch": new_epoch}})
            return

        # Иначе — стартуем новый запуск узла
        await db.tasks.update_one({"id": task_id, "graph.nodes.node_id": node_id},
                                  {"$set": {"graph.nodes.$.status": RunState.running,
                                            "graph.nodes.$.started_at": now_dt(),
                                            "graph.nodes.$.attempt_epoch": new_epoch}})
        cancel_token = str(uuid.uuid4())
        cmd = CmdTaskStart(cmd=CommandKind.TASK_START, input_ref=node.get("io", {}).get("input_ref"),
                           input_inline=node.get("io", {}).get("input_inline"),
                           batching=node.get("io", {}).get("batching"), cancel_token=cancel_token)
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
        await self.bus.send_cmd(env)

    # ── Event handlers ──────────────────────────────────────────────────
    async def _record_worker_event(self, env: Envelope) -> None:
        edoc = {
            "task_id": env.task_id,
            "node_id": env.node_id,
            "step_type": env.step_type,
            "event_hash": stable_hash({"dedup_id": env.dedup_id, "ts": env.ts}),
            "payload": env.payload,
            "ts": env.ts,
            "ts_dt": datetime.fromtimestamp(env.ts, tz=timezone.utc),
            "attempt_epoch": env.attempt_epoch,
            "created_at": now_dt(),
            "metrics_applied": False,
        }
        try:
            await db.worker_events.insert_one(edoc)
        except Exception:
            # duplicate → ok
            pass
        await db.tasks.update_one({"id": env.task_id}, {"$max": {"last_event_ts": env.ts}, "$currentDate": {"updated_at": True}})

    async def _on_task_accepted(self, env: Envelope) -> None:
        p = EvTaskAccepted.model_validate(env.payload)
        await db.tasks.update_one({"id": env.task_id, "graph.nodes.node_id": env.node_id},
            {"$set": {
                "graph.nodes.$.lease.worker_id": p.worker_id,
                "graph.nodes.$.lease.lease_id": p.lease_id,
                "graph.nodes.$.lease.deadline_ts": p.lease_deadline_ts,
            }})

    async def _on_task_heartbeat(self, env: Envelope) -> None:
        p = EvHeartbeat.model_validate(env.payload)
        await db.tasks.update_one({"id": env.task_id, "graph.nodes.node_id": env.node_id},
            {"$set": {
                "graph.nodes.$.lease.worker_id": p.worker_id,
                "graph.nodes.$.lease.lease_id": p.lease_id,
                "graph.nodes.$.lease.deadline_ts": p.lease_deadline_ts,
            },
             "$max": {"graph.nodes.$.last_event_ts": env.ts}})

    async def _apply_metrics_once(self, env: Envelope, metrics: Dict[str, int]) -> None:
        # acquire-once by event_hash
        res = await db.worker_events.find_one_and_update(
            {"task_id": env.task_id, "node_id": env.node_id, "event_hash": stable_hash({"dedup_id": env.dedup_id, "ts": env.ts}),
             "metrics_applied": {"$ne": True}},
            {"$set": {"metrics_applied": True}}
        )
        if res:
            # merge node stats
            inc = {f"graph.nodes.$.stats.{k}": int(v) for k, v in (metrics or {}).items()}
            if inc:
                await db.tasks.update_one({"id": env.task_id, "graph.nodes.node_id": env.node_id},
                                          {"$inc": inc, "$currentDate": {"updated_at": True}})

    async def _on_batch_ok(self, env: Envelope) -> None:
        p = EvBatchOk.model_validate(env.payload)
        await self._apply_metrics_once(env, p.metrics or {})
        # При желании: записать shard artifacts_ref в коллекцию artifacts
        if p.artifacts_ref:
            await db.artifacts.update_one(
                {"task_id": env.task_id, "node_id": env.node_id, "shard_id": p.artifacts_ref.get("shard_id")},
                {"$setOnInsert": {
                    "task_id": env.task_id, "node_id": env.node_id,
                    "attempt_epoch": env.attempt_epoch, "status": "partial",
                    "meta": p.metrics or {}, "payload": None, "worker_id": p.worker_id,
                    "updated_at": now_dt()
                },
                 "$currentDate": {"updated_at": True}}, upsert=True)

    async def _on_batch_failed(self, env: Envelope) -> None:
        p = EvBatchFailed.model_validate(env.payload)
        await self._apply_metrics_once(env, {})  # можно инкрементировать dlq/failed_batches
        # Маркировать частичные ошибки; полный fail решает финализатор/классификатор

    async def _on_task_done(self, env: Envelope) -> None:
        p = EvTaskDone.model_validate(env.payload)
        await self._apply_metrics_once(env, p.metrics or {})
        # Пометить artifacts complete
        if p.artifacts_ref:
            await db.artifacts.update_one(
                {"task_id": env.task_id, "node_id": env.node_id},
                {"$set": {"status": "complete", "meta": p.metrics or {}, "updated_at": now_dt()}, "$setOnInsert": {
                    "task_id": env.task_id, "node_id": env.node_id, "attempt_epoch": env.attempt_epoch
                }}, upsert=True)
        await db.tasks.update_one({"id": env.task_id, "graph.nodes.node_id": env.node_id},
                                  {"$set": {"graph.nodes.$.status": RunState.finished,
                                            "graph.nodes.$.finished_at": now_dt()}})

    async def _on_task_failed(self, env: Envelope) -> None:
        p = EvTaskFailed.model_validate(env.payload)
        # Классификация: если permanent → каскадная отмена всей задачи
        if p.permanent:
            await self._cascade_cancel(env.task_id, reason=f"hard_fail:{p.reason_code}")
            await db.tasks.update_one({"id": env.task_id}, {"$set": {"status": RunState.failed, "finished_at": now_dt()}})
        else:
            # мягкий fail узла → deferred + backoff
            td = await db.tasks.find_one({"id": env.task_id}, {"graph": 1})
            node = self._get_node(td, env.node_id) if td else None
            backoff = int(((node or {}).get("retry_policy") or {}).get("backoff_sec", 300))
            await db.tasks.update_one({"id": env.task_id, "graph.nodes.node_id": env.node_id},
                                      {"$set": {"graph.nodes.$.status": RunState.deferred,
                                                "graph.nodes.$.next_retry_at": now_ts() + backoff}})

    async def _on_cancelled(self, env: Envelope) -> None:
        await db.tasks.update_one({"id": env.task_id, "graph.nodes.node_id": env.node_id},
                                  {"$set": {"graph.nodes.$.status": RunState.deferred}})

    # ── Heartbeat monitor (soft/hard) ───────────────────────────────────
    async def _heartbeat_monitor(self) -> None:
        try:
            while True:
                # soft
                cur = db.tasks.find({"status": {"$in": [RunState.running, RunState.deferred]}}, {"id": 1, "last_event_ts": 1})
                async for t in cur:
                    last = int(t.get("last_event_ts") or 0)
                    if now_ts() - last >= HEARTBEAT_HARD_SEC:
                        await db.tasks.update_one({"id": t["id"]},
                                                  {"$set": {"status": RunState.failed, "finished_at": now_dt(),
                                                            "coordinator.liveness.state": "dead"}})
                    elif now_ts() - last >= HEARTBEAT_SOFT_SEC:
                        await db.tasks.update_one({"id": t["id"]},
                                                  {"$set": {"status": RunState.deferred,
                                                            "coordinator.liveness.state": "suspected",
                                                            "coordinator.liveness.suspected_at": now_dt(),
                                                            "next_retry_at": now_ts() + 60}})
                await asyncio.sleep(HB_MONITOR_TICK_SEC)
        except asyncio.CancelledError:
            return

    # ── Finalizer (invariants) ──────────────────────────────────────────
    async def _finalizer_loop(self) -> None:
        try:
            while True:
                await self._finalize_nodes_and_tasks()
                await asyncio.sleep(FINALIZER_TICK_SEC)
        except asyncio.CancelledError:
            return

    async def _finalize_nodes_and_tasks(self) -> None:
        # Узлы уже помечаются finished в _on_task_done / coordinator_fn. Здесь — финализация всей задачи.
        cur = db.tasks.find({"status": {"$in": [RunState.running, RunState.deferred]}}, {"id": 1, "graph": 1})
        async for t in cur:
            nodes = t.get("graph", {}).get("nodes") or []
            if nodes and all(n.get("status") in [RunState.finished] for n in nodes):
                # Сборка финального отчёта (пример)
                result = {"nodes": [{ "node_id": n["node_id"], "stats": n.get("stats", {}) } for n in nodes]}
                await db.tasks.update_one({"id": t["id"]},
                    {"$set": {"status": RunState.finished, "finished_at": now_dt(), "result": result}})

    # ── Cascade cancel ──────────────────────────────────────────────────
    async def _cascade_cancel(self, task_id: str, *, reason: str) -> None:
        doc = await db.tasks.find_one({"id": task_id}, {"graph": 1})
        if not doc:
            return
        envs: List[Envelope] = []
        for n in (doc.get("graph", {}).get("nodes") or []):
            if n.get("status") in [RunState.running, RunState.deferred, RunState.queued]:
                cancel = CmdTaskCancel(cmd=CommandKind.TASK_CANCEL, reason=reason, cancel_token=str(uuid.uuid4()))
                envs.append(Envelope(
                    msg_type=MsgType.cmd,
                    role=Role.coordinator,
                    dedup_id=stable_hash({"cmd": str(CommandKind.TASK_CANCEL), "task_id": task_id, "node_id": n["node_id"], "epoch": int(n.get("attempt_epoch", 0))}),
                    task_id=task_id,
                    node_id=n["node_id"],
                    step_type=n["type"],
                    attempt_epoch=int(n.get("attempt_epoch", 0)),  # fencing: текущий epoch
                    payload=cancel.model_dump()
                ))
                await db.tasks.update_one({"id": task_id, "graph.nodes.node_id": n["node_id"]},
                                          {"$set": {"graph.nodes.$.status": RunState.cancelling}})
        if envs:
            await self.bus.send_cancel_broadcast(envs)
        # дождаться grace — опционально
        await asyncio.sleep(CANCEL_GRACE_SEC)

    # ── Resume inflight on startup ──────────────────────────────────────
    async def _resume_inflight(self) -> None:
        cur = db.tasks.find({"status": {"$in": [RunState.running, RunState.deferred, RunState.queued]}}, {"id": 1, "graph": 1, "status": 1})
        async for t in cur:
            # scheduler сделает остальное: preflight → adopt/start
            pass

    # ── Indexes ─────────────────────────────────────────────────────────
    async def _ensure_indexes(self) -> None:
        try:
            await db.tasks.create_index([("status", 1), ("updated_at", 1)], name="ix_task_status_updated")
            await db.worker_events.create_index([("task_id", 1), ("node_id", 1), ("event_hash", 1)], unique=True, name="uniq_worker_event")
            await db.worker_events.create_index([("ts_dt", 1)], name="ttl_worker_events", expireAfterSeconds=14*24*3600)
            await db.worker_registry.create_index([("worker_id", 1)], unique=True, name="uniq_worker")
            await db.artifacts.create_index([("task_id", 1), ("node_id", 1)], name="ix_artifacts_task_node")
        except Exception:
            pass

# ─────────────────────────── Minimal FastAPI ───────────────────────────
app = FastAPI(title="Universal DAG Coordinator")

COORD = Coordinator()

@app.on_event("startup")
async def _startup() -> None:
    await COORD.start()

@app.on_event("shutdown")
async def _shutdown() -> None:
    await COORD.stop()

class CreateTaskBody(BaseModel):
    params: Dict[str, Any] = Field(default_factory=dict)
    graph: Dict[str, Any]

@app.post("/tasks")
async def create_task(b: CreateTaskBody) -> Dict[str, Any]:
    """
    Пример:
    {
      "params": {"search_term":"чай", "category_id":"9373"},
      "graph": {
        "nodes": [
          {"node_id":"indexer","type":"indexer","depends_on":[],"routing":{"on_success":["postproc"]}},
          {"node_id":"postproc","type":"coordinator_fn","depends_on":["indexer"],"routing":{"on_success":["enricher"]}},
          {"node_id":"enricher","type":"enricher","depends_on":["postproc"]}
        ],
        "edges": [["indexer","postproc"],["postproc","enricher"]]
      }
    }
    """
    task_id = await COORD.create_task(params=b.params, graph=b.graph)
    # scheduler подхватит
    return {"task_id": task_id}

@app.get("/tasks/{task_id}")
async def get_task(task_id: str) -> Dict[str, Any]:
    t = await db.tasks.find_one({"id": task_id})
    if not t:
        raise HTTPException(status_code=404, detail="task not found")
    t["_id"] = str(t["_id"])
    return t
