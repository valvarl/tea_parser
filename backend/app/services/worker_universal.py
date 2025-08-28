# worker_universal.py
from __future__ import annotations

import asyncio
import os
import json
import time
import uuid
import signal
import hashlib
from collections import OrderedDict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Literal, AsyncIterator
from enum import Enum

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel, Field

# ─────────────────────────── App DB (motor) ───────────────────────────
# REPLACE with your motor db (AsyncIOMotorDatabase)
# from app.db.mongo import db
class _Fake:
    def __getattr__(self, k):  # type: ignore
        raise RuntimeError("Replace _Fake db with your motor db. from app.db.mongo import db")
db = _Fake()  # REPLACE

# ─────────────────────────── Config ───────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Topics aligned with Coordinator
TOPIC_CMD_FMT = os.getenv("TOPIC_CMD_FMT", "cmd.{type}.v1")
TOPIC_STATUS_FMT = os.getenv("TOPIC_STATUS_FMT", "status.{type}.v1")
TOPIC_WORKER_ANNOUNCE = os.getenv("TOPIC_WORKER_ANNOUNCE", "workers.announce.v1")
TOPIC_QUERY = os.getenv("TOPIC_QUERY", "query.tasks.v1")
TOPIC_REPLY = os.getenv("TOPIC_REPLY", "reply.tasks.v1")

# Heartbeat / lease
LEASE_TTL_SEC = int(os.getenv("LEASE_TTL_SEC", "60"))
HEARTBEAT_INTERVAL_SEC = int(os.getenv("HEARTBEAT_INTERVAL_SEC", "20"))

# Dedup
DEDUP_CACHE_SIZE = int(os.getenv("DEDUP_CACHE_SIZE", "10000"))
DEDUP_TTL_SEC = int(os.getenv("DEDUP_TTL_SEC", "3600"))

# State
STATE_DIR = os.getenv("WORKER_STATE_DIR", "./.worker_state")
os.makedirs(STATE_DIR, exist_ok=True)

# Worker identity
WORKER_ID = os.getenv("WORKER_ID", f"w-{uuid.uuid4().hex[:8]}")
WORKER_VERSION = os.getenv("WORKER_VERSION", "1.0.0")

# ─────────────────────────── Helpers ───────────────────────────
def now_ts() -> int:
    return int(time.time())

def now_dt() -> datetime:
    return datetime.now(timezone.utc)

def stable_hash(payload: Any) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(data.encode("utf-8")).hexdigest()

def dumps(x: Any) -> bytes:
    return json.dumps(x, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

def loads(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))

def log(**kv):
    ts = datetime.now(timezone.utc).isoformat()
    print(json.dumps({"ts": ts, **kv}, ensure_ascii=False), flush=True)

# ─────────────────────────── Protocol models ───────────────────────────
class MsgType(str, Enum):
    cmd = "cmd"
    event = "event"
    heartbeat = "heartbeat"
    query = "query"
    reply = "reply"

class RoleKind(str, Enum):
    coordinator = "coordinator"
    worker = "worker"

class CommandKind(str, Enum):
    TASK_START = "TASK_START"
    TASK_CANCEL = "TASK_CANCEL"
    TASK_PAUSE = "TASK_PAUSE"
    TASK_RESUME = "TASK_RESUME"

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

class QueryKind(str, Enum):
    TASK_DISCOVER = "TASK_DISCOVER"

class ReplyKind(str, Enum):
    TASK_SNAPSHOT = "TASK_SNAPSHOT"

class Envelope(BaseModel):
    v: int = 1
    msg_type: MsgType
    role: RoleKind
    corr_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    dedup_id: str
    task_id: str
    node_id: str
    step_type: str
    attempt_epoch: int
    ts: int = Field(default_factory=now_ts)
    payload: Dict[str, Any] = Field(default_factory=dict)
    target_worker_id: Optional[str] = None

# payloads
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

# ─────────────────────────── Local persistent state ────────────────────
@dataclass
class ActiveRun:
    task_id: str
    node_id: str
    step_type: str
    attempt_epoch: int
    lease_id: str
    cancel_token: str
    started_at_ts: int
    state: str  # running|finishing|cancelling
    checkpoint: Dict[str, Any]

class LocalState:
    def __init__(self, path: str) -> None:
        self.path = path
        self._fp = os.path.join(path, f"{WORKER_ID}.json")
        self._lock = asyncio.Lock()
        self.data: Dict[str, Any] = {}
        try:
            if os.path.exists(self._fp):
                with open(self._fp, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
        except Exception:
            self.data = {}

    async def write_active(self, ar: Optional[ActiveRun]) -> None:
        async with self._lock:
            if ar is None:
                self.data.pop("active_run", None)
            else:
                self.data["active_run"] = asdict(ar)
            self.data["updated_at"] = now_ts()
            tmp = self._fp + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp, self._fp)

    def read_active(self) -> Optional[ActiveRun]:
        d = self.data.get("active_run")
        if not d: return None
        return ActiveRun(**d)

# ─────────────────────────── Role handler API ──────────────────────────
class RunContext:
    def __init__(self, cancel_flag: asyncio.Event, artifacts_writer: "ArtifactsWriter"):
        self._cancel_flag = cancel_flag
        self.artifacts = artifacts_writer

    def cancelled(self) -> bool:
        return self._cancel_flag.is_set()

class Batch(BaseModel):
    shard_id: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)

class BatchResult(BaseModel):
    success: bool
    metrics: Dict[str, int] = Field(default_factory=dict)
    artifacts_ref: Optional[Dict[str, Any]] = None
    reason_code: Optional[str] = None
    permanent: bool = False
    error: Optional[str] = None

class FinalizeResult(BaseModel):
    metrics: Dict[str, int] = Field(default_factory=dict)
    artifacts_ref: Optional[Dict[str, Any]] = None

class RoleHandler:
    role: str

    async def init(self, cfg: Dict[str, Any]) -> None:
        pass

    async def load_input(self, input_ref: Optional[Dict[str, Any]], input_inline: Optional[Dict[str, Any]]) -> Any:
        return {"input_ref": input_ref, "input_inline": input_inline}

    async def iter_batches(self, loaded: Any) -> AsyncIterator[Batch]:
        # default: single batch with inline payload
        yield Batch(shard_id=None, payload=loaded or {})

    async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
        # default: no-op success
        await asyncio.sleep(0)  # yield
        return BatchResult(success=True, metrics={"processed": 1})

    async def finalize(self, ctx: RunContext) -> Optional[FinalizeResult]:
        return FinalizeResult(metrics={})

    def classify_error(self, exc: BaseException) -> tuple[str, bool]:
        # reason_code, permanent?
        return ("unexpected_error", False)

# Example handler for demo
class EchoHandler(RoleHandler):
    role = "echo"
    async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
        if ctx.cancelled():
            return BatchResult(success=False, reason_code="cancelled", permanent=False)
        # pretend work
        await asyncio.sleep(0.1)
        return BatchResult(success=True, metrics={"echoed": 1})

# ─────────────────────────── Artifacts writer ──────────────────────────
class ArtifactsWriter:
    def __init__(self, task_id: str, node_id: str, attempt_epoch: int, worker_id: str):
        self.task_id = task_id
        self.node_id = node_id
        self.attempt_epoch = attempt_epoch
        self.worker_id = worker_id

    async def upsert_partial(self, shard_id: Optional[str], meta: Dict[str, Any]) -> Dict[str, Any]:
        await db.artifacts.update_one(
            {"task_id": self.task_id, "node_id": self.node_id, "shard_id": shard_id},
            {"$setOnInsert": {"task_id": self.task_id, "node_id": self.node_id, "attempt_epoch": self.attempt_epoch,
                              "status": "partial", "worker_id": self.worker_id, "payload": None,
                              "created_at": now_dt()},
             "$set": {"meta": meta, "updated_at": now_dt()}},
            upsert=True
        )
        return {"task_id": self.task_id, "node_id": self.node_id, "shard_id": shard_id}

    async def mark_complete(self, meta: Dict[str, Any], artifacts_ref: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        await db.artifacts.update_one(
            {"task_id": self.task_id, "node_id": self.node_id},
            {"$set": {"status": "complete", "meta": meta, "updated_at": now_dt()},
             "$setOnInsert": {"attempt_epoch": self.attempt_epoch, "worker_id": self.worker_id,
                              "created_at": now_dt()}},
            upsert=True
        )
        return artifacts_ref or {"task_id": self.task_id, "node_id": self.node_id}

# ─────────────────────────── Worker core ───────────────────────────────
class Worker:
    def __init__(self, roles: List[str], handlers: Dict[str, RoleHandler]) -> None:
        self.roles = roles
        self.handlers = handlers
        self._producer: Optional[AIOKafkaProducer] = None
        self._cmd_consumers: Dict[str, AIOKafkaConsumer] = {}
        self._query_consumer: Optional[AIOKafkaConsumer] = None

        self._busy = False
        self._busy_lock = asyncio.Lock()
        self._cancel_flag = asyncio.Event()

        self.state = LocalState(STATE_DIR)
        self.active: Optional[ActiveRun] = self.state.read_active()

        # dedup of command envelopes
        self._dedup: OrderedDict[str, int] = OrderedDict()
        self._dedup_lock = asyncio.Lock()

        self._heartbeat_task: Optional[asyncio.Task] = None
        self._main_tasks: set[asyncio.Task] = set()
        self._stopping = False

    # ── lifecycle ───────────────────────────────────────────────────────
    async def start(self) -> None:
        await self._ensure_indexes()
        self._producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, value_serializer=dumps, enable_idempotence=True)
        await self._producer.start()

        # Announce ONLINE (with resume info)
        await self._send_announce(EventKind.WORKER_ONLINE, extra={
            "worker_id": WORKER_ID, "type": ",".join(self.roles), "capabilities": {"roles": self.roles},
            "version": WORKER_VERSION, "capacity": {"tasks": 1},
            "resume": asdict(self.active) if self.active else None
        })

        # Command consumers per role
        for role in self.roles:
            topic = TOPIC_CMD_FMT.format(type=role)
            c = AIOKafkaConsumer(topic, bootstrap_servers=KAFKA_BOOTSTRAP, value_deserializer=loads,
                                 enable_auto_commit=False, auto_offset_reset="latest",
                                 group_id=f"workers.{role}.v1")
            await c.start()
            self._cmd_consumers[role] = c
            self._spawn(self._cmd_loop(role, c))

        # Query consumer (discovery)
        self._query_consumer = AIOKafkaConsumer(TOPIC_QUERY, bootstrap_servers=KAFKA_BOOTSTRAP,
                                                value_deserializer=loads, enable_auto_commit=False,
                                                auto_offset_reset="latest", group_id=f"workers.query.v1")
        await self._query_consumer.start()
        self._spawn(self._query_loop(self._query_consumer))

        # Heartbeat announce (optional every 60s)
        self._spawn(self._periodic_announce())

        # If recovering active run, set busy and resume execution (coordinator may adopt/cancel)
        if self.active:
            self._busy = True
            self._cancel_flag.clear()
            log(event="recovery_start", task_id=self.active.task_id, node_id=self.active.node_id)

    async def stop(self) -> None:
        self._stopping = True
        for t in list(self._main_tasks):
            t.cancel()
        self._main_tasks.clear()
        if self._query_consumer:
            try: await self._query_consumer.stop()
            except Exception: pass
        for c in self._cmd_consumers.values():
            try: await c.stop()
            except Exception: pass
        self._cmd_consumers.clear()
        if self._producer:
            try:
                await self._send_announce(EventKind.WORKER_OFFLINE, extra={"worker_id": WORKER_ID})
                await self._producer.stop()
            except Exception:
                pass
        self._producer = None

    def _spawn(self, coro):
        t = asyncio.create_task(coro)
        self._main_tasks.add(t)
        t.add_done_callback(self._main_tasks.discard)

    # ── Kafka send helpers ──────────────────────────────────────────────
    async def _send_status(self, role: str, env: Envelope) -> None:
        assert self._producer is not None
        topic = TOPIC_STATUS_FMT.format(type=role)
        key = f"{env.task_id}:{env.node_id}".encode("utf-8")
        await self._producer.send_and_wait(topic, env.model_dump(mode="json"), key=key)

    async def _send_announce(self, kind: EventKind, extra: Dict[str, Any]) -> None:
        assert self._producer is not None
        payload = {"kind": kind, **extra}
        env = Envelope(msg_type=MsgType.event, role=RoleKind.worker,
                       dedup_id=stable_hash({"announce": kind, "worker": WORKER_ID, "ts": now_ts()}),
                       task_id="*", node_id="*", step_type="*", attempt_epoch=0, payload=payload)
        await self._producer.send_and_wait(TOPIC_WORKER_ANNOUNCE, env.model_dump(mode="json"))

    async def _send_reply(self, env: Envelope) -> None:
        assert self._producer is not None
        key = (env.task_id or "*").encode("utf-8")
        await self._producer.send_and_wait(TOPIC_REPLY, env.model_dump(mode="json"), key=key)

    # ── Dedup for incoming commands ─────────────────────────────────────
    async def _seen_or_add(self, dedup_id: str) -> bool:
        async with self._dedup_lock:
            # purge old
            now = now_ts()
            for k in list(self._dedup.keys()):
                if now - self._dedup[k] > DEDUP_TTL_SEC:
                    self._dedup.pop(k, None)
            if dedup_id in self._dedup:
                return True
            self._dedup[dedup_id] = now
            # LRU trim
            while len(self._dedup) > DEDUP_CACHE_SIZE:
                self._dedup.popitem(last=False)
            return False

    # ── Command loop (single-concurrency) ───────────────────────────────
    async def _cmd_loop(self, role: str, consumer: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await consumer.getone()
                env = Envelope.model_validate(msg.value)
                if await self._seen_or_add(env.dedup_id):
                    await consumer.commit(); continue
                if env.msg_type != MsgType.cmd or env.role != RoleKind.coordinator:
                    await consumer.commit(); continue
                if env.step_type != role:
                    await consumer.commit(); continue
                kind = env.payload.get("cmd")
                if kind == CommandKind.TASK_START:
                    await self._handle_task_start(role, env, consumer)
                elif kind == CommandKind.TASK_CANCEL:
                    await self._handle_task_cancel(role, env, consumer)
                else:
                    await consumer.commit()
        except asyncio.CancelledError:
            return

    async def _handle_task_start(self, role: str, env: Envelope, consumer: AIOKafkaConsumer) -> None:
        cmd = CmdTaskStart.model_validate(env.payload)
        async with self._busy_lock:
            if self._busy:
                # we are busy → ignore other jobs
                await consumer.commit()
                return
            # fencing: if we are recovering a different run, ignore
            if self.active and not (self.active.task_id == env.task_id and self.active.node_id == env.node_id):
                await consumer.commit()
                return

            # accept
            lease_id = str(uuid.uuid4())
            lease_deadline = now_ts() + LEASE_TTL_SEC
            self._busy = True
            self._cancel_flag.clear()
            self.active = ActiveRun(
                task_id=env.task_id, node_id=env.node_id, step_type=role,
                attempt_epoch=env.attempt_epoch, lease_id=lease_id, cancel_token=cmd.cancel_token,
                started_at_ts=now_ts(), state="running", checkpoint={}
            )
            await self.state.write_active(self.active)

            # Announce TASK_ACCEPTED
            acc_env = Envelope(msg_type=MsgType.event, role=RoleKind.worker,
                               dedup_id=stable_hash({"acc": env.task_id, "n": env.node_id, "e": env.attempt_epoch, "lease": lease_id}),
                               task_id=env.task_id, node_id=env.node_id, step_type=role, attempt_epoch=env.attempt_epoch,
                               payload={"kind": EventKind.TASK_ACCEPTED, "worker_id": WORKER_ID,
                                        "lease_id": lease_id, "lease_deadline_ts": lease_deadline})
            await self._send_status(role, acc_env)

            # Pause all cmd-consumers to keep single-concurrency strict
            await self._pause_all_cmd_consumers()

            # Start heartbeat + run loop
            self._spawn(self._heartbeat_loop(role))
            self._spawn(self._run_handler(role, env, cmd))

            await consumer.commit()

    async def _handle_task_cancel(self, role: str, env: Envelope, consumer: AIOKafkaConsumer) -> None:
        cmd = CmdTaskCancel.model_validate(env.payload)
        # Only respect cancel that matches active run & epoch
        if self.active and self.active.task_id == env.task_id and self.active.node_id == env.node_id \
           and self.active.attempt_epoch == env.attempt_epoch:
            self._cancel_flag.set()
            self.active.state = "cancelling"
            await self.state.write_active(self.active)
            # We'll send CANCELLED when run loop observes cancellation or in finalization
        await consumer.commit()

    async def _pause_all_cmd_consumers(self) -> None:
        for role, c in self._cmd_consumers.items():
            parts = c.assignment()
            if parts:
                c.pause(*parts)

    async def _resume_all_cmd_consumers(self) -> None:
        for role, c in self._cmd_consumers.items():
            parts = c.assignment()
            if parts:
                c.resume(*parts)

    # ── Handler execution ───────────────────────────────────────────────
    async def _run_handler(self, role: str, start_env: Envelope, cmd: CmdTaskStart) -> None:
        assert self.active is not None
        handler = self.handlers.get(role)
        if not handler:
            # no handler → hard fail
            await self._emit_task_failed(role, start_env, "no_handler", True, "handler not registered")
            await self._cleanup_after_run()
            return

        try:
            await handler.init({})
            artifacts = ArtifactsWriter(self.active.task_id, self.active.node_id, self.active.attempt_epoch, WORKER_ID)
            ctx = RunContext(self._cancel_flag, artifacts)
            loaded = await handler.load_input(cmd.input_ref, cmd.input_inline)

            # iterate batches
            async for batch in handler.iter_batches(loaded):
                if self._cancel_flag.is_set():
                    raise asyncio.CancelledError()
                # optional claimed event
                # await self._emit_batch_claimed(role, start_env, batch)
                res = await handler.process_batch(batch, ctx)
                if res.success:
                    # write partial if provided
                    if batch.shard_id is not None:
                        await artifacts.upsert_partial(batch.shard_id, res.metrics or {})
                    await self._emit_batch_ok(role, start_env, batch, res)
                else:
                    await self._emit_batch_failed(role, start_env, batch, res)
                    if res.permanent:
                        raise RuntimeError(f"permanent:{res.reason_code or 'error'}")
                    # transient failure → continue (coordinator решит про deferred)

            # finalize
            fin = await handler.finalize(ctx)
            metrics = (fin.metrics if fin else {}) if fin else {}
            ref = (fin.artifacts_ref if fin else None)
            # mark complete artifacts
            ref = await artifacts.mark_complete(metrics, ref)
            await self._emit_task_done(role, start_env, metrics, ref)

        except asyncio.CancelledError:
            await self._emit_cancelled(role, start_env, "cancelled")
        except Exception as e:
            # classify
            reason, permanent = handler.classify_error(e) if handler else ("unexpected_error", False)
            await self._emit_task_failed(role, start_env, reason, permanent, str(e))
        finally:
            await self._cleanup_after_run()

    async def _cleanup_after_run(self) -> None:
        self._cancel_flag.clear()
        self.active = None
        await self.state.write_active(None)
        self._busy = False
        await self._resume_all_cmd_consumers()

    # ── Heartbeat loop ──────────────────────────────────────────────────
    async def _heartbeat_loop(self, role: str) -> None:
        assert self.active is not None
        try:
            while self._busy and not self._stopping and self.active is not None:
                lease_deadline = now_ts() + LEASE_TTL_SEC
                hb_env = Envelope(msg_type=MsgType.event, role=RoleKind.worker,
                                  dedup_id=stable_hash({"hb": self.active.task_id, "n": self.active.node_id, "e": self.active.attempt_epoch, "t": int(time.time()/HEARTBEAT_INTERVAL_SEC)}),
                                  task_id=self.active.task_id, node_id=self.active.node_id, step_type=role,
                                  attempt_epoch=self.active.attempt_epoch,
                                  payload={"kind": EventKind.TASK_HEARTBEAT, "worker_id": WORKER_ID,
                                           "lease_id": self.active.lease_id, "lease_deadline_ts": lease_deadline})
                await self._send_status(role, hb_env)
                await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
        except asyncio.CancelledError:
            return

    # ── Status emitters ────────────────────────────────────────────────
    async def _emit_batch_ok(self, role: str, base: Envelope, batch: Batch, res: BatchResult) -> None:
        env = Envelope(
            msg_type=MsgType.event, role=RoleKind.worker,
            dedup_id=stable_hash({"bok": base.task_id, "n": base.node_id, "e": base.attempt_epoch, "shard": batch.shard_id, "m": res.metrics}),
            task_id=base.task_id, node_id=base.node_id, step_type=role, attempt_epoch=base.attempt_epoch,
            payload={"kind": EventKind.BATCH_OK, "worker_id": WORKER_ID, "batch_id": None,
                     "metrics": res.metrics or {}, "artifacts_ref": res.artifacts_ref}
        )
        await self._send_status(role, env)

    async def _emit_batch_failed(self, role: str, base: Envelope, batch: Batch, res: BatchResult) -> None:
        env = Envelope(
            msg_type=MsgType.event, role=RoleKind.worker,
            dedup_id=stable_hash({"bf": base.task_id, "n": base.node_id, "e": base.attempt_epoch, "shard": batch.shard_id, "r": res.reason_code}),
            task_id=base.task_id, node_id=base.node_id, step_type=role, attempt_epoch=base.attempt_epoch,
            payload={"kind": EventKind.BATCH_FAILED, "worker_id": WORKER_ID, "batch_id": None,
                     "reason_code": res.reason_code or "error", "permanent": bool(res.permanent), "error": res.error}
        )
        await self._send_status(role, env)

    async def _emit_task_done(self, role: str, base: Envelope, metrics: Dict[str, int], artifacts_ref: Optional[Dict[str, Any]]) -> None:
        env = Envelope(
            msg_type=MsgType.event, role=RoleKind.worker,
            dedup_id=stable_hash({"td": base.task_id, "n": base.node_id, "e": base.attempt_epoch}),
            task_id=base.task_id, node_id=base.node_id, step_type=role, attempt_epoch=base.attempt_epoch,
            payload={"kind": EventKind.TASK_DONE, "worker_id": WORKER_ID, "metrics": metrics or {}, "artifacts_ref": artifacts_ref}
        )
        await self._send_status(role, env)

    async def _emit_task_failed(self, role: str, base: Envelope, reason: str, permanent: bool, error: Optional[str]) -> None:
        env = Envelope(
            msg_type=MsgType.event, role=RoleKind.worker,
            dedup_id=stable_hash({"tf": base.task_id, "n": base.node_id, "e": base.attempt_epoch, "r": reason, "p": permanent}),
            task_id=base.task_id, node_id=base.node_id, step_type=role, attempt_epoch=base.attempt_epoch,
            payload={"kind": EventKind.TASK_FAILED, "worker_id": WORKER_ID, "reason_code": reason, "permanent": bool(permanent), "error": error}
        )
        await self._send_status(role, env)

    async def _emit_cancelled(self, role: str, base: Envelope, reason: str) -> None:
        env = Envelope(
            msg_type=MsgType.event, role=RoleKind.worker,
            dedup_id=stable_hash({"c": base.task_id, "n": base.node_id, "e": base.attempt_epoch}),
            task_id=base.task_id, node_id=base.node_id, step_type=role, attempt_epoch=base.attempt_epoch,
            payload={"kind": EventKind.CANCELLED, "worker_id": WORKER_ID, "reason": reason}
        )
        await self._send_status(role, env)

    # ── Discovery (TASK_DISCOVER → TASK_SNAPSHOT) ───────────────────────
    async def _query_loop(self, consumer: AIOKafkaConsumer) -> None:
        try:
            while True:
                msg = await consumer.getone()
                env = Envelope.model_validate(msg.value)
                if env.msg_type != MsgType.query or env.role != RoleKind.coordinator:
                    await consumer.commit(); continue
                if env.payload.get("query") != QueryKind.TASK_DISCOVER:
                    await consumer.commit(); continue

                # Reply only if we can potentially handle this step_type
                if env.step_type not in self.roles:
                    await consumer.commit(); continue

                # Build snapshot
                ar = self.active
                if ar and ar.task_id == env.task_id and ar.node_id == env.node_id:
                    run_state = "cancelling" if self._cancel_flag.is_set() else "running"
                    payload = {
                        "reply": ReplyKind.TASK_SNAPSHOT,
                        "worker_id": WORKER_ID,
                        "run_state": run_state,
                        "attempt_epoch": ar.attempt_epoch,
                        "lease": {"worker_id": WORKER_ID, "lease_id": ar.lease_id, "deadline_ts": now_ts() + LEASE_TTL_SEC},
                        "progress": {},  # optionally fill
                        "artifacts": None
                    }
                else:
                    # Check artifacts existence to hint completeness
                    # (best effort: query db.artifacts quickly)
                    complete = False
                    try:
                        cnt = await db.artifacts.count_documents({"task_id": env.task_id, "node_id": env.node_id, "status": "complete"})
                        complete = cnt > 0
                    except Exception:
                        pass
                    payload = {
                        "reply": ReplyKind.TASK_SNAPSHOT,
                        "worker_id": None,
                        "run_state": "idle",
                        "attempt_epoch": ar.attempt_epoch if ar else 0,
                        "lease": None,
                        "progress": None,
                        "artifacts": {"complete": complete} if complete else None
                    }

                reply = Envelope(msg_type=MsgType.reply, role=RoleKind.worker,
                                 dedup_id=stable_hash({"snap": env.task_id, "n": env.node_id, "e": env.attempt_epoch, "w": WORKER_ID}),
                                 task_id=env.task_id, node_id=env.node_id, step_type=env.step_type,
                                 attempt_epoch=env.attempt_epoch, payload=payload, corr_id=env.corr_id)
                await self._send_reply(reply)
                await consumer.commit()
        except asyncio.CancelledError:
            return

    async def _periodic_announce(self) -> None:
        try:
            while not self._stopping:
                await asyncio.sleep(60)
                await self._send_announce(EventKind.WORKER_ONLINE, extra={
                    "worker_id": WORKER_ID, "type": ",".join(self.roles),
                    "version": WORKER_VERSION, "capacity": {"tasks": 1}
                })
        except asyncio.CancelledError:
            return

    # ── DB indexes (optional) ───────────────────────────────────────────
    async def _ensure_indexes(self) -> None:
        try:
            await db.artifacts.create_index([("task_id", 1), ("node_id", 1)], name="ix_artifacts_task_node")
        except Exception:
            pass

# ─────────────────────────── Bootstrap ─────────────────────────────────
async def _main() -> None:
    roles = [s.strip() for s in os.getenv("WORKER_ROLES", "echo").split(",") if s.strip()]
    handlers: Dict[str, RoleHandler] = {}

    # register your concrete handlers here
    # e.g., handlers["indexer"] = IndexerHandler()
    #       handlers["enricher"] = EnricherHandler()
    if "echo" in roles:
        handlers["echo"] = EchoHandler()

    worker = Worker(roles=roles, handlers=handlers)

    loop = asyncio.get_running_loop()
    stop_ev = asyncio.Event()

    def _sig(*_):
        log(event="signal_stop")
        stop_ev.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig)
        except NotImplementedError:
            pass

    await worker.start()
    await stop_ev.wait()
    await worker.stop()

if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
