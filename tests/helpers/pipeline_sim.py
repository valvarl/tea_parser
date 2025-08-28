# tests/helpers/pipeline_sim.py
import asyncio
import random
import sys
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional, Iterable

# ====================== tiny logger ======================
def _ts() -> str: 
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]

def dbg(tag: str, **kv):
    kvs = " ".join(f"{k}={kv[k]!r}" for k in kv)
    print(f"[{_ts()}] {tag}: {kvs}", flush=True)

# ---------------------- Chaos config/engine -----------------------------
@dataclass
class ChaosConfig:
    seed: int = 42
    # delays (seconds)
    broker_delay_range: Tuple[float, float] = (0.0, 0.004)   # produce / getone jitter
    consumer_poll_delay_range: Tuple[float, float] = (0.0, 0.002)
    handler_delay_range: Tuple[float, float] = (0.0, 0.006)

    # duplication/drop
    dup_prob_by_topic: Dict[str, float] = None  # e.g. {"status.": 0.1, "cmd.": 0.05}
    drop_prob_by_topic: Dict[str, float] = None # keep small or empty, or tests may stall

    # crashes
    handler_crash_prob: float = 0.0        # chance to raise transient error in process_batch
    handler_perm_crash_prob: float = 0.0   # chance to raise permanent error
    # toggles
    enable_broker_delays: bool = True
    enable_consumer_delays: bool = True
    enable_handler_delays: bool = True
    enable_dup_drop: bool = True
    enable_crashes: bool = False

    def __post_init__(self):
        if self.dup_prob_by_topic is None:  self.dup_prob_by_topic = {}
        if self.drop_prob_by_topic is None: self.drop_prob_by_topic = {}

class ChaosMonkey:
    def __init__(self, cfg: ChaosConfig):
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)

    def _topic_prob(self, topic: str, table: Dict[str, float]) -> float:
        # match by prefix
        for pref, p in table.items():
            if topic.startswith(pref): return p
        return 0.0

    async def broker_jitter(self):
        if not self.cfg.enable_broker_delays: return
        lo, hi = self.cfg.broker_delay_range
        if hi <= 0: return
        dt = self.rng.uniform(lo, hi)
        await asyncio.sleep(dt)

    async def consumer_poll_jitter(self):
        if not self.cfg.enable_consumer_delays: return
        lo, hi = self.cfg.consumer_poll_delay_range
        if hi <= 0: return
        dt = self.rng.uniform(lo, hi)
        await asyncio.sleep(dt)

    async def handler_delay(self):
        if not self.cfg.enable_handler_delays: return
        lo, hi = self.cfg.handler_delay_range
        if hi <= 0: return
        dt = self.rng.uniform(lo, hi)
        await asyncio.sleep(dt)

    def should_duplicate(self, topic: str) -> bool:
        if not self.cfg.enable_dup_drop: return False
        p = self._topic_prob(topic, self.cfg.dup_prob_by_topic)
        return self.rng.random() < p

    def should_drop(self, topic: str) -> bool:
        if not self.cfg.enable_dup_drop: return False
        p = self._topic_prob(topic, self.cfg.drop_prob_by_topic)
        return self.rng.random() < p

    def maybe_handler_crash(self, permanent: bool = False):
        if not self.cfg.enable_crashes: return
        p = self.cfg.handler_perm_crash_prob if permanent else self.cfg.handler_crash_prob
        if p > 0 and self.rng.random() < p:
            raise RuntimeError("chaos: handler permanent failure" if permanent else "chaos: handler transient failure")

# ====================== In-memory Kafka ==================
class _Rec:
    __slots__ = ("value","topic")
    def __init__(self, value, topic):
        self.value = value
        self.topic = topic

class InMemKafkaBroker:
    def __init__(self, chaos: Optional[ChaosMonkey] = None):
        # topic -> {group_id: queue}
        self.topics: Dict[str, Dict[str, asyncio.Queue]] = {}  # topic -> {group_id: queue}
        self.rev: Dict[int, str] = {}
        self.chaos = chaos or ChaosMonkey(ChaosConfig())

    def ensure_queue(self, topic: str, group_id: str) -> asyncio.Queue:
        tg = self.topics.setdefault(topic, {})
        q = tg.get(group_id)
        if q is None:
            q = asyncio.Queue()
            tg[group_id] = q
            self.rev[id(q)] = topic
            dbg("KAFKA.GROUP_BIND", topic=topic, group_id=group_id)
        return q

    def topic_of(self, q: asyncio.Queue) -> str:
        return self.rev.get(id(q), "?")

    async def produce(self, topic: str, value: Any):
        # chaos: delay + drop + duplicate
        await self.chaos.broker_jitter()
        payload = value
        msg_type = payload.get("msg_type")
        kind = (payload.get("payload") or {}).get("kind") or (payload.get("payload") or {}).get("reply")
        dbg("KAFKA.PRODUCE", topic=topic, msg_type=msg_type, kind=kind)

        if self.chaos.should_drop(topic):
            dbg("KAFKA.DROP", topic=topic)
            return

        deliver_times = 2 if self.chaos.should_duplicate(topic) else 1
        for _ in range(deliver_times):
            for q in self.topics.setdefault(topic, {}).values():
                await q.put(_Rec(value, topic))
            if deliver_times == 2:
                dbg("KAFKA.DUP", topic=topic)

BROKER = InMemKafkaBroker()

class AIOKafkaProducerMockChaos:
    def __init__(self, *_, **__): pass
    async def start(self): dbg("PRODUCER.START")
    async def stop(self): dbg("PRODUCER.STOP")
    async def send_and_wait(self, topic: str, value: Any, key: bytes | None = None):
        assert BROKER is not None, "BROKER not initialized"
        await BROKER.produce(topic, value)

class AIOKafkaConsumerMockChaos:
    def __init__(self, *topics, bootstrap_servers=None, group_id=None, value_deserializer=None,
                 enable_auto_commit=False, auto_offset_reset="latest"):
        self._topics = list(topics)
        self._group = group_id or "default"
        self._deser = value_deserializer
        self._queues: List[asyncio.Queue] = []
        self._paused = False

    async def start(self):
        assert BROKER is not None, "BROKER not initialized"
        self._queues = [BROKER.ensure_queue(t, self._group) for t in self._topics]
        dbg("CONSUMER.START", group_id=self._group, topics=self._topics)

    async def stop(self): dbg("CONSUMER.STOP", group_id=self._group)

    async def getone(self):
        assert BROKER is not None
        while True:
            if self._paused:
                await asyncio.sleep(0.01); continue
            for q in self._queues:
                try:
                    rec = q.get_nowait()
                    val = rec.value
                    msg_type = val.get("msg_type")
                    kind = (val.get("payload") or {}).get("kind") or (val.get("payload") or {}).get("reply")
                    dbg("CONSUMER.GET", group_id=self._group, topic=BROKER.topic_of(q), msg_type=msg_type, kind=kind)
                    await BROKER.chaos.consumer_poll_jitter()
                    return rec
                except asyncio.QueueEmpty:
                    continue
            await asyncio.sleep(0.003)

    async def commit(self): pass
    def pause(self, *parts): self._paused = True;  dbg("CONSUMER.PAUSE", group_id=self._group)
    def resume(self, *parts): self._paused = False; dbg("CONSUMER.RESUME", group_id=self._group)
    def assignment(self): return {("t",0)}

class AIOKafkaProducerMock:
    def __init__(self, *_, **__): pass
    async def start(self): dbg("PRODUCER.START")
    async def stop(self): dbg("PRODUCER.STOP")
    async def send_and_wait(self, topic: str, value: Any, key: bytes | None = None):
        await BROKER.produce(topic, value)

class AIOKafkaConsumerMock:
    def __init__(self, *topics, bootstrap_servers=None, group_id=None, value_deserializer=None,
                 enable_auto_commit=False, auto_offset_reset="latest"):
        self._topics = list(topics)
        self._group = group_id or "default"
        self._deser = value_deserializer
        self._queues: List[asyncio.Queue] = []
        self._paused = False

    async def start(self):
        self._queues = [BROKER.ensure_queue(t, self._group) for t in self._topics]
        dbg("CONSUMER.START", group_id=self._group, topics=self._topics)

    async def stop(self): dbg("CONSUMER.STOP", group_id=self._group)

    async def getone(self):
        while True:
            if self._paused:
                await asyncio.sleep(0.01); continue
            for q in self._queues:
                try:
                    rec = q.get_nowait()
                    val = rec.value
                    msg_type = val.get("msg_type")
                    kind = (val.get("payload") or {}).get("kind") or (val.get("payload") or {}).get("reply")
                    dbg("CONSUMER.GET", group_id=self._group, topic=BROKER.topic_of(q), msg_type=msg_type, kind=kind)
                    return rec
                except asyncio.QueueEmpty:
                    continue
            await asyncio.sleep(0.005)

    async def commit(self): pass
    def pause(self, *parts): self._paused = True;  dbg("CONSUMER.PAUSE", group_id=self._group)
    def resume(self, *parts): self._paused = False; dbg("CONSUMER.RESUME", group_id=self._group)
    def assignment(self): return {("t",0)}

# ====================== In-memory Mongo ==================
def _now_dt(): return datetime.now(timezone.utc)

class InMemCollection:
    def __init__(self, name: str):
        self.name = name
        self.rows: List[Dict[str, Any]] = []

    @staticmethod
    def _get_path(doc, path):
        cur = doc
        for p in path.split("."):
            if isinstance(cur, list) and p.isdigit():
                idx = int(p)
                if idx >= len(cur): return None
                cur = cur[idx]
            elif isinstance(cur, dict):
                cur = cur.get(p)
            else:
                return None
            if cur is None:
                return None
        return cur

    def _match(self, doc, flt):
        # normalize enums to raw values for fair comparison
        from enum import Enum
        def _norm(x):
            return x.value if isinstance(x, Enum) else x

        for k, v in (flt or {}).items():
            val = self._get_path(doc, k)
            val = _norm(val)

            if isinstance(v, dict):
                if "$in" in v:
                    in_list = [ _norm(x) for x in v["$in"] ]
                    if val not in in_list:
                        return False
                if "$lte" in v:
                    if not (val is not None and val <= _norm(v["$lte"])):
                        return False
                if "$lt" in v:
                    if not (val is not None and val < _norm(v["$lt"])):
                        return False
                if "$gte" in v:
                    if not (val is not None and val >= _norm(v["$gte"])):
                        return False
                if "$gt" in v:
                    if not (val is not None and val > _norm(v["$gt"])):
                        return False
                continue

            # спец-случай для проверки наличия node_id в списке узлов графа
            if k == "graph.nodes.node_id":
                nodes = (((doc.get("graph") or {}).get("nodes")) or [])
                if not any((n.get("node_id") == v) for n in nodes):
                    return False
                continue

            if _norm(v) != val:
                return False

        return True

    async def insert_one(self, doc):
        self.rows.append(dict(doc))
        if self.name == "outbox":
            dbg("DB.OUTBOX.INSERT", size=len(self.rows), doc_keys=list(doc.keys()))

    async def find_one(self, flt, proj=None):
        for d in self.rows:
            if self._match(d, flt): return d
        return None

    async def count_documents(self, flt):
        return sum(1 for d in self.rows if self._match(d, flt))

    def find(self, flt, proj=None):
        rows = [d for d in self.rows if self._match(d, flt)]
        class _Cur:
            def __init__(self, rows): self._rows = rows
            def sort(self, *_): return self
            def limit(self, n): self._rows = self._rows[:n]; return self
            async def __aiter__(self):
                for r in list(self._rows): yield r
        return _Cur(rows)

    async def update_one(self, flt, upd, upsert=False):
        doc = None
        for d in self.rows:
            if self._match(d, flt): doc = d; break

        created = False
        if not doc:
            if not upsert: return
            doc = {}
            self.rows.append(doc)
            created = True

        node_idx = None
        if "graph.nodes.node_id" in flt:
            target = flt["graph.nodes.node_id"]
            nodes = (((doc.get("graph") or {}).get("nodes")) or [])
            for i, n in enumerate(nodes):
                if n.get("node_id") == target:
                    node_idx = i
                    break

        def set_path(m, path, val):
            parts = path.split(".")
            cur = m
            for i, p in enumerate(parts):
                if p == "$": p = str(node_idx)
                last = i == len(parts) - 1
                if p.isdigit() and isinstance(cur, list):
                    idx = int(p)
                    while len(cur) <= idx: cur.append({})
                    if last: cur[idx] = val
                    else:
                        if not isinstance(cur[idx], dict): cur[idx] = {}
                        cur = cur[idx]
                else:
                    if last: cur[p] = val
                    else:
                        if p not in cur or not isinstance(cur[p], (dict, list)):
                            cur[p] = [] if parts[i + 1].isdigit() else {}
                        cur = cur[p]

        # $setOnInsert — только при fresh upsert
        if "$setOnInsert" in upd and created:
            for k, v in upd["$setOnInsert"].items():
                set_path(doc, k, v)
            if self.name == "artifacts" and "status" in upd["$setOnInsert"]:
                dbg("DB.ARTIFACTS.STATUS", filter=flt, new_status=str(upd["$setOnInsert"]["status"]))

        if "$set" in upd:
            if self.name == "tasks":
                for k, v in upd["$set"].items():
                    if k.endswith("graph.nodes.$.status"):
                        dbg("DB.TASK.STATUS", filter=flt, new_status=str(v))
            if self.name == "artifacts" and "status" in upd["$set"]:
                dbg("DB.ARTIFACTS.STATUS", filter=flt, new_status=str(upd["$set"]["status"]))
            for k, v in upd["$set"].items():
                set_path(doc, k, v)

        if "$inc" in upd:
            for k, v in upd["$inc"].items():
                parts = k.split(".")
                cur = doc
                for i, p in enumerate(parts):
                    if p == "$":  # подставляем индекс найденного узла
                        p = str(node_idx)
                    last = (i == len(parts) - 1)

                    if isinstance(cur, list) and p.isdigit():
                        idx = int(p)
                        while len(cur) <= idx:
                            cur.append({})
                        if last:
                            cur[idx] = int((cur[idx] or 0)) + int(v) if isinstance(cur[idx], (int, float)) else int(v)
                        else:
                            if not isinstance(cur[idx], (dict, list)):
                                # заранее создаём нужный контейнер
                                cur[idx] = [] if (i + 1 < len(parts) and parts[i + 1].isdigit()) else {}
                            cur = cur[idx]
                    else:
                        if last:
                            cur[p] = int((cur.get(p, 0) or 0)) + int(v)
                        else:
                            if p not in cur or not isinstance(cur[p], (dict, list)):
                                cur[p] = [] if (i + 1 < len(parts) and parts[i + 1].isdigit()) else {}
                            cur = cur[p]

        if "$max" in upd:
            for k, v in upd["$max"].items():
                parts = k.split(".")
                cur = doc
                for i, p in enumerate(parts):
                    if p == "$":
                        p = str(node_idx)
                    last = (i == len(parts) - 1)

                    if isinstance(cur, list) and p.isdigit():
                        idx = int(p)
                        while len(cur) <= idx:
                            cur.append({})
                        if last:
                            cur[idx] = max(int(cur[idx] or 0), int(v)) if isinstance(cur[idx], (int, float)) else int(v)
                        else:
                            if not isinstance(cur[idx], (dict, list)):
                                cur[idx] = [] if (i + 1 < len(parts) and parts[i + 1].isdigit()) else {}
                            cur = cur[idx]
                    else:
                        if last:
                            cur[p] = max(int(cur.get(p, 0) or 0), int(v))
                        else:
                            if p not in cur or not isinstance(cur[p], (dict, list)):
                                cur[p] = [] if (i + 1 < len(parts) and parts[i + 1].isdigit()) else {}
                            cur = cur[p]

        if "$currentDate" in upd:
            for k, _ in upd["$currentDate"].items():
                set_path(doc, k, _now_dt())

    async def find_one_and_update(self, flt, upd):
        doc = await self.find_one(flt)
        await self.update_one(flt, upd)
        return doc

    async def create_index(self, *a, **k): return "ok"

class InMemDB:
    def __init__(self):
        self.tasks          = InMemCollection("tasks")
        self.worker_events  = InMemCollection("worker_events")
        self.worker_registry= InMemCollection("worker_registry")
        self.artifacts      = InMemCollection("artifacts")
        self.outbox         = InMemCollection("outbox")
        self.stream_progress= InMemCollection("stream_progress")

# ====================== setup helpers ====================
def setup_env_and_imports(monkeypatch) -> Tuple[Any, Any]:
    """Подготавливает окружение, патчит Kafka на in-mem и возвращает (coordinator_dag, worker_universal)."""
    monkeypatch.setenv("WORKER_TYPES", "indexer,enricher,ocr,analyzer")
    monkeypatch.setenv("HB_SOFT_SEC", "120")
    monkeypatch.setenv("HB_HARD_SEC", "600")

    # путь до backend
    if "/home/valvarl/tea_parser/backend" not in sys.path:
        sys.path.insert(0, "/home/valvarl/tea_parser/backend")

    from app.services import coordinator_dag as cd
    from app.services import worker_universal as wu

    # гарантируем обычный asyncio внутри модулей
    import asyncio as aio
    monkeypatch.setattr(cd, "asyncio", aio, raising=False)
    monkeypatch.setattr(wu, "asyncio", aio, raising=False)

    # подменяем Kafka
    monkeypatch.setattr(cd, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(cd, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)

    # быстрые тики
    cd.SCHEDULER_TICK_SEC = 0.05
    cd.DISCOVERY_WINDOW_SEC = 0.05
    cd.FINALIZER_TICK_SEC = 0.05
    cd.HB_MONITOR_TICK_SEC = 0.2

    # outbox: по умолчанию — байпас в тестах (можно выключить выставив TEST_USE_OUTBOX=1)
    if os.getenv("TEST_USE_OUTBOX", "0") != "1":
        async def _enqueue_direct(self, *, topic: str, key: str, env):
            dbg("OUTBOX.BYPASS", topic=topic)
            await self.bus._raw_send(topic, key.encode("utf-8"), env)
        monkeypatch.setattr(cd.OutboxDispatcher, "enqueue", _enqueue_direct, raising=False)

    dbg("ENV.READY")
    return cd, wu

def install_inmemory_db(monkeypatch, cd, wu) -> InMemDB:
    db = InMemDB()
    monkeypatch.setattr(cd, "db", db, raising=True)
    monkeypatch.setattr(wu, "db", db, raising=True)
    dbg("DB.INSTALLED")
    return db

# ====================== graph helpers ====================
def build_graph(total_skus=12, batch_size=5, mini_batch=2) -> Dict[str, Any]:
    return {
      "schema_version": "1.0",
      "nodes": [
        {"node_id": "w1", "type": "indexer", "depends_on": [], "fan_in": "all",
         "io": {"input_inline": {"batch_size": batch_size, "total_skus": total_skus}}},
        {"node_id": "w2", "type": "enricher", "depends_on": ["w1"], "fan_in": "any",
         "io": {"start_when": "first_batch",
                "input_inline": {
                    "input_adapter": "pull.from_artifacts.rechunk:size",
                    "input_args": {"from_nodes": ["w1"], "size": mini_batch, "poll_ms": 50, "meta_list_key": "skus"}
                }}},
        {"node_id": "w3", "type": "coordinator_fn", "depends_on": ["w1","w2"], "fan_in": "all",
         "io": {"fn": "merge.generic", "fn_args": {"from_nodes": ["w1","w2"], "target": {"key": "w3-merged"}}}},
        {"node_id": "w4", "type": "analyzer", "depends_on": ["w3","w5"], "fan_in": "any",
         "io": {"start_when": "first_batch",
                "input_inline": {
                    "input_adapter": "pull.from_artifacts",
                    "input_args": {"from_nodes": ["w5","w3"], "poll_ms": 40}
                }}},
        {"node_id": "w5", "type": "ocr", "depends_on": ["w2"], "fan_in": "any",
         "io": {"start_when": "first_batch",
                "input_inline": {
                    "input_adapter": "pull.from_artifacts.rechunk:size",
                    "input_args": {"from_nodes": ["w2"], "size": 1, "poll_ms": 40, "meta_list_key": "enriched"}
                }}},
      ],
      "edges": [
        ["w1","w2"], ["w2","w3"], ["w1","w3"], ["w3","w4"], ["w2","w5"], ["w5","w4"]
      ],
      "edges_ex": [
        {"from":"w1","to":"w2","mode":"async","trigger":"on_batch"},
        {"from":"w2","to":"w5","mode":"async","trigger":"on_batch"},
        {"from":"w5","to":"w4","mode":"async","trigger":"on_batch"},
        {"from":"w3","to":"w4","mode":"async","trigger":"on_batch"}
      ]
    }

def prime_graph(cd, graph: Dict[str, Any]) -> Dict[str, Any]:
    for n in graph.get("nodes", []):
        st = n.get("status")
        # если нет или None/пустая строка — приводим к queued
        if st is None or (isinstance(st, str) and not st.strip()):
            n["status"] = cd.RunState.queued
        # остальное оставляем
        n.setdefault("attempt_epoch", 0)
        n.setdefault("stats", {})
        n.setdefault("lease", {})
    return graph

async def wait_task_finished(db: InMemDB, task_id: str, timeout: float = 10.0) -> Dict[str, Any]:
    t0 = time.time()
    last_log = 0.0
    while time.time() - t0 < timeout:
        t = await db.tasks.find_one({"id": task_id})
        now = time.time()
        if now - last_log >= 0.5:
            if t:
                st = t.get("status")
                nodes = {n["node_id"]: n.get("status") for n in (t.get("graph", {}).get("nodes") or [])}
                dbg("WAIT.PROGRESS", task_status=st, nodes=nodes)
            else:
                dbg("WAIT.PROGRESS", info="task_not_found_yet")
            last_log = now
        if t and t.get("status") == "finished":
            dbg("WAIT.DONE")
            return t
        await asyncio.sleep(0.05)
    raise AssertionError("task not finished in time")

# ====================== test handlers ===================
def make_test_handlers(wu) -> Dict[str, Any]:
    """Хендлеры с эмуляцией адаптера pull.from_artifacts.rechunk:size прямо в тестах."""
    class IndexerHandler(wu.RoleHandler):
        role = "indexer"
        async def load_input(self, ref, inline):
            dbg("HNDL.indexer.load_input", inline=inline)
            return inline or {}
        async def iter_batches(self, loaded):
            total = int(loaded.get("total_skus", 12))
            bs = int(loaded.get("batch_size", 5))
            skus = [f"sku-{i}" for i in range(total)]
            shard = 0
            for i in range(0, total, bs):
                chunk = skus[i:i+bs]
                dbg("HNDL.indexer.yield", shard=shard, count=len(chunk))
                yield wu.Batch(shard_id=f"w1-{shard}", payload={"skus": chunk})
                shard += 1
        async def process_batch(self, batch, ctx):
            dbg("HNDL.indexer.proc", shard=batch.shard_id)
            # ⬇️ NEW: чуть-чуть «реализма», чтобы тест 2 не ломался
            delay = float(os.getenv("TEST_IDX_PROCESS_SLEEP_SEC", "0.5"))
            await asyncio.sleep(delay)
            return wu.BatchResult(
                success=True,
                metrics={"skus": batch.payload["skus"], "count": len(batch.payload["skus"])}
            )

    class _PullFromArtifactsMixin:
        async def _emit_from_artifacts(self, *, from_nodes: List[str], size: int, meta_key: str, poll: float, shard_prefix: str):
            # ключ -> множество уже отданных элементов
            if not hasattr(self, "_emitted_items"):
                self._emitted_items = {}  # {(node, shard): set(items)}

            completed = set()
            while True:
                progressed = False

                for doc in list(wu.db.artifacts.rows):
                    node_id = doc.get("node_id")
                    if node_id not in from_nodes:
                        continue

                    shard_id = doc.get("shard_id") or ""
                    key = (node_id, shard_id)

                    meta  = doc.get("meta") or {}
                    items = list(meta.get(meta_key) or [])

                    seen = self._emitted_items.get(key, set())
                    new_items = [x for x in items if x not in seen]
                    if new_items:
                        for i in range(0, len(new_items), size):
                            chunk = new_items[i:i+size]
                            dbg("HNDL.emit", src=node_id, shard=shard_id, chunk=len(chunk), role=shard_prefix)
                            yield wu.Batch(
                                shard_id=f"{node_id}:{shard_id}:{i//size}",
                                payload={"items": chunk},
                            )
                            progressed = True
                        seen.update(new_items)
                        self._emitted_items[key] = seen

                    if doc.get("status") == "complete":
                        completed.add(node_id)

                if all(n in completed for n in from_nodes):
                    break
                if not progressed:
                    await asyncio.sleep(poll)
    
    class EnricherHandler(_PullFromArtifactsMixin, wu.RoleHandler):
        role = "enricher"
        async def load_input(self, ref, inline):
            dbg("HNDL.enricher.load_input", inline=inline)
            return {"input_inline": inline or {}}
        async def iter_batches(self, loaded):
            ii = (loaded or {}).get("input_inline") or {}
            args = ii.get("input_args", {})
            from_nodes = list(args.get("from_nodes", []))
            size = int(args.get("size", 1))
            meta_key = args.get("meta_list_key", "items")
            poll = float(args.get("poll_ms", 50)) / 1000.0
            async for b in self._emit_from_artifacts(from_nodes=from_nodes, size=size, meta_key=meta_key, poll=poll, shard_prefix="enricher"):
                yield b
        async def process_batch(self, batch, ctx):
            items = batch.payload.get("items")
            if not items:
                dbg("HNDL.enricher.proc.noop")
                return wu.BatchResult(success=True, metrics={"noop": 1})
            enriched = [{"sku": (x if isinstance(x, str) else x.get("sku", x)), "enriched": True} for x in items]
            dbg("HNDL.enricher.proc", count=len(enriched))
            # в meta положим enriched → далее OCR будет его читать
            return wu.BatchResult(success=True, metrics={"enriched": enriched, "count": len(enriched)})

    class OCRHandler(_PullFromArtifactsMixin, wu.RoleHandler):
        role = "ocr"
        async def load_input(self, ref, inline):
            dbg("HNDL.ocr.load_input", inline=inline)
            return {"input_inline": inline or {}}
        async def iter_batches(self, loaded):
            ii = (loaded or {}).get("input_inline") or {}
            args = ii.get("input_args", {})
            from_nodes = list(args.get("from_nodes", []))
            size = int(args.get("size", 1))
            meta_key = args.get("meta_list_key", "items")
            poll = float(args.get("poll_ms", 40)) / 1000.0
            async for b in self._emit_from_artifacts(from_nodes=from_nodes, size=size, meta_key=meta_key, poll=poll, shard_prefix="ocr"):
                yield b
        async def process_batch(self, batch, ctx):
            items = batch.payload.get("items")
            if not items:
                dbg("HNDL.ocr.proc.noop")
                return wu.BatchResult(success=True, metrics={"noop": 1})
            ocrd = [{"sku": (it["sku"] if isinstance(it, dict) else it), "ocr_ok": True} for it in items]
            dbg("HNDL.ocr.proc", count=len(ocrd))
            return wu.BatchResult(success=True, metrics={"ocr": ocrd, "count": len(ocrd)})

    class AnalyzerHandler(_PullFromArtifactsMixin, wu.RoleHandler):
        role = "analyzer"
        async def load_input(self, ref, inline):
            dbg("HNDL.analyzer.load_input", inline=inline)
            # ВАЖНО: вернуть сам inline, без дополнительной обёртки,
            # чтобы воркер увидел input_adapter и включил стриминг.
            return (inline or {})
        async def iter_batches(self, loaded):
            dbg("HNDL.analyzer.iter_batches", inline=loaded)
            # Читаем поток артефактов как enricher/ocr.
            args = (loaded or {}).get("input_args", {}) or {}
            from_nodes = list(args.get("from_nodes") or [])
            # По умолчанию у индексера ключ обычно 'items'
            meta_key = args.get("meta_list_key") or "skus"
            size = int(args.get("size") or 3)
            poll = float(args.get("poll_ms", 25) or 25) / 1000.0
            async for b in self._emit_from_artifacts(
                from_nodes=from_nodes,
                size=size,
                meta_key=meta_key,
                poll=poll,
                shard_prefix="analyzer",
            ):
                yield b
        async def process_batch(self, batch, ctx):
            payload = batch.payload or {}
            items = payload.get("items") or payload.get("skus") or []
            n = len(items)
            if n:
                dbg("HNDL.analyzer.proc", count=n)
                return wu.BatchResult(success=True, metrics={"count": n, "sinked": n})
            dbg("HNDL.analyzer.proc.noop")
            return wu.BatchResult(success=True, metrics={"noop": 1})

    return {
        "indexer": IndexerHandler(),
        "enricher": EnricherHandler(),
        "ocr":      OCRHandler(),
        "analyzer": AnalyzerHandler(),
    }

__all__ = [
    "AIOKafkaProducerMock","AIOKafkaConsumerMock","InMemKafkaBroker","BROKER",
    "InMemDB","setup_env_and_imports","install_inmemory_db",
    "build_graph","prime_graph","wait_task_finished","make_test_handlers","dbg"
]
