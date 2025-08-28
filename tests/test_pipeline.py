# tests/test_pipeline_with_kafka_sim.py
import asyncio
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

import pytest
import pytest_asyncio

# ---------------------- tiny logger ------------------------------------
def _ts(): return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
def _dbg(tag, **kv):
    kvs = " ".join(f"{k}={kv[k]!r}" for k in kv)
    print(f"[{_ts()}] {tag}: {kvs}", flush=True)

# -------------- In-memory Kafka ----------------------------------------
class _Rec:
    __slots__ = ("value","topic")
    def __init__(self, value, topic):
        self.value = value
        self.topic = topic

class InMemKafkaBroker:
    def __init__(self):
        # topic -> {group_id: queue}
        self.topics: Dict[str, Dict[str, asyncio.Queue]] = {}
        # reverse mapping to know topic by queue
        self.rev: Dict[int, str] = {}

    def ensure_queue(self, topic: str, group_id: str) -> asyncio.Queue:
        tg = self.topics.setdefault(topic, {})
        q = tg.get(group_id)
        if q is None:
            q = asyncio.Queue()
            tg[group_id] = q
            self.rev[id(q)] = topic
            _dbg("KAFKA.GROUP_BIND", topic=topic, group_id=group_id)
        return q

    def topic_of(self, q: asyncio.Queue) -> str:
        return self.rev.get(id(q), "?")

    async def produce(self, topic: str, value: Any):
        # Кладём во все группы этого топика
        payload = value
        msg_type = payload.get("msg_type")
        kind = (payload.get("payload") or {}).get("kind") or (payload.get("payload") or {}).get("reply")
        _dbg("KAFKA.PRODUCE", topic=topic, msg_type=msg_type, kind=kind)
        for q in self.topics.setdefault(topic, {}).values():
            await q.put(_Rec(value, topic))

BROKER = InMemKafkaBroker()

class AIOKafkaProducerMock:
    def __init__(self, *_, **__): pass
    async def start(self): _dbg("PRODUCER.START")
    async def stop(self): _dbg("PRODUCER.STOP")
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
        _dbg("CONSUMER.START", group_id=self._group, topics=self._topics)

    async def stop(self): _dbg("CONSUMER.STOP", group_id=self._group)

    async def getone(self):
        while True:
            if self._paused:
                await asyncio.sleep(0.01); continue
            # опрос по очередям
            any_q = False
            for q in self._queues:
                any_q = True
                try:
                    rec = q.get_nowait()
                    # лог на каждое получение
                    val = rec.value
                    msg_type = val.get("msg_type")
                    kind = (val.get("payload") or {}).get("kind") or (val.get("payload") or {}).get("reply")
                    _dbg("CONSUMER.GET", group_id=self._group, topic=BROKER.topic_of(q), msg_type=msg_type, kind=kind)
                    return rec
                except asyncio.QueueEmpty:
                    continue
            await asyncio.sleep(0.005)

    async def commit(self): pass
    def pause(self, *parts): 
        self._paused = True
        _dbg("CONSUMER.PAUSE", group_id=self._group)

    def resume(self, *parts): 
        self._paused = False
        _dbg("CONSUMER.RESUME", group_id=self._group)

    def assignment(self): return {("t",0)}

# -------------- In-memory Mongo (минимум) ------------------------------
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
        for k, v in (flt or {}).items():
            val = self._get_path(doc, k)

            if isinstance(v, dict):
                # $in
                if "$in" in v:
                    if val not in v["$in"]:
                        return False
                # $lte/$lt/$gte/$gt
                if "$lte" in v and not (val is not None and val <= v["$lte"]):
                    return False
                if "$lt"  in v and not (val is not None and val <  v["$lt"]):
                    return False
                if "$gte" in v and not (val is not None and val >= v["$gte"]):
                    return False
                if "$gt"  in v and not (val is not None and val >  v["$gt"]):
                    return False
                continue

            if k == "graph.nodes.node_id":
                nodes = (((doc.get("graph") or {}).get("nodes")) or [])
                if not any(n.get("node_id") == v for n in nodes):
                    return False
                continue

            if val != v:
                return False
        return True

    async def insert_one(self, doc):
        self.rows.append(dict(doc))
        if self.name == "outbox":
            _dbg("DB.OUTBOX.INSERT", size=len(self.rows), doc_keys=list(doc.keys()))

    async def find_one(self, flt, proj=None):
        for d in self.rows:
            if self._match(d, flt): return d
        return None

    async def count_documents(self, flt): 
        cnt = sum(1 for d in self.rows if self._match(d, flt))
        return cnt

    def find(self, flt, proj=None):
        rows = [d for d in self.rows if self._match(d, flt)]
        class _Cur:
            def __init__(self, rows): self._rows=rows
            def sort(self, *_): return self
            def limit(self, n): self._rows=self._rows[:n]; return self
            async def __aiter__(self):
                for r in list(self._rows): yield r
        return _Cur(rows)

    async def update_one(self, flt, upd, upsert=False):
        doc = None
        for d in self.rows:
            if self._match(d, flt):
                doc = d
                break

        created = False
        if not doc:
            if not upsert:
                return
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
                if p == "$":
                    p = str(node_idx)
                last = i == len(parts) - 1
                if p.isdigit() and isinstance(cur, list):
                    idx = int(p)
                    while len(cur) <= idx:
                        cur.append({})
                    if last:
                        cur[idx] = val
                    else:
                        if not isinstance(cur[idx], dict):
                            cur[idx] = {}
                        cur = cur[idx]
                else:
                    if last:
                        cur[p] = val
                    else:
                        if p not in cur or not isinstance(cur[p], (dict, list)):
                            # предугадаем тип следующего шага
                            cur[p] = [] if parts[i + 1].isdigit() else {}
                        cur = cur[p]

        # сначала $setOnInsert, только при fresh upsert
        if "$setOnInsert" in upd and created:
            for k, v in upd["$setOnInsert"].items():
                set_path(doc, k, v)
            if self.name == "artifacts" and "status" in upd["$setOnInsert"]:
                _dbg("DB.ARTIFACTS.STATUS", filter=flt, new_status=str(upd["$setOnInsert"]["status"]))

        # обычный $set
        if "$set" in upd:
            if self.name == "tasks":
                for k, v in upd["$set"].items():
                    if k.endswith("graph.nodes.$.status"):
                        _dbg("DB.TASK.STATUS", filter=flt, new_status=str(v))
            if self.name == "artifacts" and "status" in upd["$set"]:
                _dbg("DB.ARTIFACTS.STATUS", filter=flt, new_status=str(upd["$set"]["status"]))
            for k, v in upd["$set"].items():
                set_path(doc, k, v)

        if "$inc" in upd:
            cur = doc
            for k, v in upd["$inc"].items():
                parts = k.split(".")
                for i, p in enumerate(parts):
                    if p == "$":
                        p = str(node_idx)
                    if i == len(parts) - 1:
                        cur[p] = int(cur.get(p, 0)) + int(v)
                    else:
                        cur = cur.setdefault(p, {})

        if "$max" in upd:
            cur = doc
            for k, v in upd["$max"].items():
                parts = k.split(".")
                for i, p in enumerate(parts):
                    if p == "$":
                        p = str(node_idx)
                    if i == len(parts) - 1:
                        cur[p] = max(int(cur.get(p, 0) or 0), int(v))
                    else:
                        cur = cur.setdefault(p, {})

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

# -------------- Pytest fixtures ----------------------------------------
@pytest.fixture(scope="function")
def env_and_imports(monkeypatch):
    # Снизим интервалы, выставим типы
    monkeypatch.setenv("WORKER_TYPES", "indexer,enricher,ocr,analyzer")
    monkeypatch.setenv("HB_SOFT_SEC", "120")
    monkeypatch.setenv("HB_HARD_SEC", "600")

    sys.path.insert(0, "/home/valvarl/tea_parser/backend")

    from app.services import coordinator_dag as cd
    from app.services import worker_universal as wu

    # Прибиваем stdlib asyncio внутрь модулей (защита от фикстуры)
    import asyncio as aio
    monkeypatch.setattr(cd, "asyncio", aio, raising=False)
    monkeypatch.setattr(wu, "asyncio", aio, raising=False)

    # Патчим Kafka в обоих модулях до старта
    monkeypatch.setattr(cd, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(cd, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)

    # >>> САМЫЙ ВАЖНЫЙ ПАТЧ: обходим outbox-луп <<<
    async def _enqueue_direct(self, *, topic: str, key: str, env):
        # сразу публикуем в "кафку", без записи в outbox
        _dbg("OUTBOX.BYPASS", topic=topic)
        await self.bus._raw_send(topic, key.encode("utf-8"), env)
    monkeypatch.setattr(cd.OutboxDispatcher, "enqueue", _enqueue_direct, raising=False)

    # Быстрые тики
    cd.SCHEDULER_TICK_SEC = 0.05
    cd.DISCOVERY_WINDOW_SEC = 0.05
    cd.FINALIZER_TICK_SEC = 0.05
    cd.HB_MONITOR_TICK_SEC = 0.2
    _dbg("ENV.READY")
    return cd, wu

@pytest.fixture
def inmemory_db(monkeypatch, env_and_imports):
    cd, wu = env_and_imports
    db = InMemDB()
    monkeypatch.setattr(cd, "db", db, raising=True)
    monkeypatch.setattr(wu, "db", db, raising=True)
    _dbg("DB.INSTALLED")
    return db

@pytest_asyncio.fixture
async def coordinator(env_and_imports, inmemory_db):
    cd, _ = env_and_imports
    coord = cd.Coordinator()
    _dbg("COORD.STARTING")
    await coord.start()
    _dbg("COORD.STARTED")
    try:
        yield coord
    finally:
        _dbg("COORD.STOPPING")
        await coord.stop()
        _dbg("COORD.STOPPED")

# -------------- Handlers for roles -------------------------------------

@pytest.fixture
def handlers(env_and_imports):
    _, wu = env_and_imports

    class IndexerHandler(wu.RoleHandler):
        role = "indexer"
        async def load_input(self, ref, inline):  # inline: {"total_skus":..., "batch_size":...}
            _dbg("HNDL.indexer.load_input", inline=inline)
            return inline or {}
        async def iter_batches(self, loaded):
            total = int(loaded.get("total_skus", 12))
            bs = int(loaded.get("batch_size", 5))
            skus = [f"sku-{i}" for i in range(total)]
            shard = 0
            for i in range(0, total, bs):
                _dbg("HNDL.indexer.yield", shard=shard, count=len(skus[i:i+bs]))
                yield wu.Batch(shard_id=f"w1-{shard}", payload={"skus": skus[i:i+bs]})
                shard += 1
        async def process_batch(self, batch, ctx):
            _dbg("HNDL.indexer.proc", shard=batch.shard_id)
            return wu.BatchResult(success=True, metrics={"skus": batch.payload["skus"], "count": len(batch.payload["skus"])})

    class EnricherHandler(wu.RoleHandler):
        role = "enricher"
        async def load_input(self, ref, inline):
            _dbg("HNDL.enricher.load_input", inline=inline)
            return {"input_inline": inline or {}}
        async def iter_batches(self, loaded):
            _dbg("HNDL.enricher.bootstrap")
            yield wu.Batch(shard_id=None, payload={"bootstrap": True})
        async def process_batch(self, batch, ctx):
            items = batch.payload.get("items")
            if not items:
                _dbg("HNDL.enricher.proc.noop")
                return wu.BatchResult(success=True, metrics={"noop": 1})
            enriched = [{"sku": (x if isinstance(x, str) else x.get("sku", x)), "enriched": True} for x in items]
            _dbg("HNDL.enricher.proc", count=len(enriched))
            return wu.BatchResult(success=True, metrics={"enriched": enriched, "count": len(enriched)})

    class OCRHandler(wu.RoleHandler):
        role = "ocr"
        async def load_input(self, ref, inline):
            _dbg("HNDL.ocr.load_input", inline=inline)
            return {"input_inline": inline or {}}
        async def iter_batches(self, loaded):
            _dbg("HNDL.ocr.bootstrap")
            yield wu.Batch(shard_id=None, payload={"bootstrap": True})
        async def process_batch(self, batch, ctx):
            items = batch.payload.get("items")
            if not items:
                _dbg("HNDL.ocr.proc.noop")
                return wu.BatchResult(success=True, metrics={"noop": 1})
            ocrd = [{"sku": (it["sku"] if isinstance(it, dict) else it), "ocr_ok": True} for it in items]
            _dbg("HNDL.ocr.proc", count=len(ocrd))
            return wu.BatchResult(success=True, metrics={"ocr": ocrd, "count": len(ocrd)})

    class AnalyzerHandler(wu.RoleHandler):
        role = "analyzer"
        async def load_input(self, ref, inline):
            _dbg("HNDL.analyzer.load_input", inline=inline)
            return {"input_inline": inline or {}}
        async def iter_batches(self, loaded):
            _dbg("HNDL.analyzer.bootstrap")
            yield wu.Batch(shard_id=None, payload={"bootstrap": True})
        async def process_batch(self, batch, ctx):
            items = batch.payload.get("items")
            if items:
                _dbg("HNDL.analyzer.proc", count=len(items))
                return wu.BatchResult(success=True, metrics={"sinked": len(items), "count": len(items)})
            _dbg("HNDL.analyzer.proc.noop")
            return wu.BatchResult(success=True, metrics={"noop": 1})

    return {
        "indexer": IndexerHandler(),
        "enricher": EnricherHandler(),
        "ocr": OCRHandler(),
        "analyzer": AnalyzerHandler(),
    }

@pytest_asyncio.fixture
async def worker(env_and_imports, handlers):
    _, wu = env_and_imports

    w_indexer  = wu.Worker(roles=["indexer"],  handlers={"indexer":  handlers["indexer"]})
    w_enricher = wu.Worker(roles=["enricher"], handlers={"enricher": handlers["enricher"]})
    w_ocr      = wu.Worker(roles=["ocr"],      handlers={"ocr":      handlers["ocr"]})
    w_analyzer = wu.Worker(roles=["analyzer"], handlers={"analyzer": handlers["analyzer"]})

    for name, w in (("indexer", w_indexer), ("enricher", w_enricher), ("ocr", w_ocr), ("analyzer", w_analyzer)):
        _dbg("WORKER.STARTING", role=name)
        await w.start()
        _dbg("WORKER.STARTED", role=name)

    try:
        yield (w_indexer, w_enricher, w_ocr, w_analyzer)
    finally:
        for name, w in (("indexer", w_indexer), ("enricher", w_enricher), ("ocr", w_ocr), ("analyzer", w_analyzer)):
            _dbg("WORKER.STOPPING", role=name)
            await w.stop()
            _dbg("WORKER.STOPPED", role=name)

# -------------- Graph ---------------------------------------------------
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

# -------------- Utils ---------------------------------------------------
async def wait_task_finished(db, task_id, timeout=8.0):
    t0 = time.time()
    last_log = 0.0
    while time.time() - t0 < timeout:
        t = await db.tasks.find_one({"id": task_id})
        now = time.time()
        if now - last_log >= 0.5:
            # периодический прогресс-лог
            if t:
                st = t.get("status")
                nodes = {n["node_id"]: n.get("status") for n in (t.get("graph", {}).get("nodes") or [])}
                _dbg("WAIT.PROGRESS", task_status=st, nodes=nodes)
            else:
                _dbg("WAIT.PROGRESS", info="task_not_found_yet")
            last_log = now
        if t and t.get("status") == "finished":
            _dbg("WAIT.DONE")
            return t
        await asyncio.sleep(0.05)
    raise AssertionError("task not finished in time")

def prime_graph(cd, graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    Проставляет начальные статусы и epoch узлам графа,
    чтобы Coordinator._node_ready начал их планировать.
    """
    for n in graph.get("nodes", []):
        n.setdefault("status", cd.RunState.queued)
        n.setdefault("attempt_epoch", 0)
        # опционально — пустые метрики/lease, не обязательно:
        n.setdefault("stats", {})
        n.setdefault("lease", {})
    return graph

# -------------- The test ------------------------------------------------
@pytest.mark.asyncio
async def test_e2e_streaming_with_kafka_sim(env_and_imports, inmemory_db, coordinator, worker):
    cd, _ = env_and_imports

    # строим граф и ПРАЙМИМ статусы
    graph = build_graph(total_skus=12, batch_size=5, mini_batch=3)
    graph = prime_graph(cd, graph)
    print("PRIMED_GRAPH_STATUSES:", {n["node_id"]: n["status"] for n in graph["nodes"]}, flush=True)

    # создаём таску
    task_id = await coordinator.create_task(params={}, graph=graph)
    _dbg("TEST.TASK_CREATED", task_id=task_id)

    # ждём завершения
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    # проверки (как было)
    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    _dbg("TEST.FINAL.STATUSES", statuses=st)
    assert st["w1"] == cd.RunState.finished
    assert st["w2"] == cd.RunState.finished
    assert st["w3"] == cd.RunState.finished
    assert st["w5"] == cd.RunState.finished
    assert st["w4"] == cd.RunState.finished

    a1 = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w1"})
    a2 = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w2"})
    a5 = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w5"})
    _dbg("TEST.FINAL.ART_CNT", w1=a1, w2=a2, w5=a5)
    assert a1 > 0 and a2 > 0 and a5 > 0