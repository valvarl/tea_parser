import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import pytest
import pytest_asyncio

# ---------------------- tiny logger ------------------------------------
def _ts(): return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
def dbg(tag, **kv):
    kvs = " ".join(f"{k}={kv[k]!r}" for k in kv)
    print(f"[{_ts()}] {tag}: {kvs}", flush=True)

# ---------------------- In-memory Kafka --------------------------------
class _Rec:
    __slots__ = ("value","topic")
    def __init__(self, value, topic):
        self.value = value
        self.topic = topic

class InMemKafkaBroker:
    def __init__(self):
        self.topics: Dict[str, Dict[str, asyncio.Queue]] = {}
        self.rev: Dict[int, str] = {}

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
        payload = value
        msg_type = payload.get("msg_type")
        kind = (payload.get("payload") or {}).get("kind") or (payload.get("payload") or {}).get("reply")
        dbg("KAFKA.PRODUCE", topic=topic, msg_type=msg_type, kind=kind)
        for q in self.topics.setdefault(topic, {}).values():
            await q.put(_Rec(value, topic))

BROKER = InMemKafkaBroker()

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

# ---------------------- In-memory Mongo --------------------------------
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
                if "$in" in v and val not in v["$in"]: return False
                if "$lte" in v and not (val is not None and val <= v["$lte"]): return False
                if "$lt"  in v and not (val is not None and val <  v["$lt"]):  return False
                if "$gte" in v and not (val is not None and val >= v["$gte"]): return False
                if "$gt"  in v and not (val is not None and val >  v["$gt"]):  return False
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
                doc = d; break

        created = False
        if not doc:
            if not upsert: return
            doc = {}
            self.rows.append(doc)
            created = True

        # resolve index for $.paths if any (only for tasks.graph.nodes.$)
        node_idx = None
        if "graph.nodes.node_id" in flt:
            target = flt["graph.nodes.node_id"]
            nodes = (((doc.get("graph") or {}).get("nodes")) or [])
            for i,n in enumerate(nodes):
                if n.get("node_id")==target: node_idx=i; break

        def set_path(m, path, val):
            parts = path.split("."); cur=m
            for i,p in enumerate(parts):
                if p=="$": p=str(node_idx)
                last = i==len(parts)-1
                if p.isdigit() and isinstance(cur, list):
                    idx=int(p)
                    while len(cur)<=idx:
                        cur.append({})
                    if last: cur[idx]=val
                    else:
                        if not isinstance(cur[idx], dict): cur[idx]={}
                        cur = cur[idx]
                else:
                    if last: cur[p]=val
                    else:
                        if p not in cur or not isinstance(cur[p], (dict, list)):
                            cur[p] = [] if parts[i+1].isdigit() else {}
                        cur = cur[p]

        if "$setOnInsert" in upd and created:
            for k,v in upd["$setOnInsert"].items():
                set_path(doc,k,v)
            if self.name == "artifacts" and "status" in upd["$setOnInsert"]:
                dbg("DB.ARTIFACTS.STATUS", filter=flt, new_status=str(upd["$setOnInsert"]["status"]))

        if "$set" in upd:
            if self.name == "tasks":
                for k,v in upd["$set"].items():
                    if k.endswith("graph.nodes.$.status"):
                        dbg("DB.TASK.STATUS", filter=flt, new_status=str(v))
            if self.name == "artifacts" and "status" in upd["$set"]:
                dbg("DB.ARTIFACTS.STATUS", filter=flt, new_status=str(upd["$set"]["status"]))
            for k,v in upd["$set"].items():
                set_path(doc,k,v)

        if "$currentDate" in upd:
            for k,_ in upd["$currentDate"].items():
                set_path(doc,k,_now_dt())

    async def find_one_and_update(self, flt, upd):
        d = await self.find_one(flt); await self.update_one(flt, upd); return d

    async def create_index(self, *a, **k): return "ok"

class InMemDB:
    def __init__(self):
        self.tasks          = InMemCollection("tasks")
        self.worker_events  = InMemCollection("worker_events")
        self.worker_registry= InMemCollection("worker_registry")
        self.artifacts      = InMemCollection("artifacts")
        self.outbox         = InMemCollection("outbox")
        self.stream_progress= InMemCollection("stream_progress")

# ---------------------- helpers ----------------------------------------
def prime_graph(cd, graph: Dict[str, Any]) -> Dict[str, Any]:
    for n in graph.get("nodes", []):
        n.setdefault("status", cd.RunState.queued)
        n.setdefault("attempt_epoch", 0)
        n.setdefault("stats", {})
        n.setdefault("lease", {})
    return graph

async def wait_all_finished(db: InMemDB, task_id: str, timeout: float = 12.0):
    t0 = time.time()
    last = 0.0
    while time.time() - t0 < timeout:
        t = await db.tasks.find_one({"id": task_id})
        now = time.time()
        if now - last > 0.5:
            if t:
                nodes = {n["node_id"]: n.get("status") for n in (t.get("graph",{}).get("nodes") or [])}
                dbg("WAIT.PROGRESS", task=t.get("status"), nodes=nodes)
            last = now
        if t and t.get("status") == "finished":
            return t
        await asyncio.sleep(0.05)
    raise AssertionError("task not finished in time")

# ---------------------- fixtures ---------------------------------------
@pytest.fixture(scope="function")
def env_and_imports(monkeypatch):
    import sys
    sys.path.insert(0, "/home/valvarl/tea_parser/backend")

    from app.services import coordinator_dag as cd
    from app.services import worker_universal as wu

    import asyncio as aio
    monkeypatch.setattr(cd, "asyncio", aio, raising=False)
    monkeypatch.setattr(wu, "asyncio", aio, raising=False)

    monkeypatch.setattr(cd, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(cd, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)

    # байпас outbox → отправляем напрямую в "кафку"
    async def _enqueue_direct(self, *, topic: str, key: str, env):
        dbg("OUTBOX.BYPASS", topic=topic)
        await self.bus._raw_send(topic, key.encode("utf-8"), env)
    monkeypatch.setattr(cd.OutboxDispatcher, "enqueue", _enqueue_direct, raising=False)

    # быстрые тики
    cd.SCHEDULER_TICK_SEC   = 0.05
    cd.DISCOVERY_WINDOW_SEC = 0.05
    cd.FINALIZER_TICK_SEC   = 0.05
    cd.HB_MONITOR_TICK_SEC  = 0.2

    return cd, wu

@pytest.fixture
def inmemory_db(monkeypatch, env_and_imports):
    cd, wu = env_and_imports
    db = InMemDB()
    monkeypatch.setattr(cd, "db", db, raising=True)
    monkeypatch.setattr(wu, "db", db, raising=True)
    return db

@pytest_asyncio.fixture
async def coordinator(env_and_imports, inmemory_db):
    cd, _ = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        yield coord
    finally:
        await coord.stop()

# ---------------------- Handlers ---------------------------------------
@pytest.fixture
def handlers(env_and_imports):
    _, wu = env_and_imports

    class SourceShardHandler(wu.RoleHandler):
        """
        Источник: отдаёт N элементов батчами (с shard_id), чтобы воркер делал upsert_partial.
        В финале — mark_complete с общим счётчиком.
        """
        role = "src"
        def __init__(self): self._total = 0
        async def load_input(self, ref, inline):
            self._total = 0
            return inline or {}
        async def iter_batches(self, loaded):
            total = int(loaded.get("total", 11))
            bs    = int(loaded.get("batch_size", 4))
            items = [f"sku-{i}" for i in range(total)]
            shard = 0
            for i in range(0, total, bs):
                part = items[i:i+bs]
                yield wu.Batch(shard_id=f"s-{shard}", payload={"items": part})
                shard += 1
        async def process_batch(self, batch, ctx):
            lst = batch.payload["items"]
            self._total += len(lst)
            # метаданные каждой партиции — список items
            return wu.BatchResult(success=True, metrics={"items": lst, "count": len(lst)})
        async def finalize(self, ctx):
            return wu.FinalizeResult(metrics={"source_total": self._total})

    class CollectorHandler(wu.RoleHandler):
        """
        Даунстрим: читает всё через pull.from_artifacts.rechunk:size и считает суммарно.
        """
        role = "collector"
        def __init__(self): self._seen = 0
        async def load_input(self, ref, inline):
            self._seen = 0
            return {"input_inline": inline or {}}
        async def iter_batches(self, loaded):
            yield wu.Batch(shard_id=None, payload={"bootstrap": True})
        async def process_batch(self, batch, ctx):
            items = batch.payload.get("items", [])
            self._seen += len(items)
            return wu.BatchResult(success=True, metrics={"seen": len(items)})
        async def finalize(self, ctx):
            return wu.FinalizeResult(metrics={"total_received": self._seen})

    class AHandler(wu.RoleHandler):
        role = "A"
        async def load_input(self, ref, inline): return inline or {}
        async def iter_batches(self, loaded):
            yield wu.Batch(shard_id="a0", payload={"items": ["a1", "a2"]})
        async def process_batch(self, batch, ctx):
            return wu.BatchResult(success=True, metrics={"items": batch.payload["items"]})
    class BHandler(wu.RoleHandler):
        role = "B"
        async def load_input(self, ref, inline): return inline or {}
        async def iter_batches(self, loaded):
            yield wu.Batch(shard_id="b0", payload={"items": ["b1"]})
        async def process_batch(self, batch, ctx):
            return wu.BatchResult(success=True, metrics={"items": batch.payload["items"]})

    return {
        "src": SourceShardHandler(),
        "collector": CollectorHandler(),
        "A": AHandler(),
        "B": BHandler(),
    }

@pytest_asyncio.fixture
async def workers(env_and_imports, handlers):
    _, wu = env_and_imports
    w_src  = wu.Worker(roles=["src"],       handlers={"src": handlers["src"]})
    w_col  = wu.Worker(roles=["collector"], handlers={"collector": handlers["collector"]})
    w_a    = wu.Worker(roles=["A"],         handlers={"A": handlers["A"]})
    w_b    = wu.Worker(roles=["B"],         handlers={"B": handlers["B"]})
    for w in (w_src, w_col, w_a, w_b): await w.start()
    try:
        yield {"src": w_src, "collector": w_col, "A": w_a, "B": w_b}
    finally:
        for w in (w_src, w_col, w_a, w_b): await w.stop()

# ---------------------- Graphs -----------------------------------------
def graph_partial_and_collect(total=11, batch_size=4, rechunk=3) -> Dict[str, Any]:
    return {
        "schema_version": "1.0",
        "nodes": [
            {"node_id": "w1", "type": "src", "depends_on": [], "fan_in": "all",
             "io": {"input_inline": {"total": total, "batch_size": batch_size}}},
            {"node_id": "w2", "type": "collector", "depends_on": ["w1"], "fan_in": "any",
             "io": {"start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts.rechunk:size",
                        "input_args": {"from_nodes": ["w1"], "size": rechunk, "poll_ms": 25, "meta_list_key": "items"}
                    }}},
        ],
        "edges": [["w1","w2"]],
        "edges_ex": [{"from":"w1","to":"w2","mode":"async","trigger":"on_batch"}],
    }

def graph_merge_generic() -> Dict[str, Any]:
    # Два источника -> merge.generic -> (без воркера)
    return {
        "schema_version":"1.0",
        "nodes":[
            {"node_id":"a","type":"A","depends_on":[],"fan_in":"all","io":{"input_inline":{}}},
            {"node_id":"b","type":"B","depends_on":[],"fan_in":"all","io":{"input_inline":{}}},
            {"node_id":"m","type":"coordinator_fn","depends_on":["a","b"],"fan_in":"all",
             "io":{"fn":"merge.generic","fn_args":{"from_nodes":["a","b"],"target":{"key":"merged"}}}},
        ],
        "edges":[["a","m"],["b","m"]],
    }

# ---------------------- TESTS ------------------------------------------

@pytest.mark.asyncio
async def test_partial_shards_and_finalize_stream_read_all(env_and_imports, inmemory_db, coordinator, workers):
    """
    Источник w1 (src) выдаёт батчи с shard_id → воркер делает upsert_partial.
    В финале — mark_complete. Коллектор w2 читает через pull.from_artifacts.rechunk:size и
    пишет в свой complete-артефакт total_received.

    Проверяем:
      * есть partial по каждому шардy w1
      * есть complete для w1
      * артефакт w2.complete.total_received == total
    """
    cd, _ = env_and_imports
    total, bs, rechunk = 11, 4, 3

    graph = prime_graph(cd, graph_partial_and_collect(total=total, batch_size=bs, rechunk=rechunk))
    task_id = await coordinator.create_task(params={}, graph=graph)

    tdoc = await wait_all_finished(inmemory_db, task_id, timeout=12.0)

    # partial-шарды для w1
    # ожидаем ceil(11/4)=3
    shard_cnt = 0
    for d in inmemory_db.artifacts.rows:
        if d.get("task_id")==task_id and d.get("node_id")=="w1" and d.get("status")=="partial":
            shard_cnt += 1
    assert shard_cnt == 3, f"expected 3 partial shards for w1, got {shard_cnt}"

    # complete артефакт для w1
    a_w1 = await inmemory_db.artifacts.find_one({"task_id": task_id, "node_id": "w1", "status": "complete"})
    assert a_w1 is not None, "expected w1 complete artifact"

    # complete артефакт для w2 и проверка total_received
    a_w2 = await inmemory_db.artifacts.find_one({"task_id": task_id, "node_id": "w2", "status": "complete"})
    assert a_w2 is not None, "expected w2 complete artifact"
    meta = (a_w2 or {}).get("meta") or {}
    assert int(meta.get("total_received", -1)) == total, f"collector should receive {total} items, got {meta.get('total_received')}"

@pytest.mark.asyncio
async def test_merge_generic_creates_complete_artifact(env_and_imports, inmemory_db, coordinator, workers):
    """
    coordinator_fn: merge.generic объединяет результаты узлов 'a' и 'b'.
    Проверяем, что:
      * таска завершается
      * есть complete-артефакт у merge-узла 'm'
      * у узлов 'a' и 'b' есть артефакты (partial/complete — зависит от реализаций), главное — complete у 'm'
    """
    cd, _ = env_and_imports
    graph = prime_graph(cd, graph_merge_generic())
    task_id = await coordinator.create_task(params={}, graph=graph)

    tdoc = await wait_all_finished(inmemory_db, task_id, timeout=12.0)

    # артефакт у merge-узла
    a_m = await inmemory_db.artifacts.find_one({"task_id": task_id, "node_id": "m", "status": "complete"})
    assert a_m is not None, "expected merge node 'm' to publish complete artifact"

    # sanity: источники тоже должны что-то оставить
    cnt_a = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "a"})
    cnt_b = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "b"})
    assert cnt_a > 0 and cnt_b > 0, "upstreams should have artifacts too"
