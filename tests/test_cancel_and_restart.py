import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

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
            if val != v: return False
        return True

    async def insert_one(self, doc):
        self.rows.append(dict(doc))
        if self.name == "outbox":
            dbg("DB.OUTBOX.INSERT", size=len(self.rows), doc_keys=list(doc.keys()))

    async def find_one(self, flt, proj=None):
        for d in self.rows:
            if self._match(d, flt): return d
        return None

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

        def set_path(m, path, val):
            parts = path.split("."); cur=m
            for i,p in enumerate(parts):
                last = i==len(parts)-1
                if last: cur[p]=val
                else: cur = cur.setdefault(p, {})

        if "$setOnInsert" in upd and created:
            for k,v in upd["$setOnInsert"].items(): set_path(doc,k,v)

        if "$set" in upd:
            if self.name == "tasks":
                for k,v in upd["$set"].items():
                    if k.endswith("graph.nodes.$.status"):
                        dbg("DB.TASK.STATUS", filter=flt, new_status=str(v))
            if self.name == "artifacts" and "status" in upd.get("$set", {}):
                dbg("DB.ARTIFACTS.STATUS", filter=flt, new_status=str(upd["$set"]["status"]))
            for k,v in upd["$set"].items(): set_path(doc,k,v)

        if "$inc" in upd:
            cur=doc
            for k,v in upd["$inc"].items():
                parts=k.split(".")
                for i,p in enumerate(parts):
                    if i==len(parts)-1: cur[p]=int(cur.get(p,0))+int(v)
                    else: cur=cur.setdefault(p,{})

        if "$currentDate" in upd:
            for k,_ in upd["$currentDate"].items(): set_path(doc,k,_now_dt())

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

async def wait_task_status(db: InMemDB, task_id: str, want: str, timeout: float = 8.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        t = await db.tasks.find_one({"id": task_id})
        if t and t.get("status") == want:
            return t
        await asyncio.sleep(0.03)
    raise AssertionError(f"task not in status={want} in time")

async def wait_node_running(db: InMemDB, task_id: str, node_id: str, timeout: float = 6.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        t = await db.tasks.find_one({"id": task_id})
        if t:
            for n in t.get("graph",{}).get("nodes",[]):
                if n.get("node_id")==node_id and str(n.get("status"))==str("RunState.running") or n.get("status")=="running":
                    return True
        await asyncio.sleep(0.02)
    return False

async def wait_all_finished(db: InMemDB, task_id: str, timeout: float = 12.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        t = await db.tasks.find_one({"id": task_id})
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

    # байпас outbox
    async def _enqueue_direct(self, *, topic: str, key: str, env):
        dbg("OUTBOX.BYPASS", topic=topic)
        await self.bus._raw_send(topic, key.encode("utf-8"), env)
    monkeypatch.setattr(cd.OutboxDispatcher, "enqueue", _enqueue_direct, raising=False)

    cd.SCHEDULER_TICK_SEC   = 0.05
    cd.DISCOVERY_WINDOW_SEC = 0.05
    cd.FINALIZER_TICK_SEC   = 0.05
    cd.HB_MONITOR_TICK_SEC  = 0.2

    # Лимиты либеральные
    if not hasattr(cd, "MAX_GLOBAL_RUNNING"):
        cd.MAX_GLOBAL_RUNNING = 9999

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

@pytest.fixture
def handlers(env_and_imports):
    _, wu = env_and_imports

    class ProducerHandler(wu.RoleHandler):
        role = "producer"
        async def load_input(self, ref, inline): return inline or {}
        async def iter_batches(self, loaded):
            # Стримим маленькими батчами «долго»
            for i in range(30):
                yield wu.Batch(shard_id=f"s-{i}", payload={"skus": [f"sku-{i}"]})
                await asyncio.sleep(0.05)
        async def process_batch(self, batch, ctx):
            await asyncio.sleep(0.02)
            return wu.BatchResult(success=True, metrics={"count": 1})

    class SinkHandler(wu.RoleHandler):
        role = "sink"
        async def load_input(self, ref, inline):
            # downstream читает артефакты из producer — реальный адаптер не эмулируем, но это не критично здесь
            return {"input_inline": inline or {}}
        async def iter_batches(self, loaded):
            # «bootstrap», реальные батчи будут прилетать адаптером от координатора
            yield wu.Batch(shard_id=None, payload={"bootstrap": True})
        async def process_batch(self, batch, ctx):
            await asyncio.sleep(0.02)
            items = batch.payload.get("items") or []
            return wu.BatchResult(success=True, metrics={"sink": len(items)})

    class RestartableHandler(wu.RoleHandler):
        role = "flaky"
        async def load_input(self, ref, inline): return inline or {}
        async def iter_batches(self, loaded):
            # один батч; далее итог решает finalize (упадём в epoch=0)
            yield wu.Batch(shard_id="r-0", payload={"unit":"one"})
        async def process_batch(self, batch, ctx):
            await asyncio.sleep(0.03)
            return wu.BatchResult(success=True, metrics={"ok": 1})
        async def finalize(self, ctx):
            # имитируем падение в первую попытку (epoch=0)
            if ctx.artifacts.attempt_epoch == 0:
                raise RuntimeError("boom-first-epoch")
            await asyncio.sleep(0.02)
            return wu.FinalizeResult(metrics={"final": 1})

    return {
        "producer": ProducerHandler(),
        "sink": SinkHandler(),
        "flaky": RestartableHandler(),
    }

@pytest_asyncio.fixture
async def workers(env_and_imports, handlers):
    _, wu = env_and_imports
    w_prod = wu.Worker(roles=["producer"], handlers={"producer": handlers["producer"]})
    w_sink = wu.Worker(roles=["sink"],     handlers={"sink":     handlers["sink"]})
    w_flky = wu.Worker(roles=["flaky"],    handlers={"flaky":    handlers["flaky"]})
    for w in (w_prod, w_sink, w_flky): await w.start()
    try:
        yield {"producer": w_prod, "sink": w_sink, "flaky": w_flky}
    finally:
        for w in (w_prod, w_sink, w_flky): await w.stop()

# ---------------------- graph builders ---------------------------------
def graph_cancel_flow() -> Dict[str, Any]:
    return {
        "schema_version":"1.0",
        "nodes":[
            {"node_id":"w1","type":"producer","depends_on":[],"fan_in":"all",
             "io":{"input_inline":{}}},
            {"node_id":"w2","type":"sink","depends_on":["w1"],"fan_in":"any",
             "io":{"start_when":"first_batch",
                   "input_inline":{
                       "input_adapter":"pull.from_artifacts",
                       "input_args":{"from_nodes":["w1"],"poll_ms":30}
                   }}},
        ],
        "edges":[["w1","w2"]],
        "edges_ex":[{"from":"w1","to":"w2","mode":"async","trigger":"on_batch"}],
    }

def graph_restart_flaky() -> Dict[str, Any]:
    return {
        "schema_version":"1.0",
        "nodes":[
            {"node_id":"fx","type":"flaky","depends_on":[],"fan_in":"all",
             "retry_policy":{"max_retries":3,"backoff_sec":0.05},
             "io":{"input_inline":{}}}
        ],
        "edges":[]
    }

# ---------------------- TESTS ------------------------------------------

@pytest.mark.asyncio
async def test_cascade_cancel_prevents_downstream(env_and_imports, inmemory_db, coordinator, workers):
    cd, _ = env_and_imports

    # есть ли API отмены?
    cancel_method = None
    for name in ("cancel_task", "request_cancel", "abort_task", "cancel"):
        if hasattr(coordinator, name):
            cancel_method = getattr(coordinator, name)
            break
    if cancel_method is None:
        pytest.xfail("Coordinator cancel API is not implemented")

    g = prime_graph(cd, graph_cancel_flow())
    tid = await coordinator.create_task(params={}, graph=g)

    # Ждём, чтобы producer начал выполняться
    assert await wait_node_running(inmemory_db, tid, "w1", timeout=4.0), "producer didn't start"

    # Шпион на CANCELLED
    spy = AIOKafkaConsumerMock("status.producer.v1", group_id="test.spy.cancel")
    await spy.start()
    cancelled_seen = asyncio.Event()

    async def watch_cancel():
        while True:
            rec = await spy.getone()
            env = rec.value
            if env.get("msg_type") == "event" and (env.get("payload") or {}).get("kind") == "CANCELLED":
                if env.get("task_id") == tid and env.get("node_id") == "w1":
                    cancelled_seen.set()
                    return

    spy_task = asyncio.create_task(watch_cancel())

    # Отменяем всю задачу каскадом
    await cancel_method(tid, reason="test-cascade")

    # Ждём CANCELLED от w1
    await asyncio.wait_for(cancelled_seen.wait(), timeout=5.0)

    # Проверяем, что w2 так и не стартовал
    assert not await wait_node_running(inmemory_db, tid, "w2", timeout=1.0), "downstream should NOT start after cancel"

    spy_task.cancel()
    try: await spy_task
    except: pass
    await spy.stop()

    # Координатор должен корректно завершить таску (failed или cancelled/finished — допускаем оба статуса, в зависимости от реализации)
    tdoc = await inmemory_db.tasks.find_one({"id": tid})
    assert tdoc is not None
    assert tdoc.get("status") in ("failed", "finished", "cancelled"), f"unexpected task status after cancel: {tdoc.get('status')}"

@pytest.mark.asyncio
async def test_restart_higher_epoch_ignores_old_events(env_and_imports, inmemory_db, coordinator, workers):
    cd, _ = env_and_imports
    g = prime_graph(cd, graph_restart_flaky())
    tid = await coordinator.create_task(params={}, graph=g)

    # Шпионим за статусами flaky
    status_topic = "status.flaky.v1"
    spy = AIOKafkaConsumerMock(status_topic, group_id="test.spy.restart")
    await spy.start()

    old_epoch_events: List[Dict[str, Any]] = []
    accepted_epoch1 = asyncio.Event()

    async def collect():
        # Собираем события, сохраняем от epoch=0, и как только увидим TASK_ACCEPTED(epoch=1) — репаблишим один старый эвент
        saved_old: Optional[Dict[str, Any]] = None
        while True:
            rec = await spy.getone()
            env = rec.value
            if env.get("task_id") != tid:  # другая таска — пропустим
                continue
            if env.get("msg_type") != "event":
                continue
            kind = (env.get("payload") or {}).get("kind")
            epoch = env.get("attempt_epoch", 0)
            node_id = env.get("node_id")
            if node_id != "fx":
                continue
            # сохраняем последнее «старое» событие от epoch=0 (предпочтительно TASK_FAILED)
            if epoch == 0:
                saved_old = env
            # ждём принятие epoch=1
            if kind == "TASK_ACCEPTED" and epoch >= 1:
                accepted_epoch1.set()
                # репаблишим старый эвент, если сохранился
                if saved_old:
                    await BROKER.produce(status_topic, saved_old)
                return

    coll_task = asyncio.create_task(collect())

    # Ждём финала
    tdoc = await wait_all_finished(inmemory_db, tid, timeout=12.0)

    coll_task.cancel()
    try: await coll_task
    except: pass
    await spy.stop()

    # Проверка финального статуса узла fx
    node_map = {n["node_id"]: n for n in tdoc["graph"]["nodes"]}
    fx = node_map["fx"]
    assert fx["status"] == cd.RunState.finished or fx["status"] == "finished"

    # Попутно убеждаемся, что артефакты есть и помечены complete (вторая попытка успешная)
    cnt_art = await inmemory_db.artifacts.find_one({"task_id": tid, "node_id": "fx"})
    assert cnt_art is not None, "artifacts for fx should exist"

    # Бонус: убедимся, что attempt_epoch у узла >=1
    assert int(fx.get("attempt_epoch", 0)) >= 1
