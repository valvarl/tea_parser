import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Set

import pytest
import pytest_asyncio

# ---------------------- tiny logger ------------------------------------
def _ts(): return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
def dbg(tag, **kv):
    kvs = " ".join(f"{k}={kv[k]!r}" for k in kv)
    print(f"[{_ts()}] {tag}: {kvs}", flush=True)

# ---------------------- In-memory Kafka --------------------------------
class _Rec:
    __slots__ = ("value", "topic")
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
            def __init__(self, rows): self._rows = rows
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
            for k,v in upd["$setOnInsert"].items(): set_path(doc, k, v)

        if "$set" in upd:
            # небольшие логи ради диагностики
            if self.name == "tasks":
                for k,v in upd["$set"].items():
                    if k.endswith("graph.nodes.$.status"):
                        dbg("DB.TASK.STATUS", filter=flt, new_status=str(v))
                    if k.endswith("graph.nodes.$.lease"):
                        dbg("DB.TASK.LEASE", filter=flt, lease=v)
            for k,v in upd["$set"].items(): set_path(doc,k,v)

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

# ---------------------- Helpers ----------------------------------------
def prime_graph(cd, graph: Dict[str, Any]) -> Dict[str, Any]:
    for n in graph.get("nodes", []):
        n.setdefault("status", cd.RunState.queued)
        n.setdefault("attempt_epoch", 0)
        n.setdefault("stats", {})
        n.setdefault("lease", {})
    return graph

async def wait_all_tasks_finished(db: InMemDB, ids: List[str], timeout: float = 12.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        done = 0
        for tid in ids:
            t = await db.tasks.find_one({"id": tid})
            if t and t.get("status") == "finished":
                done += 1
        if done == len(ids):
            return
        await asyncio.sleep(0.05)
    raise AssertionError("tasks not finished in time")

async def gather_running_counts(db: InMemDB) -> Tuple[int, Dict[str,int]]:
    """
    Возвращает:
     - общее число running-узлов;
     - по типам (node['type']).
    """
    total = 0
    per_type: Dict[str,int] = {}
    for t in db.tasks.rows:
        for n in (t.get("graph",{}).get("nodes") or []):
            if n.get("status") == "running":
                total += 1
                per_type[n.get("type")] = per_type.get(n.get("type"), 0) + 1
    return total, per_type

# ---------------------- Fixtures ---------------------------------------
@pytest.fixture(scope="function")
def env_and_imports(monkeypatch):
    import sys
    sys.path.insert(0, "/home/valvarl/tea_parser/backend")

    from app.services import coordinator_dag as cd
    from app.services import worker_universal as wu

    # stdlib asyncio внутрь модулей
    import asyncio as aio
    monkeypatch.setattr(cd, "asyncio", aio, raising=False)
    monkeypatch.setattr(wu, "asyncio", aio, raising=False)

    # Kafka mocks
    monkeypatch.setattr(cd, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(cd, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)

    # Обходим outbox для скорости (в этом файле тестируем не доставку)
    async def _enqueue_direct(self, *, topic: str, key: str, env):
        dbg("OUTBOX.BYPASS", topic=topic)
        await self.bus._raw_send(topic, key.encode("utf-8"), env)
    monkeypatch.setattr(cd.OutboxDispatcher, "enqueue", _enqueue_direct, raising=False)

    # Быстрые тики
    cd.SCHEDULER_TICK_SEC   = 0.05
    cd.DISCOVERY_WINDOW_SEC = 0.05
    cd.FINALIZER_TICK_SEC   = 0.05
    cd.HB_MONITOR_TICK_SEC  = 0.2

    # выставим дефолтные лимиты (в тестах будем менять локально)
    if not hasattr(cd, "MAX_GLOBAL_RUNNING"):
        cd.MAX_GLOBAL_RUNNING = 9999
    if not hasattr(cd, "MAX_TYPE_CONCURRENCY"):
        # может быть dict или None — тест сам проверит наличие нужного поведения
        cd.MAX_TYPE_CONCURRENCY = None

    return cd, wu

@pytest.fixture
def inmemory_db(monkeypatch, env_and_imports):
    cd, wu = env_and_imports
    db = InMemDB()
    monkeypatch.setattr(cd, "db", db, raising=True)
    monkeypatch.setattr(wu, "db", db, raising=True)
    return db

# ---------------------- Handlers ---------------------------------------
@pytest.fixture
def handlers(env_and_imports):
    _, wu = env_and_imports

    async def sleepy(dt: float): await asyncio.sleep(dt)

    class SlowHandler(wu.RoleHandler):
        role = "slow"
        async def load_input(self, ref, inline):
            return inline or {}
        async def iter_batches(self, loaded):
            # один батч на узел
            yield wu.Batch(shard_id=None, payload={"unit": "one"})
        async def process_batch(self, batch, ctx):
            await sleepy(0.30)
            return wu.BatchResult(success=True, metrics={"ok": 1})

    class FastHandler(wu.RoleHandler):
        role = "fast"
        async def load_input(self, ref, inline): return inline or {}
        async def iter_batches(self, loaded):
            yield wu.Batch(shard_id=None, payload={"unit": "one"})
        async def process_batch(self, batch, ctx):
            await sleepy(0.10)
            return wu.BatchResult(success=True, metrics={"ok": 1})

    return {"slow": SlowHandler(), "fast": FastHandler()}

@pytest_asyncio.fixture
async def workers(env_and_imports, handlers):
    _, wu = env_and_imports
    # Запускаем пустой набор — сами решим в тестах, кого поднимать
    class _Pool:
        def __init__(self): self.objs=[]
        async def add(self, roles, reg):
            w = wu.Worker(roles=roles, handlers=reg)
            await w.start()
            self.objs.append(w)
            return w
        async def stop_all(self):
            for w in self.objs:
                await w.stop()
            self.objs.clear()
    pool = _Pool()
    try:
        yield pool
    finally:
        await pool.stop_all()

@pytest_asyncio.fixture
async def coordinator(env_and_imports, inmemory_db):
    cd, _ = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        yield coord
    finally:
        await coord.stop()

# ---------------------- Graph builders ---------------------------------
def graph_many_roots(role: str, n: int) -> Dict[str, Any]:
    return {
        "schema_version": "1.0",
        "nodes": [
            {"node_id": f"{role}-{i}", "type": role, "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}
            for i in range(n)
        ],
        "edges": [],
    }

def graph_two_types(slow_n: int, fast_n: int) -> Dict[str, Any]:
    nodes = [
        {"node_id": f"slow-{i}", "type": "slow", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}
        for i in range(slow_n)
    ] + [
        {"node_id": f"fast-{i}", "type": "fast", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}
        for i in range(fast_n)
    ]
    return {"schema_version":"1.0", "nodes": nodes, "edges": []}

# ---------------------- Tests ------------------------------------------

@pytest.mark.asyncio
async def test_max_global_running_limit(env_and_imports, inmemory_db, coordinator, workers, handlers, monkeypatch):
    cd, _ = env_and_imports
    # Глобальный лимит = 2
    cd.MAX_GLOBAL_RUNNING = 2

    # 5 независимых узлов одного типа "slow"
    g = prime_graph(cd, graph_many_roots("slow", 5))
    tid = await coordinator.create_task(params={}, graph=g)

    # 3 воркера одного типа доступны (ресурсов достаточно, чтобы уйти выше лимита, если бы координатор не ограничивал)
    await workers.add(["slow"], {"slow": handlers["slow"]})
    await workers.add(["slow"], {"slow": handlers["slow"]})
    await workers.add(["slow"], {"slow": handlers["slow"]})

    # наблюдаем параллелизм
    max_running = 0
    async def monitor():
        nonlocal max_running
        end = time.time() + 6.0
        while time.time() < end:
            total, _ = await gather_running_counts(inmemory_db)
            max_running = max(max_running, total)
            await asyncio.sleep(0.03)

    mon = asyncio.create_task(monitor())
    await wait_all_tasks_finished(inmemory_db, [tid], timeout=10.0)
    mon.cancel()
    try: await mon
    except: pass

    assert max_running <= 2, f"MAX_GLOBAL_RUNNING violated, max_running={max_running}"

@pytest.mark.asyncio
async def test_max_type_concurrency_limits(env_and_imports, inmemory_db, coordinator, workers, handlers, monkeypatch):
    cd, _ = env_and_imports
    # если координатор не поддерживает per-type лимиты — xfail
    if getattr(cd, "MAX_TYPE_CONCURRENCY", None) is None:
        pytest.xfail("Coordinator doesn't expose MAX_TYPE_CONCURRENCY — feature not implemented")

    # Глобальный большой, чтобы влиял только per-type
    cd.MAX_GLOBAL_RUNNING = 99
    cd.MAX_TYPE_CONCURRENCY = {"slow": 1, "fast": 2}

    g = prime_graph(cd, graph_two_types(slow_n=4, fast_n=5))
    tid = await coordinator.create_task(params={}, graph=g)

    # поднимем 2 воркера slow и 3 воркера fast — ресурсов с запасом
    await workers.add(["slow"], {"slow": handlers["slow"]})
    await workers.add(["slow"], {"slow": handlers["slow"]})
    await workers.add(["fast"], {"fast": handlers["fast"]})
    await workers.add(["fast"], {"fast": handlers["fast"]})
    await workers.add(["fast"], {"fast": handlers["fast"]})

    max_running_total = 0
    max_running_slow  = 0
    max_running_fast  = 0

    async def monitor():
        nonlocal max_running_total, max_running_slow, max_running_fast
        end = time.time() + 8.0
        while time.time() < end:
            total, per_type = await gather_running_counts(inmemory_db)
            max_running_total = max(max_running_total, total)
            max_running_slow  = max(max_running_slow, per_type.get("slow", 0))
            max_running_fast  = max(max_running_fast, per_type.get("fast", 0))
            await asyncio.sleep(0.03)

    mon = asyncio.create_task(monitor())
    await wait_all_tasks_finished(inmemory_db, [tid], timeout=12.0)
    mon.cancel()
    try: await mon
    except: pass

    assert max_running_slow <= 1, f"slow type concurrency violated: {max_running_slow}"
    assert max_running_fast <= 2, f"fast type concurrency violated: {max_running_fast}"

@pytest.mark.asyncio
async def test_multi_workers_same_type_rr_distribution(env_and_imports, inmemory_db, coordinator, workers, handlers):
    cd, _ = env_and_imports
    cd.MAX_GLOBAL_RUNNING = 99

    # 6 независимых узлов типа "slow"
    g = prime_graph(cd, graph_many_roots("slow", 6))
    tid = await coordinator.create_task(params={}, graph=g)

    # два воркера одного типа
    await workers.add(["slow"], {"slow": handlers["slow"]})
    await workers.add(["slow"], {"slow": handlers["slow"]})

    # "шпион" на статусах slow — собираем TASK_ACCEPTED по worker_id
    consumer = AIOKafkaConsumerMock(f"status.slow.v1", group_id="test.spy")
    await consumer.start()
    seen_workers: Set[str] = set()

    async def spy():
        end = time.time() + 8.0
        while time.time() < end:
            rec = await consumer.getone()
            env = rec.value
            if env.get("msg_type") == "event" and (env.get("payload") or {}).get("kind") == "TASK_ACCEPTED":
                w = (env.get("payload") or {}).get("worker_id")
                t = env.get("task_id")
                if t == tid and w:
                    seen_workers.add(w)
            await asyncio.sleep(0.001)

    spy_task = asyncio.create_task(spy())
    await wait_all_tasks_finished(inmemory_db, [tid], timeout=10.0)
    spy_task.cancel()
    try: await spy_task
    except: pass
    await consumer.stop()

    # хотя бы два разных воркера должны были принять разные узлы
    assert len(seen_workers) >= 2, f"expected distribution across >=2 workers, got {seen_workers}"

@pytest.mark.asyncio
async def test_concurrent_tasks_respect_global_limit(env_and_imports, inmemory_db, coordinator, workers, handlers):
    cd, _ = env_and_imports
    # Глобальный лимит = 2, пер-тайпов нет
    cd.MAX_GLOBAL_RUNNING = 2

    # Две задачи, каждая со множеством независимых slow-узлов
    g1 = prime_graph(cd, graph_many_roots("slow", 4))
    g2 = prime_graph(cd, graph_many_roots("slow", 5))
    t1 = await coordinator.create_task(params={}, graph=g1)
    t2 = await coordinator.create_task(params={}, graph=g2)

    # 3 воркера slow (ресурсов выше, чем лимит)
    await workers.add(["slow"], {"slow": handlers["slow"]})
    await workers.add(["slow"], {"slow": handlers["slow"]})
    await workers.add(["slow"], {"slow": handlers["slow"]})

    max_running = 0
    async def monitor():
        nonlocal max_running
        end = time.time() + 10.0
        while time.time() < end:
            total, _ = await gather_running_counts(inmemory_db)
            max_running = max(max_running, total)
            await asyncio.sleep(0.03)

    mon = asyncio.create_task(monitor())
    await wait_all_tasks_finished(inmemory_db, [t1, t2], timeout=14.0)
    mon.cancel()
    try: await mon
    except: pass

    assert max_running <= 2, f"global limit violated across concurrent tasks: {max_running}"
