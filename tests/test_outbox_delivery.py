import asyncio
import time
import pytest
import pytest_asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List

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
            def sort(self, *_): return self          # упрощённо: сорт не нужен для теста
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
            for k,v in upd["$set"].items():
                set_path(doc,k,v)
                if self.name == "outbox" and k == "state":
                    dbg("DB.OUTBOX.STATE", new=v)

        if "$inc" in upd:
            for k,v in upd["$inc"].items():
                # not used here
                pass

        if "$max" in upd:
            for k,v in upd["$max"].items(): set_path(doc,k,max(doc.get(k,0), v))
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

# ---------------------- Fixtures: env ----------------------------------
@pytest.fixture(scope="function")
def env_and_imports(monkeypatch):
    """
    Настраиваем окружение специально под проверки outbox:
    - включаем in-memory Kafka;
    - НЕ заменяем OutboxDispatcher.enqueue (важно!);
    - настраиваем короткие тики.
    """
    import sys, os
    sys.path.insert(0, "/home/valvarl/tea_parser/backend")

    from app.services import coordinator_dag as cd

    # патчим asyncio внутри модуля на stdlib
    import asyncio as aio
    monkeypatch.setattr(cd, "asyncio", aio, raising=False)

    # Kafka mocks
    monkeypatch.setattr(cd, "AIOKafkaProducer", AIOKafkaProducerMock, raising=True)
    monkeypatch.setattr(cd, "AIOKafkaConsumer", AIOKafkaConsumerMock, raising=True)

    # ускоряем outbox-луп
    cd.OUTBOX_DISPATCH_TICK_SEC = 0.05
    cd.OUTBOX_MAX_RETRY         = 5
    cd.OUTBOX_BACKOFF_MIN_MS    = 1000    # целые секунды (см. //1000 в коде)
    cd.OUTBOX_BACKOFF_MAX_MS    = 4000

    # без джиттера — чтобы детерминированно проверять >=1с задержку
    monkeypatch.setattr(cd, "_jitter_ms", lambda base_ms: base_ms, raising=False)

    # коэффициенты координатора не важны, но пусть быстрые
    cd.SCHEDULER_TICK_SEC   = 0.05
    cd.DISCOVERY_WINDOW_SEC = 0.05
    cd.FINALIZER_TICK_SEC   = 0.05
    cd.HB_MONITOR_TICK_SEC  = 0.2
    return cd

@pytest.fixture
def inmemory_db(monkeypatch, env_and_imports):
    cd = env_and_imports
    db = InMemDB()
    monkeypatch.setattr(cd, "db", db, raising=True)
    return db

@pytest_asyncio.fixture
async def coordinator(env_and_imports, inmemory_db, monkeypatch):
    cd = env_and_imports
    coord = cd.Coordinator()
    dbg("COORD.STARTING")
    await coord.start()
    dbg("COORD.STARTED")
    try:
        yield coord
    finally:
        dbg("COORD.STOPPING")
        await coord.stop()
        dbg("COORD.STOPPED")

# ---------------------- Tests -----------------------------------------

@pytest.mark.asyncio
async def test_outbox_retry_backoff(env_and_imports, inmemory_db, coordinator, monkeypatch):
    """
    Роняем _raw_send на первые 2 попытки → outbox переводит запись в retry,
    увеличивает attempts и next_attempt_at, затем успешно отправляет и помечает 'sent'.
    """
    cd = env_and_imports

    # счётчик попыток конкретно для (topic,key)
    attempts: Dict[str, int] = {}
    topic = "test.topic.retry"
    key   = "k1"

    orig_raw = coordinator.bus._raw_send

    async def flaky_raw_send(t: str, k: bytes, env):
        kk = f"{t}:{k.decode()}"
        n = attempts.get(kk, 0)
        attempts[kk] = n + 1
        dbg("RAW_SEND.TRY", kk=kk, attempt=n+1)
        if n < 2:  # первые 2 раза кидаем ошибку
            raise RuntimeError("broker temporary down")
        await orig_raw(t, k, env)

    monkeypatch.setattr(coordinator.bus, "_raw_send", flaky_raw_send, raising=True)

    # готовим envelope (любой)
    env = cd.Envelope(
        msg_type=cd.MsgType.cmd, role=cd.Role.coordinator,
        dedup_id="d-1", task_id="t", node_id="n", step_type="echo",
        attempt_epoch=1, payload={"kind":"TEST"}
    )

    # кидаем в outbox
    await coordinator.outbox.enqueue(topic=topic, key=key, env=env)

    # ждём 2 перехода в retry, затем sent
    async def wait_state(expect, timeout=4.0):
        t0 = time.time()
        while time.time() - t0 < timeout:
            d = await inmemory_db.outbox.find_one({"topic": topic, "key": key})
            if d and d.get("state") == expect:
                return d
            await asyncio.sleep(0.03)
        raise AssertionError(f"outbox not in '{expect}'")

    d1 = await wait_state("retry", timeout=2.0)   # после 1-го фейла
    assert int(d1.get("attempts", 0)) == 1
    assert int(d1.get("next_attempt_at", 0)) >= int(time.time()) + 1  # backoff ≥1s

    d2 = await wait_state("retry", timeout=3.0)   # после 2-го фейла
    assert int(d2.get("attempts", 0)) == 2
    assert int(d2.get("next_attempt_at", 0)) >= int(time.time()) + 1  # снова ≥1s

    d3 = await wait_state("sent", timeout=4.0)    # на 3-й раз — успех
    assert int(attempts[f"{topic}:{key}"]) == 3

@pytest.mark.asyncio
async def test_outbox_exactly_once_fp_uniqueness(env_and_imports, inmemory_db, coordinator, monkeypatch):
    """
    Повторная попытка enqueue с тем же (topic,key,dedup_id) → в outbox ровно одна запись,
    и _raw_send вызывается ровно один раз (симуляция уникального индекса по fp).
    """
    cd = env_and_imports
    topic = "test.topic.once"
    key   = "k2"

    # эмулируем уникальный индекс по fp на стороне InMemDB
    seen_fp = set()
    orig_insert = inmemory_db.outbox.insert_one

    async def unique_insert_one(doc):
        fp = doc.get("fp")
        if fp in seen_fp:
            dbg("DB.OUTBOX.DUP_FP_BLOCK", fp=fp)
            raise RuntimeError("duplicate key on fp")
        seen_fp.add(fp)
        await orig_insert(doc)

    inmemory_db.outbox.insert_one = unique_insert_one  # type: ignore

    # считаем реальные вызовы raw_send
    sent_calls = []
    orig_raw = coordinator.bus._raw_send
    async def counting_raw_send(t, k, env):
        sent_calls.append((t, k.decode(), env.dedup_id))
        await orig_raw(t, k, env)
    monkeypatch.setattr(coordinator.bus, "_raw_send", counting_raw_send, raising=True)

    # 2 одинаковых Envelope → одинаковый fp
    env1 = cd.Envelope(
        msg_type=cd.MsgType.cmd, role=cd.Role.coordinator,
        dedup_id="same-dedup", task_id="t", node_id="n", step_type="echo",
        attempt_epoch=1, payload={"kind":"TEST"}
    )
    env2 = cd.Envelope(
        msg_type=cd.MsgType.cmd, role=cd.Role.coordinator,
        dedup_id="same-dedup", task_id="t", node_id="n", step_type="echo",
        attempt_epoch=1, payload={"kind":"TEST"}
    )

    # enqueue дважды
    await coordinator.outbox.enqueue(topic=topic, key=key, env=env1)
    await coordinator.outbox.enqueue(topic=topic, key=key, env=env2)  # должен быть проигнорен из-за dup fp

    # дождёмся отправки
    t0 = time.time()
    while time.time() - t0 < 2.0 and not any(s for s in sent_calls if s[0] == topic and s[1] == key):
        await asyncio.sleep(0.02)

    # проверяем, что отправка была ровно одна
    cnt = sum(1 for s in sent_calls if s[0] == topic and s[1] == key)
    assert cnt == 1, f"expected exactly one send, got {cnt}"

    # и что в outbox ровно одна запись
    docs = [r for r in inmemory_db.outbox.rows if r.get("topic")==topic and r.get("key")==key]
    assert len(docs) == 1
    assert docs[0].get("state") == "sent"
