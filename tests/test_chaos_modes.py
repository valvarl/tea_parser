# tests/test_chaos_modes.py
import asyncio
import sys
from typing import Any, Dict

import pytest
import pytest_asyncio

from tests.helpers.pipeline_sim import (
    ChaosConfig, ChaosMonkey, InMemKafkaBroker, InMemDB,
    AIOKafkaProducerMockChaos, AIOKafkaConsumerMockChaos,
    BROKER, dbg, prime_graph
)

# ---------------------- fixtures: env, chaos, db, coord, workers --------
@pytest.fixture(scope="function")
def chaos():
    # мягкий режим: задержки и дубли; drop выключен, краши выключены
    cfg = ChaosConfig(
        seed=123,
        broker_delay_range=(0.0, 0.003),
        consumer_poll_delay_range=(0.0, 0.002),
        handler_delay_range=(0.0, 0.006),
        dup_prob_by_topic={"status.": 0.08, "cmd.": 0.05},
        drop_prob_by_topic={},            # важно: без дропов по умолчанию
        handler_crash_prob=0.0,
        handler_perm_crash_prob=0.0,
        enable_crashes=False
    )
    return ChaosMonkey(cfg)

@pytest.fixture(scope="function")
def env_and_imports(monkeypatch, chaos):
    # глобальный брокер с хаосом
    from tests.helpers import pipeline_sim as sim
    sim.BROKER = InMemKafkaBroker(chaos)

    sys.path.insert(0, "/home/valvarl/tea_parser/backend")
    from app.services import coordinator_dag as cd
    from app.services import worker_universal as wu

    import asyncio as aio
    monkeypatch.setattr(cd, "asyncio", aio, raising=False)
    monkeypatch.setattr(wu, "asyncio", aio, raising=False)

    monkeypatch.setattr(cd, "AIOKafkaProducer", AIOKafkaProducerMockChaos, raising=True)
    monkeypatch.setattr(cd, "AIOKafkaConsumer", AIOKafkaConsumerMockChaos, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaProducer", AIOKafkaProducerMockChaos, raising=True)
    monkeypatch.setattr(wu, "AIOKafkaConsumer", AIOKafkaConsumerMockChaos, raising=True)

    # обходим outbox, чтобы не усложнять доставку в этом наборе тестов
    async def _enqueue_direct(self, *, topic: str, key: str, env):
        dbg("OUTBOX.BYPASS", topic=topic)
        await self.bus._raw_send(topic, key.encode("utf-8"), env)
    monkeypatch.setattr(cd.OutboxDispatcher, "enqueue", _enqueue_direct, raising=False)

    # тики
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

# ---------------------- handlers (с хаос-задержками) --------------------
@pytest.fixture
def handlers(env_and_imports, chaos):
    _, wu = env_and_imports

    class Indexer(wu.RoleHandler):
        role = "indexer"
        async def load_input(self, ref, inline): return inline or {}
        async def iter_batches(self, loaded):
            total = int(loaded.get("total_skus", 12))
            bs    = int(loaded.get("batch_size", 5))
            skus  = [f"sku-{i}" for i in range(total)]
            shard = 0
            for i in range(0, total, bs):
                yield wu.Batch(shard_id=f"w1-{shard}", payload={"skus": skus[i:i+bs]})
                shard += 1
        async def process_batch(self, batch, ctx):
            await chaos.handler_delay()
            return wu.BatchResult(success=True, metrics={"skus": batch.payload["skus"], "count": len(batch.payload["skus"])})

    class Enricher(wu.RoleHandler):
        role = "enricher"
        async def load_input(self, ref, inline): return {"input_inline": inline or {}}
        async def iter_batches(self, loaded): yield wu.Batch(shard_id=None, payload={"bootstrap": True})
        async def process_batch(self, batch, ctx):
            await chaos.handler_delay()
            items = batch.payload.get("items") or []
            if not items:
                return wu.BatchResult(success=True, metrics={"noop": 1})
            # transient crash?
            chaos.maybe_handler_crash(permanent=False)
            enriched = [{"sku": (x if isinstance(x, str) else x.get("sku", x)), "enriched": True} for x in items]
            return wu.BatchResult(success=True, metrics={"enriched": enriched, "count": len(enriched)})

    class OCR(wu.RoleHandler):
        role = "ocr"
        async def load_input(self, ref, inline): return {"input_inline": inline or {}}
        async def iter_batches(self, loaded): yield wu.Batch(shard_id=None, payload={"bootstrap": True})
        async def process_batch(self, batch, ctx):
            await chaos.handler_delay()
            items = batch.payload.get("items") or []
            if not items: return wu.BatchResult(success=True, metrics={"noop": 1})
            ocrd = [{"sku": (it["sku"] if isinstance(it, dict) else it), "ocr_ok": True} for it in items]
            return wu.BatchResult(success=True, metrics={"ocr": ocrd, "count": len(ocrd)})

    class Analyzer(wu.RoleHandler):
        role = "analyzer"
        async def load_input(self, ref, inline): return {"input_inline": inline or {}}
        async def iter_batches(self, loaded): yield wu.Batch(shard_id=None, payload={"bootstrap": True})
        async def process_batch(self, batch, ctx):
            await chaos.handler_delay()
            items = batch.payload.get("items") or []
            return wu.BatchResult(success=True, metrics={"sinked": len(items), "count": len(items)} if items else {"noop": 1})

    return {
        "indexer": Indexer(),
        "enricher": Enricher(),
        "ocr": OCR(),
        "analyzer": Analyzer(),
    }

@pytest_asyncio.fixture
async def workers(env_and_imports, handlers):
    _, wu = env_and_imports
    w_indexer  = wu.Worker(roles=["indexer"],  handlers={"indexer":  handlers["indexer"]})
    w_enricher = wu.Worker(roles=["enricher"], handlers={"enricher": handlers["enricher"]})
    w_ocr      = wu.Worker(roles=["ocr"],      handlers={"ocr":      handlers["ocr"]})
    w_analyzer = wu.Worker(roles=["analyzer"], handlers={"analyzer": handlers["analyzer"]})
    for w in (w_indexer, w_enricher, w_ocr, w_analyzer): await w.start()
    try:
        yield {"w1": w_indexer, "w2": w_enricher, "w3": w_ocr, "w4": w_analyzer}
    finally:
        for w in (w_indexer, w_enricher, w_ocr, w_analyzer): await w.stop()

# ---------------------- graphs -----------------------------------------
def graph_stream() -> Dict[str, Any]:
    return {
        "schema_version": "1.0",
        "nodes": [
            {"node_id": "w1", "type": "indexer", "depends_on": [], "fan_in": "all",
             "io": {"input_inline": {"batch_size": 5, "total_skus": 18}}},
            {"node_id": "w2", "type": "enricher", "depends_on": ["w1"], "fan_in": "any",
             "io": {"start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts.rechunk:size",
                        "input_args": {"from_nodes": ["w1"], "size": 3, "poll_ms": 30, "meta_list_key": "skus"}
                    }}},
            {"node_id": "w5", "type": "ocr", "depends_on": ["w2"], "fan_in": "any",
             "io": {"start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts.rechunk:size",
                        "input_args": {"from_nodes": ["w2"], "size": 2, "poll_ms": 25, "meta_list_key": "enriched"}
                    }}},
            {"node_id": "w4", "type": "analyzer", "depends_on": ["w5"], "fan_in": "any",
             "io": {"start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts",
                        "input_args": {"from_nodes": ["w5"], "poll_ms": 25}
                    }}},
        ],
        "edges": [["w1","w2"],["w2","w5"],["w5","w4"]],
        "edges_ex": [
            {"from":"w1","to":"w2","mode":"async","trigger":"on_batch"},
            {"from":"w2","to":"w5","mode":"async","trigger":"on_batch"},
            {"from":"w5","to":"w4","mode":"async","trigger":"on_batch"},
        ],
    }

# ---------------------- utils ------------------------------------------
async def wait_finished(db: InMemDB, task_id: str, timeout: float = 15.0):
    import time
    t0 = time.time(); last = 0.0
    while time.time() - t0 < timeout:
        t = await db.tasks.find_one({"id": task_id})
        now = time.time()
        if now - last > 0.6:
            nodes = {n["node_id"]: n.get("status") for n in (t or {}).get("graph",{}).get("nodes", [])}
            dbg("WAIT", status=(t or {}).get("status"), nodes=nodes)
            last = now
        if t and t.get("status") == "finished":
            return t
        await asyncio.sleep(0.05)
    raise AssertionError("task not finished in time")

# ---------------------- TESTS ------------------------------------------

@pytest.mark.asyncio
async def test_chaos_delays_and_duplications(env_and_imports, inmemory_db, coordinator, workers):
    """
    Случайные задержки в брокере/handlers, дубли сообщений. Дропы отключены.
    Ожидаем, что пайплайн завершается, а артефакты на w1/w2/w5 присутствуют.
    """
    cd, _ = env_and_imports
    g = prime_graph(cd, graph_stream())
    task_id = await coordinator.create_task(params={}, graph=g)
    tdoc = await wait_finished(inmemory_db, task_id, timeout=20.0)

    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    assert all(st[n] == cd.RunState.finished for n in ("w1","w2","w5","w4"))

    for nid in ("w1","w2","w5"):
        cnt = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": nid})
        assert cnt > 0, f"no artifacts for {nid}"

@pytest.mark.asyncio
async def test_chaos_worker_restart_mid_stream(env_and_imports, inmemory_db, coordinator, workers):
    """
    Рестарт воркера 'enricher' в процессе стриминга.
    Ожидаем корректное завершение всей задачи.
    """
    cd, wu = env_and_imports
    g = prime_graph(cd, graph_stream())
    task_id = await coordinator.create_task(params={}, graph=g)

    async def restart_enricher():
        await asyncio.sleep(0.35)  # чуть подождать, чтобы успел стартовать
        dbg("CHAOS", action="worker_restart", role="enricher")
        # стоп
        await workers["w2"].stop()
        # старт новым инстансом того же типа
        new_w2 = wu.Worker(roles=["enricher"], handlers=workers["w2"].handlers)
        await new_w2.start()
        # заменить в словаре, чтобы финализатор фикстуры корректно стопнул
        workers["w2"] = new_w2

    asyncio.create_task(restart_enricher())
    tdoc = await wait_finished(inmemory_db, task_id, timeout=25.0)

    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    assert all(st[n] == cd.RunState.finished for n in ("w1","w2","w5","w4"))

@pytest.mark.asyncio
async def test_chaos_coordinator_restart(env_and_imports, inmemory_db, coordinator, workers):
    """
    Рестарт координатора во время работы задачи.
    Проверяем, что пайплайн завершается.
    """
    cd, _ = env_and_imports
    g = prime_graph(cd, graph_stream())
    task_id = await coordinator.create_task(params={}, graph=g)

    async def restart_coord():
        await asyncio.sleep(0.3)
        dbg("CHAOS", action="coord_restart")
        await coordinator.stop()
        await coordinator.start()

    asyncio.create_task(restart_coord())
    tdoc = await wait_finished(inmemory_db, task_id, timeout=25.0)

    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    assert all(st[n] == cd.RunState.finished for n in ("w1","w2","w5","w4"))
