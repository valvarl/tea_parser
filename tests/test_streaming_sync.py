import asyncio
import pytest
import pytest_asyncio

from tests.helpers.pipeline_sim import (
    setup_env_and_imports, install_inmemory_db, make_test_handlers,
    prime_graph, wait_task_finished, dbg,
)

# ───────────────────────── Fixtures ─────────────────────────

@pytest.fixture(scope="function")
def env_and_imports(monkeypatch):
    cd, wu = setup_env_and_imports(monkeypatch)
    return cd, wu

@pytest.fixture
def inmemory_db(monkeypatch, env_and_imports):
    cd, wu = env_and_imports
    return install_inmemory_db(monkeypatch, cd, wu)

@pytest.fixture
def handlers(env_and_imports):
    _, wu = env_and_imports
    return make_test_handlers(wu)

@pytest_asyncio.fixture
async def coord(env_and_imports, inmemory_db):
    cd, _ = env_and_imports
    c = cd.Coordinator()
    dbg("COORD.STARTING")
    await c.start()
    dbg("COORD.STARTED")
    try:
        yield c
    finally:
        dbg("COORD.STOPPING")
        await c.stop()
        dbg("COORD.STOPPED")

@pytest_asyncio.fixture
async def workers_indexer_analyzer(env_and_imports, handlers):
    """
    Минимальный набор на 1 апстрим + 1 даунстрим.
    """
    _, wu = env_and_imports
    w_idx = wu.Worker(roles=["indexer"],  handlers={"indexer":  handlers["indexer"]})
    w_ana = wu.Worker(roles=["analyzer"], handlers={"analyzer": handlers["analyzer"]})
    for name, w in (("indexer", w_idx), ("analyzer", w_ana)):
        dbg("WORKER.STARTING", role=name)
        await w.start()
        dbg("WORKER.STARTED", role=name)
    try:
        yield (w_idx, w_ana)
    finally:
        for name, w in (("indexer", w_idx), ("analyzer", w_ana)):
            dbg("WORKER.STOPPING", role=name)
            await w.stop()
            dbg("WORKER.STOPPED", role=name)

@pytest_asyncio.fixture
async def workers_3indexers_analyzer(env_and_imports, handlers):
    """
    Три независимых воркера одной роли 'indexer' → реальный параллельный мульти-стрим в один 'analyzer'.
    (В in-mem Kafka обе консюмеры делят одну group-очередь, что даёт конкуренцию).
    """
    _, wu = env_and_imports
    w_idx1 = wu.Worker(roles=["indexer"],  handlers={"indexer":  handlers["indexer"]})
    w_idx2 = wu.Worker(roles=["indexer"],  handlers={"indexer":  handlers["indexer"]})
    w_idx3 = wu.Worker(roles=["indexer"],  handlers={"indexer":  handlers["indexer"]})
    w_ana  = wu.Worker(roles=["analyzer"], handlers={"analyzer": handlers["analyzer"]})
    workers = [("indexer#1", w_idx1), ("indexer#2", w_idx2), ("indexer#3", w_idx3), ("analyzer", w_ana)]
    for name, w in workers:
        dbg("WORKER.STARTING", role=name)
        await w.start()
        dbg("WORKER.STARTED", role=name)
    try:
        yield (w_idx1, w_idx2, w_idx3, w_ana)
    finally:
        for name, w in workers:
            dbg("WORKER.STOPPING", role=name)
            await w.stop()
            dbg("WORKER.STOPPED", role=name)

# ───────────────────────── Small helpers ─────────────────────────

async def _get_task(db, task_id):
    return await db.tasks.find_one({"id": task_id})

def _node_status(doc, node_id):
    for n in (doc.get("graph", {}).get("nodes") or []):
        if n.get("node_id") == node_id:
            return n.get("status")
    return None

async def wait_node_running(db, task_id, node_id, timeout=5.0):
    from time import time
    t0 = time()
    while time() - t0 < timeout:
        doc = await _get_task(db, task_id)
        if doc:
            st = _node_status(doc, node_id)
            if str(st).endswith("running"):
                return doc
        await asyncio.sleep(0.02)
    raise AssertionError(f"node {node_id} not running in time")

async def wait_node_not_running_for(db, task_id, node_id, hold=0.6, timeout=5.0):
    """
    Убедиться, что узел не стартует заданное окно 'hold' после события,
    полезно для негативных проверок (после_upstream_complete).
    """
    from time import time
    t0 = time()
    seen_running = False
    while time() - t0 < hold:
        doc = await _get_task(db, task_id)
        st = _node_status(doc or {}, node_id)
        if str(st).endswith("running"):
            seen_running = True
            break
        await asyncio.sleep(0.03)
    assert not seen_running, f"{node_id} unexpectedly started during hold window"

def _make_indexer(node_id, total, batch):
    return {
        "node_id": node_id, "type": "indexer",
        "depends_on": [], "fan_in": "all",
        "io": {"input_inline": {"batch_size": batch, "total_skus": total}},
        "status": None, "attempt_epoch": 0
    }

# ───────────────────────── Tests ─────────────────────────

@pytest.mark.asyncio
async def test_start_when_first_batch_starts_early(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    start_when=first_batch: downstream (analyzer) должен перейти в running ПОКА upstream (indexer) ещё в running.
    """
    cd, _ = env_and_imports

    # Один апстрим выдаёт несколько батчей, чтобы downstream мог стартануть по первому батчу
    u = _make_indexer("u", total=12, batch=4)  # 3 батча

    d = {
        "node_id": "d", "type": "analyzer",
        "depends_on": ["u"], "fan_in": "all",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["u"], "poll_ms": 30}}},
        "status": None, "attempt_epoch": 0
    }

    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u","d"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("T.FIRSTBATCH.CREATED", task_id=task_id)

    # Ждём старта downstream
    doc_when_d_runs = await wait_node_running(inmemory_db, task_id, "d", timeout=6.0)
    st_u = _node_status(doc_when_d_runs, "u")
    dbg("FIRSTBATCH.START_OBSERVED", u=st_u, d="running")

    # Апстрим к этому моменту ещё не обязан быть finished
    assert not str(st_u).endswith("finished"), "Upstream already finished, early start не проверяется"

    # В конце — всё финишит
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    final = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("FIRSTBATCH.FINAL", statuses=final)
    assert final["u"] == cd.RunState.finished
    assert final["d"] == cd.RunState.finished

@pytest.mark.asyncio
async def test_after_upstream_complete_delays_start(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Без start_when (поведение по умолчанию): downstream не должен стартовать, пока upstream не завершился.
    """
    cd, _ = env_and_imports

    u = _make_indexer("u", total=10, batch=5)  # два батча → будет заметное окно running
    d = {
        "node_id":"d", "type":"analyzer",
        "depends_on":["u"], "fan_in":"all",
        "io": {"input_inline": {"input_adapter":"pull.from_artifacts",
                                "input_args":{"from_nodes":["u"], "poll_ms": 30}}},
        "status": None, "attempt_epoch": 0
    }

    graph = {"schema_version":"1.0", "nodes":[u,d], "edges":[["u","d"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("T.AFTERCOMP.CREATED", task_id=task_id)

    # Убеждаемся, что downstream НЕ стартует некоторое время, пока upstream должен быть running
    await wait_node_not_running_for(inmemory_db, task_id, "d", hold=0.8)

    # Дожимаем до полного финиша
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    final = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("AFTERCOMP.FINAL", statuses=final)
    assert final["u"] == cd.RunState.finished
    assert final["d"] == cd.RunState.finished

@pytest.mark.asyncio
async def test_multistream_fanin_stream_to_one_downstream(env_and_imports, inmemory_db, coord, workers_3indexers_analyzer):
    """
    Мульти-стрим: три upstream (u1,u2,u3) параллельно стримят в один downstream (d).
    D стартует по first_batch и получает смешанный поток артефактов.
    Проверяем ранний старт и итоговые метрики.
    """
    cd, _ = env_and_imports

    # Три источника, у каждого несколько батчей
    u1 = _make_indexer("u1", total=9,  batch=3)  # 3 батча
    u2 = _make_indexer("u2", total=8,  batch=4)  # 2 батча
    u3 = _make_indexer("u3", total=12, batch=3)  # 4 батча

    d = {
        "node_id": "d", "type": "analyzer",
        "depends_on": ["u1","u2","u3"], "fan_in": "any",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["u1","u2","u3"], "poll_ms": 25}}},
        "status": None, "attempt_epoch": 0
    }

    graph = {
        "schema_version": "1.0",
        "nodes": [u1,u2,u3,d],
        "edges": [["u1","d"],["u2","d"],["u3","d"]],
    }
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("T.MULTISTREAM.CREATED", task_id=task_id)

    # Ждём раннего старта downstream
    doc_when_d_runs = await wait_node_running(inmemory_db, task_id, "d", timeout=8.0)
    st_u = {nid: _node_status(doc_when_d_runs, nid) for nid in ("u1","u2","u3")}
    dbg("MULTISTREAM.START_OBSERVED", u1=st_u["u1"], u2=st_u["u2"], u3=st_u["u3"], d="running")

    # Не все апстримы должны быть finished к моменту старта D
    assert sum(1 for s in st_u.values() if str(s).endswith("finished")) < 3

    # Дожидаемся финала
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=14.0)
    final = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("MULTISTREAM.FINAL", statuses=final)
    assert final["d"] == cd.RunState.finished
    assert final["u1"] == cd.RunState.finished
    assert final["u2"] == cd.RunState.finished
    assert final["u3"] == cd.RunState.finished

    # Немного sanity-чеков по метрикам анализатора: суммарно он должен принять
    # все элементы из u1,u2,u3. В stats узла 'd' копятся $inc('count', ...).
    # Возьмём снимок документа и проверим, что count >= 29 (9+8+12).
    # (Точный разбиение по батчам не важно — главное, чтобы пришёл весь поток.)
    d_node = next(n for n in tdoc["graph"]["nodes"] if n["node_id"] == "d")
    got = int(((d_node.get("stats") or {}).get("count") or 0))
    dbg("MULTISTREAM.D.COUNT", count=got)
    assert got >= (9 + 8 + 12)

# ───────────────────────── Extra metric helpers ─────────────────────────

def _node_by_id(doc, node_id):
    for n in (doc.get("graph", {}).get("nodes") or []):
        if n.get("node_id") == node_id:
            return n
    return {}

def _get_count(doc, node_id):
    node = _node_by_id(doc or {}, node_id)
    return int(((node.get("stats") or {}).get("count") or 0))

# ───────────────────────── Extra metric tests ─────────────────────────

@pytest.mark.asyncio
async def test_metrics_single_stream_exact_count(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Один апстрим -> один даунстрим: проверяем, что у analyzer точный count == total.
    """
    cd, _ = env_and_imports

    total, batch = 13, 5  # будет 5+5+3
    u = _make_indexer("u", total=total, batch=batch)
    d = {
        "node_id": "d", "type": "analyzer",
        "depends_on": ["u"], "fan_in": "all",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["u"], "poll_ms": 25}}},
        "status": None, "attempt_epoch": 0
    }
    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u","d"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    got = _get_count(tdoc, "d")
    dbg("METRICS.SINGLE", expect=total, got=got)
    assert got == total


@pytest.mark.asyncio
async def test_metrics_multistream_exact_sum(env_and_imports, inmemory_db, coord, workers_3indexers_analyzer):
    """
    Три апстрима параллельно -> один даунстрим: проверяем точный суммарный count.
    """
    cd, _ = env_and_imports

    totals = {"u1": 6, "u2": 7, "u3": 9}  # сумма 22
    u1 = _make_indexer("u1", total=totals["u1"], batch=3)
    u2 = _make_indexer("u2", total=totals["u2"], batch=4)
    u3 = _make_indexer("u3", total=totals["u3"], batch=4)

    d = {
        "node_id": "d", "type": "analyzer",
        "depends_on": ["u1","u2","u3"], "fan_in": "any",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["u1","u2","u3"], "poll_ms": 20}}},
        "status": None, "attempt_epoch": 0
    }
    graph = {"schema_version": "1.0",
             "nodes": [u1, u2, u3, d],
             "edges": [["u1","d"], ["u2","d"], ["u3","d"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=14.0)

    expect = sum(totals.values())
    got = _get_count(tdoc, "d")
    dbg("METRICS.MULTI", expect=expect, got=got)
    assert got == expect


@pytest.mark.asyncio
async def test_metrics_partial_batches_exact_count(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Остаток в последнем батче: count должен ровно равняться total.
    """
    cd, _ = env_and_imports

    total, batch = 10, 3  # 3+3+3+1
    u = _make_indexer("u", total=total, batch=batch)
    d = {
        "node_id": "d", "type": "analyzer",
        "depends_on": ["u"], "fan_in": "all",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["u"], "poll_ms": 25}}},
        "status": None, "attempt_epoch": 0
    }
    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u","d"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    got = _get_count(tdoc, "d")
    dbg("METRICS.PARTIAL", expect=total, got=got)
    assert got == total


@pytest.mark.asyncio
async def test_metrics_isolation_between_tasks(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Две задачи подряд: метрики не должны «перетекать» между документами задач.
    """
    cd, _ = env_and_imports

    async def run_once(total, batch):
        u = _make_indexer("u", total=total, batch=batch)
        d = {
            "node_id": "d", "type": "analyzer",
            "depends_on": ["u"], "fan_in": "all",
            "io": {"start_when": "first_batch",
                   "input_inline": {"input_adapter": "pull.from_artifacts",
                                    "input_args": {"from_nodes": ["u"], "poll_ms": 25}}},
            "status": None, "attempt_epoch": 0
        }
        g = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u","d"]]}
        g = prime_graph(cd, g)
        tid = await coord.create_task(params={}, graph=g)
        tdoc = await wait_task_finished(inmemory_db, tid, timeout=12.0)
        return tid, _get_count(tdoc, "d")

    tid1, c1 = await run_once(7, 3)
    tid2, c2 = await run_once(11, 4)

    dbg("METRICS.ISOLATION", task1=tid1, count1=c1, task2=tid2, count2=c2)
    assert c1 == 7
    assert c2 == 11


# Для идемпотентности понадобятся моки брокера/продюсера
from tests.helpers.pipeline_sim import BROKER, AIOKafkaProducerMock  # type: ignore

@pytest.mark.asyncio
async def test_metrics_idempotent_on_duplicate_status_events(env_and_imports, inmemory_db, coord, workers_indexer_analyzer, monkeypatch):
    """
    Дублируем status-события (BATCH_OK/TASK_DONE) и проверяем, что stats.count у analyzer НЕ удваивается.
    """
    cd, _ = env_and_imports

    # локально «задвоим» отправку для статус-ивентов
    orig_send = AIOKafkaProducerMock.send_and_wait

    async def dup_status(self, topic, value, key=None):
        await orig_send(self, topic, value, key)
        if topic.startswith("status.") and (value or {}).get("msg_type") == "event":
            kind = ((value.get("payload") or {}).get("kind") or "")
            if kind in ("BATCH_OK", "TASK_DONE"):
                # тот же envelope: у координатора одинаковый dedup_id → должно схлопнуться
                await BROKER.produce(topic, value)

    monkeypatch.setattr(
        "tests.helpers.pipeline_sim.AIOKafkaProducerMock.send_and_wait",
        dup_status,
        raising=True
    )

    total, batch = 18, 6  # 3 батча
    u = _make_indexer("u", total=total, batch=batch)
    d = {
        "node_id": "d", "type": "analyzer",
        "depends_on": ["u"], "fan_in": "all",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["u"], "poll_ms": 25}}},
        "status": None, "attempt_epoch": 0
    }
    graph = {"schema_version": "1.0", "nodes": [u, d], "edges": [["u","d"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    got = _get_count(tdoc, "d")
    dbg("METRICS.DEDUP", expect=total, got=got)
    assert got == total

