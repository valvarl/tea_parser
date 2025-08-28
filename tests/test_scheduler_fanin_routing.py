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
    db = install_inmemory_db(monkeypatch, cd, wu)
    return db

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
    Поднимаем минимальный набор воркеров для сценариев fan-in:
    один worker indexer (обработает всех предков последовательно)
    и один worker analyzer (обработает downstream).
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

# ───────────────────────── Helpers ─────────────────────────

async def _get_task_doc(db, task_id):
    return await db.tasks.find_one({"id": task_id})

def _node_status(doc, node_id):
    for n in (doc.get("graph", {}).get("nodes") or []):
        if n.get("node_id") == node_id:
            return n.get("status")
    return None

def _set_io(node, **io_patch):
    node.setdefault("io", {}).update(io_patch)
    return node

def _make_indexer_node(node_id, total, batch):
    return {
        "node_id": node_id, "type": "indexer",
        "depends_on": [], "fan_in": "all",
        "io": {"input_inline": {"batch_size": batch, "total_skus": total}},
        "status": None, "attempt_epoch": 0
    }

# Ждём, когда конкретный узел перейдёт в нужный статус, возвращаем снимок документа
async def wait_node_running(db, task_id, node_id, timeout=5.0):
    from time import time
    t0 = time()
    while time() - t0 < timeout:
        doc = await _get_task_doc(db, task_id)
        if doc:
            st = _node_status(doc, node_id)
            if str(st).endswith("running"):
                return doc
        await asyncio.sleep(0.02)
    raise AssertionError(f"node {node_id} not running in time")

# ───────────────────────── Tests ─────────────────────────

@pytest.mark.asyncio
async def test_fanin_any_starts_early(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Fan-in ANY: downstream должен стартовать, как только готов ХОТЯ БЫ ОДИН из предков.
    Мы намеренно поднимаем 1 worker indexer → предки выполняются последовательно.
    Downstream 'd_any' стартует по 'first_batch' (poll из артефактов).
    """
    cd, _ = env_and_imports

    # Три предка-индексера (u1,u2,u3) с разными объёмами
    u1 = _make_indexer_node("u1", total=4,  batch=4)
    u2 = _make_indexer_node("u2", total=8,  batch=4)
    u3 = _make_indexer_node("u3", total=12, batch=6)

    # ANY downstream
    d_any = {
        "node_id": "d_any", "type": "analyzer",
        "depends_on": ["u1","u2","u3"], "fan_in": "any",
        "io": {"start_when": "first_batch",
               "input_inline": {
                   "input_adapter": "pull.from_artifacts",
                   "input_args": {"from_nodes": ["u1","u2","u3"], "poll_ms": 40}
               }},
        "status": None, "attempt_epoch": 0
    }

    graph = {"schema_version": "1.0", "nodes":[u1,u2,u3,d_any], "edges":[["u1","d_any"],["u2","d_any"],["u3","d_any"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("TEST.ANY.TASK_CREATED", task_id=task_id)

    # Момент, когда d_any стал running
    doc_at_start = await wait_node_running(inmemory_db, task_id, "d_any", timeout=8.0)
    s1 = _node_status(doc_at_start, "u1")
    s2 = _node_status(doc_at_start, "u2")
    s3 = _node_status(doc_at_start, "u3")
    dbg("ANY.START_OBSERVED", u1=s1, u2=s2, u3=s3)

    # Ожидаем, что НЕ все предки к этому моменту "finished"
    finished = [str(s).endswith("finished") for s in (s1,s2,s3)]
    assert sum(1 for x in finished if x) < 3, "ANY не должен ждать всех предков"

    # Дождёмся финиша всей таски
    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("ANY.FINAL.STATUS", statuses=st)
    assert st["d_any"] == cd.RunState.finished

@pytest.mark.asyncio
async def test_fanin_all_waits_all_parents(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Fan-in ALL: downstream 'd_all' должен перейти в running только после того,
    как ВСЕ предки завершат (или по крайней мере будут готовы по правилам координатора).
    """
    cd, _ = env_and_imports

    a = _make_indexer_node("a", total=6, batch=3)   # два батча
    b = _make_indexer_node("b", total=10, batch=5)  # два батча

    d_all = {
        "node_id": "d_all", "type": "analyzer",
        "depends_on": ["a","b"], "fan_in": "all",
        # намеренно БЕЗ start_when=first_batch (по умолчанию после завершения предков)
        "io": {"input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["a","b"], "poll_ms": 40}}},
        "status": None, "attempt_epoch": 0
    }

    graph = {"schema_version": "1.0", "nodes":[a,b,d_all], "edges":[["a","d_all"],["b","d_all"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("TEST.ALL.TASK_CREATED", task_id=task_id)

    doc_at_start = await wait_node_running(inmemory_db, task_id, "d_all", timeout=8.0)
    sa = _node_status(doc_at_start, "a")
    sb = _node_status(doc_at_start, "b")
    dbg("ALL.START_OBSERVED", a=sa, b=sb)

    # К моменту запуска d_all оба предка уже должны быть finished
    assert str(sa).endswith("finished") and str(sb).endswith("finished")

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("ALL.FINAL.STATUS", statuses=st)
    assert st["d_all"] == cd.RunState.finished

@pytest.mark.asyncio
@pytest.mark.xfail(reason="Fan-in 'count:n' пока не реализован в coordinator_dag", strict=False)
async def test_fanin_count_n(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Fan-in COUNT:N: downstream стартует, когда готовы хотя бы N из предков.
    Каркас теста — xfail, пока координатор не умеет 'count:n'.
    """
    cd, _ = env_and_imports

    p1 = _make_indexer_node("p1", total=4, batch=4)
    p2 = _make_indexer_node("p2", total=6, batch=3)
    p3 = _make_indexer_node("p3", total=8, batch=4)

    d_cnt = {
        "node_id": "d_cnt", "type": "analyzer",
        "depends_on": ["p1","p2","p3"], "fan_in": "count:2",
        "io": {"start_when": "first_batch",
               "input_inline": {"input_adapter": "pull.from_artifacts",
                                "input_args": {"from_nodes": ["p1","p2","p3"], "poll_ms": 40}}},
        "status": None, "attempt_epoch": 0
    }

    graph = {"schema_version":"1.0", "nodes":[p1,p2,p3,d_cnt],
             "edges":[["p1","d_cnt"],["p2","d_cnt"],["p3","d_cnt"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("TEST.COUNTN.TASK_CREATED", task_id=task_id)

    doc_at_start = await wait_node_running(inmemory_db, task_id, "d_cnt", timeout=8.0)
    s1 = _node_status(doc_at_start, "p1")
    s2 = _node_status(doc_at_start, "p2")
    s3 = _node_status(doc_at_start, "p3")
    ready = [str(s).endswith("finished") for s in (s1,s2,s3)]
    assert sum(1 for x in ready if x) >= 2

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    assert _node_status(tdoc, "d_cnt") == cd.RunState.finished

@pytest.mark.asyncio
@pytest.mark.xfail(reason="Приоритет 'edges' над 'routing.on_success' не покрыт/не реализован", strict=False)
async def test_edges_vs_routing_priority(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Если у графа заданы явные edges И у узла есть routing.on_success, должны побеждать edges.
    Каркас — xfail, пока в координаторе нет приоритезации.
    """
    cd, _ = env_and_imports

    src = _make_indexer_node("src", total=3, batch=3)

    only_edges = {
        "node_id": "only_edges", "type": "analyzer",
        "depends_on": ["src"], "fan_in": "all",
        "io": {"input_inline": {"input_adapter":"pull.from_artifacts",
                                "input_args": {"from_nodes": ["src"], "poll_ms": 40}}},
        "status": None, "attempt_epoch": 0
    }
    should_be_ignored_by_edges = {
        "node_id":"should_not_run", "type":"analyzer",
        "depends_on":["src"], "fan_in":"all",
        "io":{"input_inline":{"input_adapter":"pull.from_artifacts",
                              "input_args":{"from_nodes":["src"], "poll_ms": 40}}},
        "status": None, "attempt_epoch": 0
    }

    src["routing"] = {"on_success": ["should_not_run"]}  # при наличии edges это должно быть проигнорировано

    graph = {
        "schema_version":"1.0",
        "nodes":[src, only_edges, should_be_ignored_by_edges],
        "edges":[["src","only_edges"]]  # явная связь только с only_edges
    }
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("TEST.EDGES_ROUTES.TASK_CREATED", task_id=task_id)

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("EDGES_ROUTES.FINAL.STATUS", statuses=st)

    assert st["only_edges"] == cd.RunState.finished
    assert st["should_not_run"] in (None, cd.RunState.queued), "routing.on_success не должен запускать узел при заданных edges"

@pytest.mark.asyncio
async def test_coordinator_fn_merge_without_worker(env_and_imports, inmemory_db, coord, workers_indexer_analyzer):
    """
    Узел типа 'coordinator_fn' должен стартовать и завершиться без воркера,
    а его артефакты — стать источником для downstream.
    """
    cd, _ = env_and_imports

    u1 = _make_indexer_node("u1", total=5, batch=5)
    u2 = _make_indexer_node("u2", total=7, batch=7)

    merge = {
        "node_id": "merge", "type": "coordinator_fn",
        "depends_on": ["u1","u2"], "fan_in": "all",
        "io": {"fn": "merge.generic", "fn_args": {"from_nodes":["u1","u2"], "target":{"key":"merged-k"}}},
        "status": None, "attempt_epoch": 0
    }

    sink = {
        "node_id": "sink", "type": "analyzer",
        "depends_on": ["merge"], "fan_in": "all",
        "io": {"start_when":"first_batch",
               "input_inline":{"input_adapter":"pull.from_artifacts",
                               "input_args":{"from_nodes":["merge"], "poll_ms": 40}}},
        "status": None, "attempt_epoch": 0
    }

    graph = {"schema_version":"1.0", "nodes":[u1,u2,merge,sink], "edges":[["u1","merge"],["u2","merge"],["merge","sink"]]}
    graph = prime_graph(cd, graph)

    task_id = await coord.create_task(params={}, graph=graph)
    dbg("TEST.MERGE.TASK_CREATED", task_id=task_id)

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("MERGE.FINAL.STATUS", statuses=st)
    assert st["merge"] == cd.RunState.finished
    assert st["sink"]  == cd.RunState.finished

    # Артефакты для merge должны существовать и быть complete
    cnt = await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "merge", "status": "complete"})
    dbg("MERGE.ART.COUNT", count=cnt)
    assert cnt >= 1
