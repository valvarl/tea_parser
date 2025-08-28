# tests/test_pipeline_smoke.py
import pytest
import pytest_asyncio

from tests.helpers.pipeline_sim import (
    setup_env_and_imports, install_inmemory_db, make_test_handlers,
    build_graph, prime_graph, wait_task_finished, dbg,
)

@pytest.fixture(scope="function")
def env_and_imports(monkeypatch):
    cd, wu = setup_env_and_imports(monkeypatch)
    return cd, wu

@pytest.fixture
def inmemory_db(monkeypatch, env_and_imports):
    cd, wu = env_and_imports
    db = install_inmemory_db(monkeypatch, cd, wu)
    return db

@pytest_asyncio.fixture
async def coordinator(env_and_imports, inmemory_db):
    cd, _ = env_and_imports
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

@pytest.fixture
def handlers(env_and_imports):
    _, wu = env_and_imports
    return make_test_handlers(wu)

@pytest_asyncio.fixture
async def worker(env_and_imports, handlers):
    _, wu = env_and_imports

    w_indexer  = wu.Worker(roles=["indexer"],  handlers={"indexer":  handlers["indexer"]})
    w_enricher = wu.Worker(roles=["enricher"], handlers={"enricher": handlers["enricher"]})
    w_ocr      = wu.Worker(roles=["ocr"],      handlers={"ocr":      handlers["ocr"]})
    w_analyzer = wu.Worker(roles=["analyzer"], handlers={"analyzer": handlers["analyzer"]})

    for name, w in (("indexer", w_indexer), ("enricher", w_enricher), ("ocr", w_ocr), ("analyzer", w_analyzer)):
        dbg("WORKER.STARTING", role=name)
        await w.start()
        dbg("WORKER.STARTED", role=name)

    try:
        yield (w_indexer, w_enricher, w_ocr, w_analyzer)
    finally:
        for name, w in (("indexer", w_indexer), ("enricher", w_enricher), ("ocr", w_ocr), ("analyzer", w_analyzer)):
            dbg("WORKER.STOPPING", role=name)
            await w.stop()
            dbg("WORKER.STOPPED", role=name)

# ---------------------- сам тест ------------------------
@pytest.mark.asyncio
async def test_e2e_streaming_with_kafka_sim(env_and_imports, inmemory_db, coordinator, worker):
    cd, _ = env_and_imports

    graph = prime_graph(cd, build_graph(total_skus=12, batch_size=5, mini_batch=3))
    task_id = await coordinator.create_task(params={}, graph=graph)
    dbg("TEST.TASK_CREATED", task_id=task_id)

    tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)

    st = {n["node_id"]: n["status"] for n in tdoc["graph"]["nodes"]}
    dbg("TEST.FINAL.STATUSES", statuses=st)
    assert st["w1"] == cd.RunState.finished
    assert st["w2"] == cd.RunState.finished
    assert st["w3"] == cd.RunState.finished
    assert st["w5"] == cd.RunState.finished
    assert st["w4"] == cd.RunState.finished

    # есть partial у w1,w2,w5
    assert await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w1"}) > 0
    assert await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w2"}) > 0
    assert await inmemory_db.artifacts.count_documents({"task_id": task_id, "node_id": "w5"}) > 0
