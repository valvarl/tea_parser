# tests/test_cancel_and_restart.py
import asyncio
import time
from enum import Enum

import pytest
import pytest_asyncio

from tests.helpers.pipeline_sim import (
    setup_env_and_imports,
    install_inmemory_db,
    prime_graph,
    wait_task_finished,
    make_test_handlers,
    AIOKafkaConsumerMock,
    BROKER,
    dbg,
)

# ---------------------- small helpers ----------------------

async def wait_node_running(db, task_id: str, node_id: str, timeout: float = 6.0) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        tdoc = await db.tasks.find_one({"id": task_id})
        if tdoc:
            for n in (tdoc.get("graph", {}) or {}).get("nodes", []):
                st = n.get("status")
                if isinstance(st, Enum):
                    st = st.value
                if n.get("node_id") == node_id and st == "running":
                    return True
        await asyncio.sleep(0.02)
    return False

def graph_cancel_flow():
    """
    w1=indexer -> w2=analyzer, где analyzer стартует на первом батче (через pull.from_artifacts).
    Это эквивалент исходной пары producer->sink.
    """
    return {
        "schema_version": "1.0",
        "nodes": [
            {"node_id": "w1", "type": "indexer", "depends_on": [], "fan_in": "all",
             "io": {"input_inline": {"batch_size": 5, "total_skus": 50}}},
            {"node_id": "w2", "type": "analyzer", "depends_on": ["w1"], "fan_in": "any",
             "io": {"start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts",
                        "input_args": {"from_nodes": ["w1"], "poll_ms": 30, "meta_list_key": "skus"}
                    }}},
        ],
        "edges": [["w1", "w2"]],
        "edges_ex": [{"from": "w1", "to": "w2", "mode": "async", "trigger": "on_batch"}],
    }

def graph_restart_flaky():
    """Один узел 'flaky' с ретраями: на epoch=0 падает в finalize, затем перезапускается и завершается успешно."""
    return {
        "schema_version": "1.0",
        "nodes": [
            {"node_id": "fx", "type": "flaky", "depends_on": [], "fan_in": "all",
             "retry_policy": {"max_retries": 3, "backoff_sec": 0.05},
             "io": {"input_inline": {}}}
        ],
        "edges": []
    }

# ---------------------- fixtures ----------------------

@pytest.fixture(scope="function")
def env_and_imports(monkeypatch):
    # Используем готовый сетап из helpers (патчит Kafka/asyncio/Outbox и пр.)
    # Объявляем ровно те роли, что нужны в тестах.
    return setup_env_and_imports(monkeypatch, worker_types="indexer,analyzer,flaky")

@pytest.fixture
def inmemory_db(monkeypatch, env_and_imports):
    cd, wu = env_and_imports
    return install_inmemory_db(monkeypatch, cd, wu)

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
    """
    Берём готовые indexer/analyzer из helpers.make_test_handlers
    и добавляем test-only flaky, который валится в finalize на первом эпохе.
    """
    _, wu = env_and_imports
    base = make_test_handlers(wu)

    class FlakyHandler(wu.RoleHandler):
        role = "flaky"
        async def load_input(self, ref, inline): return inline or {}
        async def iter_batches(self, loaded):
            # один батч; исход решает finalize
            yield wu.Batch(batch_uid="r-0", payload={"unit": "one"})
        async def process_batch(self, batch, ctx):
            await asyncio.sleep(0.03)
            return wu.BatchResult(success=True, metrics={"ok": 1})
        async def finalize(self, ctx):
            # Первая попытка у координатора — epoch==1
            cur_epoch = getattr(ctx, "attempt_epoch", getattr(ctx.artifacts, "attempt_epoch", 0))
            if cur_epoch == 1:
                raise RuntimeError("boom-first-epoch")
            await asyncio.sleep(0.02)
            return wu.FinalizeResult(metrics={"final": 1})

    return {
        "indexer":  base["indexer"],
        "analyzer": base["analyzer"],
        "flaky":    FlakyHandler(),
    }

@pytest_asyncio.fixture
async def workers(env_and_imports, handlers):
    """
    Поднимаем по воркеру для каждой роли. Реюзим Worker из worker_universal.
    """
    _, wu = env_and_imports
    w_idx = wu.Worker(roles=["indexer"],  handlers={"indexer":  handlers["indexer"]})
    w_ana = wu.Worker(roles=["analyzer"], handlers={"analyzer": handlers["analyzer"]})
    w_flk = wu.Worker(roles=["flaky"],    handlers={"flaky":    handlers["flaky"]})
    for w in (w_idx, w_ana, w_flk):
        await w.start()
    try:
        yield {"indexer": w_idx, "analyzer": w_ana, "flaky": w_flk}
    finally:
        for w in (w_idx, w_ana, w_flk):
            await w.stop()

# ---------------------- tests ----------------------

@pytest.mark.asyncio
async def test_cascade_cancel_prevents_downstream(env_and_imports, inmemory_db, coordinator, workers):
    cd, _ = env_and_imports

    # находим API отмены (название может отличаться в разных версиях)
    cancel_method = None
    for name in ("cancel_task", "request_cancel", "abort_task", "cancel"):
        if hasattr(coordinator, name):
            cancel_method = getattr(coordinator, name)
            break
    if cancel_method is None:
        pytest.xfail("Coordinator cancel API is not implemented")

    g = prime_graph(cd, graph_cancel_flow())
    tid = await coordinator.create_task(params={}, graph=g)

    # ждём старта продюсера (indexer)
    assert await wait_node_running(inmemory_db, tid, "w1", timeout=4.0), "indexer didn't start"

    # шпионим CANCELLED для indexer
    spy = AIOKafkaConsumerMock("status.indexer.v1", group_id="test.spy.cancel")
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

    # отменяем всю задачу каскадом
    asyncio.create_task(cancel_method(tid, reason="test-cascade"))

    # ждём CANCELLED от w1
    await asyncio.wait_for(cancelled_seen.wait(), timeout=5.0)

    # downstream analyzer не должен стартовать после отмены
    assert not await wait_node_running(inmemory_db, tid, "w2", timeout=1.0), \
        "downstream should NOT start after cancel"

    spy_task.cancel()
    try:
        await spy_task
    except Exception:
        pass
    await spy.stop()

    # итоговая проверка: допускаем разные финальные статусы,
    # но как минимум должен стоять флаг отмены у координатора
    tdoc = await inmemory_db.tasks.find_one({"id": tid})
    assert tdoc is not None

    st = tdoc.get("status")
    if isinstance(st, Enum):
        st = st.value

    assert (
        st in ("failed", "finished", "cancelled")
        or ((tdoc.get("coordinator") or {}).get("cancelled") is True)
    ), f"unexpected task status after cancel: {tdoc.get('status')}"

@pytest.mark.asyncio
async def test_restart_higher_epoch_ignores_old_events(env_and_imports, inmemory_db, coordinator, workers):
    cd, _ = env_and_imports
    g = prime_graph(cd, graph_restart_flaky())
    tid = await coordinator.create_task(params={}, graph=g)

    # шпионим статусный топик flaky и репаблишим одно старое событие epoch=0 после принятия epoch=1
    status_topic = "status.flaky.v1"
    spy = AIOKafkaConsumerMock(status_topic, group_id="test.spy.restart")
    await spy.start()

    async def collect_and_inject():
        saved_old = None
        while True:
            rec = await spy.getone()
            env = rec.value
            if env.get("task_id") != tid or env.get("msg_type") != "event":
                continue
            if env.get("node_id") != "fx":
                continue

            epoch = int(env.get("attempt_epoch", 0))
            kind = (env.get("payload") or {}).get("kind")

            # запомним последнее "старое" событие (предпочтительно TASK_FAILED)
            if epoch == 0:
                saved_old = env

            # как только увидим принятие новой эпохи — вбрасываем старое
            if kind == "TASK_ACCEPTED" and epoch >= 1:
                if saved_old:
                    await BROKER.produce(status_topic, saved_old)
                return

    coll_task = asyncio.create_task(collect_and_inject())

    # ждём окончания задачи
    tdoc = await wait_task_finished(inmemory_db, tid, timeout=12.0)

    coll_task.cancel()
    try:
        await coll_task
    except Exception:
        pass
    await spy.stop()

    # проверяем финальный статус узла fx
    node_map = {n["node_id"]: n for n in tdoc["graph"]["nodes"]}
    fx = node_map["fx"]
    st = fx.get("status")
    if isinstance(st, Enum):
        st = st.value
    assert st == "finished"

    # артефакты должны существовать (вторая попытка успешная)
    cnt_art = await inmemory_db.artifacts.find_one({"task_id": tid, "node_id": "fx"})
    assert cnt_art is not None, "artifacts for fx should exist"

    # и попытка должна быть >= 1
    assert int(fx.get("attempt_epoch", 0)) >= 1

@pytest.mark.asyncio
async def test_cancel_before_any_start_keeps_all_nodes_idle(env_and_imports, inmemory_db, coordinator, workers):
    cd, _ = env_and_imports

    # Граф, у которого у продюсера зависимость на несуществующий узел → он не стартует.
    g = prime_graph(cd, {
        "schema_version": "1.0",
        "nodes": [
            {"node_id": "w1", "type": "indexer", "depends_on": ["__missing__"], "fan_in": "all",
             "io": {"input_inline": {"batch_size": 5, "total_skus": 10}}},
            {"node_id": "w2", "type": "analyzer", "depends_on": ["w1"], "fan_in": "any",
             "io": {"start_when": "first_batch",
                    "input_inline": {
                        "input_adapter": "pull.from_artifacts",
                        "input_args": {"from_nodes": ["w1"], "poll_ms": 30, "meta_list_key": "skus"}
                    }}},
        ],
        "edges": [["w1", "w2"]],
        "edges_ex": [{"from": "w1", "to": "w2", "mode": "async", "trigger": "on_batch"}],
    })

    # найдём API отмены
    cancel_method = None
    for name in ("cancel_task", "request_cancel", "abort_task", "cancel"):
        if hasattr(coordinator, name):
            cancel_method = getattr(coordinator, name)
            break
    if cancel_method is None:
        pytest.xfail("Coordinator cancel API is not implemented")

    tid = await coordinator.create_task(params={}, graph=g)

    # Отменяем немедленно (не ждём возврата, чтобы не упираться в grace)
    asyncio.create_task(cancel_method(tid, reason="cancel-before-start"))

    # Ни один узел не должен войти в running
    assert not await wait_node_running(inmemory_db, tid, "w1", timeout=1.0)
    assert not await wait_node_running(inmemory_db, tid, "w2", timeout=1.0)


@pytest.mark.asyncio
async def test_cancel_on_deferred_prevents_retry(env_and_imports, inmemory_db, coordinator, workers):
    """
    Узел flaky на epoch=0 падает в finalize → становится deferred с коротким backoff.
    Отменяем задачу сразу после TASK_FAILED(epoch=0) и убеждаемся, что epoch>=1 не принимается.
    """
    cd, _ = env_and_imports

    # Правим backoff ДО prime_graph
    base = graph_restart_flaky()
    base["nodes"][0]["retry_policy"]["backoff_sec"] = 1.0
    g = prime_graph(cd, base)

    # найдём API отмены
    cancel_method = None
    for name in ("cancel_task", "request_cancel", "abort_task", "cancel"):
        if hasattr(coordinator, name):
            cancel_method = getattr(coordinator, name)
            break
    if cancel_method is None:
        pytest.xfail("Coordinator cancel API is not implemented")

    tid = await coordinator.create_task(params={}, graph=g)

    status_topic = "status.flaky.v1"
    spy = AIOKafkaConsumerMock(status_topic, group_id="test.spy.defer_cancel")
    await spy.start()

    cancel_triggered = asyncio.Event()
    higher_epoch_accepted = asyncio.Event()

    async def watcher():
        while True:
            rec = await spy.getone()
            env = rec.value
            if env.get("task_id") != tid or env.get("msg_type") != "event" or env.get("node_id") != "fx":
                continue
            epoch = int(env.get("attempt_epoch", 0))
            kind = (env.get("payload") or {}).get("kind")

            if kind == "TASK_FAILED" and epoch == 1:
                # Отмену запускаем в фоне, чтобы не ждать CANCEL_GRACE_SEC
                asyncio.create_task(cancel_method(tid, reason="cancel-on-deferred"))
                cancel_triggered.set()
                return

            # Любое принятие попытки старше первой будет считаться «неожиданным рестартом»
            if kind == "TASK_ACCEPTED" and epoch >= 2:
                higher_epoch_accepted.set()

    wtask = asyncio.create_task(watcher())

    # Дождались, что отмена инициирована
    await asyncio.wait_for(cancel_triggered.wait(), timeout=5.0)

    # Проверяем, что новая эпоха НЕ стартовала в окно чуть больше backoff'а
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(higher_epoch_accepted.wait(), timeout=1.8)

    wtask.cancel()
    try:
        await wtask
    except Exception:
        pass
    await spy.stop()


@pytest.mark.asyncio
async def test_restart_higher_epoch_ignores_old_batch_ok(env_and_imports, inmemory_db, coordinator, workers):
    """
    После принятия эпохи 1 вбрасываем старый BATCH_OK(epoch=0).
    Координатор должен его проигнорировать (фенсинг по attempt_epoch).
    """
    cd, _ = env_and_imports
    g = prime_graph(cd, graph_restart_flaky())
    tid = await coordinator.create_task(params={}, graph=g)

    status_topic = "status.flaky.v1"
    spy = AIOKafkaConsumerMock(status_topic, group_id="test.spy.old_bok")
    await spy.start()

    async def collect_and_inject():
        saved_bok = None
        while True:
            rec = await spy.getone()
            env = rec.value
            if env.get("task_id") != tid or env.get("msg_type") != "event" or env.get("node_id") != "fx":
                continue
            epoch = int(env.get("attempt_epoch", 0))
            kind = (env.get("payload") or {}).get("kind")

            # запомним любой BATCH_OK старой эпохи
            if epoch == 0 and kind == "BATCH_OK":
                saved_bok = env

            # как только увидим принятие epoch>=1 — вбрасываем старый BATCH_OK
            if kind == "TASK_ACCEPTED" and epoch >= 1:
                if saved_bok:
                    await BROKER.produce(status_topic, saved_bok)
                return

    coll_task = asyncio.create_task(collect_and_inject())

    # ждём окончания задачи
    tdoc = await wait_task_finished(inmemory_db, tid, timeout=12.0)

    coll_task.cancel()
    try:
        await coll_task
    except Exception:
        pass
    await spy.stop()

    # узел завершился успешно и с повышенной эпохой
    node_map = {n["node_id"]: n for n in tdoc["graph"]["nodes"]}
    fx = node_map["fx"]
    st = fx.get("status")
    if isinstance(st, Enum):
        st = st.value
    assert st == "finished"
    assert int(fx.get("attempt_epoch", 0)) >= 1

    # убедимся, что дубль BATCH_OK не «добавил» лишних метрик (по batch_uid одна запись)
    cnt = 0
    cur = inmemory_db.metrics_raw.find({"task_id": tid, "node_id": "fx", "batch_uid": "r-0"})
    async for _ in cur:
        cnt += 1
    assert cnt == 1, f"expected 1 metrics doc for batch_uid r-0, got {cnt}"
