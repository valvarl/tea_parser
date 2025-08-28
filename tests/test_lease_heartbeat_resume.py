import asyncio
import os
import tempfile
import pytest
import pytest_asyncio

from tests.helpers.pipeline_sim import (
    setup_env_and_imports,
    install_inmemory_db,
    prime_graph,
    wait_task_finished,
    dbg,
)

# ───────────────────────── Helpers: handlers ─────────────────────────

def build_sleepy_handler(wu, role: str, *, batches=1, sleep_s=1.2):
    """
    Обработчик, который "висит" в process_batch заданное время,
    чтобы можно было поймать SOFT/HARD по heartbeat.
    """
    class Sleepy(wu.RoleHandler):
        def __init__(self, role):
            self.role = role
        async def load_input(self, ref, inline): return {}
        async def iter_batches(self, loaded):
            # Ровно batches батчей
            for i in range(batches):
                yield wu.Batch(shard_id=f"{self.role}-{i}", payload={"i": i})
        async def process_batch(self, batch, ctx):
            await asyncio.sleep(sleep_s)
            return wu.BatchResult(success=True, metrics={"count": 1})
        async def finalize(self, ctx):
            return wu.FinalizeResult(metrics={})
    return Sleepy(role=role)

def build_noop_query_only_role(wu, role: str):
    """
    Воркер только для ответа на TASK_DISCOVER (никакой работы).
    Координатор всё равно спросит snapshot, а воркер вернёт idle/complete по артефактам.
    """
    class Noop(wu.RoleHandler):
        def __init__(self, role):
            self.role = role
        async def load_input(self, ref, inline): return {}
        async def iter_batches(self, loaded):
            yield wu.Batch(shard_id=f"{self.role}-0", payload={})
        async def process_batch(self, batch, ctx):
            return wu.BatchResult(success=True, metrics={"noop": 1})
    return Noop(role=role)

# ───────────────────────── Fixtures ─────────────────────────

@pytest.fixture(scope="function")
def env_and_imports(monkeypatch):
    """
    Узко под этот набор:
    - ускоряем монитор HB;
    - настраиваем очень маленькие SOFT/HARD пороги, чтобы поймать переходы.
    """
    cd, wu = setup_env_and_imports(
        monkeypatch,
        worker_types="sleepy,noop"
    )

    # Быстрые циклы координатора
    cd.SCHEDULER_TICK_SEC   = 0.05
    cd.DISCOVERY_WINDOW_SEC = 0.05
    cd.FINALIZER_TICK_SEC   = 0.05
    cd.HB_MONITOR_TICK_SEC  = 0.05

    return cd, wu

@pytest.fixture
def inmemory_db(monkeypatch, env_and_imports):
    cd, wu = env_and_imports
    return install_inmemory_db(monkeypatch, cd, wu)

# ───────────────────────── Test 1: Heartbeat SOFT ⇒ deferred, потом восстановление ─────────────────────────

@pytest_asyncio.fixture
async def worker_soft(env_and_imports, monkeypatch):
    """
    SOFT тест: выставляем HEARTBEAT_INTERVAL_SEC чуть длиннее SOFT, но сильно меньше HARD.
    """
    cd, wu = env_and_imports
    # Порог для координатора
    cd.HEARTBEAT_SOFT_SEC = 0.4
    cd.HEARTBEAT_HARD_SEC = 5.0
    # Интервал heartbeats у воркера
    wu.HEARTBEAT_INTERVAL_SEC = 1.0

    w = wu.Worker(roles=["sleepy"], handlers={"sleepy": build_sleepy_handler(wu, "sleepy", batches=1, sleep_s=1.6)})
    dbg("WORKER.STARTING", role="sleepy-soft")
    await w.start()
    dbg("WORKER.STARTED", role="sleepy-soft")
    try:
        yield w
    finally:
        dbg("WORKER.STOPPING", role="sleepy-soft")
        await w.stop()
        dbg("WORKER.STOPPED", role="sleepy-soft")

@pytest.mark.asyncio
async def test_heartbeat_soft_deferred_then_recovers(env_and_imports, inmemory_db, worker_soft):
    """
    При редких heartbeats (раз в 1с) и SOFT=0.4 координатор успевает
    перевести таску в deferred, но потом прилетает следующий heartbeat,
    узел дорабатывает и таска завершается finished.
    """
    cd, _ = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = prime_graph(cd, {
            "schema_version": "1.0",
            "nodes": [{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all",
                       "io": {"input_inline": {}}}],
            "edges": []
        })
        task_id = await coord.create_task(params={}, graph=graph)

        # Подождём немного и проверим, что статус таски хотя бы раз стал 'deferred'
        async def saw_deferred(timeout=3.0):
            from time import time
            t0 = time()
            while time() - t0 < timeout:
                t = await inmemory_db.tasks.find_one({"id": task_id})
                if t and str(t.get("status")) == str(cd.RunState.deferred):
                    return True
                await asyncio.sleep(0.03)
            return False

        assert await saw_deferred(), "ожидали перевод задачи в deferred по SOFT"

        # Затем дождёмся обычного завершения
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=8.0)
        assert str(tdoc.get("status")) == str(cd.RunState.finished)
        node = [n for n in tdoc["graph"]["nodes"] if n["node_id"] == "s"][0]
        assert str(node.get("status")) == str(cd.RunState.finished)
    finally:
        await coord.stop()

# ───────────────────────── Test 2: Heartbeat HARD ⇒ failed ─────────────────────────

@pytest_asyncio.fixture
async def worker_hard(env_and_imports, monkeypatch):
    """
    HARD тест: ставим огромный HEARTBEAT_INTERVAL_SEC у воркера и крошечные SOFT/HARD.
    Координатор пометит таску failed.
    """
    cd, wu = env_and_imports
    cd.HEARTBEAT_SOFT_SEC = 0.2
    cd.HEARTBEAT_HARD_SEC = 0.5
    wu.HEARTBEAT_INTERVAL_SEC = 10.0  # фактически heartbeat не придёт вовремя

    # Сделаем работу дольше HARD, чтобы не успела завершиться "сама".
    w = wu.Worker(roles=["sleepy"], handlers={"sleepy": build_sleepy_handler(wu, "sleepy", batches=1, sleep_s=1.2)})
    dbg("WORKER.STARTING", role="sleepy-hard")
    await w.start()
    dbg("WORKER.STARTED", role="sleepy-hard")
    try:
        yield w
    finally:
        dbg("WORKER.STOPPING", role="sleepy-hard")
        await w.stop()
        dbg("WORKER.STOPPED", role="sleepy-hard")

@pytest.mark.asyncio
async def test_heartbeat_hard_marks_task_failed(env_and_imports, inmemory_db, worker_hard):
    cd, _ = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = prime_graph(cd, {
            "schema_version": "1.0",
            "nodes": [{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all",
                       "io": {"input_inline": {}}}],
            "edges": []
        })
        task_id = await coord.create_task(params={}, graph=graph)

        # Ждём, когда координатор пометит таску failed
        from time import time
        t0 = time()
        while time() - t0 < 4.0:
            t = await inmemory_db.tasks.find_one({"id": task_id})
            if t and str(t.get("status")) == str(cd.RunState.failed):
                break
            await asyncio.sleep(0.03)
        else:
            raise AssertionError("ожидали переход задачи в failed по HARD")

        # Дополнительно: узел не finished
        t = await inmemory_db.tasks.find_one({"id": task_id})
        node = [n for n in t["graph"]["nodes"] if n["node_id"] == "s"][0]
        assert str(node.get("status")) != str(cd.RunState.finished)
    finally:
        await coord.stop()

# ───────────────────────── Test 3: Resume inflight (усыновление) ─────────────────────────

@pytest.mark.asyncio
async def test_resume_inflight_worker_restarts_with_local_state(env_and_imports, inmemory_db, monkeypatch, tmp_path):
    """
    Рестарт воркера с сохранённым LocalState:
    - первый воркер стартует работу и сохраняет active_run в state-файл;
    - мы его останавливаем;
    - стартуем новый воркер с тем же WORKER_ID и тем же WORKER_STATE_DIR;
    - координатор по TASK_DISCOVER не запускает новую попытку (attempt_epoch не увеличивается),
      т.е. "усыновляет" узел (по крайней мере не дублирует старт).
    NB: сам «реальный» resume исполнения в текущей версии worker_universal не реализован,
        тест проверяет именно отсутствие двойного старта.
    """
    cd, wu = env_and_imports

    # большие пороги, чтобы монитор HB не мешал
    cd.HEARTBEAT_SOFT_SEC = 30
    cd.HEARTBEAT_HARD_SEC = 60
    wu.HEARTBEAT_INTERVAL_SEC = 100  # не важно в этом тесте

    # фиксируем WORKER_ID и каталог state
    worker_id = "w-resume"
    monkeypatch.setenv("WORKER_ID", worker_id)
    monkeypatch.setenv("WORKER_STATE_DIR", str(tmp_path))

    # Воркер #1 — начинает работу и сохраняет active_run
    w1 = wu.Worker(roles=["sleepy"], handlers={"sleepy": build_sleepy_handler(wu, "sleepy", batches=1, sleep_s=2.0)})
    await w1.start()

    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = prime_graph(cd, {
            "schema_version": "1.0",
            "nodes": [{"node_id": "s", "type": "sleepy", "depends_on": [], "fan_in": "all",
                       "io": {"input_inline": {}}}],
            "edges": []
        })
        task_id = await coord.create_task(params={}, graph=graph)

        # Дождёмся, когда узел перейдёт в running и появится attempt_epoch=1
        async def wait_running():
            from time import time
            t0 = time()
            while time() - t0 < 2.5:
                t = await inmemory_db.tasks.find_one({"id": task_id})
                if t:
                    n = [n for n in t["graph"]["nodes"] if n["node_id"] == "s"][0]
                    if str(n.get("status")) == str(cd.RunState.running):
                        return int(n.get("attempt_epoch", 0))
                await asyncio.sleep(0.03)
            raise AssertionError("узел не перешёл в running вовремя")
        epoch_before = await wait_running()

        # Останавливаем воркер #1 — state уже сохранён
        await w1.stop()

        # Стартуем воркер #2 с тем же WORKER_ID/STATE_DIR (он поднимет active_run в память)
        w2 = wu.Worker(roles=["sleepy"], handlers={"sleepy": build_sleepy_handler(wu, "sleepy", batches=1, sleep_s=0.1)})
        await w2.start()

        # Даём координатору шанс послать TASK_DISCOVER и принять решение "не стартовать заново"
        await asyncio.sleep(0.5)

        # Проверим, что epoch не увеличился (координатор не начал новую попытку)
        t = await inmemory_db.tasks.find_one({"id": task_id})
        n = [n for n in t["graph"]["nodes"] if n["node_id"] == "s"][0]
        epoch_after = int(n.get("attempt_epoch", 0))
        assert epoch_after == epoch_before, "ожидали, что координатор не начнёт новую попытку во время усыновления"

        # Завершаем воркера #2 (текущая реализация не дополняет реальный resume, поэтому таску не дожидаемся)
        await w2.stop()
    finally:
        await coord.stop()

# ───────────────────────── Test 4: TASK_DISCOVER + artifacts.complete ⇒ skip start ─────────────────────────

@pytest_asyncio.fixture
async def worker_noop_query(env_and_imports):
    """
    Воркер роли 'noop' — нужен только для ответа на TASK_DISCOVER.
    """
    cd, wu = env_and_imports
    w = wu.Worker(roles=["noop"], handlers={"noop": build_noop_query_only_role(wu, "noop")})
    await w.start()
    try:
        yield w
    finally:
        await w.stop()

@pytest.mark.asyncio
async def test_task_discover_complete_artifacts_skips_node_start(env_and_imports, inmemory_db, worker_noop_query):
    """
    Если в артефактах уже есть complete по (task_id,node_id), воркер в TASK_SNAPSHOT
    вернёт artifacts.complete=True, координатор должен пометить узел finished
    и не посылать TASK_START.
    """
    cd, wu = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = prime_graph(cd, {
            "schema_version": "1.0",
            "nodes": [{"node_id": "x", "type": "noop", "depends_on": [], "fan_in": "all",
                       "io": {"input_inline": {}}}],
            "edges": []
        })
        task_id = await coord.create_task(params={}, graph=graph)

        # Поместим заранее complete-артефакт, чтобы worker_noop в TASK_SNAPSHOT увидел complete=True
        await inmemory_db.artifacts.update_one(
            {"task_id": task_id, "node_id": "x"},
            {"$set": {"status": "complete"}}, upsert=True
        )

        # Подождём чуть-чуть, чтобы префлайт успел отработать и пометить узел finished без старта
        await asyncio.sleep(0.3)

        t = await inmemory_db.tasks.find_one({"id": task_id})
        node = [n for n in t["graph"]["nodes"] if n["node_id"] == "x"][0]
        assert str(node.get("status")) == str(cd.RunState.finished), "узел должен быть завершён без запуска"
    finally:
        await coord.stop()
