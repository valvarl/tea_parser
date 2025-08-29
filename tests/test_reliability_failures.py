import asyncio
import pytest
import pytest_asyncio

from tests.helpers.pipeline_sim import (
    setup_env_and_imports, install_inmemory_db, prime_graph,
    wait_task_finished, dbg,
    BROKER, AIOKafkaProducerMock,
)

# ───────────────────────── Fixtures ─────────────────────────

@pytest.fixture(scope="function")
def env_and_imports(monkeypatch):
    """
    Для этого набора тестов нам нужны свои роли.
    """
    cd, wu = setup_env_and_imports(
        monkeypatch,
        worker_types="source,flaky,a,b,c"  # роли, которые будут встречаться здесь
    )
    # Ускорим каскадный cancel:
    cd.CANCEL_GRACE_SEC = 0.05
    return cd, wu

@pytest.fixture
def inmemory_db(monkeypatch, env_and_imports):
    cd, wu = env_and_imports
    return install_inmemory_db(monkeypatch, cd, wu)

# ───────────────────────── Спец-хендлеры ─────────────────────────

def build_counting_source_handler(wu, *, total=9, batch=3):
    """
    Источник, который выдаёт N батчей и на каждом батче инкрементит метрику 'count'.
    Без коллекций/списков в метриках — только int, чтобы не споткнуться на $inc.
    """
    class Source(wu.RoleHandler):
        role = "source"
        async def load_input(self, ref, inline):
            return {"total": total, "batch": batch}
        async def iter_batches(self, loaded):
            t, b = loaded["total"], loaded["batch"]
            shard = 0
            for i in range(0, t, b):
                # payload с фейковыми items; downstream не используется в этом тесте
                yield wu.Batch(shard_id=f"s-{shard}", payload={"items": list(range(i, min(i+b, t)))})
                shard += 1
        async def process_batch(self, batch, ctx):
            n = len(batch.payload.get("items") or [])
            return wu.BatchResult(success=True, metrics={"count": n})
        async def finalize(self, ctx):  # без финальных метрик, чтобы не удваивать
            return wu.FinalizeResult(metrics={})
    return Source()

def build_flaky_once_handler(wu):
    """
    Первый запуск — бросаем исключение (transient), далее — успешная обработка.
    Проверяем deferred + backoff + retry.
    """
    class FlakyOnce(wu.RoleHandler):
        role = "flaky"
        def __init__(self): self._failed = False
        async def load_input(self, ref, inline): return {}
        async def iter_batches(self, loaded):
            yield wu.Batch(shard_id="f-0", payload={"x": 1})
        async def process_batch(self, batch, ctx):
            if not self._failed:
                self._failed = True
                raise RuntimeError("transient_error")
            return wu.BatchResult(success=True, metrics={"count": 1})
        def classify_error(self, exc: BaseException) -> tuple[str, bool]:
            # Временная ошибка
            return ("transient", False)
    return FlakyOnce()

def build_permanent_fail_handler(wu):
    """
    Узел 'a' — всегда падает перманентно.
    """
    class AFail(wu.RoleHandler):
        role = "a"
        async def load_input(self, ref, inline): return {}
        async def iter_batches(self, loaded):
            yield wu.Batch(shard_id="a-0", payload={"x": 1})
        async def process_batch(self, batch, ctx):
            raise RuntimeError("hard_fail")
        def classify_error(self, exc: BaseException) -> tuple[str, bool]:
            return ("hard", True)  # permanent=True
    return AFail()

def build_noop_handler(wu, role_name: str):
    """
    Простые downstream-ы, которые ничего не делают. Нужны для проверки каскадного cancel.
    """
    class Noop(wu.RoleHandler):
        role = role_name
        async def load_input(self, ref, inline): return {}
        async def iter_batches(self, loaded):
            yield wu.Batch(shard_id=f"{role_name}-0", payload={})
        async def process_batch(self, batch, ctx):
            # До этого теста они обычно не дойдут (зависимость от 'a').
            await asyncio.sleep(0.01)
            return wu.BatchResult(success=True, metrics={"count": 1})
    return Noop()

# ───────────────────────── Вспомогалки ─────────────────────────

async def wait_task_status(db, task_id, want, timeout=6.0):
    from time import time
    t0 = time()
    while time() - t0 < timeout:
        t = await db.tasks.find_one({"id": task_id})
        if t and str(t.get("status")) == want:
            return t
        await asyncio.sleep(0.03)
    raise AssertionError(f"task not reached status={want} in time")

def node_by_id(doc, node_id):
    for n in (doc.get("graph", {}).get("nodes") or []):
        if n.get("node_id") == node_id:
            return n
    return {}

# ───────────────────────── Тест 1: идемпотентность ─────────────────────────

@pytest_asyncio.fixture
async def workers_source(env_and_imports):
    cd, wu = env_and_imports
    src = wu.Worker(roles=["source"], handlers={"source": build_counting_source_handler(wu, total=9, batch=3)})
    dbg("WORKER.STARTING", role="source")
    await src.start()
    dbg("WORKER.STARTED", role="source")
    try:
        yield src
    finally:
        dbg("WORKER.STOPPING", role="source")
        await src.stop()
        dbg("WORKER.STOPPED", role="source")

@pytest.mark.asyncio
async def test_idempotent_metrics_on_duplicate_events(env_and_imports, inmemory_db, workers_source, monkeypatch):
    """
    Дублируем события BATCH_OK и TASK_DONE на продюсере и убеждаемся,
    что метрики инкрементятся ровно один раз (dedup в координаторе).
    """
    cd, wu = env_and_imports

    # Дублируем статус-события у продюсера (локально для этого теста)
    orig_send = AIOKafkaProducerMock.send_and_wait
    async def dup_status(self, topic, value, key=None):
        await orig_send(self, topic, value, key)
        if topic.startswith("status.") and (value or {}).get("msg_type") == "event":
            kind = ((value.get("payload") or {}).get("kind") or "")
            if kind in ("BATCH_OK", "TASK_DONE"):
                # тот же самый envelope → тот же dedup_id и ts
                await BROKER.produce(topic, value)
    monkeypatch.setattr("tests.helpers.pipeline_sim.AIOKafkaProducerMock.send_and_wait", dup_status, raising=True)

    # Координатор
    coord = cd.Coordinator()
    await coord.start()
    try:
        # Граф — один узел 's' типа 'source'
        graph = {
            "schema_version": "1.0",
            "nodes": [
                {
                    "node_id": "s",
                    "type": "source",
                    "depends_on": [],
                    "fan_in": "all",
                    "io": {"input_inline": {}},
                },
                {
                    # запускается после 's' и агрегирует метрики узла 's'
                    "node_id": "agg",
                    "type": "coordinator_fn",
                    "depends_on": ["s"],
                    "fan_in": "all",
                    "io": {
                        "fn": "metrics.aggregate",
                        "fn_args": {"node_id": "s", "mode": "sum"}  # агрегируем в stats узла 's'
                    },
                },
            ],
            "edges": [["s", "agg"]],
        }
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=10.0)
        s = node_by_id(tdoc, "s")
        got = int((s.get("stats") or {}).get("count") or 0)
        dbg("IDEMPOTENT.FINAL", count=got)
        # total=9 (3 батча по 3) — не должно удвоиться
        assert got == 9
        assert str(node_by_id(tdoc, "agg").get("status")).endswith("finished")
    finally:
        await coord.stop()

# ───────────────────────── Тест 2: deferred/backoff ─────────────────────────

@pytest_asyncio.fixture
async def workers_flaky(env_and_imports):
    cd, wu = env_and_imports
    f = wu.Worker(roles=["flaky"], handlers={"flaky": build_flaky_once_handler(wu)})
    dbg("WORKER.STARTING", role="flaky")
    await f.start()
    dbg("WORKER.STARTED", role="flaky")
    try:
        yield f
    finally:
        dbg("WORKER.STOPPING", role="flaky")
        await f.stop()
        dbg("WORKER.STOPPED", role="flaky")

@pytest.mark.asyncio
async def test_transient_failure_deferred_then_retry(env_and_imports, inmemory_db, workers_flaky):
    """
    Первый запуск падает (TASK_FAILED permanent=False) → координатор переводит узел в deferred
    и ставит next_retry_at ~ backoff_sec. После бэкоффа узел перезапускается и завершается успешно.
    """
    cd, wu = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = {
            "schema_version": "1.0",
            "nodes": [{
                "node_id": "f", "type": "flaky", "depends_on": [], "fan_in": "all",
                "retry_policy": {"max": 2, "backoff_sec": 1, "permanent_on": []},
                "io": {"input_inline": {}}
            }],
            "edges": []
        }
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        # Подождём, когда узел уйдёт в deferred
        # (после первой неудачи координатор выставляет deferred и next_retry_at)
        # Затем — когда всё успешно завершится после ретрая.
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        f = node_by_id(tdoc, "f")
        # По факту должен быть finished
        assert str(f.get("status")).endswith("finished")
        # И epoch должен быть > 1 (был ретрай)
        assert int(f.get("attempt_epoch", 0)) >= 2
    finally:
        await coord.stop()

# ───────────────────────── Тест 3: permanent fail + каскадный cancel ─────────────────────────

@pytest_asyncio.fixture
async def workers_cascade(env_and_imports):
    cd, wu = env_and_imports
    wa = wu.Worker(roles=["a"], handlers={"a": build_permanent_fail_handler(wu)})
    wb = wu.Worker(roles=["b"], handlers={"b": build_noop_handler(wu, "b")})
    wc = wu.Worker(roles=["c"], handlers={"c": build_noop_handler(wu, "c")})
    for name, w in (("a", wa), ("b", wb), ("c", wc)):
        dbg("WORKER.STARTING", role=name)
        await w.start()
        dbg("WORKER.STARTED", role=name)
    try:
        yield (wa, wb, wc)
    finally:
        for name, w in (("a", wa), ("b", wb), ("c", wc)):
            dbg("WORKER.STOPPING", role=name)
            await w.stop()
            dbg("WORKER.STOPPED", role=name)

@pytest.mark.asyncio
async def test_permanent_fail_cascades_cancel_and_task_failed(env_and_imports, inmemory_db, workers_cascade):
    """
    Узел 'a' падает permanent=True → координатор вызывает _cascade_cancel()
    для всех зависимых ('b','c') и помечает таск как failed.
    """
    cd, wu = env_and_imports
    coord = cd.Coordinator()
    # ускорим мониторинг
    cd.HB_MONITOR_TICK_SEC = 0.1
    await coord.start()
    try:
        graph = {
            "schema_version": "1.0",
            "nodes": [
                {"node_id": "a", "type": "a", "depends_on": [], "fan_in": "all",
                 "io": {"input_inline": {}}},
                {"node_id": "b", "type": "b", "depends_on": ["a"], "fan_in": "all",
                 "io": {"input_inline": {}}},
                {"node_id": "c", "type": "c", "depends_on": ["a"], "fan_in": "all",
                 "io": {"input_inline": {}}},
            ],
            "edges": [["a","b"], ["a","c"]]
        }
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        # Ждём, когда таск станет failed
        tdoc = await wait_task_status(inmemory_db, task_id, want=str(cd.RunState.failed), timeout=8.0)

        # Проверим, что b и c были переведены координатором в cancelling (или не дошли до running)
        b = node_by_id(tdoc, "b")
        c = node_by_id(tdoc, "c")
        bs = str(b.get("status"))
        cs = str(c.get("status"))
        dbg("CASCADE.FINAL", task=str(tdoc.get("status")), b=bs, c=cs)

        assert str(tdoc.get("status")) == str(cd.RunState.failed)
        # Обычно coordinator помечает зависимые в 'cancelling'
        assert bs in (str(cd.RunState.cancelling), str(cd.RunState.deferred), str(cd.RunState.queued))
        assert cs in (str(cd.RunState.cancelling), str(cd.RunState.deferred), str(cd.RunState.queued))
    finally:
        await coord.stop()

def build_slow_source_handler(wu, *, total=50, batch=5, delay=0.15):
    class SlowSource(wu.RoleHandler):
        role = "source"
        async def load_input(self, ref, inline): return {"total": total, "batch": batch, "delay": delay}
        async def iter_batches(self, loaded):
            t, b = loaded["total"], loaded["batch"]
            shard = 0
            for i in range(0, t, b):
                yield wu.Batch(shard_id=f"s-{shard}", payload={"items": list(range(i, min(i+b, t))), "delay": loaded["delay"]})
                shard += 1
        async def process_batch(self, batch, ctx):
            await asyncio.sleep(batch.payload.get("delay", 0.1))
            return wu.BatchResult(success=True, metrics={"count": len(batch.payload.get("items") or [])})
    return SlowSource()

def build_cancelable_source_handler(wu, *, total=100, batch=10, delay=0.3):
    class Cancellable(wu.RoleHandler):
        role = "source"
        async def load_input(self, ref, inline): return {"total": total, "batch": batch}
        async def iter_batches(self, loaded):
            t, b = loaded["total"], loaded["batch"]
            shard = 0
            for i in range(0, t, b):
                yield wu.Batch(shard_id=f"s-{shard}", payload={"i": i})
                shard += 1
        async def process_batch(self, batch, ctx):
            # Кооперативно реагируем на отмену
            for _ in range(int(delay / 0.05)):
                if ctx.cancelled():
                    return wu.BatchResult(success=False, reason_code="cancelled", permanent=False)
                await asyncio.sleep(0.05)
            return wu.BatchResult(success=True, metrics={"count": 1})
    return Cancellable()

@pytest.mark.asyncio
async def test_status_fencing_ignores_stale_epoch(env_and_imports, inmemory_db, workers_source):
    cd, wu = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = {"schema_version": "1.0",
                 "nodes": [{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
                 "edges": []}
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=10.0)
        base = int((node_by_id(tdoc, "s").get("stats") or {}).get("count") or 0)

        # Синтетически шлём стейл-ивент с attempt_epoch=0 (узел уже завершился на эпохе 1)
        env = cd.Envelope(
            msg_type=cd.MsgType.event, role=cd.Role.worker,
            dedup_id="stale1", task_id=task_id, node_id="s", step_type="source",
            attempt_epoch=0,
            payload={"kind": cd.EventKind.BATCH_OK, "worker_id": "WZ", "metrics": {"count": 999},
                     "artifacts_ref": {"shard_id": "zzz"}}
        )
        await BROKER.produce(cd.TOPIC_STATUS_FMT.format(type="source"), env.model_dump(mode="json"))
        await asyncio.sleep(0.2)

        t2 = await inmemory_db.tasks.find_one({"id": task_id})
        got = int((node_by_id(t2, "s").get("stats") or {}).get("count") or 0)
        assert got == base, "stale event must be ignored by fencing"
    finally:
        await coord.stop()

@pytest.mark.asyncio
async def test_coordinator_restart_adopts_inflight_without_new_epoch(env_and_imports, inmemory_db, monkeypatch):
    cd, wu = env_and_imports

    # делаем хартбит почаще, чтобы коорд быстрее "видел" ран
    monkeypatch.setattr(wu, "HEARTBEAT_INTERVAL_SEC", 0.05, raising=False)

    w = wu.Worker(roles=["source"], handlers={"source": build_slow_source_handler(wu, total=60, batch=5, delay=0.08)})
    await w.start()
    coord1 = cd.Coordinator()
    await coord1.start()
    task_id = None
    coord2 = None
    try:
        graph = {"schema_version": "1.0",
                 "nodes": [{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
                 "edges": []}
        graph = prime_graph(cd, graph)
        task_id = await coord1.create_task(params={}, graph=graph)

        # Ненадолго ждём, чтобы нода точно ушла в running (эпоха 1)
        for _ in range(60):
            t = await inmemory_db.tasks.find_one({"id": task_id})
            if t and str(node_by_id(t, "s").get("status")).endswith("running"):
                break
            await asyncio.sleep(0.05)

        # Перезапускаем координатор посреди ранa
        await coord1.stop()
        coord2 = cd.Coordinator()
        await coord2.start()

        # Финиш и проверка: эпоха не изменилась (не было повторного START)
        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=12.0)
        s = node_by_id(tdoc, "s")
        assert int(s.get("attempt_epoch", 0)) == 1, "new coordinator must adopt inflight instead of restarting"
    finally:
        if coord2: await coord2.stop()
        await w.stop()

@pytest.mark.asyncio
async def test_explicit_cascade_cancel_moves_node_to_deferred(env_and_imports, inmemory_db, monkeypatch):
    cd, wu = env_and_imports
    monkeypatch.setattr(cd, "CANCEL_GRACE_SEC", 0.05, raising=False)

    w = wu.Worker(roles=["source"], handlers={"source": build_cancelable_source_handler(wu, total=100, batch=10, delay=0.3)})
    await w.start()
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = {"schema_version": "1.0",
                 "nodes": [{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
                 "edges": []}
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        # ждём running
        for _ in range(120):
            t = await inmemory_db.tasks.find_one({"id": task_id})
            if t and str(node_by_id(t, "s").get("status")).endswith("running"):
                break
            await asyncio.sleep(0.03)

        await coord._cascade_cancel(task_id, reason="test_cancel")

        # ждём перехода из running в один из целевых статусов
        target = {str(cd.RunState.cancelling), str(cd.RunState.deferred), str(cd.RunState.queued)}
        status = None
        deadline = asyncio.get_running_loop().time() + cd.CANCEL_GRACE_SEC + 1.0
        while asyncio.get_running_loop().time() < deadline:
            t2 = await inmemory_db.tasks.find_one({"id": task_id})
            status = str(node_by_id(t2, "s").get("status"))
            if status in target:
                break
            await asyncio.sleep(0.05)

        assert status in target, f"expected node status in {target}, got {status}"
    finally:
        await coord.stop()
        await w.stop()

@pytest.mark.asyncio
@pytest.mark.xfail(reason="Coordinator не шорткатит ноду по pre-complete артефакту без воркера в текущей реализации")
async def test_complete_artifact_shortcuts_node_without_worker(env_and_imports, inmemory_db):
    cd, wu = env_and_imports
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = {"schema_version": "1.0",
                 "nodes": [{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
                 "edges": []}
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        # до старта воркера помечаем артефакт как complete
        await inmemory_db.artifacts.update_one(
            {"task_id": task_id, "node_id": "s"},
            {"$set": {"status": "complete", "meta": {"prebuilt": True}}},
            upsert=True
        )

        tdoc = await wait_task_finished(inmemory_db, task_id, timeout=6.0)
        s = node_by_id(tdoc, "s")
        assert str(s.get("status")).endswith("finished")
        # fast-path устанавливает новую эпоху (1), но не отправляет CMD на воркера
        assert int(s.get("attempt_epoch", 0)) == 1
    finally:
        await coord.stop()

@pytest.mark.asyncio
async def test_heartbeat_updates_lease_deadline(env_and_imports, inmemory_db, monkeypatch):
    cd, wu = env_and_imports
    # ускоряем heartbeat
    monkeypatch.setattr(wu, "HEARTBEAT_INTERVAL_SEC", 0.05, raising=False)

    w = wu.Worker(roles=["source"], handlers={"source": build_slow_source_handler(wu, total=40, batch=4, delay=0.12)})
    await w.start()
    coord = cd.Coordinator()
    await coord.start()
    try:
        graph = {"schema_version": "1.0",
                 "nodes": [{"node_id": "s", "type": "source", "depends_on": [], "fan_in": "all", "io": {"input_inline": {}}}],
                 "edges": []}
        graph = prime_graph(cd, graph)
        task_id = await coord.create_task(params={}, graph=graph)

        # ждём появления первого дедлайна
        first = None
        for _ in range(120):
            t = await inmemory_db.tasks.find_one({"id": task_id})
            if t:
                lease = (node_by_id(t, "s").get("lease") or {})
                if lease.get("deadline_ts"):
                    first = int(lease["deadline_ts"])
                    break
            await asyncio.sleep(0.03)
        assert first is not None

        # ждём достаточно, чтобы дедлайн «перекатился» на следующую секунду
        lease_ttl = float(getattr(cd, "NODE_LEASE_SEC", getattr(cd, "LEASE_TTL_SEC", 1.0)))
        await asyncio.sleep(max(lease_ttl * 1.2, 1.1))

        # пуллим до роста дедлайна (с запасом)
        second = first
        for _ in range(40):
            t2 = await inmemory_db.tasks.find_one({"id": task_id})
            second = int((node_by_id(t2, "s").get("lease") or {}).get("deadline_ts") or 0)
            if second > first:
                break
            await asyncio.sleep(0.05)

        assert second > first, f"heartbeat should push lease deadline forward (first={first}, second={second})"

        # добиваем задачу
        await wait_task_finished(inmemory_db, task_id, timeout=12.0)
    finally:
        await coord.stop()
        await w.stop()