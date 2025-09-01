# tests/test_outbox_dispatcher.py
import asyncio
import time

import pytest
import pytest_asyncio

from tests.helpers.pipeline_sim import (
    setup_env_and_imports,
    install_inmemory_db,
    dbg,
)

# ───────────────────────── Fixtures ─────────────────────────

@pytest.fixture(scope="function")
def env_and_imports(monkeypatch):
    """
    ВАЖНО: включаем реальный Outbox (без байпаса) через TEST_USE_OUTBOX=1
    и ускоряем тики/делаем backoff детерминированным.
    """
    monkeypatch.setenv("TEST_USE_OUTBOX", "1")
    cd, _ = setup_env_and_imports(monkeypatch)

    # быстрые тики аутбокса
    cd.OUTBOX_DISPATCH_TICK_SEC = 0.05
    cd.OUTBOX_MAX_RETRY = 5
    cd.OUTBOX_BACKOFF_MIN_MS = 1000
    cd.OUTBOX_BACKOFF_MAX_MS = 4000

    # убираем джиттер, если функция есть в модуле
    monkeypatch.setattr(cd, "_jitter_ms", lambda base_ms: base_ms, raising=False)
    return cd


@pytest.fixture
def inmemory_db(monkeypatch, env_and_imports):
    cd = env_and_imports
    return install_inmemory_db(monkeypatch, cd, cd)


@pytest_asyncio.fixture
async def coordinator(env_and_imports, inmemory_db):
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


# ───────────────────────── Tests ─────────────────────────

@pytest.mark.asyncio
async def test_outbox_retry_backoff(env_and_imports, inmemory_db, coordinator, monkeypatch):
    """
    Первые 2 вызова _raw_send падают → запись Outbox уходит в 'retry' с backoff ≥ 1s,
    attempts растёт, на 3-й раз — 'sent'.
    """
    cd = env_and_imports
    topic, key = "test.topic.retry", "k1"

    attempts = {}
    orig_raw = coordinator.bus._raw_send

    async def flaky_raw_send(t: str, k: bytes, env):
        kk = f"{t}:{k.decode()}"
        attempts[kk] = attempts.get(kk, 0) + 1
        dbg("RAW_SEND.TRY", kk=kk, attempt=attempts[kk])
        if attempts[kk] <= 2:
            raise RuntimeError("broker temporary down")
        await orig_raw(t, k, env)

    monkeypatch.setattr(coordinator.bus, "_raw_send", flaky_raw_send, raising=True)

    env = cd.Envelope(
        msg_type=cd.MsgType.cmd,
        role=cd.Role.coordinator,
        dedup_id="d-1",
        task_id="t",
        node_id="n",
        step_type="echo",
        attempt_epoch=1,
        payload={"kind": "TEST"},
    )

    await coordinator.outbox.enqueue(topic=topic, key=key, env=env)

    async def wait_state(expect: str, timeout: float = 4.0):
        t0 = time.time()
        while time.time() - t0 < timeout:
            d = await inmemory_db.outbox.find_one({"topic": topic, "key": key})
            if d and d.get("state") == expect:
                return d
            await asyncio.sleep(0.03)
        raise AssertionError(f"outbox not in '{expect}'")

    # после 1-го фейла — retry, attempts=1, next_attempt_at ≥ now+1s
    d1 = await wait_state("retry", timeout=2.0)
    assert int(d1.get("attempts", 0)) == 1
    assert int(d1.get("next_attempt_at", 0)) >= int(time.time()) + 1

    # дождёмся attempts >= 2 (вторая неудачная попытка)
    async def wait_attempts_ge(n: int, timeout: float = 4.0):
        t0 = time.time()
        while time.time() - t0 < timeout:
            d = await inmemory_db.outbox.find_one({"topic": topic, "key": key})
            if d and d.get("state") == "retry" and int(d.get("attempts", 0)) >= n:
                return d
            await asyncio.sleep(0.03)
        raise AssertionError(f"outbox attempts not >= {n}")

    d2 = await wait_attempts_ge(2, timeout=4.0)
    assert int(d2.get("attempts", 0)) == 2
    assert int(d2.get("next_attempt_at", 0)) >= int(time.time()) + 1

    # третья попытка — успех
    d3 = await wait_state("sent", timeout=4.0)
    assert attempts[f"{topic}:{key}"] == 3, attempts


@pytest.mark.asyncio
async def test_outbox_exactly_once_fp_uniqueness(env_and_imports, inmemory_db, coordinator, monkeypatch):
    """
    Два enqueue с одинаковым (topic,key,dedup_id) → одна запись в outbox и ровно один реальный send.
    """
    cd = env_and_imports
    topic, key = "test.topic.once", "k2"

    # эмуляция уникального индекса по fp (на уровне InMemDB)
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

    # считаем реальные отправки
    sent_calls = []
    orig_raw = coordinator.bus._raw_send

    async def counting_raw_send(t, k, env):
        sent_calls.append((t, k.decode(), env.dedup_id))
        await orig_raw(t, k, env)

    monkeypatch.setattr(coordinator.bus, "_raw_send", counting_raw_send, raising=True)

    env1 = cd.Envelope(
        msg_type=cd.MsgType.cmd,
        role=cd.Role.coordinator,
        dedup_id="same-dedup",
        task_id="t",
        node_id="n",
        step_type="echo",
        attempt_epoch=1,
        payload={"kind": "TEST"},
    )
    env2 = cd.Envelope(
        msg_type=cd.MsgType.cmd,
        role=cd.Role.coordinator,
        dedup_id="same-dedup",
        task_id="t",
        node_id="n",
        step_type="echo",
        attempt_epoch=1,
        payload={"kind": "TEST"},
    )

    await coordinator.outbox.enqueue(topic=topic, key=key, env=env1)
    await coordinator.outbox.enqueue(topic=topic, key=key, env=env2)  # должен быть проигнорирован (dup fp)

    # ждём, пока произойдёт отправка
    t0 = time.time()
    while time.time() - t0 < 2.0 and not any(s for s in sent_calls if s[0] == topic and s[1] == key):
        await asyncio.sleep(0.02)

    # отправка ровно одна
    sent_cnt = sum(1 for s in sent_calls if s[0] == topic and s[1] == key)
    assert sent_cnt == 1, f"expected exactly one send, got {sent_cnt}"

    # и в outbox ровно одна запись по (topic,key) со статусом sent
    docs = [r for r in inmemory_db.outbox.rows if r.get("topic") == topic and r.get("key") == key]
    assert len(docs) == 1
    assert docs[0].get("state") == "sent"
