from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.core.logging import configure_logging
from app.db.mongo import db
from app.models.task import TaskStatus
from app.services.coordinator import Coordinator

# ─────────── env ───────────
BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_COORDINATOR_CMD = os.getenv("TOPIC_COORDINATOR_CMD", "coordinator_cmd")

STATUS_TIMEOUT_SEC = int(os.getenv("STATUS_TIMEOUT_SEC", "300"))
HEARTBEAT_DEADLINE_SEC = int(os.getenv("HEARTBEAT_DEADLINE_SEC", "900"))
HEARTBEAT_CHECK_INTERVAL = int(os.getenv("HEARTBEAT_CHECK_INTERVAL", "60"))
COORDINATOR_POLL_INTERVAL = int(os.getenv("COORDINATOR_POLL_INTERVAL", "15"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", os.getenv("GIT_SHA", "dev"))

# ─────────── logging ───────────
configure_logging(service="coordinator-service", worker="coordinator-service", level=LOG_LEVEL)
logger = logging.getLogger("coordinator-service")


# ─────────── helpers ───────────

def _dumps(x: Any) -> bytes:
    return json.dumps(x, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _loads(x: bytes) -> Any:
    return json.loads(x.decode("utf-8"))


def _now_ts() -> int:
    return int(time.time())


# ─────────── command handling ───────────

async def _handle_command(payload: Dict[str, Any], coord: Coordinator) -> None:
    cmd = (payload.get("cmd") or "").lower()

    if cmd == "start_task":
        task_id = payload.get("task_id")
        if not task_id:
            logger.warning("start_task ignored: missing task_id")
            return

        task = await db.scraping_tasks.find_one({"id": task_id})
        if not task:
            logger.warning("start_task ignored: task not found task_id=%s", task_id)
            return

        params = (task.get("params") or {})
        search_term = params.get("search_term") or ""
        category_id = params.get("category_id") or "9373"
        max_pages = int(params.get("max_pages", 3))

        logger.info(
            "start_task received: task_id=%s q=%r cat=%s pages=%s",
            task_id, search_term, category_id, max_pages
        )
        await coord.run_scrape_task(
            search_term=search_term,
            task_id=task_id,
            category_id=category_id,
            max_pages=max_pages,
        )
        return

    if cmd == "resume_inflight":
        logger.info("resume_inflight command received")
        await coord.resume_inflight()
        return

    logger.warning("unknown coordinator cmd: %r", cmd)


# ─────────── loops ───────────

async def _consume_commands(coord: Coordinator, stop: asyncio.Event) -> None:
    """
    Listens coordinator_cmd and handles start_task/resume_inflight commands.
    Manual commit is used to ensure idempotency. Unrecognized messages are still committed.
    """
    cons = AIOKafkaConsumer(
        TOPIC_COORDINATOR_CMD,
        bootstrap_servers=BOOT,
        group_id="coordinator-service",
        value_deserializer=_loads,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await cons.start()
    logger.info("commands consumer started topic=%s", TOPIC_COORDINATOR_CMD)
    try:
        while not stop.is_set():
            try:
                msg = await asyncio.wait_for(cons.getone(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                payload = msg.value or {}
                await _handle_command(payload, coord)
            except Exception as exc:
                logger.exception("command handling failed: %s", exc)
            finally:
                try:
                    await cons.commit()
                except Exception as exc:
                    logger.warning("command commit failed: %s", exc)
    finally:
        await cons.stop()
        logger.info("commands consumer stopped")


async def _poll_pending(coord: Coordinator, stop: asyncio.Event) -> None:
    """
    Fallback: periodically picks tasks with status=pending and starts them.
    """
    logger.info("pending poller started interval=%ss", COORDINATOR_POLL_INTERVAL)
    while not stop.is_set():
        try:
            # Pick newest pending tasks first
            cur = db.scraping_tasks.find({"status": TaskStatus.pending}).sort("created_at", -1).limit(20)
            async for t in cur:
                task_id = t["id"]
                params = (t.get("params") or {})
                search_term = params.get("search_term") or ""
                category_id = params.get("category_id") or "9373"
                max_pages = int(params.get("max_pages", 3))
                logger.info("poll picked pending task=%s", task_id)
                try:
                    await coord.run_scrape_task(
                        search_term=search_term,
                        task_id=task_id,
                        category_id=category_id,
                        max_pages=max_pages,
                    )
                except Exception as exc:
                    logger.exception("failed to start pending task=%s: %s", task_id, exc)
        except Exception as exc:
            logger.exception("pending poller iteration failed: %s", exc)

        try:
            await asyncio.wait_for(stop.wait(), timeout=COORDINATOR_POLL_INTERVAL)
        except asyncio.TimeoutError:
            pass
    logger.info("pending poller stopped")


async def _heartbeat_monitor(stop: asyncio.Event) -> None:
    """
    Fails long-inactive tasks. A task is considered inactive if its last worker_event
    is older than HEARTBEAT_DEADLINE_SEC.
    """
    logger.info(
        "heartbeat monitor started deadline=%ss, interval=%ss",
        HEARTBEAT_DEADLINE_SEC, HEARTBEAT_CHECK_INTERVAL
    )
    while not stop.is_set():
        now = _now_ts()
        try:
            # Find running or stalled tasks
            cur = db.scraping_tasks.find(
                {"status": {"$in": [TaskStatus.running, "stalled"]}},
                {"id": 1}
            )
            async for t in cur:
                task_id = t["id"]
                ev = await db.worker_events.find_one(
                    {"task_id": task_id},
                    sort=[("ts", -1)],
                    projection={"ts": 1},
                )
                last_ts = (ev or {}).get("ts") or 0
                if last_ts and now - last_ts <= HEARTBEAT_DEADLINE_SEC:
                    continue

                # No recent events: mark as failed
                reason = "heartbeat_timeout" if last_ts else "inactivity_timeout"
                await db.scraping_tasks.update_one(
                    {"id": task_id},
                    {
                        "$set": {
                            "status": TaskStatus.failed,
                            "error_message": reason,
                            "finished_at": asyncio.get_running_loop().time(),  # will be overwritten by push_status if needed
                        },
                        "$currentDate": {"updated_at": True},
                    },
                )
                logger.warning("task %s failed due to %s", task_id, reason)
        except Exception as exc:
            logger.exception("heartbeat iteration failed: %s", exc)

        try:
            await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_CHECK_INTERVAL)
        except asyncio.TimeoutError:
            pass
    logger.info("heartbeat monitor stopped")


# ─────────── entrypoint ───────────

async def main() -> None:
    logger.info(
        "coordinator service starting | version=%s topics={cmd:%s} timeouts={status:%s, heartbeat:%s}",
        SERVICE_VERSION, TOPIC_COORDINATOR_CMD, STATUS_TIMEOUT_SEC, HEARTBEAT_DEADLINE_SEC
    )

    coord = Coordinator()
    await coord.start()
    await coord.resume_inflight()

    stop = asyncio.Event()

    def _graceful_shutdown(*_: object) -> None:
        if not stop.is_set():
            stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _graceful_shutdown)
        except NotImplementedError:
            # Windows / some environments
            pass

    tasks = [
        asyncio.create_task(_consume_commands(coord, stop)),
        asyncio.create_task(_poll_pending(coord, stop)),
        asyncio.create_task(_heartbeat_monitor(stop)),
    ]

    try:
        await stop.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await coord.stop()
        logger.info("coordinator service stopped")


if __name__ == "__main__":
    asyncio.run(main())
