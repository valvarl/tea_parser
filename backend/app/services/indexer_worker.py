# indexer_handler.py
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, AsyncIterator, Dict, List, Optional

from pydantic import BaseModel, Field

from app.db.mongo import db
from app.services.indexer import ProductIndexer, CircuitOpen

# импортируйте базовые классы из worker_universal.py
from worker_universal import RoleHandler, RunContext, Batch, BatchResult, FinalizeResult

# ---- конфиг (аналог исходного) ----
INDEX_MAX_PAGES = int(os.getenv("INDEX_MAX_PAGES", "1"))
INDEX_CATEGORY_ID = os.getenv("INDEX_CATEGORY_ID", "9373")

INDEX_RETRY_MAX = int(os.getenv("INDEX_RETRY_MAX", "3"))
INDEX_RETRY_BASE_SEC = int(os.getenv("INDEX_RETRY_BASE_SEC", "60"))
INDEX_RETRY_MAX_SEC = int(os.getenv("INDEX_RETRY_MAX_SEC", "3600"))

# для upsert коллекций
COLL_BATCH_SIZE_DEF = int(os.getenv("INDEX_COLL_BATCH_SIZE", "200"))

def _now_ts() -> int:
    return int(time.time())


class _LoadedSearch(BaseModel):
    mode: str = "search"
    query: str
    category_id: str = Field(default=INDEX_CATEGORY_ID)
    max_pages: int = Field(default=INDEX_MAX_PAGES)
    headless: bool = True


class _LoadedCollections(BaseModel):
    mode: str = "collections_upsert"
    skus: List[str]
    batch_size: int = Field(default=COLL_BATCH_SIZE_DEF)


class IndexerHandler(RoleHandler):
    """
    Роль: "indexer".
    Режимы:
      - search:   поисковая индексация страниц; батч = одна страница продуктов
      - collections_upsert: апсерт списка SKU; батч = часть SKU (chunk)
    """
    role = "indexer"

    def __init__(self) -> None:
        self._indexer = ProductIndexer()

    async def init(self, cfg: Dict[str, Any]) -> None:
        # здесь можно инициализировать внешние клиенты/кэш
        return

    # ── Входные данные от координатора ─────────────────────────────────
    async def load_input(
        self,
        input_ref: Optional[Dict[str, Any]],
        input_inline: Optional[Dict[str, Any]],
    ) -> Any:
        """
        Ожидаемые варианты input_inline:
        - {"mode":"search","search_term":"чай","category_id":"9373","max_pages":3}
        - {"mode":"collections_upsert","skus":[...], "batch_size": 200}
        Back-compat: если mode отсутствует, определяем по наличию ключей.
        """
        inp = input_inline or {}
        mode = (inp.get("mode") or "").strip().lower()

        if not mode:
            # эвристика
            if "skus" in inp:
                mode = "collections_upsert"
            else:
                mode = "search"

        if mode == "collections_upsert":
            skus = [str(s).strip() for s in (inp.get("skus") or []) if str(s).strip()]
            return _LoadedCollections(
                mode="collections_upsert",
                skus=skus,
                batch_size=int(inp.get("batch_size") or COLL_BATCH_SIZE_DEF),
            )

        # default → search
        return _LoadedSearch(
            mode="search",
            query=str(inp.get("search_term") or "").strip(),
            category_id=str(inp.get("category_id") or INDEX_CATEGORY_ID),
            max_pages=int(inp.get("max_pages") or INDEX_MAX_PAGES),
            headless=True,
        )

    # ── Планирование батчей ─────────────────────────────────────────────
    async def iter_batches(self, loaded: Any) -> AsyncIterator[Batch]:
        """
        - search: отдаём батчи постранично (payload={"page_no":N,"products":[...]})
        - collections_upsert: отдаём чанки sku (payload={"skus":[...]})
        """
        if isinstance(loaded, _LoadedCollections):
            skus = loaded.skus
            bs = max(1, int(loaded.batch_size or COLL_BATCH_SIZE_DEF))
            shard = 0
            for i in range(0, len(skus), bs):
                shard += 1
                chunk = skus[i : i + bs]
                yield Batch(shard_id=str(shard), payload={"mode": "collections_upsert", "skus": chunk})
            return

        if isinstance(loaded, _LoadedSearch):
            # Мы хотим выдавать готовые страницы, чтобы process_batch занимался апсертами
            page_no = 0
            # Встроенные ретраи для CircuitOpen на уровне выдачи страниц
            attempt = 0
            while True:
                try:
                    async for products in self._indexer.iter_products(
                        query=loaded.query,
                        category=loaded.category_id,
                        start_page=1,
                        max_pages=loaded.max_pages,
                        headless=loaded.headless,
                    ):
                        page_no += 1
                        yield Batch(shard_id=str(page_no), payload={
                            "mode": "search",
                            "page_no": page_no,
                            "products": products or []
                        })
                    break  # completed
                except CircuitOpen:
                    attempt += 1
                    if attempt >= INDEX_RETRY_MAX:
                        # пробрасываем дальше — worker отправит TASK_FAILED(permanent=False) по classify_error
                        raise
                    delay = min(INDEX_RETRY_MAX_SEC, INDEX_RETRY_BASE_SEC * (2 ** (attempt - 1)))
                    await asyncio.sleep(delay)
            return

        # на всякий случай — ничего
        return

    # ── Выполнение батча ────────────────────────────────────────────────
    async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
        if ctx.cancelled():
            return BatchResult(success=False, reason_code="cancelled", permanent=False)

        payload = batch.payload or {}
        mode = payload.get("mode")

        if mode == "collections_upsert":
            return await self._process_collections_batch(payload, ctx)

        # default: search
        return await self._process_search_page(payload, ctx)

    async def _process_search_page(self, payload: Dict[str, Any], ctx: RunContext) -> BatchResult:
        products = list(payload.get("products") or [])
        page_no = int(payload.get("page_no") or 0)

        batch_skus: List[str] = []
        batch_inserted = 0
        batch_updated = 0

        for p in products:
            if ctx.cancelled():
                return BatchResult(success=False, reason_code="cancelled", permanent=False)

            sku = str((p or {}).get("sku") or "").strip()
            if not sku:
                continue

            try:
                res = await db.index.update_one(
                    {"sku": sku},
                    {
                        "$set": {
                            "last_seen_at": _now_ts(),
                            "is_active": True,
                            "status": "indexed_auto",    # как в старом коде
                        },
                        "$setOnInsert": {
                            "first_seen_at": _now_ts(),
                            "candidate_id": None,
                            "sku": sku,
                        },
                    },
                    upsert=True,
                )
                is_insert = res.upserted_id is not None
                batch_inserted += int(is_insert)
                batch_updated += int(not is_insert)
                batch_skus.append(sku)
            except Exception as e:
                # не валим батч: просто логика best-effort
                # координация ошибку на батче не требует, метрики отражают апсерты
                # (можно добавить отдельную метрику "upsert_errors")
                pass

        metrics = {
            "indexed": batch_inserted + batch_updated,
            "inserted": batch_inserted,
            "updated": batch_updated,
            "pages": 1,
        }
        # artifacts_ref можно не возвращать — координатор использует метрики
        return BatchResult(success=True, metrics=metrics)

    async def _process_collections_batch(self, payload: Dict[str, Any], ctx: RunContext) -> BatchResult:
        skus: List[str] = [str(s).strip() for s in (payload.get("skus") or []) if str(s).strip()]
        if not skus:
            return BatchResult(success=True, metrics={"pages": 0, "indexed": 0, "inserted": 0, "updated": 0})

        now_ts = _now_ts()
        inserted = 0
        updated = 0

        for sku in skus:
            if ctx.cancelled():
                return BatchResult(success=False, reason_code="cancelled", permanent=False)
            try:
                res = await db.index.update_one(
                    {"sku": sku},
                    {
                        "$set": {
                            "last_seen_at": now_ts,
                            "is_active": True,
                            "status": "pending_review",   # как в исходном collections-flow
                        },
                        "$setOnInsert": {
                            "first_seen_at": now_ts,
                            "candidate_id": None,
                            "sku": sku,
                        },
                    },
                    upsert=True,
                )
                is_insert = res.upserted_id is not None
                inserted += int(is_insert)
                updated += int(not is_insert)
            except Exception:
                # best-effort
                pass

        metrics = {
            "indexed": inserted + updated,
            "inserted": inserted,
            "updated": updated,
            "pages": 0,
        }
        return BatchResult(success=True, metrics=metrics)

    # ── Финализация ─────────────────────────────────────────────────────
    async def finalize(self, ctx: RunContext) -> Optional[FinalizeResult]:
        # Для indexer в базовой версии ничего финализировать не требуется.
        # Если нужна агрегация или выгрузка артефактов — добавьте здесь.
        return FinalizeResult(metrics={})

    # ── Классификация ошибок ────────────────────────────────────────────
    def classify_error(self, exc: BaseException) -> tuple[str, bool]:
        # CircuitOpen — транзиентная (permanent=False), даём координатору отложить/ретраить
        if isinstance(exc, CircuitOpen):
            return ("circuit_open", False)
        return ("unexpected_error", False)
