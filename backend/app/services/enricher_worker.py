# enricher_handler.py
from __future__ import annotations

import os
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from app.db.mongo import db
from app.services.enricher import ProductEnricher

# базовые интерфейсы из универсального воркера
from worker_universal import RoleHandler, RunContext, Batch, BatchResult, FinalizeResult

# ───── конфиг по умолчанию (совместим со старым воркером) ─────
ENRICH_CONCURRENCY   = int(os.getenv("ENRICH_CONCURRENCY", "6"))
ENRICH_REVIEWS       = os.getenv("ENRICH_REVIEWS", "true").lower() == "true"
ENRICH_REVIEWS_LIMIT = int(os.getenv("ENRICH_REVIEWS_LIMIT", "20"))
ENRICH_BATCH_SIZE    = int(os.getenv("ENRICH_BATCH_SIZE", "12"))
CURRENCY             = os.getenv("CURRENCY", "RUB")
ENRICH_RETRY_MAX     = int(os.getenv("ENRICH_RETRY_MAX", "3"))

def _now_ts() -> int:
    return int(time.time())

def _digits_only(val: Any) -> Optional[int]:
    if val is None:
        return None
    s = str(val)
    n = "".join(ch for ch in s if ch.isdigit())
    return int(n) if n else None

def _extract_prices(row: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    pr = row.get("price") or {}
    card = _digits_only(pr.get("cardPrice"))
    orig = _digits_only(pr.get("originalPrice"))
    disc = _digits_only(pr.get("price"))
    return card, orig, disc

def _pick_cover(row: Dict[str, Any]) -> Optional[str]:
    gallery = (row.get("states", {}) or {}).get("gallery", {}) or {}
    images = gallery.get("images") or []
    first_img = images[0]["src"] if images and isinstance(images[0], dict) else None
    return row.get("cover_image") or gallery.get("coverImage") or first_img

def _pick_title(row: Dict[str, Any]) -> Optional[str]:
    return row.get("name") or (row.get("states", {}) or {}).get("seo", {}).get("name")

def _build_candidate(row: Dict[str, Any], first_seen_at_ts: Optional[int]) -> Dict[str, Any]:
    from datetime import datetime
    created_dt = datetime.utcfromtimestamp(int(first_seen_at_ts)) if first_seen_at_ts else datetime.utcnow()
    return {
        "sku": row["sku"],
        "created_at": created_dt,
        "updated_at": datetime.utcnow(),
        "title": _pick_title(row),
        "cover_image": _pick_cover(row),
        "description": row.get("description"),
        "characteristics": {
            "full": row.get("characteristics"),
            "short": (row.get("states", {}) or {}).get("shortCharacteristics"),
        },
        "gallery": (row.get("states", {}) or {}).get("gallery", {}),
        "aspects": (row.get("states", {}) or {}).get("aspects", {}),
        "collections": (row.get("states", {}) or {}).get("collections", {}),
        "nutrition": (row.get("states", {}) or {}).get("nutrition", {}),
        "seo": (row.get("states", {}) or {}).get("seo", {}),
        "other_offers": row.get("other_offers"),
    }

async def _save_price_snapshot(candidate_id: Any, disc_price: Optional[int], orig_price: Optional[int]) -> None:
    from datetime import datetime
    await db.prices.insert_one(
        {
            "candidate_id": candidate_id,
            "captured_at": datetime.utcnow(),
            "price_current": disc_price,
            "price_old": orig_price,
            "currency": CURRENCY,
        }
    )

async def _upsert_candidate(doc: Dict[str, Any]) -> Tuple[Any, bool]:
    """
    Upsert кандидата по sku. Возвращает (candidate_id, is_insert).
    Линкует candidate_id обратно в db.index.
    """
    from datetime import datetime
    soi = {"created_at": doc["created_at"], "sku": doc["sku"]}
    sa  = {k: v for k, v in doc.items() if k not in ("created_at", "sku")}
    sa["updated_at"] = datetime.utcnow()

    res = await db.candidates.update_one({"sku": doc["sku"]}, {"$set": sa, "$setOnInsert": soi}, upsert=True)
    is_insert = res.upserted_id is not None

    if is_insert:
        candidate_id = res.upserted_id
    else:
        got = await db.candidates.find_one({"sku": doc["sku"]}, {"_id": 1})
        candidate_id = got["_id"]

    # link to index
    await db.index.update_one({"sku": doc["sku"]}, {"$set": {"candidate_id": candidate_id}})
    return candidate_id, is_insert

async def _bulk_upsert_reviews(reviews: List[Dict[str, Any]]) -> int:
    saved = 0
    for rv in reviews:
        for tkey in ("created_at", "published_at", "updated_at"):
            if isinstance(rv.get(tkey), str):
                rv[tkey] = _digits_only(rv[tkey])
        try:
            await db.reviews.update_one(
                {"uuid": rv.get("uuid") or f"{rv.get('sku')}::{rv.get('published_at')}"},
                {"$set": rv},
                upsert=True,
            )
            saved += 1
        except Exception:
            # мягко игнорим, чтобы не заваливать батч
            pass
    return saved

# ───── загруженные настройки от координатора ─────
class _LoadedEnrich(BaseModel):
    mode: str = "enrich_skus"           # фиксируем режим
    skus: List[str] = Field(default_factory=list)
    batch_size: int = Field(default=ENRICH_BATCH_SIZE)
    concurrency: int = Field(default=ENRICH_CONCURRENCY)
    reviews: bool = Field(default=ENRICH_REVIEWS)
    reviews_limit: int = Field(default=ENRICH_REVIEWS_LIMIT)
    # fallback: если список пуст — брать sku из index без candidate_id
    fallback_index_scan: bool = True
    fallback_limit: int = 0  # 0 = без лимита

# ───────────────────────────────────────────────────────────────────────
class EnricherHandler(RoleHandler):
    """
    Роль 'enricher' для универсального воркера.
    Вход: {"mode":"enrich_skus","skus":[...],"batch_size":12,"concurrency":6,"reviews":true,"reviews_limit":20}
    Если skus пуст, по умолчанию берёт из db.index те, у кого отсутствует candidate_id (можно отключить).
    """
    role = "enricher"

    def __init__(self) -> None:
        # создаём клиент так же, как в старом воркере
        self._enricher = ProductEnricher(
            concurrency=2,           # финальные значения поставим при run из loaded
            headless=True,
            reviews=ENRICH_REVIEWS,
            reviews_limit=ENRICH_REVIEWS_LIMIT,
            states=True,
            state_ids=[
                "webGallery",
                "webPrice",
                "webAspects",
                "webCollections",
                "webNutritionInfo",
                "webShortCharacteristics",
            ],
            state_regex=False,
            similar_offers=True,
        )
        self._settings: Optional[_LoadedEnrich] = None

    async def init(self, cfg: Dict[str, Any]) -> None:
        return

    # ── входные данные от координатора ─────────────────────────────────
    async def load_input(
        self,
        input_ref: Optional[Dict[str, Any]],
        input_inline: Optional[Dict[str, Any]],
    ) -> Any:
        inp = input_inline or {}
        if not inp:
            inp = {"mode": "enrich_skus"}

        mode = (inp.get("mode") or "enrich_skus").lower()
        if mode != "enrich_skus":
            # поддерживаем только enrich_skus в этом обработчике
            mode = "enrich_skus"

        skus = [str(s).strip() for s in (inp.get("skus") or []) if str(s).strip()]
        st = _LoadedEnrich(
            mode="enrich_skus",
            skus=skus,
            batch_size=int(inp.get("batch_size") or ENRICH_BATCH_SIZE),
            concurrency=int(inp.get("concurrency") or ENRICH_CONCURRENCY),
            reviews=bool(inp.get("reviews") if "reviews" in inp else ENRICH_REVIEWS),
            reviews_limit=int(inp.get("reviews_limit") or ENRICH_REVIEWS_LIMIT),
            fallback_index_scan=bool(inp.get("fallback_index_scan", True)),
            fallback_limit=int(inp.get("fallback_limit") or 0),
        )
        self._settings = st
        # применим динамические настройки к клиенту
        self._enricher.concurrency  = st.concurrency
        self._enricher.want_reviews = st.reviews
        self._enricher.reviews_limit= st.reviews_limit
        return st

    # ── планируем батчи ────────────────────────────────────────────────
    async def iter_batches(self, loaded: Any) -> AsyncIterator[Batch]:
        assert isinstance(loaded, _LoadedEnrich)
        skus = list(loaded.skus)

        # fallback: если список пуст — соберём из индекса
        if not skus and loaded.fallback_index_scan:
            q = {"candidate_id": None}
            proj = {"sku": 1, "_id": 0}
            if loaded.fallback_limit and loaded.fallback_limit > 0:
                rows = await db.index.find(q, proj).limit(loaded.fallback_limit).to_list(None)
            else:
                rows = await db.index.find(q, proj).to_list(None)
            skus = [str(r["sku"]) for r in rows if r.get("sku")]

        if not skus:
            # один пустой батч с нулевыми метриками
            yield Batch(shard_id="0", payload={"skus": []})
            return

        bs = max(1, int(loaded.batch_size or ENRICH_BATCH_SIZE))
        shard = 0
        for i in range(0, len(skus), bs):
            shard += 1
            chunk = skus[i : i + bs]
            yield Batch(shard_id=str(shard), payload={"skus": chunk})

    # ── обработка одного батча ─────────────────────────────────────────
    async def process_batch(self, batch: Batch, ctx: RunContext) -> BatchResult:
        skus: List[str] = [str(s).strip() for s in (batch.payload or {}).get("skus", []) if str(s).strip()]
        if ctx.cancelled():
            return BatchResult(success=False, reason_code="cancelled", permanent=False)
        if not skus:
            # пустой батч — просто ок
            return BatchResult(success=True, metrics={"processed": 0, "inserted": 0, "updated": 0, "reviews_saved": 0, "dlq": 0})

        # подготовим base_rows из индекса: first_seen + ссылка
        index_rows = await db.index.find({"sku": {"$in": skus}}).to_list(None)
        base_rows: List[Dict[str, Any]] = []
        for d in index_rows:
            sku = str(d.get("sku") or "").strip()
            if not sku:
                continue
            base_rows.append({
                "sku": sku,
                "first_seen_at": d.get("first_seen_at"),
                "link": f"/product/{sku}/",
            })

        # если чего-то нет в индексе — всё равно пробуем
        for sku in skus:
            if not any(r["sku"] == sku for r in base_rows):
                base_rows.append({"sku": sku, "first_seen_at": None, "link": f"/product/{sku}/"})

        # обогащение
        try:
            enriched_rows, reviews_rows, failed_rows = await self._enricher.enrich(base_rows)
        except Exception as e:
            # транзиентная ошибка на батче — сообщаем координатору BATCH_FAILED
            return BatchResult(success=False, reason_code="unexpected_error", permanent=False, error=str(e))

        fs_map = {r["sku"]: r.get("first_seen_at") for r in base_rows}

        processed = 0
        inserted_cnt = 0
        updated_cnt = 0
        transient_failed = 0
        saved_reviews = 0

        # сохраняем кандидатов + цены
        for row in enriched_rows:
            if ctx.cancelled():
                return BatchResult(success=False, reason_code="cancelled", permanent=False)
            try:
                sku = row["sku"]
                candidate_doc = _build_candidate(row, fs_map.get(sku))
                candidate_id, is_insert = await _upsert_candidate(candidate_doc)
                card_price, orig_price, disc_price = _extract_prices(row)
                await _save_price_snapshot(candidate_id, disc_price or card_price, orig_price)
                processed += 1
                inserted_cnt += int(is_insert)
                updated_cnt += int(not is_insert)
            except Exception:
                transient_failed += 1  # не ломаем батч

        # отзывы
        if reviews_rows:
            try:
                saved_reviews = await _bulk_upsert_reviews(reviews_rows)
            except Exception:
                # игнорируем, метрики останутся без reviews_saved
                pass

        # DLQ (hard failures от enricher.enrich)
        dlq_saved = 0
        if failed_rows:
            from datetime import datetime
            docs = [
                {
                    "task_id": None,  # координатор знает task_id, но в хендлере его нет — хранить можно и без него
                    "sku": r.get("sku"),
                    "reason": "retry_exhausted",
                    "attempts": ENRICH_RETRY_MAX,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "batch_hint": batch.shard_id,
                    "source": "enricher_handler",
                }
                for r in failed_rows
            ]
            try:
                if docs:
                    await db.enrich_dlq.insert_many(docs, ordered=False)
                    dlq_saved = len(docs)
            except Exception:
                pass

        # необязательный partial-артефакт — сохранит метрики по shard_id
        try:
            await ctx.artifacts.upsert_partial(batch.shard_id, {
                "processed": processed, "inserted": inserted_cnt,
                "updated": updated_cnt, "reviews_saved": saved_reviews, "dlq": dlq_saved
            })
        except Exception:
            pass

        metrics = {
            "processed": processed,
            "inserted": inserted_cnt,
            "updated": updated_cnt,
            "reviews_saved": saved_reviews,
            "dlq": dlq_saved,
        }
        return BatchResult(success=True, metrics=metrics)

    # ── финализация задачи (опционально) ───────────────────────────────
    async def finalize(self, ctx: RunContext) -> Optional[FinalizeResult]:
        # здесь можно агрегировать метрики/артефакты по всем батчам
        return FinalizeResult(metrics={})

    # ── классификация ошибок ────────────────────────────────────────────
    def classify_error(self, exc: BaseException) -> tuple[str, bool]:
        # По умолчанию считаем ошибки транзиентными — координатор решит, откладывать/ретраить ноду
        return ("unexpected_error", False)
