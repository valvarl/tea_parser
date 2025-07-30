from __future__ import annotations
"""ProductEnricher — модуль «второго прохода» для Ozon‑парсера.

Этот файл больше **не** содержит CLI‑обёртки: теперь он предоставляет
только класс `ProductEnricher`, который можно вызывать из другого кода.

Пример использования:
```python
from indexer import ProductIndexer
from enricher import ProductEnricher

idx = ProductIndexer()
base_rows = await idx.search_products("пуэр")

enricher = ProductEnricher(reviews=True, reviews_limit=30)
rows_plus, reviews = await enricher.enrich(base_rows)
```
"""

import asyncio
import json
import random
import re
import urllib.parse as u
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from camoufox.async_api import AsyncCamoufox

# ─────────── Константы / regexp ─────────────────────────────────────────────
ENTRY_RE      = re.compile(r"/api/entrypoint-api\.bx/page/json/v2\?url=")
TODAY: date   = date.today()

# ─────────── Вспомогательные утилиты (скопированы из indexer) ───────────────
NARROW_SPACE  = "\u2009"
PRICE_RE      = re.compile(r"\d+")
MONTHS_RU     = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

def digits_only(text: str) -> str:
    return re.sub(r"[^0-9.]", "", text.replace(" ", "")
                 .replace(NARROW_SPACE, "").replace("\xa0", ""))

def clean_price(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    m = PRICE_RE.search(digits_only(text))
    return int(m.group()) if m else None

def parse_delivery(text: Optional[str]) -> Optional[date]:
    if not text:
        return None
    txt = text.lower().strip()
    if txt == "сегодня":
        return TODAY
    if txt == "завтра":
        return TODAY + timedelta(days=1)
    if txt == "послезавтра":
        return TODAY + timedelta(days=2)
    m = re.match(r"(\d{1,2})\s+([а-яё]+)", txt)
    if m:
        day, month_name = int(m.group(1)), m.group(2)
        month = MONTHS_RU.get(month_name)
        if month:
            year = TODAY.year
            parsed = date(year, month, day)
            if parsed < TODAY - timedelta(days=2):
                parsed = date(year + 1, month, day)
            return parsed
    return None

# ─────────── PDP helpers ────────────────────────────────────────────────────

def collect_raw_widget(json_page: Dict[str, Any], prefix: str) -> str:
    """Вернуть JSON‑строку widgetStates, чей ключ начинается с prefix."""
    for k, raw in json_page.get("widgetStates", {}).items():
        if k.startswith(prefix):
            return raw
    return ""

def collect_description(json_page: Dict[str, Any]) -> str:
    return collect_raw_widget(json_page, "webDescription-")

def collect_characteristics(json_page: Dict[str, Any]) -> str:
    return collect_raw_widget(json_page, "webCharacteristics-")

def extract_reviews(obj: Dict[str, Any], limit: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for rev in obj.get("productReviewList", []):
        if len(out) >= limit:
            break
        text = rev.get("text", "").strip()
        rating = rev.get("grade")
        author = rev.get("author") or ""
        dt_str = rev.get("date") or ""
        out.append({"author": author, "rating": rating, "date": dt_str, "text": text})
    return out

# ─────────── Класс Enricher ────────────────────────────────────────────────
class ProductEnricher:
    """Асинхронный обогатитель SKU‑карточек из списка indexer.py."""

    def __init__(self, *, concurrency: int = 6, headless: bool = True,
                 reviews: bool = False, reviews_limit: int = 20) -> None:
        self.concurrency   = max(1, concurrency)
        self.headless      = headless
        self.want_reviews  = reviews
        self.reviews_limit = reviews_limit

    # ------------------------------------------------------------------
    async def _grab_pdp(self, ctx, headers: Dict[str, str], row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Скачать PDP‑данные + (опц.) отзывы."""
        path_part = row["link"].split("ozon.ru")[-1]
        pdp_api = ("https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url=" +
                   u.quote(f"{path_part}?layout_container=pdpPage2column&layout_page_index=2", safe=""))

        resp = await ctx.request.get(pdp_api, headers=headers)
        if resp.status != 200:
            return row, []
        pdp_json = await resp.json()

        # raw widgets
        row["charcs_json"] = collect_characteristics(pdp_json) or None
        row["description"] = collect_description(pdp_json) or None

        reviews: List[Dict[str, Any]] = []
        if self.want_reviews:
            reviews_api = ("https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url=" +
                           u.quote(f"{path_part}?layout_container=pdpReviews&layout_page_index=2", safe=""))
            r2 = await ctx.request.get(reviews_api, headers=headers)
            if r2.status == 200:
                rev_json = await r2.json()
                rev_obj = next((json.loads(raw) for raw in rev_json.get("widgetStates", {}).values()
                                if "productReviewList" in raw), {})
                reviews = extract_reviews(rev_obj or {}, self.reviews_limit)
                for rv in reviews:
                    rv["sku"] = row["sku"]
        # небольшая задержка → имитируем человекоподобность
        await asyncio.sleep(random.uniform(0.7, 1.4))
        return row, reviews

    # ------------------------------------------------------------------
    async def enrich(self, rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Обогатить список товаров. Возвращает (rows_plus, reviews)."""
        if not rows:
            return [], []

        async with AsyncCamoufox(headless=self.headless) as browser:
            ctx = await browser.new_context(locale="ru-RU")
            page = await ctx.new_page()

            first_headers: Dict[str, str] = {}

            async def capture_entry(resp):
                nonlocal first_headers
                if ENTRY_RE.search(resp.url) and resp.status == 200 and not first_headers:
                    raw_h = await resp.request.all_headers()
                    first_headers = {k: v for k, v in raw_h.items() if not k.startswith(":")}
                    first_headers.pop("accept-encoding", None)

            # Открываем первый товар лишь для захвата cookies/headers
            page.on("response", capture_entry)
            await page.goto(rows[0]["link"], timeout=60_000, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(1.0, 1.8))
            # -- если не удалось поймать headers: всё равно попробуем

            sem = asyncio.Semaphore(self.concurrency)
            enriched, all_reviews = [], []

            async def worker(row):
                async with sem:
                    r, revs = await self._grab_pdp(ctx, first_headers, row)
                    enriched.append(r)
                    all_reviews.extend(revs)

            await asyncio.gather(*(worker(dict(r)) for r in rows))
            return enriched, all_reviews

# ─────────── Точек входа больше нет ─────────────────────────────────────────
