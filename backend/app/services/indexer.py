from __future__ import annotations

import asyncio
from datetime import date, timedelta
import json
import logging
import random
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urljoin

from app.utils.classify import classify_tea_type
from camoufox.async_api import AsyncCamoufox
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)


# ─────────── Регэкспы / константы ─────────────────────────────────────────────
ENTRY_RE      = re.compile(r"/api/entrypoint-api\.bx/page/json/v2\?url=%2Fsearch%2F")
PRICE_RE      = re.compile(r"\d+")
NARROW_SPACE  = "\u2009"
TODAY: date   = date.today()
MONTHS_RU     = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

# ─────────── Утилиты ──────────────────────────────────────────────────────────
def digits_only(text: str) -> str:
    return re.sub(r"[^0-9.]", "", text.replace(" ", "").replace(NARROW_SPACE, "").replace("\xa0", ""))

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


class ProductIndexer:
    def __init__(self) -> None:
        self.base_url = "https://www.ozon.ru"
        self.search_url = "https://www.ozon.ru/search/"
        self.api_url = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2"
        
        self.ua = UserAgent().random
        self.context_settings = {
            "locale": "ru-RU",
            "user_agent": self.ua,
            "viewport": {"width": 1366, "height": 768},
            "extra_http_headers": {"Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"},
        }
    
    # ─────────── Парсинг entrypoint-json ─────────────────────────────────────────
    def extract_items(self, json_page: Dict[str, Any]) -> List[Dict[str, Any]]:
        for raw in json_page.get("widgetStates", {}).values():
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            items = obj.get("items")
            if isinstance(items, list):
                good = [it for it in items if isinstance(it, dict) and "mainState" in it]
                if len(good) >= 5:
                    return good
        return []

    def grab_item(self, it: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        sku = str(it.get("sku") or "")
        if not sku:
            return None

        link = it.get("action", {}).get("link", "").split("?")[0]
        name = next((b["textAtom"]["text"]
                    for b in it["mainState"] if b["type"] == "textAtom"), None)

        price_curr = price_old = None
        price_block = next((b for b in it["mainState"] if b["type"] == "priceV2"), None)
        if price_block:
            for p in price_block["priceV2"].get("price", []):
                if p.get("textStyle") == "PRICE":
                    price_curr = clean_price(p.get("text"))
                elif p.get("textStyle") == "ORIGINAL_PRICE":
                    price_old = clean_price(p.get("text"))

        rating = reviews = None
        for b in it["mainState"]:
            if b["type"] == "labelList" and "rating" in json.dumps(b):
                labels = b["labelList"].get("items", [])
                if labels:
                    rating_txt = digits_only(labels[0]["title"])
                    rating = float(rating_txt) if rating_txt else None
                    if len(labels) > 1:
                        rev_txt = digits_only(labels[1]["title"])
                        reviews = int(rev_txt) if rev_txt else None
                break

        img_urls = [i["image"]["link"] for i in it.get("tileImage", {}).get("items", [])[:10]]
        images = "|".join(img_urls)

        mb = it.get("multiButton", {}).get("ozonButton", {}).get("addToCart", {})
        delivery_raw = mb.get("actionButton", {}).get("title") if mb else None
        delivery_date = parse_delivery(delivery_raw)
        max_qty = mb.get("quantityButton", {}).get("maxItems") if mb else None

        return {
            "sku": sku,
            "link": link,
            "name": name,
            "price_curr": price_curr,
            "price_old": price_old,
            "rating": rating,
            "reviews": reviews,
            "images": images,
            "delivery_day_raw": delivery_raw,
            "delivery_date": delivery_date,
            "max_qty": max_qty,
            "first_seen": TODAY,
        }

    async def iter_products(
        self,
        query: str,
        category: str = "9373",
        start_page: int = 1,
        max_pages: int = 10,
        headless: bool = True,
    ) -> AsyncIterator[List[Dict]]:
        """Yield product batches page by page."""
        search_url = f"{self.search_url}?text={query}&category={category}&page={start_page}"

        async with AsyncCamoufox(headless=headless) as browser:
            ctx  = await browser.new_context(**self.context_settings)
            page = await ctx.new_page()

            first_headers: Dict[str, str] = {}
            first_json: Optional[Dict[str, Any]] = None

            async def capture_entry(resp):
                nonlocal first_headers, first_json
                if ENTRY_RE.search(resp.url) and resp.status == 200 and not first_json:
                    raw_h = await resp.request.all_headers()
                    first_headers = {k: v for k, v in raw_h.items() if not k.startswith(":")}
                    first_json = await resp.json()

            page.on("response", capture_entry)

            await page.goto(search_url, timeout=60_000, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(1.5, 3.5))
            await page.mouse.wheel(0, 3000)
            await asyncio.sleep(random.uniform(1.5, 3.5))


            if not first_json:
                print("⛔ Entrypoint-API не пойман — анти-бот или новая разметка.")
                return

            seen: set[str] = set()
            json_page = first_json
            page_no = start_page

            while True:
                batch_rows: List[Dict[str, Any]] = []
                for it in self.extract_items(json_page):
                    row = self.grab_item(it)
                    if row and row["sku"] not in seen:
                        seen.add(row["sku"])
                        batch_rows.append(row)
                print(f"✓ page {page_no}: всего {len(seen)} уникальных")
                if batch_rows:
                    yield batch_rows

                page_no += 1
                if max_pages and page_no > start_page + max_pages - 1:
                    break

                next_api = f"{self.api_url}?url={quote(f'/search/?text={query}&category={category}&page={page_no}', safe='')}"
                resp = await ctx.request.get(next_api, headers=first_headers)
                if resp.status != 200:
                    print("⛔ HTTP", resp.status, "на", page_no)
                    break
                json_page = await resp.json()
                await asyncio.sleep(random.uniform(1.2, 2.5))

    async def search_products(
        self,
        query: str,
        category: str = "9373",
        start_page: int = 1,
        max_pages: int = 10,
        headless: bool = True,
    ) -> List[Dict]:
        """Return all products across the requested pages."""
        result: List[Dict] = []
        async for batch in self.iter_products(
            query=query,
            category=category,
            start_page=start_page,
            max_pages=max_pages,
            headless=headless,
        ):
            result.extend(batch)
        return result
