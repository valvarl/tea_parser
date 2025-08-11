from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from typing import Any, AsyncIterator, Dict, List, Optional
from urllib.parse import quote

from camoufox.async_api import AsyncCamoufox
from fake_useragent import UserAgent

from .utils import digits_only

logger = logging.getLogger(__name__)

ENTRY_RE = re.compile(r"/api/entrypoint-api\.bx/page/json/v2\?url=%2Fsearch%2F")


class ProductIndexer:
    def __init__(self) -> None:
        self.base_url = "https://www.ozon.ru"
        self.search_url = f"{self.base_url}/search/"
        self.api_url = f"{self.base_url}/api/entrypoint-api.bx/page/json/v2"

        ua = UserAgent().random
        self.context_settings = {
            "locale": "ru-RU",
            "user_agent": ua,
            "viewport": {"width": 1366, "height": 768},
            "extra_http_headers": {"Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"},
        }

    # ---------- parsing ----------

    @staticmethod
    def _extract_items(json_page: Dict[str, Any]) -> List[Dict[str, Any]]:
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

    @staticmethod
    def _parse_item(it: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        sku = str(it.get("sku") or "")
        if not sku:
            return None

        name = next((b["textAtom"]["text"] for b in it["mainState"] if b.get("type") == "textAtom"), None)

        rating = reviews = None
        for b in it["mainState"]:
            if b.get("type") == "labelList" and "rating" in json.dumps(b):
                labels = b.get("labelList", {}).get("items", [])
                if labels:
                    rating = float(digits_only(labels[0].get("title")) or 0) or None
                    if len(labels) > 1:
                        reviews = int(digits_only(labels[1].get("title")) or 0) or None
                break

        cover_image = None
        imgs = it.get("tileImage", {}).get("items", [])
        if imgs:
            cover_image = imgs[0].get("image", {}).get("link")

        return {
            "sku": sku,
            "name": name,
            "rating": rating,
            "reviews": reviews,
            "cover_image": cover_image,
        }

    # ---------- network / iteration ----------

    @staticmethod
    async def _capture_entry_headers_and_json(resp, headers_sink: Dict[str, str], json_sink: Dict[str, Any]) -> None:
        if json_sink or resp.status != 200 or not ENTRY_RE.search(resp.url):
            return
        raw_h = await resp.request.all_headers()
        headers_sink.update({k: v for k, v in raw_h.items() if not k.startswith(":")})
        json_sink.update(await resp.json())

    async def iter_products(
        self,
        query: str,
        category: str = "9373",
        start_page: int = 1,
        max_pages: int = 10,
        headless: bool = True,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        search_url = f"{self.search_url}?text={query}&category={category}&page={start_page}"

        async with AsyncCamoufox(headless=headless) as browser:
            ctx = await browser.new_context(**self.context_settings)
            page = await ctx.new_page()

            first_headers: Dict[str, str] = {}
            first_json_box: Dict[str, Any] = {}

            page.on(
                "response",
                lambda resp: asyncio.create_task(
                    self._capture_entry_headers_and_json(resp, first_headers, first_json_box)
                ),
            )

            await page.goto(search_url, timeout=60_000, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(1.5, 3.5))
            await page.mouse.wheel(0, 3000)
            await asyncio.sleep(random.uniform(1.5, 3.5))

            if not first_json_box:
                logger.warning("Entrypoint JSON not captured; possible anti-bot or changed markup.")
                return

            seen: set[str] = set()
            json_page = first_json_box
            page_no = start_page

            while True:
                batch: List[Dict[str, Any]] = []
                for it in self._extract_items(json_page):
                    row = self._parse_item(it)
                    if row and row["sku"] not in seen:
                        seen.add(row["sku"])
                        batch.append(row)

                if batch:
                    yield batch
                logger.info("page %s: %s unique items", page_no, len(seen))

                page_no += 1
                if max_pages and page_no > start_page + max_pages - 1:
                    break

                next_api = (
                    f"{self.api_url}?url={quote(f'/search/?text={query}&category={category}&page={page_no}', safe='')}"
                )
                resp = await ctx.request.get(next_api, headers=first_headers)
                if resp.status != 200:
                    logger.warning("HTTP %s on page %s", resp.status, page_no)
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
    ) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        async for batch in self.iter_products(
            query=query,
            category=category,
            start_page=start_page,
            max_pages=max_pages,
            headless=headless,
        ):
            result.extend(batch)
        return result
