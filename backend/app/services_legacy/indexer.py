from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from typing import Any, AsyncIterator, Dict, List, Optional
from urllib.parse import quote

from camoufox.async_api import AsyncCamoufox
from fake_useragent import UserAgent

from .utils import digits_only

logger = logging.getLogger(__name__)

ENTRY_RE = re.compile(r"/api/entrypoint-api\.bx/page/json/v2\?url=%2Fsearch%2F")


class CircuitOpen(RuntimeError):
    def __init__(self, message: str, next_retry_at: int) -> None:
        super().__init__(message)
        self.next_retry_at = next_retry_at


class ProductIndexer:
    def __init__(
        self,
        *,
        max_retries: int = 3,
        retry_base_delay: float = 1.0,  # seconds
        circuit_threshold: int = 5,  # consecutive failures to open circuit
        circuit_cooldown: int = 600,  # seconds
    ) -> None:
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

        self.max_retries = max(1, int(max_retries))
        self.retry_base_delay = float(retry_base_delay)
        self.circuit_threshold = max(1, int(circuit_threshold))
        self.circuit_cooldown = max(30, int(circuit_cooldown))

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

        name = next(
            (b.get("textAtom", {}).get("text") for b in it.get("mainState", []) if b.get("type") == "textAtom"), None
        )

        rating = reviews = None
        for b in it.get("mainState", []):
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

    # ---------- network utils with retry/backoff ----------

    async def _req_json(
        self,
        ctx,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        allow_status: range = range(200, 300),
    ) -> Dict[str, Any]:
        attempt = 0
        while True:
            try:
                resp = await ctx.request.get(url, headers=headers)
                status = resp.status
                if status not in allow_status:
                    # retry on 429 and 5xx
                    if status == 429 or 500 <= status <= 599:
                        raise RuntimeError(f"http_status_{status}")
                    raise RuntimeError(f"http_status_{status}_noretry")
                try:
                    return await resp.json()
                except Exception as e:
                    # retry on JSON decode
                    raise json.JSONDecodeError("bad json", doc="", pos=0) from e

            except json.JSONDecodeError as e:
                attempt += 1
                if attempt >= self.max_retries:
                    logger.error("JSON decode failed after %s tries: %s", attempt, e)
                    raise
            except Exception as e:
                attempt += 1
                # retry only for network/429/5xx buckets (marked above)
                if "http_status_" in str(e) and str(e).endswith("_noretry"):
                    raise
                if attempt >= self.max_retries:
                    logger.error("request failed after %s tries: %s", attempt, e)
                    raise
            # backoff with jitter
            delay = self.retry_base_delay * (2 ** (attempt - 1))
            delay *= 1.0 + random.uniform(0, 0.25)
            await asyncio.sleep(delay)

    async def _prime_session(
        self,
        ctx,
        query: str,
        category: str,
        start_page: int,
    ) -> Dict[str, Any]:
        # try up to two passes to capture entry headers + first JSON
        headers: Dict[str, str] = {}
        first_json: Dict[str, Any] = {}

        async def capture(resp):
            nonlocal headers, first_json
            if first_json or resp.status != 200 or not ENTRY_RE.search(resp.url):
                return
            raw_h = await resp.request.all_headers()
            headers = {k: v for k, v in raw_h.items() if not k.startswith(":")}
            first_json = await resp.json()

        page = await ctx.new_page()
        page.on("response", lambda r: asyncio.create_task(capture(r)))

        async def open_and_scroll(url: str) -> None:
            await page.goto(url, timeout=60_000, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(1.2, 2.2))
            await page.mouse.wheel(0, 3000)
            await asyncio.sleep(random.uniform(1.2, 2.2))

        # attempt 1: original search url
        url1 = f"{self.search_url}?text={query}&category={category}&page={start_page}"
        await open_and_scroll(url1)

        # attempt 2: same url with a cache-buster if not captured
        if not first_json:
            url2 = f"{url1}&t={int(time.time()*1000)}"
            await open_and_scroll(url2)

        if not first_json:
            # give one direct API try with retries, to salvage headers-less flow
            api_url = (
                f"{self.api_url}?url={quote(f'/search/?text={query}&category={category}&page={start_page}', safe='')}"
            )
            try:
                first_json = await self._req_json(ctx, api_url, headers=headers or None)
            except Exception:
                first_json = {}

        if not first_json:
            # open circuit: can't proceed now
            next_retry_at = int(time.time()) + self.circuit_cooldown
            raise CircuitOpen("entrypoint not captured", next_retry_at)

        return {"headers": headers, "json": first_json}

    # ---------- iteration with circuit breaker ----------

    async def iter_products(
        self,
        query: str,
        category: str = "9373",
        start_page: int = 1,
        max_pages: int = 10,
        headless: bool = True,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        failures = 0
        seen: set[str] = set()

        async with AsyncCamoufox(headless=headless) as browser:
            ctx = await browser.new_context(**self.context_settings)

            primed = await self._prime_session(ctx, query, category, start_page)
            headers = primed["headers"]
            json_page = primed["json"]
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

                page_no += 1
                if max_pages and page_no > start_page + max_pages - 1:
                    break

                try:
                    next_api = f"{self.api_url}?url={quote(f'/search/?text={query}&category={category}&page={page_no}', safe='')}"
                    json_page = await self._req_json(ctx, next_api, headers=headers)
                    failures = 0
                except Exception as e:
                    failures += 1
                    logger.warning(
                        "page fetch failed (page=%s, fail=%s/%s): %s", page_no, failures, self.circuit_threshold, e
                    )
                    if failures >= self.circuit_threshold:
                        next_retry_at = int(time.time()) + self.circuit_cooldown
                        raise CircuitOpen("circuit opened due to consecutive failures", next_retry_at)
                    # soft backoff between page attempts
                    await asyncio.sleep(self.retry_base_delay * (2 ** (failures - 1)) * (1.0 + random.uniform(0, 0.25)))

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
