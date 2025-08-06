"""
enricher.py

Пример:
    enr = ProductEnricher(
        reviews=True, reviews_limit=40,
        states=True, state_ids=["state-searchResultsV2", r"state-pdp.*"], state_regex=True
    )
    rows_plus, reviews = await enr.enrich(base_rows)
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import urllib.parse as u
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from camoufox.async_api import AsyncCamoufox
from playwright.async_api import BrowserContext, Page

from .utils import collect_raw_widgets, clear_widget_meta, digits_only

ENTRY_RE      = re.compile(r"/api/entrypoint-api\.bx/page/json/v2\?url=")
REVIEW_WIDGET_RE = re.compile(r"webListReviews-\d+-reviewshelfpaginator-\d+")   

_JS_PULL = """
({ids = [], useRegex = false} = {}) => {
  const ok = id =>
    !ids.length ||
    ids.some(p => useRegex ? new RegExp(p).test(id)
                           : (id === p || id.includes(p)));

  const out = {};
  document.querySelectorAll('div[data-state]').forEach(el => {
    if (!ok(el.id)) return;
    const raw = el.dataset.state;
    if (!raw) return;
    try { out[el.id] = JSON.parse(raw); }
    catch { out[el.id] = raw; }
  });
  return out;
}
"""

class ProductEnricher:
    """Асинхронный обогатитель SKU-карточек: описание, характеристики, отзывы, state-div-ы."""

    def __init__(
        self,
        *,
        concurrency: int = 6,
        headless: bool = True,
        # reviews
        reviews: bool = False,
        reviews_limit: int = 0,
        # state-div
        states: bool = False,
        state_ids: Optional[List[str]] = None,
        state_regex: bool = False,
        state_wait: int = 7_000,
    ) -> None:
        
        self.base_url      = "https://www.ozon.ru"

        self.concurrency   = max(1, concurrency)
        self.headless      = headless

        self.want_reviews  = reviews
        self.reviews_limit = reviews_limit

        self.want_states   = states
        self.state_ids     = state_ids or []
        self.state_regex   = state_regex
        self.state_wait    = max(1_000, state_wait)     # ≥1 с

    def _filter_reviews(self, rev: Dict[str, Any]) -> Dict[str, Any]:
        """Оставляем только нужные поля + нормализуем дату."""
        return {
            "author":       rev.get("author"),         # dict
            "content":      rev.get("content"),        # dict
            "comments":     rev.get("comments"),       # dict
            "usefulness":   rev.get("usefulness"),     # dict
            "created_at":   rev.get("createdAt"),
            "published_at": rev.get("publishedAt"),
            "updated_at":   rev.get("updatedAt"),
            "is_purchased": rev.get("isItemPurchased"),
            "uuid":         rev.get("uuid"),
            "version":      rev.get("version"),
        }

    async def _collect_reviews_shelf(self, ctx, headers, path_part, sku) -> List[Dict[str, Any]]:
        reviews: List[Dict[str, Any]] = []
        next_path: Optional[str] = (
            f"{path_part}?layout_container=reviewshelfpaginator"
            "&layout_page_index=1&reviewsVariantMode=1"
        )

        total_reviews = -1
        reviews_limit = self.reviews_limit

        while next_path and (reviews_limit == 0 or len(reviews) < reviews_limit):
            shelf_api = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url=" + \
                        u.quote(next_path, safe="")
            resp = await ctx.request.get(shelf_api, headers=headers)
            if resp.status != 200: break
            data = await resp.json()

            raw_widget = next((v for k, v in data.get("widgetStates", {}).items()
                               if REVIEW_WIDGET_RE.match(k)), None)
            if not raw_widget: break
            try:
                widget_obj = json.loads(raw_widget)
            except Exception:
                widget_obj = {}

            if total_reviews == -1:
                total_reviews = widget_obj.get("paging", {}).get("total", 0)
                if total_reviews < 1:
                    return []
                reviews_limit = min(total_reviews, reviews_limit)

            for rev in widget_obj.get("reviews", []):
                review_parsed = {**self._filter_reviews(rev), "sku": sku}
                if review_parsed.get("content", {}).get("comment", "") == "":
                    break
                reviews.append(review_parsed)
                if len(reviews) >= self.reviews_limit:
                    break

            next_path = data.get("nextPage") or data.get("pageInfo", {}).get("url")
            await asyncio.sleep(random.uniform(0.9, 1.5))

        return reviews[:reviews_limit]
    
    async def _gentle_scroll(self, page: Page, steps: int = 6, pause: float = .8):
        h = await page.evaluate("()=>document.body.scrollHeight")
        for _ in range(steps):
            await page.mouse.wheel(0, h // steps)
            await asyncio.sleep(pause)

    async def _collect_states_divs(
        self,
        ctx: BrowserContext,
        url: str,
        ids: List[str],
        use_regex: bool,
        wait_ms: int,
    ) -> Dict[str, Any]:
        page = await ctx.new_page()
        await page.goto(url, timeout=60_000, wait_until="domcontentloaded")
        await asyncio.sleep(1.5)
        await self._gentle_scroll(page)
        await asyncio.sleep(1.0)

        try:
            await page.wait_for_selector("div[data-state]", state="attached", timeout=wait_ms)
            await page.wait_for_function(
                "document.querySelectorAll('div[data-state]').length > 0", timeout=0
            )
        except Exception:
            # ничего страшного — просто не нашли state-div
            return {}

        payload = {"ids": ids, "useRegex": use_regex}
        return await page.evaluate(_JS_PULL, payload)
    
    def _extract_states_divs(self, states: Dict[str, Any], name: str):
        key = next((k for k in states if re.search(fr'{name}', k)), None)
        attr = states.pop(key) if key else None
        return attr
    
    def _process_charcs(self, property: Dict[str, Any]) -> tuple[str, list[str]]:
        return {
            "id": property.get("id", "").split("_")[0],
            "title": property.get("title", {}).get("textRs", [{}])[0].get("content", "").lower(),
            "values": [v.get("text", "").split(",")[0].lower() for v in property.get("values", [])]
        } 
    
    def _process_aspects(self, aspect: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "aspectKey": aspect.get("aspectKey", None),
            "aspectName": aspect.get("aspectName", None),
            "variants": [
                {
                    "sku": v["sku"],
                    "title": v.get("data", {}).get("title", ""),
                    "coverImage": v.get("data", {}).get("coverImage", None),
                }
                for v in aspect.get("variants", [])
            ]
        }

    def _clear_states_divs(self, states: Dict[str, Any]) -> Dict[str, Any]:
        gallery = self._extract_states_divs(states, "webGallery")
        if gallery is not None:
            states["gallery"] = {key: gallery.get(key) for key in ("coverImage", "images", "videos")}

        _ = self._extract_states_divs(states, "webPriceDecreasedCompact")
        price = self._extract_states_divs(states, "webPrice")
        if price is not None:
            states["price"] = {
                "cardPrice": digits_only(price.get("cardPrice", None)),
                "originalPrice": digits_only(price.get("originalPrice", None)),
                "price": digits_only(price.get("price", None)),
                "pricePerUnit": digits_only(price.get("pricePerUnit")),
                "measurePerUnit": price.get("measurePerUnit", None)
            }

        short_charcs = self._extract_states_divs(states, "webShortCharacteristics")
        if short_charcs is not None:
            short_charcs = short_charcs.get("characteristics", [])
            states["shortCharacteristics"] = [self._process_charcs(property) for property in short_charcs]

        aspects = self._extract_states_divs(states, "webAspects")
        if aspects is not None:
            states["aspects"] = [self._process_aspects(aspect) for aspect in aspects.get("aspects", [])]

        return states
    
    async def _grab_pdp(
        self,
        ctx,
        headers: Dict[str, str],
        row: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Скачать PDP‑данные + (опц.) отзывы."""
        path_part = row["link"].split("ozon.ru")[-1]
        pdp_api = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url=" + \
                  u.quote(f"{path_part}?layout_container=pdpPage2column&layout_page_index=2", safe="")

        resp = await ctx.request.get(pdp_api, headers=headers)
        if resp.status != 200:
            return row, []
        pdp_json = await resp.json()

        # raw widgets
        row.update(self._collect_raw_widgets(pdp_json))

        # reviews
        reviews: List[Dict[str, Any]] = []
        if self.want_reviews:
            reviews = await self._collect_reviews_shelf(ctx, headers, path_part, row["sku"])

        # state-div
        if self.want_states:
            url_full = self.base_url + row["link"]
            states = await self._collect_states_divs(
                ctx, url_full, self.state_ids, self.state_regex, self.state_wait
            )
            clear_widget_meta(states)
            states = self._clear_states_divs(states)
            row["states"] = states

        await asyncio.sleep(random.uniform(0.7, 1.4))
        return row, reviews

    async def enrich(
        self,
        rows: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
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
            await page.goto(self.base_url + rows[0]["link"],
                            timeout=60_000, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(1.0, 1.8))

            sem = asyncio.Semaphore(self.concurrency)
            enriched, all_reviews = [], []

            async def worker(row):
                async with sem:
                    r, revs = await self._grab_pdp(ctx, first_headers, row)
                    enriched.append(r)
                    all_reviews.extend(revs)

            await asyncio.gather(*(worker(dict(r)) for r in rows))
            return enriched, all_reviews
        
    def _collect_raw_widgets(self, json_page: Dict[str, Any]):
        charcs_json = collect_raw_widgets(json_page, "webCharacteristics-")
        descr_blocks = collect_raw_widgets(json_page, "webDescription-")

        if descr_blocks is None:
            descr_json = {}
        elif len(descr_blocks) == 2:
            if 'richAnnotationJson' in descr_blocks[0]:
                descr_json = {"annotation": descr_blocks[0]["richAnnotationJson"]}
                descr_json.update(descr_blocks[1])
            elif 'richAnnotationJson' in descr_blocks[1]:
                descr_json = {"annotation": descr_blocks[1]["richAnnotationJson"]}
                descr_json.update(descr_blocks[0])
            else:
                raise RuntimeError("No richAnnotationJson collected")
        elif len(descr_blocks) == 1:
            if 'richAnnotationJson' in descr_blocks[0]:
                descr_json = {"annotation": descr_blocks[0]["richAnnotationJson"]}
            else:
                raise RuntimeError("No richAnnotationJson collected")
        else:
            raise RuntimeError("Somethig wierd during richAnnotationJson collecting")
        
        return {"characteristics": charcs_json, "description": descr_json}
