# enricher.py

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
import urllib.parse as u
from typing import Any, Dict, List, Optional, Tuple

from camoufox.async_api import AsyncCamoufox
from playwright.async_api import BrowserContext, Page
from playwright.async_api import TimeoutError as PWTimeout

from .utils import clear_widget_meta, collect_raw_widgets, digits_only
from .validation import DataParsingError
from .validation import Validator as V

logger = logging.getLogger(__name__)

ENTRY_RE = re.compile(r"/api/entrypoint-api\.bx/page/json/v2\?url=")
REVIEW_WIDGET_RE = re.compile(r"webListReviews-\d+-reviewshelfpaginator-\d+")

# Pulls JSON from <div data-state id="...">, optionally filtering by ids (strings or regex patterns).
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


# -------- helpers --------


def _parse_json_maybe(raw: Any) -> Any:
    if isinstance(raw, str):
        cleaned = raw.replace("\\u002F", "/").replace("\\\\", "\\")
        try:
            return json.loads(cleaned)
        except Exception:
            return cleaned
    return raw or {}


def _regex_search_key(d: Dict[str, Any], pat: str) -> Optional[str]:
    for k in d:
        if re.search(rf"{pat}", k):
            return k
    return None


# -------- main class --------


class ProductEnricher:
    def __init__(
        self,
        *,
        concurrency: int = 6,
        headless: bool = True,
        reviews: bool = False,
        reviews_limit: int = 0,
        states: bool = False,
        state_ids: Optional[List[str]] = None,
        state_regex: bool = False,
        state_wait: int = 7_000,
        similar_offers: bool = False,
    ) -> None:
        self.base_url = "https://www.ozon.ru"
        self.concurrency = max(1, concurrency)
        self.headless = headless

        self.want_reviews = reviews
        self.reviews_limit = max(0, reviews_limit)

        self.want_states = states
        self.state_ids = state_ids or []
        self.state_regex = state_regex
        self.state_wait = max(1_000, state_wait)

        self.want_similar_offers = similar_offers

    # ----- public -----

    async def enrich(self, rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        if not rows:
            return [], []

        async with AsyncCamoufox(headless=self.headless) as browser:
            ctx = await browser.new_context(locale="ru-RU")
            await ctx.route("**/*", self._block_heavy_assets)

            # Prime headers/cookies from the first product page.
            first_headers: Dict[str, str] = {}
            page = await ctx.new_page()
            page.on("response", lambda resp: asyncio.create_task(self._capture_entry_headers(resp, first_headers)))

            await page.goto(self.base_url + rows[0]["link"], timeout=60_000, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(1.0, 1.8))

            sem = asyncio.Semaphore(self.concurrency)
            enriched: List[Dict[str, Any]] = []
            collected_reviews: List[Dict[str, Any]] = []

            async def worker(row: Dict[str, Any]) -> None:
                async with sem:
                    r, revs = await self._grab_pdp(ctx, first_headers, dict(row))
                    enriched.append(r)
                    collected_reviews.extend(revs)

            await asyncio.gather(*(worker(r) for r in rows))
            return enriched, collected_reviews

    # ----- network / page ops -----

    async def _capture_entry_headers(self, resp, sink: Dict[str, str]) -> None:
        if sink or resp.status != 200 or not ENTRY_RE.search(resp.url):
            return
        headers = await resp.request.all_headers()
        sink.update({k: v for k, v in headers.items() if not k.startswith(":")})
        sink.pop("accept-encoding", None)

    async def _block_heavy_assets(self, route):
        r = route.request
        if r.resource_type in ("image", "media", "font") or re.search(
            r"\.(?:png|jpe?g|webp|gif|svg|mp4|webm|mov)(?:\?|$)", r.url
        ):
            await route.abort()
        else:
            await route.continue_()

    async def _grab_pdp(
        self, ctx: BrowserContext, headers: Dict[str, str], row: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        link = row["link"]
        sku = row["sku"]

        path_part = link.split("ozon.ru")[-1]
        pdp_api = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url=" + u.quote(
            f"{path_part}?layout_container=pdpPage2column&layout_page_index=2", safe=""
        )

        resp = await ctx.request.get(pdp_api, headers=headers)
        if resp.status != 200:
            await asyncio.sleep(random.uniform(0.7, 1.4))
            return row, []

        pdp_json = await resp.json()

        # raw widgets
        row.update(self._collect_widgets(pdp_json, sku=sku))

        # reviews
        reviews: List[Dict[str, Any]] = []
        if self.want_reviews:
            reviews = await self._collect_reviews(ctx, headers, path_part, sku=sku)

        # state divs
        if self.want_states:
            url_full = self.base_url + link
            states = await self._collect_state_divs(
                ctx=ctx,
                url=url_full,
                ids=self.state_ids,
                use_regex=self.state_regex,
                wait_ms=self.state_wait,
                collect_nuxt=True,
            )
            clear_widget_meta(states)
            row["states"] = self._normalize_states(states, sku=sku)

        # similar/other offers
        if self.want_similar_offers:
            offers = await self._collect_similar_offers(ctx, headers, sku=sku)
            if offers is not None:
                row["other_offers"] = offers

        await asyncio.sleep(random.uniform(0.7, 1.4))
        return row, reviews

    async def _collect_reviews(
        self, ctx: BrowserContext, headers: Dict[str, str], path_part: str, *, sku: str
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        next_path: Optional[str] = (
            f"{path_part}?layout_container=reviewshelfpaginator&layout_page_index=1&reviewsVariantMode=1"
        )

        total = None
        hard_limit = self.reviews_limit or float("inf")

        while next_path and len(out) < hard_limit:
            shelf_api = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url=" + u.quote(next_path, safe="")
            resp = await ctx.request.get(shelf_api, headers=headers)
            if resp.status != 200:
                break

            data = await resp.json()
            raw_widget = next((v for k, v in data.get("widgetStates", {}).items() if REVIEW_WIDGET_RE.match(k)), None)
            if not raw_widget:
                break

            try:
                widget = json.loads(raw_widget)
            except Exception:
                widget = {}

            if total is None:
                total = max(0, widget.get("paging", {}).get("total", 0))
                if total == 0:
                    return []
                hard_limit = min(hard_limit, total)

            for rev in widget.get("reviews", []):
                item = {**self._filter_review(rev), "sku": sku}
                if item.get("content", {}).get("comment", "") == "":
                    break
                out.append(item)
                if len(out) >= hard_limit:
                    break

            next_path = data.get("nextPage") or data.get("pageInfo", {}).get("url")
            await asyncio.sleep(random.uniform(0.9, 1.5))

        return out[: int(hard_limit) if hard_limit != float("inf") else None]

    async def _collect_state_divs(
        self,
        ctx: BrowserContext,
        url: str,
        ids: List[str],
        use_regex: bool,
        wait_ms: int,
        collect_nuxt: bool = True,
    ) -> Dict[str, Any]:
        page = await ctx.new_page()
        await page.goto(url, timeout=60_000, wait_until="domcontentloaded")
        await asyncio.sleep(1.5)
        await self._gentle_scroll(page)
        await asyncio.sleep(1.0)

        try:
            await page.wait_for_selector("div[data-state]", state="attached", timeout=wait_ms)
            await page.wait_for_function("document.querySelectorAll('div[data-state]').length > 0", timeout=0)
        except Exception:
            return {}

        payload = {"ids": ids, "useRegex": use_regex}
        div_states = await page.evaluate(_JS_PULL, payload)

        nuxt_state = {}
        if collect_nuxt:
            nuxt_state = await self._read_nuxt_state(page)

        return {"__NUXT__": nuxt_state, **div_states}

    async def _gentle_scroll(self, page: Page, steps: int = 6, pause: float = 0.8) -> None:
        h = await page.evaluate("()=>document.body.scrollHeight")
        for _ in range(steps):
            await page.mouse.wheel(0, h // steps)
            await asyncio.sleep(pause)

    async def _read_nuxt_state(self, page: Page, *, debug: bool = False, hydrate_timeout: int = 1_500) -> dict:
        t0 = time.perf_counter()

        def dbg(msg: str) -> None:
            if debug:
                logger.debug(msg)

        # 1) direct
        raw = await page.evaluate("() => window.__NUXT__ && (window.__NUXT__.state ?? window.__NUXT__._state)")
        if raw is not None:
            dbg(f"nuxt direct {(time.perf_counter()-t0)*1000:.1f} ms")
            return _parse_json_maybe(raw)

        # 2) inline script
        raw = await page.evaluate(
            """() => {
                const m = [...document.scripts]
                  .map(s => s.textContent.match(
                    /window\\.__NUXT__\\.state\\s*=\\s*(['"`]?)(.*?)\\1[;\\n]/s))
                  .find(Boolean);
                return m ? m[2] : null;
            }"""
        )
        if raw is not None:
            dbg(f"nuxt inline {(time.perf_counter()-t0)*1000:.1f} ms")
            return _parse_json_maybe(raw)

        # 3) hydrate wait
        try:
            await page.wait_for_function(
                "() => window.__NUXT__ && (window.__NUXT__.state ?? window.__NUXT__._state)",
                timeout=hydrate_timeout,
            )
            raw = await page.evaluate("() => window.__NUXT__.state ?? window.__NUXT__._state")
            dbg(f"nuxt hydrate {(time.perf_counter()-t0)*1000:.1f} ms")
            return _parse_json_maybe(raw)
        except PWTimeout:
            dbg(f"nuxt timeout ({hydrate_timeout} ms)")
            return {}

    async def _collect_similar_offers(
        self,
        ctx: BrowserContext,
        headers: Dict[str, str],
        sku: str,
    ) -> Optional[Dict[str, Any]]:
        path = f"/modal/otherOffersFromSellers?product_id={sku}&page_changed=true"
        url = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url=" + u.quote(path, safe="")

        resp = await ctx.request.get(url, headers=headers)
        if resp.status != 200:
            return None

        data = await resp.json()
        ws = data.get("widgetStates", {}) or {}

        # We only need the JSON under the first webSellerList-* key.
        key = next((k for k in ws.keys() if k.startswith("webSellerList-")), None)
        if not key:
            return None

        try:
            parsed = json.loads(ws[key])
        except Exception:
            return None

        def safe_price(seller: dict, *keys: str) -> Optional[int]:
            val = seller.get("price") or {}
            for k in keys:
                val = val.get(k, {}) if isinstance(val, dict) else {}
            return digits_only(val) if val else None

        results = [
            {
                "sku": s.get("sku"),
                "seller_id": s.get("id"),
                "card_price": safe_price(s, "cardPrice", "price"),
                "orig_price": safe_price(s, "originalPrice"),
                "disc_price": safe_price(s, "price"),
            }
            for s in parsed.get("sellers", [])
        ]

        return results or None

    # ----- data shaping -----

    def _filter_review(self, rev: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "author": rev.get("author"),
            "content": rev.get("content"),
            "comments": rev.get("comments"),
            "usefulness": rev.get("usefulness"),
            "created_at": rev.get("createdAt"),
            "published_at": rev.get("publishedAt"),
            "updated_at": rev.get("updatedAt"),
            "is_purchased": rev.get("isItemPurchased"),
            "uuid": rev.get("uuid"),
            "version": rev.get("version"),
        }

    def _normalize_states(self, states: Dict[str, Any], *, sku) -> Dict[str, Any]:
        def pop_match(pat: str) -> Optional[Any]:
            key = _regex_search_key(states, pat)
            return states.pop(key) if key else None

        gallery = pop_match("webGallery")
        if gallery is not None:
            states["gallery"] = {k: gallery.get(k) for k in ("coverImage", "images", "videos")}

        _ = pop_match("webPriceDecreasedCompact")
        price = pop_match("webPrice")
        if price is not None:
            states["price"] = {
                "cardPrice": digits_only(price.get("cardPrice")),
                "originalPrice": digits_only(price.get("originalPrice")),
                "price": digits_only(price.get("price")),
                "pricePerUnit": digits_only(price.get("pricePerUnit")),
                "measurePerUnit": price.get("measurePerUnit"),
            }

        short = pop_match("webShortCharacteristics")
        if short is not None:
            items = short.get("characteristics", [])
            states["shortCharacteristics"] = [self._shape_short_char(p) for p in items]

        aspects = pop_match("webAspects")
        if aspects is not None:
            states["aspects"] = [self._shape_aspect(a) for a in aspects.get("aspects", [])]

        collections = pop_match("webCollections")
        if collections is not None:
            states["collections"] = [self._shape_collection(t) for t in V.get_key(collections, "tiles")]

        nutrition = pop_match("webNutritionInfo")
        if nutrition is not None:
            states["nutrition_info"] = V.get_key(nutrition, "values")

        nuxt = pop_match("__NUXT__")
        if nuxt is not None:
            seo = nuxt.get("seo", {}).get("script", [])
            V.require_non_empty_list(seo, entity_id=sku)
            seo_json = json.loads(seo[0].get("innerHTML", "") or "{}")
            states["seo"] = seo_json

        return states

    def _shape_short_char(self, prop: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": prop.get("id", "").split("_")[0],
            "title": prop.get("title", {}).get("textRs", [{}])[0].get("content", "").lower(),
            "values": [v.get("text", "").split(",")[0].lower() for v in prop.get("values", [])],
        }

    def _shape_aspect(self, aspect: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "aspectKey": aspect.get("aspectKey"),
            "aspectName": aspect.get("aspectName"),
            "variants": [
                {
                    "sku": v.get("sku"),
                    "title": v.get("data", {}).get("title", ""),
                    "coverImage": v.get("data", {}).get("coverImage"),
                }
                for v in aspect.get("variants", [])
            ],
        }

    def _shape_collection(self, tile: Dict[str, Any]) -> Dict[str, Any]:
        return {"sku": V.get_key(tile, "sku"), "picture": V.get_key(tile, "picture")}

    def _shape_specs_char(self, prop: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": prop.get("key", ""),
            "title": (prop.get("name", "") or "").lower(),
            "values": [v.get("text", "").lower() for v in prop.get("values", [])],
        }

    def _collect_widgets(self, json_page: Dict[str, Any], *, sku: str) -> Dict[str, Any]:
        # Characteristics
        charcs_json = collect_raw_widgets(json_page, "webCharacteristics-")
        charcs_json = V.require_non_empty_list(charcs_json)[0]
        charcs_json = V.require_non_empty_list(V.get_key(charcs_json, "characteristics"))
        charcs_json = [self._shape_specs_char(prop) for ch in charcs_json for prop in V.get_key(ch, "short")]

        # Description blocks
        descr_blocks = V.require_non_empty_list(collect_raw_widgets(json_page, "webDescription-"))
        if len(descr_blocks) == 2:
            if any(k in descr_blocks[0] for k in ("richAnnotation", "richAnnotationJson")):
                content_blocks, specs = descr_blocks
            else:
                specs, content_blocks = descr_blocks
        elif len(descr_blocks) == 1:
            content_blocks, specs = descr_blocks[0], None
        else:
            raise RuntimeError("Unexpected description widgets layout")

        descr_json = {
            "content_blocks": self._shape_description(content_blocks, sku=sku),
            "specs": V.get_key(specs, "characteristics"),
        }

        return {"characteristics": charcs_json, "description": descr_json}

    def _shape_description(self, description: Dict[str, Any], *, sku: str) -> List[Dict[str, Any]]:
        # HTML fallback
        if "richAnnotation" in description and description.get("richAnnotationType") == "HTML":
            return [
                {
                    "img": {"alt": "", "src": ""},
                    "video": [],
                    "text": description["richAnnotation"],
                    "text_items": [],
                    "title": "",
                    "title_items": [],
                }
            ]

        content_list = V.get_key(
            V.get_key(description, "richAnnotationJson", entity_id=sku, field_path="description"),
            "content",
            entity_id=sku,
            field_path="description.richAnnotationJson",
        )

        out: List[Dict[str, Any]] = []
        for idx, content in enumerate(
            V.require_non_empty_list(content_list, entity_id=sku, field_path="description.richAnnotationJson.content")
        ):
            blocks = content.get("blocks")
            if blocks is None:
                looks_like_block = any(k in content for k in ("img", "text", "title")) or (
                    content.get("widgetName") == "raVideo"
                )
                if looks_like_block:
                    blocks = [content]
                else:
                    raise DataParsingError(
                        code="missing_blocks",
                        message="No `blocks` and content is not a block",
                        entity_id=sku,
                        field_path=f"description.richAnnotationJson.content[{idx}]",
                        actual=content,
                    )

            for block in V.require_non_empty_list(
                blocks, entity_id=sku, field_path=f"description.richAnnotationJson.content[{idx}].blocks"
            ):
                out.append(
                    {
                        "img": {
                            "alt": block.get("img", {}).get("alt", ""),
                            "src": block.get("img", {}).get("src", ""),
                        },
                        "video": block.get("sources", []) if block.get("widgetName") == "raVideo" else [],
                        "text": block.get("text", {}).get("content", ""),
                        "text_items": [it.get("content") for it in block.get("text", {}).get("items", [])],
                        "title": block.get("title", {}).get("content", ""),
                        "title_items": [it.get("content") for it in block.get("title", {}).get("items", [])],
                    }
                )
        return out
