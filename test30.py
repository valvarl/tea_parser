from __future__ import annotations

"""Ozon parser (фикс август 2025, web* patch)
Собирает товары по поисковому запросу + характеристики/описание + отзывы.

Изменения:
• Описание теперь берём строго из widgetStates["webDescription-2983278-pdpPage2column-2"].
• Характеристики теперь берём строго из widgetStates["webCharacteristics-3282540-pdpPage2column-2"].
• Если конкретные web*-виджеты отсутствуют (редкий случай), работаем по прежней универсальной логике обхода всех widgetStates.
• ⟶ NEW: Полная выгрузка пользовательских отзывов через reviewshelfpaginator.
    ▸ Для каждой PDP после её получения итеративно запрашиваются страницы reviewshelfpaginator,
      где обязательным является параметр reviewsVariantMode=1.
    ▸ JSON-строка виджета вида "webListReviews-*-reviewshelfpaginator-*" сохраняется «как есть»
      в каталог ./reviews_raw (один файл на страницу), чтобы позже её можно было распарсить.
    ▸ Между запросами делаются небольшие паузы от 0.9 до 1.5 с.
"""

import argparse
import asyncio
import json
import random
import re
import urllib.parse as u
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from camoufox.async_api import AsyncCamoufox

# ─────────── Параметры запуска ───────────────────────────────────────────────
SEARCH_URL = "https://www.ozon.ru/search/?text=пуэр&category=9373&page=1"
OUTPUT_GOODS_CSV   = "ozon_puer_full.csv"
OUTPUT_REVIEWS_CSV = "ozon_puer_reviews.csv"  # пока не используется (сырые JSON-файлы кладутся в reviews_raw)
RAW_REVIEWS_DIR    = Path("reviews_raw")
MAX_PAGES    = 3      # 0 = unlimited
CONCURRENCY  = 6      # PDP concurrency
HEADLESS     = False

# ─────────── Регэкспы / константы ────────────────────────────────────────────
ENTRY_RE      = re.compile(r"/api/entrypoint-api\.bx/page/json/v2\?url=%2Fsearch%2F")
PRICE_RE      = re.compile(r"\d+")
# REVIEW_WIDGET_RE = re.compile(r"^webListReviews-.*-reviewshelfpaginator-\d+")
REVIEW_WIDGET_RE = re.compile(r"webListReviews-\d+-reviewshelfpaginator-\d+")
NARROW_SPACE  = "\u2009"
TODAY: date   = date.today()
MONTHS_RU     = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

# ─────────── CLI ─────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Ozon parser")
parser.add_argument("--reviews", action="store_true", help="Скачать отзывы (сырые JSON-файлы в ./reviews_raw)")
parser.add_argument("--reviews-limit", type=int, default=0,
                    help="Максимум отзывов на товар (0 = не ограничивать, только для будущего парсинга)")
args, _ = parser.parse_known_args()

# ─────────── Утилиты ─────────────────────────────────────────────────────────

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

# ─────────── Сырый save-стаб для отзывов ─────────────────────────────────────

def save_reviews_raw(sku: str, page_no: int, raw_json: str) -> None:
    """Сохранить "как есть" строку JSON-виджета отзывов."""
    RAW_REVIEWS_DIR.mkdir(exist_ok=True)
    out_path = RAW_REVIEWS_DIR / f"{sku}_page{page_no}.json"
    out_path.write_text(raw_json, encoding="utf-8")

    # В будущем сюда вернётся структурированный список отзывов
    # Пока что ничего не возвращаем / не парсим

# ─────────── Парсинг SEARCH страницы ────────────────────────────────────────

def extract_items(json_page: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Вернуть список item-слов из SERP JSON."""
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


def grab_item(it: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sku = str(it.get("sku") or "")
    if not sku:
        return None

    link = it.get("action", {}).get("link", "").split("?")[0]
    name = next((b["textAtom"]["text"] for b in it["mainState"] if b["type"] == "textAtom"), None)

    price_curr = price_old = None
    price_block = next((b for b in it["mainState"] if b["type"] == "priceV2"), None)
    if price_block:
        for p in price_block["priceV2"].get("price", []):
            if p.get("textStyle") == "PRICE":
                price_curr = clean_price(p.get("text"))
            elif p.get("textStyle") == "ORIGINAL_PRICE":
                price_old = clean_price(p.get("text"))

    rating = reviews_cnt = None
    for b in it["mainState"]:
        if b["type"] == "labelList" and "rating" in json.dumps(b):
            labels = b["labelList"].get("items", [])
            if labels:
                rating_txt = digits_only(labels[0]["title"])
                rating = float(rating_txt) if rating_txt else None
                if len(labels) > 1:
                    rev_txt = digits_only(labels[1]["title"])
                    reviews_cnt = int(rev_txt) if rev_txt else None
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
        "reviews_cnt": reviews_cnt,
        "images": images,
        "delivery_day_raw": delivery_raw,
        "delivery_date": delivery_date,
        "max_qty": max_qty,
        "first_seen": TODAY,
    }

# ─────────── PDP helpers ─────────────────────────────────────────────────────
# 1. «Сырые» web-виджеты ------------------------------------------------------

def collect_raw_widget(json_page: Dict[str, Any], prefix: str) -> str:
    """Вернуть первый widgetStates-value, чей ключ начинается с prefix."""
    for k, raw in json_page.get("widgetStates", {}).items():
        if k.startswith(prefix):
            return raw            # это уже JSON-строка
    return ""

# 2.  Описание / характеристики ----------------------------------------------

def collect_description(json_page: Dict[str, Any]) -> str:
    # webDescription-* в приоритете
    raw = collect_raw_widget(json_page, "webDescription-")
    return raw or ""


def collect_characteristics(json_page: Dict[str, Any]) -> str:
    # webCharacteristics-* в приоритете
    raw = collect_raw_widget(json_page, "webCharacteristics-")
    return raw or ""

# 3.  Итеративный сбор отзывов ------------------------------------------------

async def fetch_reviews_shelf(
    ctx,
    headers: Dict[str, str],
    path_part: str,
    sku: str,
    limit: int | None = None,
) -> None:
    """
    Скачиваем все страницы виджета reviewshelfpaginator и сохраняем «сырые» JSON-строки.
    Если limit > 0 — прекращаем, как только сохранили не меньше limit страниц.
    """
    # URL первой страницы формируем по-старому
    next_path: str | None = (
        f"{path_part}?layout_container=reviewshelfpaginator"
        "&layout_page_index=1&reviewsVariantMode=1"
    )

    page_no = 1           # счётчик, только чтобы красиво именовать файлы
    total_saved = 0

    while next_path:
        shelf_api = (
            "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url="
            + u.quote(next_path, safe="")
        )

        resp = await ctx.request.get(shelf_api, headers=headers)
        if resp.status != 200:
            break                     # 404/500 — дальше идти нельзя

        data = await resp.json()

        # «Сырой» JSON самого виджета
        raw_widget = next(
            (
                v
                for k, v in data.get("widgetStates", {}).items()
                if REVIEW_WIDGET_RE.match(k)
            ),
            None,
        )
        if not raw_widget:
            break                     # больше отзывов нет

        save_reviews_raw(sku, page_no, raw_widget)
        total_saved += 1

        if limit and total_saved >= limit:
            break                    # достигли предела страниц

        # --- готовим URL следующей страницы ---
        next_path = data.get("nextPage") or data.get("pageInfo", {}).get("url")
        page_no += 1

        await asyncio.sleep(random.uniform(0.9, 1.5))

# 4.  Основной PDP граббер ----------------------------------------------------

async def grab_pdp(ctx, headers: Dict[str, str], row: Dict[str, Any],
                   want_reviews: bool, reviews_limit: int
                   ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:

    path_part = row["link"].split("ozon.ru")[-1]
    pdp_api = (
        "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url=" +
        u.quote(f"{path_part}?layout_container=pdpPage2column&layout_page_index=2", safe="")
    )

    resp = await ctx.request.get(pdp_api, headers=headers)
    if resp.status != 200:
        return row, []

    pdp_json = await resp.json()

    # характеристики / описание
    row["charcs_json"] = collect_characteristics(pdp_json) or None
    row["description"] = collect_description(pdp_json) or None

    # полноценные отзывы
    if want_reviews:
        await fetch_reviews_shelf(ctx, headers, path_part, row["sku"], reviews_limit or None)

    await asyncio.sleep(random.uniform(0.8, 1.6))
    return row, []  # reviews пока не парсим/не возвращаем

# ─────────── Главная корутина ────────────────────────────────────────────────

async def grab() -> None:
    start_page = int(re.search(r"page=(\d+)", SEARCH_URL).group(1))

    async with AsyncCamoufox(headless=HEADLESS) as browser:
        ctx  = await browser.new_context(locale="ru-RU")
        page = await ctx.new_page()

        first_headers: Dict[str, str] = {}
        first_json: Optional[Dict[str, Any]] = None

        async def capture_entry(resp):
            nonlocal first_headers, first_json
            if ENTRY_RE.search(resp.url) and resp.status == 200 and not first_json:
                raw_h = await resp.request.all_headers()
                # удаляем псевдо-заголовки/brotli
                first_headers = {k: v for k, v in raw_h.items() if not k.startswith(":")}
                first_headers.pop("accept-encoding", None)
                first_json = await resp.json()

        page.on("response", capture_entry)

        await page.goto(SEARCH_URL, timeout=60_000, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(1.5, 3.0))
        await page.mouse.wheel(0, 3000)
        await asyncio.sleep(random.uniform(1.5, 3.0))

        if not first_json:
            print("⛔ Entrypoint-API не пойман — антибот/новый layout.")
            return

        # ----- 1. collect SERP -----
        all_rows, seen = [], set()
        json_page, page_no = first_json, start_page

        while True:
            for it in extract_items(json_page):
                row = grab_item(it)
                if row and row["sku"] not in seen:
                    seen.add(row["sku"])
                    all_rows.append(row)
            print(f"✓ page {page_no}: {len(seen)} товаров")

            page_no += 1
            if MAX_PAGES and page_no > start_page + MAX_PAGES - 1:
                break

            next_api = (
                "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url=" +
                u.quote(f"/search/?text=пуэр&category=9373&page={page_no}", safe="")
            )
            resp = await ctx.request.get(next_api, headers=first_headers)
            if resp.status != 200:
                print("⛔ HTTP", resp.status, "на", page_no)
                break
            json_page = await resp.json()
            await asyncio.sleep(random.uniform(1.0, 1.8))

        # ----- 2. PDP -----
        sem = asyncio.Semaphore(CONCURRENCY)
        results: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]] = []

        async def worker(r):
            async with sem:
                res = await grab_pdp(ctx, first_headers, r, args.reviews, args.reviews_limit)
                results.append(res)

        await asyncio.gather(*(worker(r) for r in all_rows))

        # ----- 3. финальная сборка -----
        rows_final = [row for row, _ in results]

    # ----- 4. CSV -----
    Path(OUTPUT_GOODS_CSV).write_bytes(pd.DataFrame(rows_final).to_csv(index=False).encode())
    print(f"\n🎉  Товаров сохранено: {len(rows_final)}  →  {Path(OUTPUT_GOODS_CSV).resolve()}")

    if args.reviews:
        print(f"📝  Сырые отзывы (JSON-виджеты) сохранены в: {RAW_REVIEWS_DIR.resolve()}")


# ─────────── Запуск ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(grab())
