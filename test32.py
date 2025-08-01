#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
camou_state_dump.py
~~~~~~~~~~~~~~~~~~~
Выгружает любые <div id="state-…"> с data-state из страниц Ozon
(или любого SPA) через AsyncCamoufox: stealth-Chromium + анти-бот-обход.
"""

import argparse, asyncio, json, re, sys
from pathlib import Path
from typing import List, Dict, Any

from camoufox import AsyncCamoufox   #  ←  Camoufox!
from playwright.async_api import BrowserContext, Page


# ─────────── JS-сниппет для выборки блоков ────────────────────────────────
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


# ─────────── приёмные корутины ─────────────────────────────────────────────
async def gentle_scroll(page: Page, steps: int = 6, pause: float = .8):
    h = await page.evaluate("()=>document.body.scrollHeight")
    for _ in range(steps):
        await page.mouse.wheel(0, h // steps)
        await asyncio.sleep(pause)


async def fetch_states(ctx: BrowserContext,
                       url: str,
                       patterns: List[str],
                       regex: bool,
                       wait: int) -> Dict[str, Any]:
    page = await ctx.new_page()
    print("⇢ Открываю:", url)
    await page.goto(url, timeout=60_000, wait_until="domcontentloaded")
    await asyncio.sleep(1.5)
    await gentle_scroll(page)
    await asyncio.sleep(1.0)

    try:
        # ⬇ 1. ждём ПЕРВОЕ появление любого state-div (вне зависимости от видимости)
        await page.wait_for_selector("div[data-state]", state="attached", timeout=15_000)

        # ⬇ 2. перестраховка: даём Nuxt/React «додуть» всё, что хочет
        await page.wait_for_function(
            "document.querySelectorAll('div[data-state]').length > 0", timeout=0
        )
        #   timeout=0  →   функция ждёт «сколько надо», пока условие станет True
    except Exception:
        print(f"⚠  div[data-state] не появился за {wait/1000:.0f} с")

    payload = {"ids": patterns, "useRegex": regex}
    return await page.evaluate(_JS_PULL, payload)


# ─────────── CLI ───────────────────────────────────────────────────────────
def cli() -> argparse.Namespace:
    p = argparse.ArgumentParser("Camoufox Ozon state dumper")
    p.add_argument("url")
    p.add_argument("-s", "--state", action="append",
                   help="ID/подстрока/RegExp блока state-…  (можно несколько)")
    p.add_argument("--regex", action="store_true",
                   help="Трактовать --state как RegExp")
    p.add_argument("-o", "--out", default="states.json",
                   help="Имя файла JSON (по умолчанию states.json)")
    p.add_argument("--headed", action="store_true",
                   help="Запуск с UI-браузером (по умолч. headless)")
    p.add_argument("--wait", type=int, default=7000,
                   help="Ожидание (мс) появления data-state (7000)")
    return p.parse_args()


# ─────────── запуск ────────────────────────────────────────────────────────
async def main() -> None:
    args = cli()
    async with AsyncCamoufox(headless=not args.headed) as browser:
        ctx = await browser.new_context(locale="ru-RU")
        data = await fetch_states(ctx, args.url,
                                  args.state or [], args.regex, args.wait)

    if not data:
        print("⛔  Ничего не вытянулось — проверьте фильтры/URL/таймаут.")
        return

    Path(args.out).write_text(json.dumps(data, ensure_ascii=False, indent=2))
    print(f"✅  Сохранено: {len(data)} блок(ов) → {Path(args.out).resolve()}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit("⏹  Прервано пользователем")
