from __future__ import annotations

import asyncio
import random
from typing import Dict

from camoufox.async_api import AsyncCamoufox
from fake_useragent import UserAgent

UA = UserAgent().random

async def safe_goto(page, url, *, timeout_ms=60_000) -> bool:
    await page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
    await asyncio.sleep(random.uniform(3, 5))       
    return not (await page.title()).lower().startswith("antibot")

async def ozon_ping(timeout_ms: int = 60_000) -> Dict:
    async with AsyncCamoufox(headless=True) as browser:
        ctx = await browser.new_context(
            locale="ru-RU",
            user_agent=UA,
            viewport={"width": 1366, "height": 768},
            extra_http_headers={"Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"},
        )
        page = await ctx.new_page()

        try:
            ok = await safe_goto(page, "https://www.ozon.ru/search/?text=пуэр", timeout_ms=timeout_ms)
            if not ok:
                return {"status": "error", "error": "Cloudflare challenge on main page"}

            title = await page.title()
            url   = page.url
            # html  = (await page.content()).lower()
            # geo_blocked = any(t in html for t in ("выберите ваш город", "choose your city"))

        finally:
            try:
                await page.close()
            except Exception:
                pass
            try:
                await ctx.close()
            except Exception:
                pass

    return {
        "status": "success",
        "title": title,
        "url": url,
    }
