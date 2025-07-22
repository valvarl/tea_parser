"""Высоко-уровневый скрейпер Ozon, предназначенный для фоновой
задачи `scrape_tea_products_task` и отладочных энд-пойнтов.

⚠️  Обратите внимание
---------------------
* Весь «тяжёлый» код Playwright оставлен без изменений, лишь
  скорректированы импорты и вызовы классификатора чая.
* Капчей и прокси управляем через utils.captcha / utils.proxy.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
import uuid
from datetime import datetime
from typing import Dict, List
from urllib.parse import quote, urljoin

from fake_useragent import UserAgent
from playwright.async_api import async_playwright
from playwright_stealth import Stealth  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# Внутренние импорты проекта
# ──────────────────────────────────────────────────────────────────────────────

from app.utils.proxy import proxy_pool
from app.utils.captcha import captcha_solver
from app.utils.classify import classify_tea_type
from app.core.logging import configure_logging

# инициализируем логгер
logger = logging.getLogger(__name__)
configure_logging()


ua = UserAgent()  # глобальный генератор user-agent'ов


# ──────────────────────────────────────────────────────────────────────────────
# Класс-скрейпер
# ──────────────────────────────────────────────────────────────────────────────

class OzonScraper:
    def __init__(self) -> None:
        self.base_url = "https://www.ozon.ru"
        self.search_url = "https://www.ozon.ru/search/"
        self.api_url = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2"
        self.graphql_url = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2"
        self.composer_url = "https://www.ozon.ru/api/composer-api.bx/page/json/v2"

        self.browser = None
        self.page = None
        self.playwright = None

        self.request_count = 0
        self.captcha_encounters = 0
        self.debug_mode = True

        self.rns_uuid: str | None = None
        self.csrf_token: str | None = None
        self.cookies: Dict[str, str] = {}

        # Russian region settings (Moscow)
        self.region_settings = {
            "ozon_regions": "213000000",  # Moscow region code
            "geo_region": "Moscow",
            "timezone": "Europe/Moscow",
            "accept_language": "ru-RU,ru;q=0.9,en;q=0.8",
        }

    # ------------------------------------------------------------------ #
    # Init / teardown
    # ------------------------------------------------------------------ #

    async def init_browser(self) -> None:
        """Запускает Chromium+Playwright с «русскими» настройками."""
        self.playwright = await async_playwright().start()
        self.user_agent = ua.random

        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--disable-gpu",
                "--window-size=1920,1080",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor",
                "--lang=ru-RU",
            ],
        )

        context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=self.user_agent,
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            extra_http_headers={
                "Accept-Language": self.region_settings["accept_language"],
                "Accept": (
                    "text/html,application/xhtml+xml,"
                    "application/xml;q=0.9,image/webp,*/*;q=0.8"
                ),
                "DNT": "1",
                "Upgrade-Insecure-Requests": "1",
            },
        )

        # фикс региона «Москва»
        await context.add_cookies(
            [
                {
                    "name": "ozon_regions",
                    "value": self.region_settings["ozon_regions"],
                    "domain": ".ozon.ru",
                    "path": "/",
                }
            ]
        )

        # anti-bot JS-хаки
        await context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins',  { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages',{ get: () => ['ru-RU','ru','en-US','en'] });
            Object.defineProperty(Intl.DateTimeFormat.prototype, 'resolvedOptions', {
                value: () => ({ timeZone: 'Europe/Moscow' })
            });
            """
        )

        self.page = await context.new_page()

        # прокси, если настроен
        proxy = proxy_pool.get_proxy()
        if proxy:
            logger.info("Using proxy: %s", proxy)

        await self.initialize_session()

    async def close_browser(self) -> None:
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def initialize_session(self) -> None:
        """Получение rns_uuid, csrf-токена и куков."""
        try:
            logger.info("Initializing Ozon session…")
            await self.page.goto(self.base_url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(2, 4))

            for cookie in await self.page.context.cookies():
                self.cookies[cookie["name"]] = cookie["value"]
                match cookie["name"]:
                    case "__Secure-rns_uuid":
                        self.rns_uuid = cookie["value"]
                    case "guest":
                        self.csrf_token = cookie["value"]

            # если сайт требует выбрать город — выбираем Москву
            if "выберите ваш город" in (await self.page.content()).lower():
                await self.set_moscow_region()

            logger.info("Session initialized successfully")
        except Exception as exc:  # pragma: no cover
            logger.exception("Error initializing session: %s", exc)

    async def set_moscow_region(self) -> None:
        try:
            region_btn = await self.page.query_selector("[data-widget='regionSelector']")
            if region_btn:
                await region_btn.click()
                await asyncio.sleep(1)
                moscow = await self.page.query_selector("text=Москва")
                if moscow:
                    await moscow.click()
                    await asyncio.sleep(2)
                    logger.info("Moscow region set")
        except Exception as exc:  # pragma: no cover
            logger.warning("Error setting Moscow region: %s", exc)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    async def handle_anti_bot(self, html: str) -> bool:
        """Обнаруживает Cloudflare / капчу, пытается решить или ждёт."""
        symptoms = (
            "cloudflare",
            "just a moment",
            "checking your browser",
            "captcha",
            "verify you are human",
            "robot",
        )
        if not any(x in html.lower() for x in symptoms):
            return True

        self.captcha_encounters += 1
        logger.warning("Anti-bot detected (%d)", self.captcha_encounters)

        if "captcha" in html.lower():
            # в проде берём site_key со страницы
            res = await captcha_solver.solve_captcha("dummy_key", self.page.url)
            if res["success"]:
                logger.info("Captcha solved via %s", res["service"])
                await asyncio.sleep(5)
                return True

        await asyncio.sleep(random.uniform(30, 60))
        return False

    # ------------------------------------------------------------------ #
    # Поиск товаров (entrypoint API → HTML fallback)
    # ------------------------------------------------------------------ #

    async def search_products(
        self,
        query: str,
        max_pages: int = 5,
    ) -> List[Dict]:
        """Главная точка входа: ищем товары, возвращаем list[dict]."""
        try:
            logger.info("Using entrypoint API interception scraper")
            data = await self.search_products_entrypoint(query, max_pages)
            if data:
                return data

            logger.info("Fallback to HTML scraper")
            return await self.search_products_html(query, max_pages)
        except Exception as exc:  # pragma: no cover
            logger.exception("search_products error: %s", exc)
            return []

    # ---------- entrypoint API (stealth) ---------- #

    async def search_products_entrypoint(
        self,
        query: str,
        max_pages: int = 10,
    ) -> List[Dict]:
        """Перехватываем запросы /api/entrypoint-api.bx/... в реальном браузере."""
        search_url = f"{self.search_url}?text={query}&category=9373&page=1"

        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(headless=False)
            ctx = await browser.new_context(locale="ru-RU")
            page = await ctx.new_page()

            first_headers, first_json = {}, None

            async def save_first(resp):
                nonlocal first_headers, first_json
                if (
                    re.search(
                        r"/api/entrypoint-api\.bx/page/json/v2\?url=%2Fsearch%2F",
                        resp.url,
                    )
                    and resp.status == 200
                    and not first_json
                ):
                    first_headers = await resp.request.all_headers()
                    first_json = await resp.json()

            page.on("response", save_first)
            await page.goto(search_url, timeout=60_000)
            await page.mouse.wheel(0, 4000)
            await asyncio.sleep(5)

            if not first_json:
                logger.warning("Entry API response not captured")
                await browser.close()
                return []

            api_base = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2"
            all_items, json_page, page_no = [], first_json, 1

            while True:
                try:
                    grid_key = next(
                        k
                        for k, v in json_page["widgetStates"].items()
                        if '"items":[' in v
                    )
                    grid = json.loads(json_page["widgetStates"][grid_key])
                    all_items.extend(grid.get("items", []))
                except Exception as exc:  # pragma: no cover
                    logger.error("Failed to parse grid: %s", exc)
                    break

                next_path = (
                    grid.get("nextUrl")
                    or grid.get("pageInfo", {}).get("nextUrl")
                    or grid.get("pagination", {}).get("nextUrl")
                )
                if not next_path:
                    break

                page_no += 1
                if max_pages and page_no > max_pages:
                    break

                next_api = f"{api_base}?url={quote(next_path, safe='')}"
                resp = await ctx.request.get(next_api, headers=first_headers)
                json_page = await resp.json()

            await browser.close()

        products = []
        for item in all_items:
            try:
                name = next(
                    b for b in item.get("mainState", []) if b.get("type") == "textAtom"
                )["textAtom"]["text"]
                price_blk = next(
                    b for b in item.get("mainState", []) if b.get("type") == "priceV2"
                )["priceV2"]["price"]
                price_text = next(
                    t["text"] for t in price_blk if t.get("textStyle") == "PRICE"
                ).replace("\u2009", " ")
                url = urljoin(self.base_url, item.get("action", {}).get("link", ""))
                pd = {"name": name, "price": price_text, "product_url": url}
                pd.update(classify_tea_type(name))
                products.append(pd)
            except Exception:  # pragma: no cover
                continue

        return products

    # ---------- чистый HTML-парсинг (fallback) ---------- #
    # (методы search_products_html, extract_products_from_page и др.
    #   оставлены без изменений, см. полный исходник)                      #
    # ------------------------------------------------------------------ #

    # ... здесь остаются методы search_products_html, extract_products_from_page,
    #     extract_product_data, save_debug_html, check_for_no_products,
    #     scrape_product_details и т.д.  (с вашими исходными реализациями) ...

    # ------------------------------------------------------------------ #
    # Вспомогательные: классификатор
    # ------------------------------------------------------------------ #

    @staticmethod
    def classify_tea_type(name: str) -> Dict:
        """Просто обёртка над utils.classify.classify_tea_type."""
        return classify_tea_type(name)


# ──────────────────────────────────────────────────────────────────────────────
# Глобальный экземпляр, используемый по всему проекту
# ──────────────────────────────────────────────────────────────────────────────

scraper = OzonScraper()