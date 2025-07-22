from fastapi import FastAPI, APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime
import asyncio
import json
import random
import time
from fake_useragent import UserAgent
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
import httpx
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse, quote
import base64
import hmac, hashlib, time, base64
import socket
import requests

SECRET = "Tz178Fksyu4oAs14"

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Create the main app without a prefix
app = FastAPI(title="Chinese Tea Scraper API", version="1.0.0")

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_public_ip():
    try:
        return requests.get("https://api.ipify.org").text
    except:
        return "unknown"

logger.warning(f"⚠️ Current IP: {get_public_ip()}")


# User agent rotation
ua = UserAgent()

# Captcha solving services configuration (stubbed for now)
CAPTCHA_SERVICES = {
    "capsolver": {
        "api_key": os.environ.get("CAPSOLVER_API_KEY", "stubbed_key"),
        "client_key": os.environ.get("CAPSOLVER_CLIENT_KEY", "stubbed_key"),
        "endpoint": "https://api.capsolver.com/createTask"
    },
    "2captcha": {
        "api_key": os.environ.get("TWOCAPTCHA_API_KEY", "stubbed_key"),
        "endpoint": "https://2captcha.com/in.php"
    }
}

# Proxy configuration (stubbed)
PROXY_POOL = os.environ.get("PROXY_POOL", "").split(",") if os.environ.get("PROXY_POOL") else []

# Tea search keywords - comprehensive list
TEA_KEYWORDS = {
    "base_terms": [
        "пуэр", "пуер", "pu-erh", "pu erh", "puer",
        "китайский чай", "chinese tea", "чай китай",
        "чёрный чай", "черный чай", "black tea",
        "зелёный чай", "зеленый чай", "green tea",
        "улун", "оолонг", "oolong",
        "белый чай", "white tea",
        "дахунпао", "да хун пао", "da hong pao",
        "лунцзин", "longjing", "dragon well",
        "те гуань инь", "tie guan yin", "tieguanyin"
    ],
    "forms": [
        "блин", "блинчик", "плитка", "таблетка",
        "прессованный", "рассыпной", "листовой",
        "cake", "brick", "тuo", "то ча"
    ],
    "regions": [
        "юннань", "yunnan", "фуцзянь", "fujian",
        "аньхой", "anhui", "чжэцзян", "zhejiang",
        "гуанси", "guangxi", "гуандун", "guangdong"
    ],
    "grades": [
        "шэн", "шен", "sheng", "сырой",
        "шу", "shu", "shou", "готовый",
        "молодой", "выдержанный", "aged"
    ],
    "years": [str(year) for year in range(2010, 2025)]
}

# Pydantic models








class OzonScraper:
    def __init__(self):
        self.base_url = "https://www.ozon.ru"
        self.search_url = "https://www.ozon.ru/search/"
        self.api_url = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2"
        self.graphql_url = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2"
        self.composer_url = "https://www.ozon.ru/api/composer-api.bx/page/json/v2"
        self.session = None
        self.browser = None
        self.page = None
        self.request_count = 0
        self.captcha_encounters = 0
        self.debug_mode = True
        self.rns_uuid = None
        self.csrf_token = None
        self.cookies = {}
        
        # Russian region settings (Moscow)
        self.region_settings = {
            "ozon_regions": "213000000",  # Moscow region code
            "geo_region": "Moscow",
            "timezone": "Europe/Moscow",
            "accept_language": "ru-RU,ru;q=0.9,en;q=0.8"
        }
        
    async def init_browser(self):
        """Initialize Playwright browser with stealth settings and Russian region"""
        self.playwright = await async_playwright().start()

        self.user_agent = ua.random
        
        # Browser configuration for stealth with Russian locale
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--disable-gpu',
                '--window-size=1920,1080',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--lang=ru-RU'
            ]
        )
        
        # Create context with Russian settings
        context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent=self.user_agent,
            locale='ru-RU',
            timezone_id='Europe/Moscow',
            extra_http_headers={
                'Accept-Language': self.region_settings["accept_language"],
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
        )
        
        # Set Russian region cookie
        await context.add_cookies([
            {
                'name': 'ozon_regions',
                'value': self.region_settings["ozon_regions"],
                'domain': '.ozon.ru',
                'path': '/'
            }
        ])
        
        # Add stealth scripts
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            
            Object.defineProperty(navigator, 'languages', {
                get: () => ['ru-RU', 'ru', 'en-US', 'en'],
            });
            
            // Mock timezone
            Object.defineProperty(Intl.DateTimeFormat.prototype, 'resolvedOptions', {
                value: function() {
                    return { timeZone: 'Europe/Moscow' };
                }
            });
        """)
        
        self.page = await context.new_page()
        
        # Set proxy if available
        proxy = proxy_pool.get_proxy()
        if proxy:
            logger.info(f"Using proxy: {proxy}")
            
        # Initialize session by visiting main page and getting tokens
        await self.initialize_session()
    
    async def initialize_session(self):
        """Initialize session with proper cookies and tokens"""
        try:
            logger.info("Initializing Ozon session...")
            
            # Visit main page to get cookies and tokens
            await self.page.goto(self.base_url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(2, 4))
            
            # Get cookies from browser
            cookies = await self.page.context.cookies()
            
            # Extract important tokens
            for cookie in cookies:
                if cookie['name'] == '__Secure-rns_uuid':
                    self.rns_uuid = cookie['value']
                    logger.info(f"Got rns_uuid: {self.rns_uuid[:20]}...")
                elif cookie['name'] == 'guest':
                    self.csrf_token = cookie['value']
                    logger.info(f"Got CSRF token: {self.csrf_token[:20]}...")
                elif cookie['name'] == 'device_uid':
                    self.device_uid = cookie['value']
                
                self.cookies[cookie['name']] = cookie['value']
            
            if not hasattr(self, "device_uid"):
                self.device_uid = str(uuid.uuid4())
            
            # Check if region selection is required
            content = await self.page.content()
            if "выберите ваш город" in content.lower() or "choose your city" in content.lower():
                logger.warning("Region selection required - attempting to set Moscow")
                await self.set_moscow_region()
                
            logger.info("Session initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing session: {e}")
    
    async def set_moscow_region(self):
        """Set Moscow as the region"""
        try:
            # Look for region selector
            region_button = await self.page.query_selector("[data-widget='regionSelector']")
            if region_button:
                await region_button.click()
                await asyncio.sleep(1)
                
                # Look for Moscow option
                moscow_option = await self.page.query_selector("text=Москва")
                if moscow_option:
                    await moscow_option.click()
                    await asyncio.sleep(2)
                    logger.info("Moscow region set successfully")
                    
        except Exception as e:
            logger.error(f"Error setting Moscow region: {e}")
    
    async def debug_log_request(self, url: str, method: str, payload: dict = None, response: dict = None):
        """Log detailed request/response for debugging"""
        if self.debug_mode:
            logger.info(f"🔍 DEBUG REQUEST: {method} {url}")
            if payload:
                logger.info(f"📤 Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")
            if response:
                logger.info(f"📥 Response: {json.dumps(response, indent=2, ensure_ascii=False)[:1000]}...")
    
    
    async def close_browser(self):
        """Close browser and cleanup"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
    
    async def handle_anti_bot(self, page_content: str) -> bool:
        """Detect and handle anti-bot measures"""
        # Check for common anti-bot indicators
        anti_bot_indicators = [
            "cloudflare",
            "just a moment",
            "checking your browser",
            "captcha",
            "verify you are human",
            "robot"
        ]
        
        page_lower = page_content.lower()
        detected = any(indicator in page_lower for indicator in anti_bot_indicators)
        
        if detected:
            self.captcha_encounters += 1
            logger.warning(f"Anti-bot measure detected (encounter #{self.captcha_encounters})")
            
            # Try to solve captcha
            if "captcha" in page_lower:
                # Extract captcha details (simplified)
                site_key = "stubbed_site_key"  # In production, extract from page
                result = await captcha_solver.solve_captcha(site_key, self.page.url)
                
                if result["success"]:
                    logger.info(f"Captcha solved using {result['service']}")
                    # In production, submit solution and wait for redirect
                    await asyncio.sleep(5)
                    return True
            
            # Wait and retry
            await asyncio.sleep(random.uniform(30, 60))
            return False
        
        return True
    
    async def search_products(self, query: str, max_pages: int = 5) -> List[Dict]:
        """Search for tea products on Ozon with improved data extraction"""
        products = []
        
        try:
           
            # Try entrypoint API interception if API search fails
            logger.info("Trying entrypoint API interception scraper")
            entry_products = await self.search_products_entrypoint(query, max_pages)
            if entry_products:
                logger.info(f"Entrypoint scraper found {len(entry_products)} products")
                return entry_products

            # Fallback to HTML scraping if all else fails
            logger.info("Falling back to HTML scraping")
            return await self.search_products_html(query, max_pages)
            
        except Exception as e:
            logger.error(f"Error in search_products: {e}")
            entry_products = await self.search_products_entrypoint(query, max_pages)
            if entry_products:
                logger.info(f"Entrypoint scraper recovered {len(entry_products)} products after error")
                return entry_products
            
            # Try HTML scraping as final fallback
            return await self.search_products_html(query, max_pages)
    
    async def get_tea_category_id(self) -> str:
        """Get category ID for tea products"""
        try:
            # Navigate to tea category page
            tea_url = "https://www.ozon.ru/category/chay-9373/"
            await self.page.goto(tea_url, wait_until="domcontentloaded")
            await asyncio.sleep(2)
            
            # Extract category ID from URL or page data
            current_url = self.page.url
            category_match = re.search(r'category/.*?-(\d+)/', current_url)
            if category_match:
                category_id = category_match.group(1)
                logger.info(f"Found tea category ID: {category_id}")
                return category_id
            
            # Try to find it in page data
            content = await self.page.content()
            category_match = re.search(r'"categoryId":"(\d+)"', content)
            if category_match:
                category_id = category_match.group(1)
                logger.info(f"Extracted tea category ID from page: {category_id}")
                return category_id
            
        except Exception as e:
            logger.error(f"Error getting tea category ID: {e}")
        
        return "9373"  # Default tea category ID
    
    async def search_products_entrypoint(self, query: str, max_pages: int = 10) -> List[Dict]:
        """Scrape products using entrypoint API interception (fallback)."""
        search_url = f"{self.search_url}?text={query}&category=9373&page=1"

        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(headless=False)
            ctx = await browser.new_context(locale="ru-RU")
            page = await ctx.new_page()

            first_headers, first_json = {}, None

            async def save_first(resp):
                nonlocal first_headers, first_json
                if re.search(r"/api/entrypoint-api\.bx/page/json/v2\?url=%2Fsearch%2F", resp.url) and resp.status == 200 and not first_json:
                    first_headers = await resp.request.all_headers()
                    first_json = await resp.json()

            page.on("response", save_first)
            await page.goto(search_url, timeout=60000)
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
                    grid_key = next(k for k, v in json_page["widgetStates"].items() if '"items":[' in v)
                    grid = json.loads(json_page["widgetStates"][grid_key])
                    all_items.extend(grid.get("items", []))
                except Exception as e:
                    logger.error(f"Failed to parse grid: {e}")
                    break

                if "nextUrl" in grid:
                    next_path = grid["nextUrl"]
                elif "pageInfo" in grid and "nextUrl" in grid["pageInfo"]:
                    next_path = grid["pageInfo"]["nextUrl"]
                elif "pagination" in grid and "nextUrl" in grid["pagination"]:
                    next_path = grid["pagination"]["nextUrl"]
                else:
                    break

                page_no += 1
                if max_pages and page_no > max_pages:
                    break

                next_api = f"{api_base}?url={quote(next_path, safe='')}"
                resp = await ctx.request.get(next_api, headers=first_headers)
                json_page = await resp.json()

            await browser.close()

        products = []
        for it in all_items:
            print(it)
            try:
                name = next(b for b in it.get("mainState", []) if b.get("type") == "textAtom")["textAtom"]["text"]
                price_blk = next(b for b in it.get("mainState", []) if b.get("type") == "priceV2")["priceV2"]["price"]
                price_text = next(t["text"] for t in price_blk if t.get("textStyle") == "PRICE").replace("\u2009", " ")
                url = urljoin(self.base_url, it.get("action", {}).get("link", ""))
                products.append({"name": name, "price": price_text, "product_url": url})
            except Exception:
                continue

        return products
    

# Global scraper instance
scraper = OzonScraper()

# Search query generator


# Background task for scraping




@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
