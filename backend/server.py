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
import httpx
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
import base64

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
class TeaProduct(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    ozon_id: Optional[str] = None
    sku_id: Optional[str] = None
    name: str
    price: Optional[float] = None
    original_price: Optional[float] = None
    discount: Optional[int] = None
    rating: Optional[float] = None
    reviews_count: Optional[int] = None
    description: Optional[str] = None
    characteristics: Optional[Dict[str, Any]] = None
    images: List[str] = []
    category_path: Optional[str] = None
    category_id: Optional[str] = None
    brand: Optional[str] = None
    seller: Optional[str] = None
    availability: Optional[str] = None
    delivery_info: Optional[str] = None
    weight: Optional[str] = None
    tea_type: Optional[str] = None
    tea_region: Optional[str] = None
    tea_year: Optional[str] = None
    tea_grade: Optional[str] = None
    is_pressed: Optional[bool] = None
    raw_data: Optional[Dict[str, Any]] = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

class TeaProductCreate(BaseModel):
    name: str
    price: Optional[float] = None
    description: Optional[str] = None

class TeaReview(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    product_id: str
    ozon_review_id: Optional[str] = None
    author: Optional[str] = None
    author_id: Optional[str] = None
    rating: Optional[int] = None
    title: Optional[str] = None
    text: Optional[str] = None
    pros: Optional[str] = None
    cons: Optional[str] = None
    photos: List[str] = []
    helpful_count: Optional[int] = None
    review_date: Optional[datetime] = None
    verified_purchase: Optional[bool] = None
    seller_response: Optional[str] = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)

class ScrapingTask(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    search_term: str
    status: str = "pending"  # pending, running, completed, failed
    total_products: int = 0
    scraped_products: int = 0
    failed_products: int = 0
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

class ScrapingStats(BaseModel):
    total_tasks: int = 0
    running_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    total_products: int = 0
    captcha_solves: int = 0
    proxy_switches: int = 0
    error_rate: float = 0.0

class ProxyPool:
    def __init__(self, proxies: List[str] = None):
        self.proxies = proxies or []
        self.current_index = 0
        self.failed_proxies = set()
        
    def get_proxy(self):
        if not self.proxies:
            return None
        
        available_proxies = [p for i, p in enumerate(self.proxies) if i not in self.failed_proxies]
        if not available_proxies:
            # Reset failed proxies if all are failed
            self.failed_proxies.clear()
            available_proxies = self.proxies
        
        proxy = available_proxies[self.current_index % len(available_proxies)]
        self.current_index += 1
        return proxy
    
    def mark_failed(self, proxy: str):
        if proxy in self.proxies:
            self.failed_proxies.add(self.proxies.index(proxy))

# Global proxy pool
proxy_pool = ProxyPool(PROXY_POOL)

class CaptchaSolver:
    def __init__(self):
        self.services = CAPTCHA_SERVICES
        self.solve_count = 0
    
    async def solve_captcha(self, site_key: str, page_url: str, captcha_type: str = "hcaptcha"):
        """Solve captcha using available services (stubbed implementation)"""
        self.solve_count += 1
        
        # Stubbed implementation - in production this would make actual API calls
        logger.info(f"Solving {captcha_type} captcha for {page_url}")
        
        # Simulate solving time
        await asyncio.sleep(random.uniform(10, 30))
        
        # Return a stubbed response
        return {
            "success": True,
            "solution": "stubbed_solution_token",
            "service": "capsolver" if random.choice([True, False]) else "2captcha"
        }

captcha_solver = CaptchaSolver()

class OzonScraper:
    def __init__(self):
        self.base_url = "https://www.ozon.ru"
        self.search_url = "https://www.ozon.ru/search/"
        self.api_url = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2"
        self.graphql_url = "https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2"
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
            user_agent=ua.random,
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
                
                self.cookies[cookie['name']] = cookie['value']
            
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
    
    async def search_products_api(self, query: str, category_id: str = None, page: int = 1) -> Dict:
        """Search products using Ozon API with proper authentication"""
        try:
            # Modern Ozon GraphQL search payload
            payload = {
                "operationName": "searchProducts",
                "variables": {
                    "query": query,
                    "page": page,
                    "categoryId": category_id or "",
                    "sort": "score",
                    "filters": [],
                    "withPromo": True,
                    "withInstallment": True,
                    "withPremium": True
                },
                "query": '''
                    query searchProducts($query: String!, $page: Int!, $categoryId: String, $sort: String, $filters: [FilterInput!], $withPromo: Boolean, $withInstallment: Boolean, $withPremium: Boolean) {
                        searchProducts(
                            query: $query
                            page: $page
                            categoryId: $categoryId
                            sort: $sort
                            filters: $filters
                            withPromo: $withPromo
                            withInstallment: $withInstallment
                            withPremium: $withPremium
                        ) {
                            products {
                                id
                                title
                                price {
                                    original
                                    current
                                }
                                rating {
                                    value
                                    count
                                }
                                images {
                                    original
                                    thumbnail
                                }
                                url
                                seller {
                                    name
                                }
                                categoryId
                                availability
                                delivery {
                                    text
                                }
                                attributes {
                                    name
                                    value
                                }
                            }
                            totalCount
                            hasNextPage
                        }
                    }
                '''
            }
            
            # Headers with authentication
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': self.region_settings["accept_language"],
                'User-Agent': ua.random,
                'Referer': f'https://www.ozon.ru/search/?text={query}',
                'Origin': 'https://www.ozon.ru',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
            }
            
            # Add authentication headers
            if self.rns_uuid:
                headers['x-o3-rns_uuid'] = self.rns_uuid
            if self.csrf_token:
                headers['x-o3-csrf-token'] = self.csrf_token
            
            # Make API request
            response = await self.page.evaluate(f'''
                async () => {{
                    const response = await fetch('{self.graphql_url}', {{
                        method: 'POST',
                        headers: {json.dumps(headers)},
                        body: JSON.stringify({json.dumps(payload)})
                    }});
                    return await response.json();
                }}
            ''')
            
            await self.debug_log_request(self.graphql_url, "POST", payload, response)
            
            return response
            
        except Exception as e:
            logger.error(f"Error in API search: {e}")
            return {}
    
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
        """Search for tea products on Ozon"""
        products = []
        
        try:
            # Navigate to search page
            search_url = f"{self.search_url}?text={query}&category_id=&type=&page=1"
            await self.page.goto(search_url, wait_until="domcontentloaded")
            
            # Handle initial anti-bot check
            content = await self.page.content()
            if not await self.handle_anti_bot(content):
                logger.error("Failed to bypass anti-bot measures")
                return products
            
            # Wait for products to load
            await asyncio.sleep(random.uniform(2, 5))
            
            # Extract products from current page
            page_products = await self.extract_products_from_page()
            products.extend(page_products)
            
            # Navigate through additional pages
            for page_num in range(2, max_pages + 1):
                try:
                    # Navigate to next page
                    next_url = f"{self.search_url}?text={query}&category_id=&type=&page={page_num}"
                    await self.page.goto(next_url, wait_until="domcontentloaded")
                    
                    # Random delay
                    await asyncio.sleep(random.uniform(3, 7))
                    
                    # Extract products
                    page_products = await self.extract_products_from_page()
                    if not page_products:
                        break
                    
                    products.extend(page_products)
                    
                    # Rate limiting
                    await asyncio.sleep(random.uniform(2, 5))
                    
                except Exception as e:
                    logger.error(f"Error scraping page {page_num}: {e}")
                    break
                    
        except Exception as e:
            logger.error(f"Error searching products: {e}")
        
        return products
    
    async def extract_products_from_page(self) -> List[Dict]:
        """Extract product information from current page"""
        products = []
        
        try:
            # Wait for products to load
            await self.page.wait_for_selector("[data-widget='searchResultsV2']", timeout=10000)
            
            # Extract product cards
            product_cards = await self.page.query_selector_all("[data-widget='searchResultsV2'] [data-test-id='tile-clickable-element']")
            
            for card in product_cards:
                try:
                    product_data = await self.extract_product_data(card)
                    if product_data:
                        products.append(product_data)
                except Exception as e:
                    logger.error(f"Error extracting product data: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting products from page: {e}")
        
        return products
    
    async def extract_product_data(self, card_element) -> Dict:
        """Extract individual product data from card element"""
        product_data = {}
        
        try:
            # Extract name
            name_element = await card_element.query_selector("[data-test-id='tile-name']")
            if name_element:
                product_data["name"] = await name_element.text_content()
            
            # Extract price
            price_element = await card_element.query_selector("[data-test-id='tile-price']")
            if price_element:
                price_text = await price_element.text_content()
                # Extract numeric price
                price_match = re.search(r'(\d+)', price_text.replace(' ', ''))
                if price_match:
                    product_data["price"] = float(price_match.group(1))
            
            # Extract rating
            rating_element = await card_element.query_selector("[data-test-id='tile-rating']")
            if rating_element:
                rating_text = await rating_element.text_content()
                rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                if rating_match:
                    product_data["rating"] = float(rating_match.group(1))
            
            # Extract reviews count
            reviews_element = await card_element.query_selector("[data-test-id='tile-review-count']")
            if reviews_element:
                reviews_text = await reviews_element.text_content()
                reviews_match = re.search(r'(\d+)', reviews_text)
                if reviews_match:
                    product_data["reviews_count"] = int(reviews_match.group(1))
            
            # Extract product URL
            link_element = await card_element.query_selector("a")
            if link_element:
                href = await link_element.get_attribute("href")
                if href:
                    product_data["product_url"] = urljoin(self.base_url, href)
                    # Extract product ID from URL
                    id_match = re.search(r'/product/.*?-(\d+)/', href)
                    if id_match:
                        product_data["ozon_id"] = id_match.group(1)
            
            # Extract image
            img_element = await card_element.query_selector("img")
            if img_element:
                img_src = await img_element.get_attribute("src")
                if img_src:
                    product_data["images"] = [img_src]
            
            # Classify tea type based on name
            if "name" in product_data:
                product_data.update(self.classify_tea_type(product_data["name"]))
            
            return product_data
            
        except Exception as e:
            logger.error(f"Error extracting product data: {e}")
            return {}
    
    def classify_tea_type(self, name: str) -> Dict:
        """Classify tea type based on product name"""
        name_lower = name.lower()
        classification = {}
        
        # Detect tea type
        if any(term in name_lower for term in ["пуэр", "пуер", "pu-erh", "pu erh", "puer"]):
            classification["tea_type"] = "пуэр"
        elif any(term in name_lower for term in ["улун", "оолонг", "oolong"]):
            classification["tea_type"] = "улун"
        elif any(term in name_lower for term in ["зелёный", "зеленый", "green"]):
            classification["tea_type"] = "зелёный"
        elif any(term in name_lower for term in ["чёрный", "черный", "black"]):
            classification["tea_type"] = "чёрный"
        elif any(term in name_lower for term in ["белый", "white"]):
            classification["tea_type"] = "белый"
        
        # Detect if pressed
        if any(term in name_lower for term in ["блин", "блинчик", "плитка", "прессованный", "cake", "brick"]):
            classification["is_pressed"] = True
        elif any(term in name_lower for term in ["рассыпной", "листовой", "loose"]):
            classification["is_pressed"] = False
        
        # Detect grade for puer
        if classification.get("tea_type") == "пуэр":
            if any(term in name_lower for term in ["шэн", "шен", "sheng", "сырой"]):
                classification["tea_grade"] = "шэн"
            elif any(term in name_lower for term in ["шу", "shu", "shou", "готовый"]):
                classification["tea_grade"] = "шу"
        
        # Detect region
        for region in TEA_KEYWORDS["regions"]:
            if region in name_lower:
                classification["tea_region"] = region
                break
        
        # Detect year
        for year in TEA_KEYWORDS["years"]:
            if year in name_lower:
                classification["tea_year"] = year
                break
        
        return classification
    
    async def scrape_product_details(self, product_url: str) -> Dict:
        """Scrape detailed product information"""
        try:
            await self.page.goto(product_url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(2, 4))
            
            # Handle anti-bot measures
            content = await self.page.content()
            if not await self.handle_anti_bot(content):
                return {}
            
            # Extract detailed information
            details = {}
            
            # Extract description
            desc_element = await self.page.query_selector("[data-widget='webProductDescription']")
            if desc_element:
                details["description"] = await desc_element.text_content()
            
            # Extract characteristics
            char_elements = await self.page.query_selector_all("[data-widget='webCharacteristics'] [data-test-id='property-row']")
            characteristics = {}
            
            for element in char_elements:
                key_element = await element.query_selector("[data-test-id='property-name']")
                value_element = await element.query_selector("[data-test-id='property-value']")
                
                if key_element and value_element:
                    key = await key_element.text_content()
                    value = await value_element.text_content()
                    characteristics[key.strip()] = value.strip()
            
            details["characteristics"] = characteristics
            
            # Extract additional images
            img_elements = await self.page.query_selector_all("[data-widget='webGallery'] img")
            images = []
            for img in img_elements:
                src = await img.get_attribute("src")
                if src:
                    images.append(src)
            details["images"] = images
            
            return details
            
        except Exception as e:
            logger.error(f"Error scraping product details: {e}")
            return {}

# Global scraper instance
scraper = OzonScraper()

# Search query generator
def generate_search_queries(limit: int = 50) -> List[str]:
    """Generate comprehensive search queries for tea products"""
    queries = []
    
    # Base queries
    queries.extend(TEA_KEYWORDS["base_terms"][:10])
    
    # Combinations
    for base_term in TEA_KEYWORDS["base_terms"][:5]:
        for form in TEA_KEYWORDS["forms"][:3]:
            queries.append(f"{base_term} {form}")
        
        for region in TEA_KEYWORDS["regions"][:3]:
            queries.append(f"{base_term} {region}")
    
    # Specific puer queries
    puer_queries = [
        "пуэр блин 357",
        "шэн пуэр",
        "шу пуэр",
        "пуэр юннань",
        "китайский пуэр",
        "пуэр чай блинчик",
        "пуэр плитка",
        "пуэр рассыпной"
    ]
    queries.extend(puer_queries)
    
    # Remove duplicates and limit
    unique_queries = list(set(queries))
    return unique_queries[:limit]

# Background task for scraping
async def scrape_tea_products_task(search_term: str, task_id: str):
    """Background task for scraping tea products"""
    
    # Update task status
    await db.scraping_tasks.update_one(
        {"id": task_id},
        {
            "$set": {
                "status": "running",
                "started_at": datetime.utcnow()
            }
        }
    )
    
    try:
        # Initialize browser
        await scraper.init_browser()
        
        # Search for products
        products = await scraper.search_products(search_term, max_pages=3)
        
        scraped_count = 0
        failed_count = 0
        
        for product_data in products:
            try:
                # Create tea product
                tea_product = TeaProduct(**product_data)
                
                # Check if product already exists
                existing = await db.tea_products.find_one({"ozon_id": tea_product.ozon_id})
                
                if existing:
                    # Update existing product
                    await db.tea_products.update_one(
                        {"ozon_id": tea_product.ozon_id},
                        {"$set": tea_product.dict()}
                    )
                else:
                    # Insert new product
                    await db.tea_products.insert_one(tea_product.dict())
                
                scraped_count += 1
                
                # Update task progress
                await db.scraping_tasks.update_one(
                    {"id": task_id},
                    {
                        "$set": {
                            "scraped_products": scraped_count,
                            "failed_products": failed_count
                        }
                    }
                )
                
            except Exception as e:
                logger.error(f"Error processing product: {e}")
                failed_count += 1
        
        # Update task completion
        await db.scraping_tasks.update_one(
            {"id": task_id},
            {
                "$set": {
                    "status": "completed",
                    "completed_at": datetime.utcnow(),
                    "total_products": len(products),
                    "scraped_products": scraped_count,
                    "failed_products": failed_count
                }
            }
        )
        
    except Exception as e:
        logger.error(f"Error in scraping task: {e}")
        
        # Update task failure
        await db.scraping_tasks.update_one(
            {"id": task_id},
            {
                "$set": {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "error_message": str(e)
                }
            }
        )
    
    finally:
        # Close browser
        await scraper.close_browser()

# API Routes
@api_router.get("/")
async def root():
    return {"message": "Chinese Tea Scraper API", "version": "1.0.0"}

@api_router.post("/scrape/start")
async def start_scraping(background_tasks: BackgroundTasks, search_term: str = "пуэр"):
    """Start scraping tea products"""
    
    # Create scraping task
    task = ScrapingTask(search_term=search_term)
    await db.scraping_tasks.insert_one(task.dict())
    
    # Start background task
    background_tasks.add_task(scrape_tea_products_task, search_term, task.id)
    
    return {"task_id": task.id, "status": "started", "search_term": search_term}

@api_router.get("/scrape/status/{task_id}")
async def get_scraping_status(task_id: str):
    """Get scraping task status"""
    task = await db.scraping_tasks.find_one({"id": task_id})
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Remove MongoDB ObjectId to avoid serialization issues
    if "_id" in task:
        del task["_id"]
    
    return task

@api_router.get("/scrape/tasks")
async def get_scraping_tasks():
    """Get all scraping tasks"""
    tasks = await db.scraping_tasks.find().sort("created_at", -1).to_list(100)
    
    # Remove MongoDB ObjectId to avoid serialization issues
    for task in tasks:
        if "_id" in task:
            del task["_id"]
    
    return tasks

@api_router.get("/products", response_model=List[TeaProduct])
async def get_tea_products(skip: int = 0, limit: int = 50, tea_type: Optional[str] = None):
    """Get tea products with optional filtering"""
    
    query = {}
    if tea_type:
        query["tea_type"] = tea_type
    
    products = await db.tea_products.find(query).skip(skip).limit(limit).sort("scraped_at", -1).to_list(limit)
    
    # Remove MongoDB ObjectId to avoid serialization issues
    for product in products:
        if "_id" in product:
            del product["_id"]
    
    return [TeaProduct(**product) for product in products]

@api_router.get("/products/{product_id}")
async def get_tea_product(product_id: str):
    """Get specific tea product"""
    product = await db.tea_products.find_one({"id": product_id})
    
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Remove MongoDB ObjectId to avoid serialization issues
    if "_id" in product:
        del product["_id"]
    
    return TeaProduct(**product)

@api_router.delete("/products/{product_id}")
async def delete_tea_product(product_id: str):
    """Delete tea product"""
    result = await db.tea_products.delete_one({"id": product_id})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Product not found")
    
    return {"message": "Product deleted successfully"}

@api_router.get("/search/queries")
async def get_search_queries(limit: int = 50):
    """Get generated search queries"""
    return {"queries": generate_search_queries(limit)}

@api_router.get("/stats")
async def get_scraping_stats():
    """Get scraping statistics"""
    
    # Count tasks by status
    total_tasks = await db.scraping_tasks.count_documents({})
    running_tasks = await db.scraping_tasks.count_documents({"status": "running"})
    completed_tasks = await db.scraping_tasks.count_documents({"status": "completed"})
    failed_tasks = await db.scraping_tasks.count_documents({"status": "failed"})
    
    # Count products
    total_products = await db.tea_products.count_documents({})
    
    # Calculate error rate
    error_rate = (failed_tasks / total_tasks * 100) if total_tasks > 0 else 0
    
    return ScrapingStats(
        total_tasks=total_tasks,
        running_tasks=running_tasks,
        completed_tasks=completed_tasks,
        failed_tasks=failed_tasks,
        total_products=total_products,
        captcha_solves=captcha_solver.solve_count,
        error_rate=error_rate
    )

@api_router.get("/categories")
async def get_tea_categories():
    """Get tea categories and types"""
    
    # Aggregate tea types from database
    pipeline = [
        {"$group": {"_id": "$tea_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    categories = await db.tea_products.aggregate(pipeline).to_list(100)
    
    return {
        "categories": categories,
        "total_types": len(categories)
    }

@api_router.post("/products/bulk-delete")
async def bulk_delete_products(product_ids: List[str]):
    """Bulk delete tea products"""
    result = await db.tea_products.delete_many({"id": {"$in": product_ids}})
    
    return {
        "deleted_count": result.deleted_count,
        "message": f"Deleted {result.deleted_count} products"
    }

@api_router.get("/export/csv")
async def export_products_csv():
    """Export products to CSV format"""
    products = await db.tea_products.find().to_list(10000)
    
    # Convert to CSV-like format
    csv_data = []
    for product in products:
        # Remove MongoDB ObjectId to avoid serialization issues
        if "_id" in product:
            del product["_id"]
            
        csv_data.append({
            "id": product.get("id"),
            "name": product.get("name"),
            "price": product.get("price"),
            "rating": product.get("rating"),
            "tea_type": product.get("tea_type"),
            "tea_region": product.get("tea_region"),
            "is_pressed": product.get("is_pressed"),
            "scraped_at": product.get("scraped_at")
        })
    
    return {"data": csv_data, "count": len(csv_data)}

# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()