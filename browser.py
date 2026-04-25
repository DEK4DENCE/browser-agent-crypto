import asyncio
import os
import random
from playwright.async_api import async_playwright, Browser, BrowserContext, Page


class BrowserManager:
    _instance: "BrowserManager | None" = None

    def __init__(self):
        self._pw = None
        self.browser: Browser = None
        self.context: BrowserContext = None

    @classmethod
    async def get(cls) -> "BrowserManager":
        if cls._instance is None:
            cls._instance = cls()
            await cls._instance._init()
        return cls._instance

    async def _init(self):
        self._pw = await async_playwright().start()
        self.browser = await self._pw.chromium.launch(
            headless=os.getenv("HEADLESS", "false").lower() == "true",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ],
        )
        storage_state = "cookies.json" if os.path.exists("cookies.json") else None
        self.context = await self.browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            storage_state=storage_state,
        )
        await self.context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

    async def new_page(self) -> Page:
        page = await self.context.new_page()
        # Block images/fonts to speed up scraping; keep XHR/fetch
        await page.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,otf}",
            lambda route: route.abort(),
        )
        return page

    async def save_cookies(self):
        await self.context.storage_state(path="cookies.json")

    @staticmethod
    async def human_delay(min_s: float = 1.2, max_s: float = 3.5):
        await asyncio.sleep(random.uniform(min_s, max_s))

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self._pw:
            await self._pw.stop()
        BrowserManager._instance = None
