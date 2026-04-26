import asyncio
from typing import Callable, Awaitable
from browser import BrowserManager
from models import LiquidationZone

Log = Callable[[str], Awaitable[None]]

# Coinglass uses ticker symbols directly in their URLs
COINGLASS_URL = "https://www.coinglass.com/LiquidationData"


async def scrape_liquidation_zones(asset: str, log: Log = None) -> list[LiquidationZone]:
    if log:
        await log(f"Opening Coinglass liquidation data for {asset}...")

    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    zones: list[LiquidationZone] = []

    # Register BEFORE navigation
    async def on_response(response):
        try:
            if response.status != 200:
                return
            url = response.url
            if not ("liquidat" in url.lower() or "liqMap" in url or "heatmap" in url.lower()):
                return
            data = await response.json()
            items = []
            if isinstance(data, dict):
                items = data.get("data", data.get("list", data.get("liquidationMap", [])))
            elif isinstance(data, list):
                items = data
            for item in items[:50]:
                price = float(item.get("price", item.get("priceLevel", item.get("p", 0))))
                liq = float(item.get("liquidation", item.get("amount", item.get("v", item.get("value", 0)))))
                side_raw = str(item.get("side", item.get("type", item.get("direction", "long"))))
                if price > 0 and liq > 0:
                    zones.append(LiquidationZone(
                        price=price,
                        liquidation_usd=liq,
                        side="LONG" if "long" in side_raw.lower() else "SHORT",
                    ))
        except Exception:
            pass

    page.on("response", on_response)

    try:
        await page.goto(COINGLASS_URL, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(4)

        # Try to click on the asset filter / search
        try:
            # Look for a coin selector or search box
            for sel in [
                f"text={asset}",
                f"[placeholder*='search' i]",
                f"input[type='text']",
            ]:
                el = page.locator(sel).first
                if await el.count() > 0:
                    if "input" in sel or "placeholder" in sel:
                        await el.click()
                        await el.fill(asset)
                        await asyncio.sleep(2)
                        # Press enter or click first result
                        await page.keyboard.press("Enter")
                    else:
                        await el.click()
                    await asyncio.sleep(3)
                    break
        except Exception:
            pass

        # Wait for zone data
        for _ in range(10):
            if zones:
                break
            await asyncio.sleep(1)

    except Exception as e:
        if log:
            await log(f"[WARN] Coinglass: {e}")
    finally:
        await page.close()

    zones.sort(key=lambda z: z.liquidation_usd, reverse=True)

    if log:
        if zones:
            await log(f"Coinglass: {len(zones)} liquidation zones found")
        else:
            await log("[WARN] Coinglass: no liquidation data captured (site may block bots)")
    return zones
