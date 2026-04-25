import asyncio
from typing import Callable, Awaitable
from browser import BrowserManager
from models import LiquidationZone

COINGLASS_URL = "https://www.coinglass.com/LiquidationData"
COINGLASS_HEATMAP = "https://www.coinglass.com/pro/futures/LiquidationHeatMap"

Log = Callable[[str], Awaitable[None]]


async def scrape_liquidation_zones(
    asset: str,
    log: Log = None,
) -> list[LiquidationZone]:
    if log:
        await log(f"Scanning Coinglass liquidation heatmap for {asset}...")
    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    zones: list[LiquidationZone] = []

    async def handle_response(response):
        if response.status != 200:
            return
        try:
            url = response.url
            if "liquidation" in url.lower() or "liqMap" in url or "heatmap" in url.lower():
                data = await response.json()
                items = []
                if isinstance(data, dict):
                    items = data.get("data", data.get("list", []))
                elif isinstance(data, list):
                    items = data
                for item in items[:30]:
                    price = float(item.get("price", item.get("priceLevel", 0)))
                    liq_usd = float(item.get("liquidation", item.get("amount", item.get("value", 0))))
                    side_raw = item.get("side", item.get("type", "long"))
                    if price > 0 and liq_usd > 0:
                        zones.append(
                            LiquidationZone(
                                price=price,
                                liquidation_usd=liq_usd,
                                side="LONG" if "long" in str(side_raw).lower() else "SHORT",
                            )
                        )
        except Exception:
            pass

    page.on("response", handle_response)

    try:
        await page.goto(COINGLASS_URL, wait_until="domcontentloaded", timeout=25000)
        await asyncio.sleep(4)

        # try to navigate to asset-specific data
        try:
            asset_selector = page.locator(f"text={asset}").first
            if await asset_selector.count() > 0:
                await asset_selector.click()
                await asyncio.sleep(2)
        except Exception:
            pass

        await asyncio.sleep(3)
    except Exception as e:
        if log:
            await log(f"[WARN] Coinglass issue: {e}")
    finally:
        await page.close()

    # sort by size descending
    zones.sort(key=lambda z: z.liquidation_usd, reverse=True)

    if log:
        await log(f"Found {len(zones)} liquidation zones")
    return zones
