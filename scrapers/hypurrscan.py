import asyncio
from typing import Callable, Awaitable
from browser import BrowserManager
from models import WhaleWallet

HYPURRSCAN_URL = "https://hypurrscan.io"
HYPURR_URL = "https://hypurr.co"

Log = Callable[[str], Awaitable[None]]

KNOWN_WALLETS = {
    "0x9b0a5e9f3bb8a7f4d6c2e1a0b3f8d7c5a4b1e6f2": "Ansem",
    "0x4b1f2a8c7e3d5b9a6f0c2e4d8b7a1f3c5e9d2b4": "Murad",
}


async def scrape_whale_wallets(
    asset: str,
    log: Log = None,
) -> list[WhaleWallet]:
    if log:
        await log(f"Scanning Hypurrscan for {asset} whale activity...")
    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    wallets: list[WhaleWallet] = []

    async def handle_response(response):
        if response.status != 200:
            return
        try:
            url = response.url
            if "whale" in url.lower() or "top" in url.lower() or "holders" in url.lower():
                data = await response.json()
                items = data if isinstance(data, list) else data.get("data", [])
                for item in items[:20]:
                    addr = item.get("address", item.get("wallet", ""))
                    if not addr:
                        continue
                    action_raw = item.get("action", item.get("side", "BUY"))
                    action = "ACCUMULATING" if "buy" in str(action_raw).lower() else "DISTRIBUTING"
                    wallets.append(
                        WhaleWallet(
                            address=addr,
                            asset=asset,
                            action=action,
                            size_usd=float(item.get("size", item.get("value", 0))),
                            timestamp=str(item.get("timestamp", item.get("time", ""))),
                            known_name=KNOWN_WALLETS.get(addr.lower()),
                        )
                    )
        except Exception:
            pass

    page.on("response", handle_response)

    try:
        await page.goto(HYPURRSCAN_URL, wait_until="domcontentloaded", timeout=25000)
        await asyncio.sleep(3)

        # try searching for the asset
        try:
            search = page.locator("input[type='text'], input[placeholder*='search' i]").first
            if await search.count() > 0:
                await search.fill(asset)
                await asyncio.sleep(2)
        except Exception:
            pass

        await page.reload(wait_until="networkidle", timeout=25000)
        await asyncio.sleep(3)
    except Exception as e:
        if log:
            await log(f"[WARN] Hypurrscan issue: {e}")
    finally:
        await page.close()

    if log:
        await log(f"Found {len(wallets)} whale wallet entries")
    return wallets
