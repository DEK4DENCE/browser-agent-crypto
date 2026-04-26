"""
Hyperliquid browser scraper.
Registers response listeners BEFORE navigation so we never miss API calls.
Uses domcontentloaded (not networkidle) — HL keeps WebSockets open forever.
"""
import asyncio
import json
from typing import Callable, Awaitable
from browser import BrowserManager
from models import TraderStats, TraderPosition

LEADERBOARD_URL = "https://app.hyperliquid.xyz/leaderboard"
TRADE_URL = "https://app.hyperliquid.xyz/trade/{asset}"
PROFILE_URL = "https://app.hyperliquid.xyz/explorer/address/{address}"

Log = Callable[[str], Awaitable[None]]


async def scrape_market_data(asset: str, log: Log = None) -> dict:
    if log:
        await log(f"Opening Hyperliquid trade page for {asset}...")

    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    result: dict = {}

    # Register BEFORE navigation
    async def on_response(response):
        try:
            if response.status != 200:
                return
            url = response.url
            if "metaAndAssetCtxs" in url or ("info" in url and "hyperliquid" in url):
                data = await response.json()
                if isinstance(data, list) and len(data) >= 2:
                    metas = data[0].get("universe", [])
                    ctxs = data[1]
                    for i, meta in enumerate(metas):
                        if meta.get("name", "").upper() == asset.upper():
                            ctx = ctxs[i] if i < len(ctxs) else {}
                            result.update({
                                "open_interest": float(ctx.get("openInterest", 0)),
                                "funding_rate": float(ctx.get("funding", 0)),
                                "mark_price": float(ctx.get("markPx", 0)),
                                "day_volume": float(ctx.get("dayNtlVlm", 0)),
                            })
        except Exception:
            pass

    page.on("response", on_response)

    try:
        await page.goto(TRADE_URL.format(asset=asset), wait_until="domcontentloaded", timeout=30000)
        # Wait up to 15s for market data to arrive via XHR
        for _ in range(15):
            if result.get("mark_price"):
                break
            await asyncio.sleep(1)

        # DOM fallback — try reading the displayed price
        if not result.get("mark_price"):
            try:
                price_el = await page.wait_for_selector(
                    "[class*='price'], [class*='Price'], [data-testid*='price']",
                    timeout=5000,
                )
                text = await price_el.inner_text()
                nums = [float(x.replace(",", "")) for x in text.split() if x.replace(",", "").replace(".", "").isdigit()]
                if nums:
                    result["mark_price"] = nums[0]
            except Exception:
                pass

    except Exception as e:
        if log:
            await log(f"[WARN] HL trade page: {e}")
    finally:
        await page.close()

    if log:
        if result.get("mark_price"):
            await log(
                f"HL: ${result['mark_price']:,.4f} | "
                f"OI=${result.get('open_interest', 0):,.0f} | "
                f"Funding={result.get('funding_rate', 0):.4%}/8h"
            )
        else:
            await log("[WARN] HL market data not captured — site may need longer load time")

    return result


async def scrape_leaderboard(top_n: int = 20, log: Log = None) -> list[TraderStats]:
    if log:
        await log(f"Opening Hyperliquid leaderboard...")

    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    traders: list[TraderStats] = []

    async def on_response(response):
        try:
            if response.status != 200:
                return
            url = response.url
            if "leaderboard" in url.lower() or "stats-data" in url:
                data = await response.json()
                rows = data if isinstance(data, list) else data.get("leaderboardRows", [])
                for i, entry in enumerate(rows[:top_n]):
                    addr = entry.get("ethAddress", entry.get("address", ""))
                    if not addr:
                        continue
                    windows = entry.get("windowPerformances", [])
                    pnl = 0.0
                    for w in windows:
                        if isinstance(w, list) and len(w) >= 2:
                            try:
                                pnl = max(pnl, float(w[1].get("pnl", 0)))
                            except Exception:
                                pass
                        elif isinstance(w, dict):
                            pnl = max(pnl, float(w.get("pnl", 0)))
                    traders.append(TraderStats(address=addr, rank=i + 1, pnl_all_time=pnl))
        except Exception:
            pass

    page.on("response", on_response)

    try:
        await page.goto(LEADERBOARD_URL, wait_until="domcontentloaded", timeout=30000)

        # Wait up to 20s for leaderboard rows
        for _ in range(20):
            if traders:
                break
            await asyncio.sleep(1)

        # DOM fallback — read addresses from rendered table rows
        if not traders:
            if log:
                await log("  API intercept missed — trying DOM scrape...")
            try:
                # Wait for any table/list to appear
                await page.wait_for_selector(
                    "table tbody tr, [class*='row'], [class*='Row'], [class*='trader']",
                    timeout=10000,
                )
                rows = await page.query_selector_all("table tbody tr")
                for i, row in enumerate(rows[:top_n]):
                    try:
                        cells = await row.query_selector_all("td")
                        if not cells:
                            continue
                        # Address is usually in a cell as truncated hex
                        for cell in cells:
                            text = await cell.inner_text()
                            if text.startswith("0x") and len(text) >= 10:
                                traders.append(TraderStats(address=text.strip(), rank=i + 1))
                                break
                    except Exception:
                        continue
            except Exception:
                pass

    except Exception as e:
        if log:
            await log(f"[WARN] Leaderboard error: {e}")
    finally:
        await page.close()

    if log:
        await log(f"Leaderboard: {len(traders)} traders found")
    return traders


async def scrape_trader_profile(
    address: str,
    asset_filter: str = "",
    log: Log = None,
) -> TraderStats:
    if log:
        await log(f"  → Scraping {address[:10]}... profile")

    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    positions: list[TraderPosition] = []

    async def on_response(response):
        try:
            if response.status != 200:
                return
            url = response.url
            if "clearinghouseState" in url or "userState" in url or (
                "info" in url and "hyperliquid" in url
            ):
                data = await response.json()
                for pos in data.get("assetPositions", []):
                    p = pos.get("position", {})
                    coin = p.get("coin", "")
                    if asset_filter and asset_filter.upper() not in coin.upper():
                        continue
                    szi = float(p.get("szi", 0))
                    if szi == 0:
                        continue
                    lev = p.get("leverage", {})
                    lev_val = int(lev.get("value", 1)) if isinstance(lev, dict) else int(lev or 1)
                    positions.append(TraderPosition(
                        address=address,
                        asset=coin,
                        side="LONG" if szi > 0 else "SHORT",
                        size_usd=abs(float(p.get("positionValue", 0))),
                        entry_price=float(p.get("entryPx", 0)),
                        liquidation_price=float(p.get("liquidationPx") or 0) or None,
                        unrealized_pnl=float(p.get("unrealizedPnl", 0)),
                        leverage=lev_val,
                    ))
        except Exception:
            pass

    page.on("response", on_response)

    try:
        await page.goto(
            PROFILE_URL.format(address=address),
            wait_until="domcontentloaded",
            timeout=25000,
        )
        # Wait up to 10s for position data
        for _ in range(10):
            if positions:
                break
            await asyncio.sleep(1)
    except Exception as e:
        if log:
            await log(f"[WARN] Profile {address[:8]}: {e}")
    finally:
        await page.close()

    if positions and log:
        await log(f"    {len(positions)} open position(s)")

    return TraderStats(address=address, rank=0, positions=positions)
