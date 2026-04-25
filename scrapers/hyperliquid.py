import asyncio
import json
from typing import Callable, Awaitable
from browser import BrowserManager
from models import TraderStats, TraderPosition

LEADERBOARD_URL = "https://app.hyperliquid.xyz/leaderboard"
EXPLORER_URL = "https://app.hyperliquid.xyz/explorer/address/{address}"
TRADE_URL = "https://app.hyperliquid.xyz/trade/{asset}"

Log = Callable[[str], Awaitable[None]]


async def scrape_leaderboard(
    top_n: int = 20,
    log: Log = None,
) -> list[TraderStats]:
    if log:
        await log(f"Opening Hyperliquid leaderboard → {LEADERBOARD_URL}")
    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    traders: list[TraderStats] = []
    raw_rows: list[dict] = []

    async def handle_response(response):
        if "leaderboard" in response.url and response.status == 200:
            try:
                data = await response.json()
                rows = data.get("leaderboardRows", [])
                for i, entry in enumerate(rows[:top_n]):
                    raw_rows.append({"rank": i + 1, "entry": entry})
            except Exception:
                pass

    page.on("response", handle_response)

    try:
        await page.goto(LEADERBOARD_URL, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(4)

        if not raw_rows:
            # try reloading to catch the API call
            await page.reload(wait_until="networkidle", timeout=30000)
            await asyncio.sleep(5)
    except Exception as e:
        if log:
            await log(f"[WARN] Leaderboard load issue: {e}")
    finally:
        await page.close()

    for item in raw_rows:
        r = item["rank"]
        e = item["entry"]
        trader = TraderStats(
            address=e.get("ethAddress", "unknown"),
            rank=r,
            pnl_all_time=float(e.get("windowPerformances", [{}])[0].get("vlm", 0))
            if e.get("windowPerformances")
            else 0.0,
        )
        traders.append(trader)

    if log:
        await log(f"Found {len(traders)} traders on leaderboard")
    return traders


async def scrape_trader_profile(
    address: str,
    asset_filter: str = "",
    log: Log = None,
) -> TraderStats:
    if log:
        await log(f"Scraping profile: {address[:10]}...")
    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    positions: list[TraderPosition] = []
    stats_data: dict = {}

    async def handle_response(response):
        url = response.url
        if response.status != 200:
            return
        try:
            if "clearinghouseState" in url or "userState" in url or "portfolio" in url:
                data = await response.json()
                asset_positions = data.get("assetPositions", [])
                for pos in asset_positions:
                    p = pos.get("position", {})
                    coin = p.get("coin", "")
                    if asset_filter and asset_filter.lower() not in coin.lower():
                        continue
                    szi = float(p.get("szi", 0))
                    positions.append(
                        TraderPosition(
                            address=address,
                            asset=coin,
                            side="LONG" if szi > 0 else "SHORT",
                            size_usd=abs(float(p.get("positionValue", 0))),
                            entry_price=float(p.get("entryPx", 0)),
                            liquidation_price=float(p.get("liquidationPx", 0)) or None,
                            unrealized_pnl=float(p.get("unrealizedPnl", 0)),
                            leverage=int(
                                p.get("leverage", {}).get("value", 1)
                                if isinstance(p.get("leverage"), dict)
                                else p.get("leverage", 1)
                            ),
                            roi_pct=float(p.get("returnOnEquity", 0)) * 100
                            if p.get("returnOnEquity")
                            else None,
                        )
                    )
            if "userFills" in url or "volume" in url:
                data = await response.json()
                stats_data.update(data if isinstance(data, dict) else {})
        except Exception:
            pass

    page.on("response", handle_response)

    try:
        url = EXPLORER_URL.format(address=address)
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)
    except Exception as e:
        if log:
            await log(f"[WARN] Profile load issue {address[:8]}: {e}")
    finally:
        await page.close()

    if log and positions:
        await log(
            f"  → {address[:10]}... has {len(positions)} open position(s)"
        )

    return TraderStats(
        address=address,
        rank=0,
        pnl_all_time=float(stats_data.get("totalPnl", 0)),
        positions=positions,
    )


async def scrape_trade_page(asset: str, log: Log = None) -> dict:
    """Scrape the trade page for OI and funding data."""
    if log:
        await log(f"Opening trade page for {asset}...")
    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    market_data: dict = {}

    async def handle_response(response):
        if response.status != 200:
            return
        try:
            if "metaAndAssetCtxs" in response.url or "marketData" in response.url:
                data = await response.json()
                if isinstance(data, list) and len(data) >= 2:
                    metas = data[0].get("universe", [])
                    ctxs = data[1]
                    for i, meta in enumerate(metas):
                        if meta.get("name", "").upper() == asset.upper():
                            ctx = ctxs[i] if i < len(ctxs) else {}
                            market_data.update(
                                {
                                    "open_interest": float(ctx.get("openInterest", 0)),
                                    "funding_rate": float(ctx.get("funding", 0)),
                                    "mark_price": float(ctx.get("markPx", 0)),
                                    "day_volume": float(ctx.get("dayNtlVlm", 0)),
                                }
                            )
        except Exception:
            pass

    page.on("response", handle_response)

    try:
        url = TRADE_URL.format(asset=asset)
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(4)
    except Exception as e:
        if log:
            await log(f"[WARN] Trade page issue: {e}")
    finally:
        await page.close()

    if log:
        await log(
            f"HL market data: OI=${market_data.get('open_interest', 0):,.0f} "
            f"Funding={market_data.get('funding_rate', 0):.4%}"
        )
    return market_data
