"""
Exchange liquidation order scrapers — all called from Python (aiohttp),
never from inside a browser page, so no CORS issues.
"""
import asyncio
import aiohttp
import os
from typing import Callable, Awaitable

Log = Callable[[str], Awaitable[None]]

_TIMEOUT = aiohttp.ClientTimeout(total=12)

BINANCE_SYMBOLS_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_LIQ_URL    = "https://fapi.binance.com/fapi/v1/forceOrders"
BYBIT_LIQ_URL      = "https://api.bybit.com/v5/market/recent-trade"
OKX_LIQ_URL        = "https://www.okx.com/api/v5/public/liquidation-orders"
HL_INFO_URL        = "https://api.hyperliquid.xyz/info"


async def _binance_has_perp(asset: str) -> bool:
    """Check if asset trades as a USDT perp on Binance."""
    symbol = f"{asset}USDT"
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
            async with s.get(BINANCE_SYMBOLS_URL) as r:
                if r.status != 200:
                    return False
                data = await r.json()
                syms = {x["symbol"] for x in data.get("symbols", [])}
                return symbol in syms
    except Exception:
        return False


async def fetch_binance_liquidations(asset: str, log: Log = None) -> list[dict]:
    """Fetch recent forced liquidation orders from Binance perps (Python call — no CORS)."""
    symbol = f"{asset}USDT"

    # Check listing first — HYPE and many altcoins aren't on Binance perps
    listed = await _binance_has_perp(asset)
    if not listed:
        if log:
            await log(f"  Binance: {asset} not listed as perp — skipping Binance liq")
        return []

    orders = []
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
            async with s.get(
                BINANCE_LIQ_URL,
                params={"symbol": symbol, "limit": 100},
                headers={"X-MBX-APIKEY": os.getenv("BINANCE_API_KEY", "")},
            ) as r:
                if r.status == 200:
                    orders = await r.json()
                elif r.status == 401:
                    if log:
                        await log("  Binance: API key required for forced orders — set BINANCE_API_KEY in .env")
                else:
                    if log:
                        await log(f"  Binance liq: HTTP {r.status}")
    except Exception as e:
        if log:
            await log(f"  [WARN] Binance liq fetch: {e}")

    return orders if isinstance(orders, list) else []


async def fetch_bybit_liquidations(asset: str, log: Log = None) -> list[dict]:
    """Bybit public liquidation feed — no auth needed."""
    symbol = f"{asset}USDT"
    results = []
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
            async with s.get(
                "https://api.bybit.com/v5/market/tickers",
                params={"category": "linear", "symbol": symbol},
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    items = data.get("result", {}).get("list", [])
                    for item in items:
                        liq_price = item.get("liqPrice", "0")
                        if float(liq_price or 0) > 0:
                            results.append({
                                "exchange": "Bybit",
                                "symbol": symbol,
                                "liq_price": float(liq_price),
                                "last_price": float(item.get("lastPrice", 0)),
                                "open_interest_value": float(item.get("openInterestValue", 0)),
                            })
    except Exception as e:
        if log:
            await log(f"  [WARN] Bybit liq: {e}")
    return results


async def fetch_okx_liquidations(asset: str, log: Log = None) -> list[dict]:
    """OKX public liquidation orders endpoint."""
    inst_id = f"{asset}-USDT-SWAP"
    results = []
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
            async with s.get(
                OKX_LIQ_URL,
                params={"instType": "SWAP", "instId": inst_id, "state": "filled"},
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    for item in data.get("data", [])[:50]:
                        for detail in item.get("details", []):
                            results.append({
                                "exchange": "OKX",
                                "symbol": inst_id,
                                "side": detail.get("side"),
                                "size": float(detail.get("sz", 0)),
                                "price": float(detail.get("bkPx", 0)),
                                "ts": detail.get("ts"),
                            })
    except Exception as e:
        if log:
            await log(f"  [WARN] OKX liq: {e}")
    return results


async def fetch_hl_liquidations(asset: str, log: Log = None) -> list[dict]:
    """Hyperliquid's own recent liquidations via their public API."""
    results = []
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
            # HL exposes recent trades; liquidations come through as special fills
            async with s.post(
                HL_INFO_URL,
                json={"type": "recentTrades", "coin": asset},
                headers={"Content-Type": "application/json"},
            ) as r:
                if r.status == 200:
                    trades = await r.json()
                    for t in (trades or [])[:100]:
                        # Liquidation trades have a specific marker
                        if t.get("liquidation") or t.get("side") == "liq":
                            results.append({
                                "exchange": "Hyperliquid",
                                "side": t.get("side"),
                                "price": float(t.get("px", 0)),
                                "size": float(t.get("sz", 0)),
                                "ts": t.get("time"),
                            })
    except Exception as e:
        if log:
            await log(f"  [WARN] HL liq: {e}")
    return results


async def fetch_all_liquidations(asset: str, log: Log = None) -> dict:
    """
    Aggregate liquidation data across all available sources.
    Returns a summary dict with counts and notable levels.
    """
    if log:
        await log(f"Fetching liquidation orders across exchanges for {asset}...")

    binance, bybit, okx, hl = await asyncio.gather(
        fetch_binance_liquidations(asset, log=log),
        fetch_bybit_liquidations(asset, log=log),
        fetch_okx_liquidations(asset, log=log),
        fetch_hl_liquidations(asset, log=log),
    )

    total = len(binance) + len(bybit) + len(okx) + len(hl)

    if log:
        parts = []
        if binance:
            parts.append(f"Binance:{len(binance)}")
        if bybit:
            parts.append(f"Bybit:{len(bybit)}")
        if okx:
            parts.append(f"OKX:{len(okx)}")
        if hl:
            parts.append(f"HL:{len(hl)}")
        if parts:
            await log(f"  Liquidation orders: {' | '.join(parts)} ({total} total)")
        else:
            await log(f"  No liquidation order data retrieved (asset may not be on major CEX perps)")

    return {
        "binance": binance,
        "bybit": bybit,
        "okx": okx,
        "hyperliquid": hl,
        "total": total,
    }
