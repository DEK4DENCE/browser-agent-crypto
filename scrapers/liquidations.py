"""
Exchange data scrapers — all called from Python (aiohttp),
never from inside a browser page, so no CORS issues.

Binance public futures endpoints (no API key needed):
  - takerlongshortRatio — taker buy vs sell volume
  - topLongShortPositionRatio — top trader position bias
  - openInterestHist — historical open interest
  - fundingRate — historical funding rates
"""
import asyncio
import aiohttp
import os
from typing import Callable, Awaitable

Log = Callable[[str], Awaitable[None]]

_TIMEOUT = aiohttp.ClientTimeout(total=12)

BINANCE_FUTURES_BASE = "https://fapi.binance.com"
BINANCE_SYMBOLS_URL  = f"{BINANCE_FUTURES_BASE}/fapi/v1/exchangeInfo"
OKX_LIQ_URL          = "https://www.okx.com/api/v5/public/liquidation-orders"
HL_INFO_URL          = "https://api.hyperliquid.xyz/info"


async def _binance_has_perp(session: aiohttp.ClientSession, asset: str) -> bool:
    """Check if asset trades as a USDT perp on Binance futures."""
    symbol = f"{asset}USDT"
    try:
        async with session.get(BINANCE_SYMBOLS_URL) as r:
            if r.status != 200:
                return False
            data = await r.json()
            syms = {x["symbol"] for x in data.get("symbols", [])}
            return symbol in syms
    except Exception:
        return False


async def fetch_binance_liquidations(asset: str, log: Log = None) -> list[dict]:
    """
    Fetch Binance futures market data for the asset using public endpoints.
    Returns taker ratios, top trader positioning, OI history, and funding.
    No API key required — all public data endpoints.
    """
    symbol = f"{asset}USDT"
    results = []

    async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
        listed = await _binance_has_perp(s, asset)
        if not listed:
            if log:
                await log(f"  Binance: {asset} not listed on futures — skipping")
            return []

        if log:
            await log(f"  ✓ Binance: {symbol} found on futures")

        endpoints = [
            ("futures/data/takerlongshortRatio", {"symbol": symbol, "period": "1h", "limit": 24}, "taker_ls_ratio"),
            ("futures/data/topLongShortPositionRatio", {"symbol": symbol, "period": "1h", "limit": 24}, "top_trader_pos"),
            ("futures/data/openInterestHist", {"symbol": symbol, "period": "1h", "limit": 24}, "oi_history"),
            ("fapi/v1/fundingRate", {"symbol": symbol, "limit": 10}, "funding_hist"),
        ]

        for path, params, tag in endpoints:
            try:
                async with s.get(f"{BINANCE_FUTURES_BASE}/{path}", params=params) as r:
                    if r.status == 200:
                        data = await r.json()
                        if isinstance(data, list) and data:
                            for row in data:
                                row["_tag"] = tag
                                row["_symbol"] = symbol
                                results.append(row)
                            if log:
                                await log(f"  ✓ Binance {tag}: {len(data)} records")
            except Exception as e:
                if log:
                    await log(f"  [WARN] Binance {tag}: {e}")

    return results


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
