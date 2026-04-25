import asyncio
import aiohttp
from typing import Callable, Awaitable
from models import FundingRate

Log = Callable[[str], Awaitable[None]]

BINANCE_FUNDING_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
OKX_FUNDING_URL = "https://www.okx.com/api/v5/public/funding-rate"
BYBIT_FUNDING_URL = "https://api.bybit.com/v5/market/tickers?category=linear"


async def fetch_binance_funding(asset: str) -> FundingRate | None:
    symbol = f"{asset}USDT"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                BINANCE_FUNDING_URL,
                params={"symbol": symbol},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                rate = float(data.get("lastFundingRate", 0))
                return FundingRate(
                    exchange="Binance",
                    asset=asset,
                    rate_8h=round(rate * 100, 4),
                    annualized=round(rate * 100 * 3 * 365, 2),
                )
        except Exception:
            return None


async def fetch_okx_funding(asset: str) -> FundingRate | None:
    inst_id = f"{asset}-USDT-SWAP"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                OKX_FUNDING_URL,
                params={"instId": inst_id},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                items = data.get("data", [])
                if not items:
                    return None
                rate = float(items[0].get("fundingRate", 0))
                return FundingRate(
                    exchange="OKX",
                    asset=asset,
                    rate_8h=round(rate * 100, 4),
                    annualized=round(rate * 100 * 3 * 365, 2),
                )
        except Exception:
            return None


async def fetch_bybit_funding(asset: str) -> FundingRate | None:
    symbol = f"{asset}USDT"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                BYBIT_FUNDING_URL,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                items = data.get("result", {}).get("list", [])
                for item in items:
                    if item.get("symbol") == symbol:
                        rate = float(item.get("fundingRate", 0))
                        return FundingRate(
                            exchange="Bybit",
                            asset=asset,
                            rate_8h=round(rate * 100, 4),
                            annualized=round(rate * 100 * 3 * 365, 2),
                        )
        except Exception:
            return None
    return None


async def scrape_funding_rates(
    asset: str,
    log: Log = None,
) -> list[FundingRate]:
    if log:
        await log(f"Fetching funding rates from Binance / OKX / Bybit for {asset}...")

    results = await asyncio.gather(
        fetch_binance_funding(asset),
        fetch_okx_funding(asset),
        fetch_bybit_funding(asset),
    )

    rates = [r for r in results if r is not None]

    if log:
        for r in rates:
            await log(f"  {r.exchange}: {r.rate_8h:+.4f}% / 8h  ({r.annualized:+.1f}% ann.)")
        if not rates:
            await log(f"[WARN] No funding rates found for {asset}")

    return rates
