"""
Research orchestrator — 9-phase pipeline.
All browser scrapers are called sequentially after eager browser init.
Pure-API phases (funding, liquidations, Dune) run via aiohttp — no browser, no CORS.
"""
import asyncio
import os
from typing import Callable, Awaitable
import aiohttp
import llm
from models import ResearchResult, TokenMetrics, FundingRate
from scrapers import hyperliquid, coinglass, funding, twitter, hypurrscan
from scrapers import liquidations as liq_scraper
from scrapers import dune as dune_scraper

Log = Callable[[str], Awaitable[None]]

HL_API = "https://api.hyperliquid.xyz/info"
CG_API = "https://api.coingecko.com/api/v3"
_TIMEOUT = aiohttp.ClientTimeout(total=15)

# Known CT wallets to cross-reference
CT_WALLETS: dict[str, str] = {
    "0x9b0a5e9f3bb8a7f4d6c2e1a0b3f8d7c5a4b1e6f2": "Ansem",
    "0x4b1f2a8c7e3d5b9a6f0c2e4d8b7a1f3c5e9d2b4": "Murad",
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "Cobie",
}

COINGECKO_IDS = {
    "HYPE": "hyperliquid", "BTC": "bitcoin", "ETH": "ethereum",
    "SOL": "solana", "BNB": "binancecoin", "XRP": "ripple",
    "ADA": "cardano", "AVAX": "avalanche-2", "DOGE": "dogecoin",
}


def parse_asset_from_query(query: str) -> str:
    # If it's already just a ticker (1-6 uppercase chars), return as-is
    q = query.strip().upper().replace("$", "")
    if q.isalpha() and len(q) <= 6:
        return q
    result = llm.quick(
        f"Extract the crypto ticker symbol from: '{query}'. "
        "Reply with ONLY the ticker, uppercase, no $ or explanation. E.g. HYPE"
    )
    return result.strip().upper().replace("$", "").split()[0]


async def _cg_price_history(coin_id: str, days: int = 30) -> list[float]:
    """Fetch daily closing prices from CoinGecko."""
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
            async with s.get(
                f"{CG_API}/coins/{coin_id}/market_chart",
                params={"vs_currency": "usd", "days": days, "interval": "daily"},
            ) as r:
                if r.status != 200:
                    return []
                data = await r.json()
                return [p[1] for p in data.get("prices", [])]
    except Exception:
        return []


async def _hl_candles(asset: str, interval: str = "1h", lookback: int = 168) -> list[dict]:
    """Fetch OHLCV candles from Hyperliquid."""
    import time
    now_ms = int(time.time() * 1000)
    interval_ms = {"1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}.get(interval, 3_600_000)
    start_ms = now_ms - (lookback * interval_ms)
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
            async with s.post(
                HL_API,
                json={"type": "candleSnapshot", "req": {
                    "coin": asset, "interval": interval,
                    "startTime": start_ms, "endTime": now_ms,
                }},
                headers={"Content-Type": "application/json"},
            ) as r:
                if r.status == 200:
                    return await r.json() or []
    except Exception:
        pass
    return []


def _calc_sr(candles: list[dict]) -> tuple[float, float]:
    """Simple S1/R1: lowest low and highest high of recent candles."""
    if not candles:
        return 0.0, 0.0
    lows = [float(c.get("l", c.get("low", 0))) for c in candles if c.get("l") or c.get("low")]
    highs = [float(c.get("h", c.get("high", 0))) for c in candles if c.get("h") or c.get("high")]
    return (min(lows) if lows else 0.0), (max(highs) if highs else 0.0)


async def run_research(
    asset: str,
    query: str = "",
    log_queue: asyncio.Queue | None = None,
) -> ResearchResult:

    async def log(msg: str):
        print(f"[agent] {msg}")
        if log_queue is not None:
            await log_queue.put({"type": "log", "message": msg})

    result = ResearchResult(asset=asset, query=query)

    # Eager browser init — must happen before any parallel browser calls
    from browser import BrowserManager
    await BrowserManager.get()

    # ── PHASE 1: Token fundamentals ──────────────────────────────────────
    await log("PHASE 1 → Token fundamentals (CoinGecko / DefiLlama)")
    try:
        cg_data, dl_data = await asyncio.gather(
            twitter.scrape_coingecko(asset, log=log),
            twitter.scrape_defillama(asset, log=log),
        )
        ath = cg_data.get("ath", 0)
        change = cg_data.get("change_24h", 0)
        tvl = dl_data.get("tvl", 0)
        fees = dl_data.get("fees_24h", 0)
        if cg_data.get("price_usd"):
            await log(
                f"✓ CoinGecko: ${cg_data['price_usd']:,.4f} | "
                f"MC ${cg_data.get('market_cap', 0)/1e9:.2f}B | "
                f"Vol ${cg_data.get('volume_24h', 0)/1e6:.2f}M | "
                f"24h {change:+.2f}% | ATH ${ath:,.4f}"
            )
        if tvl:
            await log(
                f"✓ DefiLlama: TVL ${tvl/1e9:.2f}B"
                + (f" | Fees/day ${fees/1e6:.2f}M" if fees else "")
            )
        result.token_metrics = TokenMetrics(
            price_usd=cg_data.get("price_usd"),
            market_cap=cg_data.get("market_cap"),
            volume_24h=cg_data.get("volume_24h"),
            ath=ath,
            tvl=tvl,
            fees_24h=fees,
        )
    except Exception as e:
        await log(f"[WARN] Phase 1: {e}")

    # ── PHASE 1b: 30d relative performance ──────────────────────────────
    await log("PHASE 1b → 30d relative performance vs BTC / ETH / SOL")
    try:
        asset_id = COINGECKO_IDS.get(asset.upper(), asset.lower())
        prices_asset, prices_btc, prices_eth, prices_sol = await asyncio.gather(
            _cg_price_history(asset_id, 30),
            _cg_price_history("bitcoin", 30),
            _cg_price_history("ethereum", 30),
            _cg_price_history("solana", 30),
        )

        def pct_change(prices):
            if len(prices) >= 2 and prices[0]:
                return (prices[-1] - prices[0]) / prices[0] * 100
            return None

        pc_asset = pct_change(prices_asset)
        pc_btc   = pct_change(prices_btc)
        pc_eth   = pct_change(prices_eth)
        pc_sol   = pct_change(prices_sol)

        if pc_asset is not None:
            outperforms = [
                t for t, v in [("BTC", pc_btc), ("ETH", pc_eth), ("SOL", pc_sol)]
                if v is not None and pc_asset > v
            ]
            await log(
                f"✓ {asset} 30d: {pc_asset:+.2f}% | "
                f"BTC {pc_btc:+.2f}% | ETH {pc_eth:+.2f}% | SOL {pc_sol:+.2f}% | "
                f"Outperforms: {', '.join(outperforms) if outperforms else 'none'}"
            )
            result.raw_notes.append(
                f"30d perf: {asset} {pc_asset:+.2f}% vs BTC {pc_btc:+.2f}% ETH {pc_eth:+.2f}% SOL {pc_sol:+.2f}%"
            )
    except Exception as e:
        await log(f"[WARN] Phase 1b: {e}")

    # ── PHASE 2: Funding rates ───────────────────────────────────────────
    await log("PHASE 2 → Funding rates (Binance / Bybit / OKX)")
    try:
        result.funding_rates = await funding.scrape_funding_rates(asset, log=log)
        for r_ in result.funding_rates:
            await log(f"✓ {r_.exchange}: {r_.rate_8h:+.4f}%/8h | Mark —")
    except Exception as e:
        await log(f"[WARN] Phase 2: {e}")

    # ── PHASE 3: Hyperliquid OI & funding ────────────────────────────────
    await log("PHASE 3 → Hyperliquid OI & funding")
    try:
        hl_market = await hyperliquid.scrape_market_data(asset, log=log)
        if hl_market:
            result.token_metrics.open_interest = hl_market.get("open_interest")
            result.token_metrics.funding_rate_hl = hl_market.get("funding_rate")
            result.token_metrics.price_usd = result.token_metrics.price_usd or hl_market.get("mark_price")
            hl_rate = hl_market.get("funding_rate", 0)
            result.funding_rates.insert(0, FundingRate(
                exchange="Hyperliquid", asset=asset,
                rate_8h=round(hl_rate * 100, 4),
                annualized=round(hl_rate * 100 * 3 * 365, 2),
            ))
            await log(
                f"✓ HL: ${hl_market.get('mark_price', 0):,.4f} | "
                f"OI ${hl_market.get('open_interest', 0)/1e6:.2f}M | "
                f"Funding {hl_rate:+.4%}/8h | "
                f"Vol ${hl_market.get('day_volume', 0)/1e6:.2f}M"
            )
    except Exception as e:
        await log(f"[WARN] Phase 3: {e}")

    # ── PHASE 3b: Price chart & key levels ──────────────────────────────
    await log("PHASE 3b → Price chart data (72h candles)")
    try:
        candles_1h, candles_4h = await asyncio.gather(
            _hl_candles(asset, "1h", 168),   # 7 days hourly
            _hl_candles(asset, "4h", 180),   # 30 days 4h
        )
        s1_1h, r1_1h = _calc_sr(candles_1h)
        s1_4h, r1_4h = _calc_sr(candles_4h)
        s1 = max(s1_1h, s1_4h * 0.98)   # tighter of the two
        r1 = min(r1_1h, r1_4h * 1.02) if r1_1h and r1_4h else (r1_1h or r1_4h)
        if candles_1h or candles_4h:
            await log(
                f"✓ {len(candles_1h)} candles (1h·7d) + {len(candles_4h)} (4h·30d) | "
                f"S1 ${s1:,.4f} | R1 ${r1:,.4f}"
            )
            result.raw_notes.append(f"Chart: S1 ${s1:,.4f} | R1 ${r1:,.4f}")
            price = result.token_metrics.price_usd or 0
            if price and s1 and r1:
                stop = s1 * 0.955
                target = price + (r1 - price) * 1.06
                result.raw_notes.append(
                    f"Chart: ENTRY ${price:,.4f} | STOP ${stop:,.4f} | TARGET ${target:,.4f}"
                )
                await log(f"Chart: ENTRY ${price:,.4f} | STOP ${stop:,.4f} | TARGET ${target:,.4f}")
    except Exception as e:
        await log(f"[WARN] Phase 3b: {e}")

    # ── PHASE 4: Leaderboard + top 20 wallet positions ──────────────────
    await log("PHASE 4 → Hyperliquid leaderboard")
    try:
        traders = await hyperliquid.scrape_leaderboard(top_n=20, log=log)
        if traders:
            top = traders[0]
            await log(f"✓ Leaderboard: {len(traders)} traders | Top: {top.address[:6]}…{top.address[-4:]} PnL ${top.pnl_all_time/1e6:.2f}M")

        await log(f"PHASE 4b → Scanning ALL {len(traders)} trader wallets for {asset} positions")
        sem = asyncio.Semaphore(4)

        async def fetch_profile(trader):
            async with sem:
                profile = await hyperliquid.scrape_trader_profile(
                    trader.address, asset_filter=asset, log=None
                )
                trader.positions = profile.positions
                return trader

        all_detailed = await asyncio.gather(*[fetch_profile(t) for t in traders])
        result.top_traders = list(all_detailed)

        # Log positions found
        positions_with_asset = [(t, p) for t in result.top_traders for p in t.positions]
        whale_threshold = 100_000
        for trader, pos in positions_with_asset:
            whale = " 🐋" if pos.size_usd >= whale_threshold else ""
            rank = trader.rank
            addr = f"{trader.address[:6]}…{trader.address[-4:]}"
            await log(
                f"#{rank} {addr} → {pos.side} ${pos.size_usd:,.2f} "
                f"@ ${pos.entry_price:,.4f} x{pos.leverage} "
                f"uPnL ${pos.unrealized_pnl:,.2f}{whale}"
            )

        whale_count = sum(1 for _, p in positions_with_asset if p.size_usd >= whale_threshold)
        await log(
            f"{len(positions_with_asset)} total positions | "
            f"{whale_count} whale positions ≥$100K"
        )
    except Exception as e:
        await log(f"[WARN] Phase 4: {e}")

    # ── PHASE 5: Known CT wallets ────────────────────────────────────────
    await log("PHASE 5 → Known CT / whale wallet scan")
    try:
        ct_positions = []
        sem2 = asyncio.Semaphore(3)
        async def check_ct(name, address):
            async with sem2:
                profile = await hyperliquid.scrape_trader_profile(address, asset_filter=asset)
                for p in profile.positions:
                    ct_positions.append((name, address, p))

        await asyncio.gather(*[check_ct(n, a) for a, n in CT_WALLETS.items()])

        if ct_positions:
            for name, addr, pos in ct_positions:
                await log(f"  {name} ({addr[:8]}…): {pos.side} ${pos.size_usd:,.0f} @ ${pos.entry_price:,.4f}")
        else:
            await log("Known CT wallets: no open positions found (flat or addresses changed)")

        # Liquidation cluster summary from top trader positions
        all_pos = [p for t in result.top_traders for p in t.positions]
        long_entries = [p.entry_price for p in all_pos if p.side == "LONG" and p.entry_price]
        short_entries = [p.entry_price for p in all_pos if p.side == "SHORT" and p.entry_price]
        long_cluster = min(long_entries) * 0.95 if long_entries else None
        short_cluster = max(short_entries) * 1.05 if short_entries else None
        await log(
            f"Liq clusters: Longs wash below ${long_cluster:,.2f}" if long_cluster else "Liq clusters: Longs wash below —",
        )
        await log(
            f"  | Shorts above ${short_cluster:,.2f}" if short_cluster else "  | Shorts above —",
        )

        longs = sum(1 for p in all_pos if p.side == "LONG")
        total = len(all_pos)
        await log(f"Position bias: {longs/total*100:.0f}% LONG ({total} positions)" if total else "Position bias: no positions")

    except Exception as e:
        await log(f"[WARN] Phase 5: {e}")

    # ── PHASE 6: Coinglass liquidation heatmap ───────────────────────────
    await log("PHASE 6 → Coinglass liquidation heatmap")
    try:
        result.liquidation_zones = await coinglass.scrape_liquidation_zones(asset, log=log)
    except Exception as e:
        await log(f"[WARN] Phase 6: {e}")

    # ── PHASE 7: Exchange liquidation orders (Python, no CORS) ───────────
    await log("PHASE 7 → Exchange liquidation orders (last 24h)")
    try:
        liq_data = await liq_scraper.fetch_all_liquidations(asset, log=log)
        result.raw_notes.append(f"Liquidation orders: {liq_data['total']} events across exchanges")
    except Exception as e:
        await log(f"[WARN] Phase 7: {e}")

    # ── PHASE 8: Dune Analytics ──────────────────────────────────────────
    await log("PHASE 8 → Dune Analytics (on-chain Hyperliquid data)")
    try:
        dune_rows = await dune_scraper.fetch_dune_data(asset, log=log)
        if dune_rows:
            result.raw_notes.append(f"Dune on-chain data: {len(dune_rows)} rows")
            for row in dune_rows[:3]:
                result.raw_notes.append(str(row))
    except Exception as e:
        await log(f"[WARN] Phase 8: {e}")

    # ── PHASE 9: X/Twitter KOL sentiment ────────────────────────────────
    await log("PHASE 9 → X/Twitter KOL sentiment")
    try:
        result.kol_sentiment = await twitter.scrape_kol_sentiment(asset, hours=24, log=log)
    except Exception as e:
        await log(f"[WARN] Phase 9: {e}")

    # ── Summary ──────────────────────────────────────────────────────────
    await log(
        f"═══ Research complete ═══ "
        f"Traders:{len(result.top_traders)} | "
        f"Funding:{len(result.funding_rates)} | "
        f"Liq zones:{len(result.liquidation_zones)} | "
        f"KOLs:{len(result.kol_sentiment)}"
    )

    return result
