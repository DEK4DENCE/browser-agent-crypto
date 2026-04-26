"""
Research orchestrator — 9-phase pipeline.
All browser scrapers are called sequentially after eager browser init.
Pure-API phases (funding, liquidations, HL analytics) run via aiohttp — no browser, no CORS.
"""
import asyncio
import json
import os
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Awaitable
import aiohttp
import llm
from models import ResearchResult, TokenMetrics, FundingRate
from scrapers import hyperliquid, coinglass, funding, twitter, hypurrscan
from scrapers import liquidations as liq_scraper

Log = Callable[[str], Awaitable[None]]

HL_API = "https://api.hyperliquid.xyz/info"
CG_API = "https://api.coingecko.com/api/v3"
_TIMEOUT = aiohttp.ClientTimeout(total=15)

RUNS_DIR = Path(__file__).parent / "runs"
RUNS_DIR.mkdir(exist_ok=True)

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
    q = query.strip().upper().replace("$", "")
    if q.isalpha() and len(q) <= 6:
        return q
    result = llm.quick(
        f"Extract the crypto ticker symbol from: '{query}'. "
        "Reply with ONLY the ticker, uppercase, no $ or explanation. E.g. HYPE"
    )
    return result.strip().upper().replace("$", "").split()[0]


async def _cg_price_history(coin_id: str, days: int = 30) -> list[float]:
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
    if not candles:
        return 0.0, 0.0
    lows = [float(c.get("l", c.get("low", 0))) for c in candles if c.get("l") or c.get("low")]
    highs = [float(c.get("h", c.get("high", 0))) for c in candles if c.get("h") or c.get("high")]
    return (min(lows) if lows else 0.0), (max(highs) if highs else 0.0)


async def _fetch_hl_global_stats(session: aiohttp.ClientSession) -> dict:
    """Derive platform-wide stats by summing across all assets in metaAndAssetCtxs."""
    try:
        async with session.post(
            HL_API,
            json={"type": "metaAndAssetCtxs"},
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status == 200:
                data = await r.json()
                if isinstance(data, list) and len(data) == 2:
                    meta, ctxs = data[0], data[1]
                    total_oi = sum(
                        float(c.get("openInterest", 0)) * float(c.get("markPx", 0))
                        for c in ctxs
                    )
                    total_vol = sum(float(c.get("dayNtlVlm", 0)) for c in ctxs)
                    return {
                        "totalOI": total_oi,
                        "totalVol": total_vol,
                        "totalAssets": len(meta.get("universe", [])),
                    }
    except Exception:
        pass
    return {}


async def _fetch_hl_funding_history(session: aiohttp.ClientSession, asset: str) -> list[dict]:
    start_ts = int((time.time() - 30 * 86400) * 1000)
    try:
        async with session.post(
            HL_API,
            json={"type": "fundingHistory", "coin": asset, "startTime": start_ts},
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status == 200:
                data = await r.json()
                return data if isinstance(data, list) else []
    except Exception:
        pass
    return []


async def _fetch_hl_asset_ctx(session: aiohttp.ClientSession, asset: str) -> dict:
    try:
        async with session.post(
            HL_API,
            json={"type": "metaAndAssetCtxs"},
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status == 200:
                data = await r.json()
                if isinstance(data, list) and len(data) == 2:
                    meta, ctxs = data[0], data[1]
                    coins = [u["name"] for u in meta.get("universe", [])]
                    if asset in coins:
                        idx = coins.index(asset)
                        return ctxs[idx] if idx < len(ctxs) else {}
    except Exception:
        pass
    return {}


def _save_run(
    asset: str,
    query: str,
    report: str,
    result: "ResearchResult",
    errors: list[str],
    duration_secs: float,
):
    ts = int(time.time())
    run_id = f"{ts}_{asset}"
    data = {
        "id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "asset": asset,
        "query": query,
        "duration_secs": round(duration_secs, 1),
        "report": report,
        "errors": errors,
        "metrics": {
            "price_usd": result.token_metrics.price_usd,
            "market_cap": result.token_metrics.market_cap,
            "open_interest": result.token_metrics.open_interest,
            "funding_rate_hl": result.token_metrics.funding_rate_hl,
            "long_bias_pct": result.long_bias_pct,
            "whale_signal": result.whale_signal,
            "num_traders": len(result.top_traders),
            "num_funding": len(result.funding_rates),
        },
    }
    path = RUNS_DIR / f"run_{run_id}.json"
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


async def run_research(
    asset: str,
    query: str = "",
    log_queue: asyncio.Queue | None = None,
) -> ResearchResult:

    run_errors: list[str] = []
    run_start = time.time()

    async def log(msg: str):
        print(f"[agent] {msg}")
        if log_queue is not None:
            await log_queue.put({"type": "log", "message": msg})

    async def log_error(phase: str, exc: Exception):
        tb = traceback.format_exc()
        short = f"[ERROR] {phase}: {exc}"
        detail = f"[ERROR] {phase}: {exc}\n{tb}"
        run_errors.append(detail)
        print(detail)
        if log_queue is not None:
            await log_queue.put({"type": "log", "message": short})

    result = ResearchResult(asset=asset, query=query)

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
            await log(f"✓ DefiLlama: TVL ${tvl/1e9:.2f}B" + (f" | Fees/day ${fees/1e6:.2f}M" if fees else ""))
        result.token_metrics = TokenMetrics(
            price_usd=cg_data.get("price_usd"),
            market_cap=cg_data.get("market_cap"),
            volume_24h=cg_data.get("volume_24h"),
            ath=ath,
            tvl=tvl,
            fees_24h=fees,
        )
    except Exception as e:
        await log_error("Phase 1 (fundamentals)", e)

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
        await log_error("Phase 1b (30d perf)", e)

    # ── PHASE 2: Funding rates ───────────────────────────────────────────
    await log("PHASE 2 → Funding rates (Binance / Bybit / OKX)")
    try:
        result.funding_rates = await funding.scrape_funding_rates(asset, log=log)
        for r_ in result.funding_rates:
            await log(f"✓ {r_.exchange}: {r_.rate_8h:+.4f}%/8h")
    except Exception as e:
        await log_error("Phase 2 (funding rates)", e)

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
        else:
            await log(f"[WARN] Phase 3: {asset} not found on Hyperliquid perps")
    except Exception as e:
        await log_error("Phase 3 (HL market data)", e)

    # ── PHASE 3b: Price chart & key levels ──────────────────────────────
    await log("PHASE 3b → Price chart data (72h candles)")
    try:
        candles_1h, candles_4h = await asyncio.gather(
            _hl_candles(asset, "1h", 168),
            _hl_candles(asset, "4h", 180),
        )
        s1_1h, r1_1h = _calc_sr(candles_1h)
        s1_4h, r1_4h = _calc_sr(candles_4h)
        s1 = max(s1_1h, s1_4h * 0.98)
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
        else:
            await log(f"[WARN] Phase 3b: No candle data returned for {asset}")
    except Exception as e:
        await log_error("Phase 3b (candles)", e)

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
        await log_error("Phase 4 (leaderboard)", e)

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

        all_pos = [p for t in result.top_traders for p in t.positions]
        long_entries = [p.entry_price for p in all_pos if p.side == "LONG" and p.entry_price]
        short_entries = [p.entry_price for p in all_pos if p.side == "SHORT" and p.entry_price]
        long_cluster = min(long_entries) * 0.95 if long_entries else None
        short_cluster = max(short_entries) * 1.05 if short_entries else None
        await log(f"Liq clusters: Longs wash below ${long_cluster:,.2f}" if long_cluster else "Liq clusters: Longs wash below —")
        await log(f"  | Shorts above ${short_cluster:,.2f}" if short_cluster else "  | Shorts above —")

        longs = sum(1 for p in all_pos if p.side == "LONG")
        total = len(all_pos)
        await log(f"Position bias: {longs/total*100:.0f}% LONG ({total} positions)" if total else "Position bias: no positions")
    except Exception as e:
        await log_error("Phase 5 (CT wallets)", e)

    # ── PHASE 6: Coinglass liquidation heatmap ───────────────────────────
    await log("PHASE 6 → Coinglass liquidation heatmap")
    try:
        result.liquidation_zones = await coinglass.scrape_liquidation_zones(asset, log=log)
    except Exception as e:
        await log_error("Phase 6 (Coinglass)", e)

    # ── PHASE 7: Exchange data (Binance public + Bybit + OKX) ────────────
    await log("PHASE 7 → Exchange market data (Binance / Bybit / OKX)")
    try:
        liq_data = await liq_scraper.fetch_all_liquidations(asset, log=log)
        result.raw_notes.append(f"Exchange data: {liq_data['total']} liq events | Binance metrics fetched")
    except Exception as e:
        await log_error("Phase 7 (exchange data)", e)

    # ── PHASE 8: Hyperliquid Extended Analytics ──────────────────────────
    await log("PHASE 8 → Hyperliquid extended analytics (global stats / funding history)")
    try:
        async with aiohttp.ClientSession() as session:
            global_stats, funding_hist, asset_ctx = await asyncio.gather(
                _fetch_hl_global_stats(session),
                _fetch_hl_funding_history(session, asset),
                _fetch_hl_asset_ctx(session, asset),
            )

        if global_stats:
            total_oi = float(global_stats.get("totalOI", 0))
            total_vol = float(global_stats.get("totalVol", 0))
            total_assets = global_stats.get("totalAssets", 0)
            await log(f"  ✓ HL Platform: OI ${total_oi/1e9:.2f}B | 24h vol ${total_vol/1e9:.2f}B | {total_assets} assets listed")
            result.raw_notes.append(f"HL Platform: OI ${total_oi/1e9:.1f}B | 24h vol ${total_vol/1e9:.1f}B | {total_assets} assets")

        if asset_ctx:
            oi = float(asset_ctx.get("openInterest", 0))
            vol = float(asset_ctx.get("dayNtlVlm", 0))
            premium = float(asset_ctx.get("premium", 0))
            await log(f"  ✓ HL {asset}: OI ${oi:,.0f} | 24h vol ${vol:,.0f} | Premium {premium:+.4%}")
            result.raw_notes.append(f"HL {asset}: OI ${oi/1e6:.1f}M | 24h vol ${vol/1e6:.1f}M")
        else:
            await log(f"  [WARN] Phase 8: {asset} not found in metaAndAssetCtxs")

        if funding_hist:
            rates = [float(f.get("fundingRate", 0)) for f in funding_hist]
            avg_r = sum(rates) / len(rates) if rates else 0
            max_r = max(rates) if rates else 0
            min_r = min(rates) if rates else 0
            await log(
                f"  ✓ HL 30d funding ({len(rates)} samples): "
                f"avg {avg_r*100:.4f}% | max {max_r*100:.4f}% | min {min_r*100:.4f}%"
            )
            result.raw_notes.append(
                f"30d funding: avg {avg_r*100:.4f}%/8h ({avg_r*100*3*365:.1f}% ann.) | "
                f"max {max_r*100:.4f}% | min {min_r*100:.4f}%"
            )
        else:
            await log(f"  [WARN] Phase 8: No funding history for {asset}")

    except Exception as e:
        await log_error("Phase 8 (HL analytics)", e)

    # ── PHASE 9: X/Twitter KOL sentiment ────────────────────────────────
    await log("PHASE 9 → X/Twitter KOL sentiment")
    try:
        result.kol_sentiment = await twitter.scrape_kol_sentiment(asset, hours=24, log=log)
    except Exception as e:
        await log_error("Phase 9 (KOL sentiment)", e)

    # ── Summary ──────────────────────────────────────────────────────────
    duration = time.time() - run_start
    error_count = len(run_errors)
    await log(
        f"═══ Research complete in {duration:.0f}s ═══ "
        f"Traders:{len(result.top_traders)} | "
        f"Funding:{len(result.funding_rates)} | "
        f"Liq zones:{len(result.liquidation_zones)} | "
        f"KOLs:{len(result.kol_sentiment)} | "
        f"Errors:{error_count}"
    )
    if run_errors:
        await log(f"[ERRORS] {error_count} phase error(s) — check server.log for full tracebacks")

    return result
