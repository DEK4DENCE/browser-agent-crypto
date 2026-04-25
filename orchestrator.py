import asyncio
import os
from typing import Callable, Awaitable
import anthropic
from models import ResearchResult, TokenMetrics
from scrapers import hyperliquid, coinglass, funding, twitter, hypurrscan

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

Log = Callable[[str], Awaitable[None]]

MAX_PARALLEL = int(os.getenv("MAX_PARALLEL_SCRAPERS", "5"))


def parse_asset_from_query(query: str) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=10,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Extract the crypto ticker symbol from this query: '{query}'. "
                    "Reply with ONLY the ticker, uppercase, no $ sign, no explanation. "
                    "Example: HYPE or BTC or ETH"
                ),
            }
        ],
    )
    return response.content[0].text.strip().upper().replace("$", "")


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

    await log(f"═══ Starting research sweep: {asset} ═══")
    await log("Initializing browser...")

    # ── Phase 1: Token metrics (CoinGecko + DefiLlama) ──────────────────
    await log("PHASE 1 → Token fundamentals (CoinGecko / DefiLlama)")
    try:
        cg_data, dl_data = await asyncio.gather(
            twitter.scrape_coingecko(asset, log=log),
            twitter.scrape_defillama(asset, log=log),
        )
        result.token_metrics = TokenMetrics(
            price_usd=cg_data.get("price_usd"),
            market_cap=cg_data.get("market_cap"),
            volume_24h=cg_data.get("volume_24h"),
        )
    except Exception as e:
        await log(f"[WARN] Phase 1 error: {e}")

    # ── Phase 2: Funding rates (pure API, no browser) ────────────────────
    await log("PHASE 2 → Funding rates (Binance / OKX / Bybit)")
    try:
        result.funding_rates = await funding.scrape_funding_rates(asset, log=log)
    except Exception as e:
        await log(f"[WARN] Phase 2 error: {e}")

    # ── Phase 3: Hyperliquid trade page (OI + HL funding) ───────────────
    await log("PHASE 3 → Hyperliquid OI & funding")
    try:
        hl_market = await hyperliquid.scrape_trade_page(asset, log=log)
        result.token_metrics.open_interest = hl_market.get("open_interest")
        result.token_metrics.funding_rate_hl = hl_market.get("funding_rate")
        if hl_market.get("funding_rate") is not None:
            from models import FundingRate
            result.funding_rates.insert(
                0,
                FundingRate(
                    exchange="Hyperliquid",
                    asset=asset,
                    rate_8h=round(hl_market["funding_rate"] * 100, 4),
                    annualized=round(hl_market["funding_rate"] * 100 * 3 * 365, 2),
                ),
            )
    except Exception as e:
        await log(f"[WARN] Phase 3 error: {e}")

    # ── Phase 4: Leaderboard scrape ──────────────────────────────────────
    await log("PHASE 4 → Hyperliquid leaderboard top 20")
    try:
        traders = await hyperliquid.scrape_leaderboard(top_n=20, log=log)

        # Scrape top 5 profiles in detail (with semaphore)
        await log("PHASE 4b → Scraping top 5 trader profiles & positions")
        sem = asyncio.Semaphore(3)

        async def fetch_profile(trader):
            async with sem:
                profile = await hyperliquid.scrape_trader_profile(
                    trader.address, asset_filter=asset, log=log
                )
                trader.positions = profile.positions
                return trader

        top5 = traders[:5]
        top5_detailed = await asyncio.gather(*[fetch_profile(t) for t in top5])
        result.top_traders = list(top5_detailed) + traders[5:]
    except Exception as e:
        await log(f"[WARN] Phase 4 error: {e}")

    # ── Phase 5: Liquidation zones ───────────────────────────────────────
    await log("PHASE 5 → Coinglass liquidation heatmap")
    try:
        result.liquidation_zones = await coinglass.scrape_liquidation_zones(asset, log=log)
    except Exception as e:
        await log(f"[WARN] Phase 5 error: {e}")

    # ── Phase 6: Whale wallets ───────────────────────────────────────────
    await log("PHASE 6 → Hypurrscan whale wallets")
    try:
        result.whale_wallets = await hypurrscan.scrape_whale_wallets(asset, log=log)
    except Exception as e:
        await log(f"[WARN] Phase 6 error: {e}")

    # ── Phase 7: X/Twitter KOL sentiment ────────────────────────────────
    await log("PHASE 7 → X/Twitter KOL sentiment scan")
    try:
        result.kol_sentiment = await twitter.scrape_kol_sentiment(asset, hours=24, log=log)
    except Exception as e:
        await log(f"[WARN] Phase 7 error: {e}")

    # ── Summary ──────────────────────────────────────────────────────────
    await log(
        f"═══ Research complete ═══ "
        f"Traders:{len(result.top_traders)} | "
        f"Funding:{len(result.funding_rates)} | "
        f"Liq zones:{len(result.liquidation_zones)} | "
        f"Whales:{len(result.whale_wallets)} | "
        f"KOLs:{len(result.kol_sentiment)}"
    )
    await log(
        f"Long bias: {result.long_bias_pct:.0f}% | "
        f"Avg funding 8h: {result.avg_funding_8h:+.4f}% | "
        f"Whale signal: {result.whale_signal} | "
        f"KOL bullish: {result.kol_bullish_pct:.0f}%"
    )

    return result
