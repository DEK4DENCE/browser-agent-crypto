"""
Browser-based scrapers for CoinGecko, DefiLlama, Coinglass, and X/Twitter.
Navigates directly to known URLs — no searching, no wrong-coin results.
"""
import asyncio
import re
from typing import Callable, Awaitable
from datetime import datetime
from browser import BrowserManager
from models import KOLSentiment

Log = Callable[[str], Awaitable[None]]

# Direct coin slugs — maps ticker → CoinGecko coin page slug
COINGECKO_SLUG: dict[str, str] = {
    "HYPE": "hyperliquid",
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "binancecoin",
    "XRP": "ripple",
    "ADA": "cardano",
    "AVAX": "avalanche-2",
    "DOGE": "dogecoin",
    "DOT": "polkadot",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "AAVE": "aave",
    "OP": "optimism",
    "ARB": "arbitrum",
    "SUI": "sui",
    "APT": "aptos",
    "INJ": "injective-protocol",
    "TIA": "celestia",
    "ATOM": "cosmos",
    "NEAR": "near",
    "WIF": "dogwifcoin",
    "BONK": "bonk",
    "PEPE": "pepe",
    "SHIB": "shiba-inu",
    "GMX": "gmx",
    "JUP": "jupiter-exchange-solana",
    "PENDLE": "pendle",
}

DEFILLAMA_SLUG: dict[str, str] = {
    "HYPE": "hyperliquid",
    "AAVE": "aave",
    "UNI": "uniswap",
    "GMX": "gmx",
    "DYDX": "dydx",
    "JUP": "jupiter",
    "PENDLE": "pendle",
    "CURVE": "curve-dex",
}

BULLISH_WORDS = [
    "bullish", "buy", "long", "moon", "pump", "accumulate", "ape",
    "strong", "breakout", "support", "hold", "hodl", "entry", "load",
    "dip", "green", "rip", "higher", "up only", "giga", "based",
]
BEARISH_WORDS = [
    "bearish", "sell", "short", "dump", "correction", "rug", "fud",
    "exit", "stop", "down", "weak", "resistance", "red", "caution",
    "careful", "danger", "warning", "overbought", "topped",
]
NITTER_INSTANCES = [
    "https://nitter.net",
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
]


def classify_sentiment(text: str) -> bool:
    t = text.lower()
    return sum(1 for w in BULLISH_WORDS if w in t) >= sum(1 for w in BEARISH_WORDS if w in t)


def _parse_money(s: str) -> float:
    s = re.sub(r"[,$€£¥]", "", s).strip()
    try:
        if s.endswith("T"):
            return float(s[:-1]) * 1e12
        if s.endswith("B"):
            return float(s[:-1]) * 1e9
        if s.endswith("M"):
            return float(s[:-1]) * 1e6
        if s.endswith("K"):
            return float(s[:-1]) * 1e3
        return float(s.replace(",", ""))
    except ValueError:
        return 0.0


# ── CoinGecko ────────────────────────────────────────────────────────────────

async def scrape_coingecko(asset: str, log: Log = None) -> dict:
    slug = COINGECKO_SLUG.get(asset.upper(), asset.lower())
    url = f"https://www.coingecko.com/en/coins/{slug}"
    if log:
        await log(f"Opening CoinGecko → {url}")

    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    metrics: dict = {}

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(4)

        # Price — try multiple selectors CoinGecko uses
        for sel in [
            "[data-target='page.price']",
            "[data-coin-target='price']",
            "span[class*='text-3xl']",
            "span[class*='no-wrap']",
            "div[class*='price'] span",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    text = (await el.inner_text()).strip()
                    val = _parse_money(text)
                    if val > 0:
                        metrics["price_usd"] = val
                        break
            except Exception:
                pass

        # Evaluate JS to find price in the page if DOM selectors fail
        if not metrics.get("price_usd"):
            try:
                price_js = await page.evaluate("""
                    () => {
                        const els = document.querySelectorAll('span, div');
                        for (const el of els) {
                            const t = el.innerText;
                            if (t && t.startsWith('$') && /^\\$[\\d,.]+$/.test(t.trim())) {
                                const n = parseFloat(t.replace(/[$,]/g, ''));
                                if (n > 0.0001 && n < 1000000) return n;
                            }
                        }
                        return 0;
                    }
                """)
                if price_js:
                    metrics["price_usd"] = float(price_js)
            except Exception:
                pass

        # Market cap, volume — look for stat rows
        page_text = await page.inner_text("body")
        lines = [l.strip() for l in page_text.split("\n") if l.strip()]

        for i, line in enumerate(lines):
            if "Market Cap" in line and i + 1 < len(lines):
                val = _parse_money(lines[i + 1])
                if val > 0:
                    metrics["market_cap"] = val
            if ("24h Trading Vol" in line or "24 Hour Trading Vol" in line) and i + 1 < len(lines):
                val = _parse_money(lines[i + 1])
                if val > 0:
                    metrics["volume_24h"] = val
            if "All-Time High" in line and i + 1 < len(lines):
                val = _parse_money(lines[i + 1])
                if val > 0:
                    metrics["ath"] = val
            if "All-Time Low" in line and i + 1 < len(lines):
                val = _parse_money(lines[i + 1])
                if val > 0:
                    metrics["atl"] = val

        if log:
            p = metrics.get("price_usd", 0)
            mc = metrics.get("market_cap", 0)
            vol = metrics.get("volume_24h", 0)
            await log(
                f"CoinGecko: ${p:,.4f} | MC ${mc/1e9:.2f}B | Vol ${vol/1e6:.1f}M"
                if p else "CoinGecko: price not extracted"
            )
    except Exception as e:
        if log:
            await log(f"[WARN] CoinGecko: {e}")
    finally:
        await page.close()

    return metrics


# ── DefiLlama ────────────────────────────────────────────────────────────────

async def scrape_defillama(asset: str, log: Log = None) -> dict:
    slug = DEFILLAMA_SLUG.get(asset.upper(), asset.lower())
    url = f"https://defillama.com/protocol/{slug}"
    if log:
        await log(f"Opening DefiLlama → {url}")

    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    metrics: dict = {}

    async def on_response(response):
        try:
            if "api.llama.fi" in response.url and response.status == 200:
                data = await response.json()
                if isinstance(data, dict) and "tvl" in data:
                    tvl_series = data.get("tvl", [])
                    if tvl_series:
                        metrics["tvl"] = float(tvl_series[-1].get("totalLiquidityUSD", 0))
                    fees = data.get("totalDataChart")
                    if fees:
                        metrics["fees_24h_text"] = str(fees[-1] if fees else "")
        except Exception:
            pass

    page.on("response", on_response)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(5)

        # Read visible numbers from page body
        page_text = await page.inner_text("body")
        lines = [l.strip() for l in page_text.split("\n") if l.strip()]

        for i, line in enumerate(lines):
            l_low = line.lower()
            if "total value locked" in l_low or "tvl" in l_low:
                if i + 1 < len(lines):
                    val = _parse_money(lines[i + 1])
                    if val > 0:
                        metrics["tvl"] = val
            if "fees" in l_low and i + 1 < len(lines):
                val = _parse_money(lines[i + 1])
                if val > 0 and "fees_24h" not in metrics:
                    metrics["fees_24h"] = val
            if "revenue" in l_low and i + 1 < len(lines):
                val = _parse_money(lines[i + 1])
                if val > 0 and "revenue_24h" not in metrics:
                    metrics["revenue_24h"] = val

        if log:
            tvl = metrics.get("tvl", 0)
            await log(f"DefiLlama [{slug}]: TVL ${tvl/1e9:.3f}B" if tvl else f"DefiLlama: loaded (TVL not parsed)")
    except Exception as e:
        if log:
            await log(f"[WARN] DefiLlama: {e}")
    finally:
        await page.close()

    return metrics


# ── X / Twitter ──────────────────────────────────────────────────────────────

async def scrape_kol_sentiment(
    asset: str,
    hours: int = 24,
    max_tweets: int = 20,
    log: Log = None,
) -> list[KOLSentiment]:
    if log:
        await log(f"Scanning X/Twitter for ${asset} sentiment...")

    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    tweets: list[KOLSentiment] = []

    # Try X directly first
    try:
        url = f"https://x.com/search?q=%24{asset}&src=typed_query&f=top"
        await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        await asyncio.sleep(5)

        # Check for login wall
        content = await page.content()
        requires_login = "Log in" in content and len(content) < 50000

        if not requires_login:
            for _ in range(3):
                await page.keyboard.press("End")
                await asyncio.sleep(1.5)

            articles = await page.query_selector_all("article[data-testid='tweet']")
            for el in articles[:max_tweets]:
                try:
                    author_el = await el.query_selector("[data-testid='User-Name'] span")
                    text_el = await el.query_selector("[data-testid='tweetText']")
                    time_el = await el.query_selector("time")
                    link_el = await el.query_selector("a[href*='/status/']")
                    likes_el = await el.query_selector("[data-testid='like'] span")

                    author = (await author_el.inner_text()).strip() if author_el else "unknown"
                    text = (await text_el.inner_text()).strip() if text_el else ""
                    if not text:
                        continue
                    ts = await time_el.get_attribute("datetime") if time_el else datetime.now().isoformat()
                    href = await link_el.get_attribute("href") if link_el else ""
                    tweet_url = f"https://x.com{href}" if href and href.startswith("/") else href
                    likes_raw = (await likes_el.inner_text()).strip() if likes_el else "0"
                    try:
                        likes_raw = likes_raw.replace(",", "")
                        if "K" in likes_raw:
                            likes = int(float(likes_raw.replace("K", "")) * 1000)
                        else:
                            likes = int(likes_raw) if likes_raw.isdigit() else 0
                    except Exception:
                        likes = 0

                    tweets.append(KOLSentiment(
                        author=author, text=text[:280],
                        timestamp=ts or datetime.now().isoformat(),
                        bullish=classify_sentiment(text),
                        url=tweet_url, likes=likes,
                    ))
                except Exception:
                    continue
        else:
            if log:
                await log("  X login required — trying nitter...")
    except Exception as e:
        if log:
            await log(f"[WARN] X direct: {e}")
    finally:
        await page.close()

    # Nitter fallback
    if not tweets:
        for instance in NITTER_INSTANCES:
            page2 = await mgr.new_page()
            try:
                await page2.goto(
                    f"{instance}/search?q=%24{asset}&f=tweets",
                    wait_until="domcontentloaded", timeout=20000,
                )
                await asyncio.sleep(3)
                items = await page2.query_selector_all(".timeline-item")
                for item in items[:max_tweets]:
                    try:
                        name_el = await item.query_selector(".fullname")
                        text_el = await item.query_selector(".tweet-content")
                        author = (await name_el.inner_text()).strip() if name_el else "unknown"
                        text = (await text_el.inner_text()).strip() if text_el else ""
                        if not text:
                            continue
                        tweets.append(KOLSentiment(
                            author=author, text=text[:280],
                            timestamp=datetime.now().isoformat(),
                            bullish=classify_sentiment(text),
                        ))
                    except Exception:
                        continue
                if tweets:
                    if log:
                        await log(f"  Got {len(tweets)} tweets from {instance}")
                    break
            except Exception:
                pass
            finally:
                await page2.close()

    tweets.sort(key=lambda t: t.likes, reverse=True)

    if log:
        bull = sum(1 for t in tweets if t.bullish)
        if tweets:
            await log(f"X: {len(tweets)} tweets — {bull} bullish / {len(tweets) - bull} bearish")
        else:
            await log("[WARN] X: no tweets found — add cookies.json for X login or use nitter")

    return tweets
