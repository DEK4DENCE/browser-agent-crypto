import asyncio
import re
from typing import Callable, Awaitable
from datetime import datetime
from browser import BrowserManager
from models import KOLSentiment

Log = Callable[[str], Awaitable[None]]

X_SEARCH_URL = "https://x.com/search?q=%24{asset}&src=typed_query&f=top"

BULLISH_WORDS = [
    "bullish", "buy", "long", "moon", "pump", "accumulate", "ape",
    "strong", "breakout", "support", "hold", "hodl", "entry", "load",
    "dip", "green", "rip", "higher", "up only",
]
BEARISH_WORDS = [
    "bearish", "sell", "short", "dump", "correction", "rug", "fud",
    "exit", "stop", "down", "weak", "resistance", "red", "caution",
    "careful", "danger", "warning", "overbought",
]


def classify_sentiment(text: str) -> bool:
    text_lower = text.lower()
    bull_score = sum(1 for w in BULLISH_WORDS if w in text_lower)
    bear_score = sum(1 for w in BEARISH_WORDS if w in text_lower)
    return bull_score >= bear_score


async def scrape_kol_sentiment(
    asset: str,
    hours: int = 24,
    max_tweets: int = 20,
    log: Log = None,
) -> list[KOLSentiment]:
    if log:
        await log(f"Scanning X/Twitter for ${asset} sentiment (last {hours}h)...")
    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    tweets: list[KOLSentiment] = []

    try:
        url = X_SEARCH_URL.format(asset=asset)
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(5)

        # Check if we hit a login wall
        page_text = await page.content()
        if "log in" in page_text.lower() and "twitter" in page_text.lower():
            if log:
                await log("[WARN] X requires login — using DOM scrape of visible tweets")

        # Scroll to load more tweets
        for _ in range(4):
            await page.keyboard.press("End")
            await asyncio.sleep(1.5)

        # Extract tweet content from DOM
        tweet_elements = await page.query_selector_all("article[data-testid='tweet']")

        for el in tweet_elements[:max_tweets]:
            try:
                # Author
                author_el = await el.query_selector("[data-testid='User-Name'] span")
                author = await author_el.inner_text() if author_el else "unknown"

                # Text
                text_el = await el.query_selector("[data-testid='tweetText']")
                text = await text_el.inner_text() if text_el else ""
                if not text:
                    continue

                # Time
                time_el = await el.query_selector("time")
                timestamp = await time_el.get_attribute("datetime") if time_el else datetime.now().isoformat()

                # Link
                link_el = await el.query_selector("a[href*='/status/']")
                tweet_url = None
                if link_el:
                    href = await link_el.get_attribute("href")
                    tweet_url = f"https://x.com{href}" if href and href.startswith("/") else href

                # Likes
                likes_el = await el.query_selector("[data-testid='like'] span")
                likes_raw = await likes_el.inner_text() if likes_el else "0"
                try:
                    likes = int(likes_raw.replace(",", "").replace("K", "000").replace("M", "000000"))
                except ValueError:
                    likes = 0

                bullish = classify_sentiment(text)
                tweets.append(
                    KOLSentiment(
                        author=author,
                        text=text[:280],
                        timestamp=timestamp or datetime.now().isoformat(),
                        bullish=bullish,
                        url=tweet_url,
                        likes=likes,
                    )
                )
            except Exception:
                continue

    except Exception as e:
        if log:
            await log(f"[WARN] Twitter scrape issue: {e}")
    finally:
        await page.close()

    # sort by likes
    tweets.sort(key=lambda t: t.likes, reverse=True)

    if log:
        bullish_count = sum(1 for t in tweets if t.bullish)
        await log(
            f"X sentiment: {len(tweets)} tweets — "
            f"{bullish_count} bullish / {len(tweets) - bullish_count} bearish"
        )

    return tweets


async def scrape_coingecko(asset: str, log: Log = None) -> dict:
    """Scrape CoinGecko for token metrics."""
    if log:
        await log(f"Fetching CoinGecko data for {asset}...")
    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    metrics: dict = {}

    try:
        # Try to find the coin page
        search_url = f"https://www.coingecko.com/en/search?query={asset}"
        await page.goto(search_url, wait_until="domcontentloaded", timeout=25000)
        await asyncio.sleep(3)

        # Click the first coin result
        coin_link = page.locator("a[href*='/en/coins/']").first
        if await coin_link.count() > 0:
            href = await coin_link.get_attribute("href")
            if href:
                await page.goto(f"https://www.coingecko.com{href}", wait_until="domcontentloaded", timeout=25000)
                await asyncio.sleep(3)

                # Extract price
                price_el = page.locator("[data-target='price.price']").first
                if await price_el.count() == 0:
                    price_el = page.locator(".no-wrap span").first
                price_text = await price_el.inner_text() if await price_el.count() > 0 else ""

                # Market cap
                mc_text = ""
                mc_rows = page.locator("tr", has_text="Market Cap")
                if await mc_rows.count() > 0:
                    mc_text = await mc_rows.first.locator("td").last.inner_text()

                # Volume
                vol_text = ""
                vol_rows = page.locator("tr", has_text="24 Hour Trading Vol")
                if await vol_rows.count() > 0:
                    vol_text = await vol_rows.first.locator("td").last.inner_text()

                def parse_money(s: str) -> float:
                    s = s.replace("$", "").replace(",", "").strip()
                    if "B" in s:
                        return float(s.replace("B", "")) * 1e9
                    if "M" in s:
                        return float(s.replace("M", "")) * 1e6
                    if "K" in s:
                        return float(s.replace("K", "")) * 1e3
                    try:
                        return float(re.sub(r"[^\d.]", "", s))
                    except ValueError:
                        return 0.0

                metrics["price_usd"] = parse_money(price_text)
                metrics["market_cap"] = parse_money(mc_text)
                metrics["volume_24h"] = parse_money(vol_text)

                if log:
                    await log(
                        f"CoinGecko: ${metrics.get('price_usd', 0):,.4f} | "
                        f"MC ${metrics.get('market_cap', 0) / 1e9:.2f}B | "
                        f"Vol ${metrics.get('volume_24h', 0) / 1e6:.1f}M"
                    )
    except Exception as e:
        if log:
            await log(f"[WARN] CoinGecko scrape issue: {e}")
    finally:
        await page.close()

    return metrics


async def scrape_defillama(asset: str, log: Log = None) -> dict:
    """Scrape DefiLlama for protocol metrics."""
    if log:
        await log(f"Fetching DefiLlama data for {asset.lower()}...")
    mgr = await BrowserManager.get()
    page = await mgr.new_page()
    metrics: dict = {}

    try:
        url = f"https://defillama.com/protocol/{asset.lower()}"
        await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        await asyncio.sleep(3)

        # TVL is usually shown prominently
        tvl_el = page.locator("text=Total Value Locked").locator("..").locator("span").first
        if await tvl_el.count() > 0:
            metrics["tvl_text"] = await tvl_el.inner_text()

        if log:
            await log(f"DefiLlama TVL: {metrics.get('tvl_text', 'N/A')}")
    except Exception as e:
        if log:
            await log(f"[WARN] DefiLlama scrape issue: {e}")
    finally:
        await page.close()

    return metrics
