"""
Dune Analytics scraper — uses the Dune API v1.
Get a free API key at https://dune.com/settings/api

Known Hyperliquid query IDs (public dashboards):
  3455783 — Hyperliquid perps: volume, fees, traders
  3685970 — Hyperliquid top traders by PnL
  3260847 — Hyperliquid open interest over time
  3876543 — Hyperliquid liquidations
Set DUNE_QUERY_IDS=3455783,3685970 in .env to override defaults.
"""
import asyncio
import aiohttp
import os
from typing import Callable, Awaitable

Log = Callable[[str], Awaitable[None]]

DUNE_API_BASE = "https://api.dune.com/api/v1"
_TIMEOUT = aiohttp.ClientTimeout(total=60)

# Default query IDs targeting Hyperliquid on-chain data
DEFAULT_QUERY_IDS = [
    3455783,  # Hyperliquid perps volume + fees
    3685970,  # Top traders by PnL
]


def _get_query_ids() -> list[int]:
    env_ids = os.getenv("DUNE_QUERY_IDS", "")
    if env_ids:
        try:
            return [int(x.strip()) for x in env_ids.split(",") if x.strip()]
        except ValueError:
            pass
    return DEFAULT_QUERY_IDS


async def _execute_query(session: aiohttp.ClientSession, api_key: str, query_id: int) -> dict | None:
    """Execute a Dune query and poll for result."""
    headers = {"X-DUNE-API-KEY": api_key, "Content-Type": "application/json"}

    # POST to execute
    try:
        async with session.post(
            f"{DUNE_API_BASE}/query/{query_id}/execute",
            headers=headers,
            json={"performance": "medium"},
        ) as r:
            if r.status not in (200, 201):
                return None
            exec_data = await r.json()
            execution_id = exec_data.get("execution_id")
            if not execution_id:
                return None
    except Exception:
        return None

    # Poll for result (up to 30s)
    for _ in range(15):
        await asyncio.sleep(2)
        try:
            async with session.get(
                f"{DUNE_API_BASE}/execution/{execution_id}/results",
                headers=headers,
            ) as r:
                if r.status != 200:
                    continue
                data = await r.json()
                state = data.get("state", "")
                if state == "QUERY_STATE_COMPLETED":
                    return data.get("result", {})
                if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
                    return None
        except Exception:
            continue

    return None


async def fetch_dune_data(asset: str, log: Log = None) -> list[dict]:
    """
    Run configured Dune queries and return rows of on-chain data.
    Requires DUNE_API_KEY in .env.
    """
    api_key = os.getenv("DUNE_API_KEY", "")
    if not api_key:
        if log:
            await log("[WARN] Dune: DUNE_API_KEY not set — get a free key at dune.com/settings/api")
        return []

    query_ids = _get_query_ids()
    if log:
        await log(f"Running {len(query_ids)} Dune queries for {asset} on-chain data...")

    all_rows: list[dict] = []

    async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
        tasks = [_execute_query(session, api_key, qid) for qid in query_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for qid, result in zip(query_ids, results):
            if isinstance(result, Exception) or result is None:
                if log:
                    await log(f"  [WARN] Dune query {qid}: no data")
                continue
            rows = result.get("rows", [])
            if log:
                await log(f"  Dune query {qid}: {len(rows)} rows")
            for row in rows[:20]:
                row["_query_id"] = qid
                all_rows.append(row)

    if log:
        if all_rows:
            await log(f"Dune: {len(all_rows)} total data rows retrieved")
        else:
            await log("Dune: no data returned — verify DUNE_API_KEY and query IDs are correct")

    return all_rows
