"""
Dune Analytics — uses "Get Latest Query Result" endpoint.
GET https://api.dune.com/api/v1/query/{query_id}/results
No execution needed — returns cached results instantly, free tier safe.

Known Hyperliquid query IDs (public, pre-run):
  3525054 — HL Daily Volume
  2891521 — HL Cumulative Stats
  3196457 — HL Top Assets by OI

Set DUNE_API_KEY in .env or via the web UI settings panel.
Override query IDs: DUNE_QUERY_IDS=3525054,2891521,3196457
"""
import asyncio
import aiohttp
import os
from typing import Callable, Awaitable

Log = Callable[[str], Awaitable[None]]

DUNE_RESULTS_URL = "https://api.dune.com/api/v1/query/{query_id}/results"
_TIMEOUT = aiohttp.ClientTimeout(total=20)

DEFAULT_QUERY_IDS = [
    "3525054",  # HL Daily Volume
    "2891521",  # HL Cumulative Stats
    "3196457",  # HL Top Assets by OI
]


def _get_query_ids() -> list[str]:
    env_ids = os.getenv("DUNE_QUERY_IDS", "")
    if env_ids:
        return [x.strip() for x in env_ids.split(",") if x.strip()]
    return DEFAULT_QUERY_IDS


async def _fetch_latest_result(
    session: aiohttp.ClientSession,
    api_key: str,
    query_id: str,
) -> dict | None:
    """
    GET /api/v1/query/{query_id}/results — returns the last cached result.
    No execution credits consumed, responds immediately.
    """
    url = DUNE_RESULTS_URL.format(query_id=query_id)
    try:
        async with session.get(
            url,
            params={"limit": 10},
            headers={"x-dune-api-key": api_key},
        ) as r:
            if r.status == 401:
                return {"_error": "invalid_key"}
            if r.status == 404:
                return {"_error": f"query {query_id} not found or not public"}
            if r.status != 200:
                return {"_error": f"HTTP {r.status}"}
            data = await r.json()
            rows = data.get("result", {}).get("rows", [])
            cols = data.get("result", {}).get("metadata", {}).get("column_names", [])
            return {
                "query_id": query_id,
                "rows": rows,
                "columns": cols,
                "row_count": len(rows),
            }
    except Exception as e:
        return {"_error": str(e)}


async def fetch_dune_data(asset: str, log: Log = None) -> list[dict]:
    """
    Fetch latest results from all configured Dune queries.
    Returns list of result dicts with rows.
    """
    # Key can come from env or runtime store (set via /api/keys endpoint)
    from server import get_key  # import at call time to avoid circular import
    api_key = get_key("DUNE_API_KEY")

    if not api_key:
        if log:
            await log("[WARN] Dune: DUNE_API_KEY not set — add it in ⚙ settings or .env")
        return []

    query_ids = _get_query_ids()
    if log:
        await log(f"Fetching {len(query_ids)} Dune queries (cached results, no credits used)...")

    all_rows: list[dict] = []

    async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
        tasks = [_fetch_latest_result(session, api_key, qid) for qid in query_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for qid, result in zip(query_ids, results):
            if isinstance(result, Exception):
                if log:
                    await log(f"  [WARN] Dune query {qid}: {result}")
                continue
            if result is None or result.get("_error"):
                err = result.get("_error", "no data") if result else "no data"
                if log:
                    await log(f"  [WARN] Dune query {qid}: {err}")
                continue
            rows = result.get("rows", [])
            if log:
                await log(f"  ✓ Dune {qid}: {len(rows)} rows — {result.get('columns', [])}")
            for row in rows:
                row["_query_id"] = qid
                all_rows.append(row)

    if log:
        if all_rows:
            await log(f"✓ Dune: {len(all_rows)} total data points retrieved")
        else:
            await log("[WARN] Dune: no data — queries may need to be run on dune.com first")

    return all_rows
