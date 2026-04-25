#!/usr/bin/env python3
"""
Crypto Browser Agent — CLI entry point.

Usage:
  python main.py "should I ape HYPE right now?"     # one-shot CLI
  python main.py --server                            # start web UI on :8000
  python main.py --swarm BTC ETH HYPE SOL           # parallel multi-asset
"""
import asyncio
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


async def run_cli(query: str):
    from orchestrator import run_research, parse_asset_from_query
    from synthesizer import synthesize
    from browser import BrowserManager

    print(f"\n[agent] Query: {query}")
    asset = parse_asset_from_query(query)
    print(f"[agent] Asset identified: {asset}\n")

    result = await run_research(asset, query=query)
    print("\n[agent] Synthesizing report...\n")
    report = synthesize(result)

    print("\n" + "=" * 70)
    print(report)
    print("=" * 70 + "\n")

    # Save report
    out = Path(f"report_{asset}.md")
    out.write_text(report, encoding="utf-8")
    print(f"[agent] Report saved → {out.resolve()}")

    await BrowserManager.get().then(None) if False else None
    mgr = BrowserManager._instance
    if mgr:
        await mgr.close()


async def run_swarm(assets: list[str]):
    """Run research on multiple assets in parallel (agent swarm)."""
    from orchestrator import run_research
    from synthesizer import synthesize
    from browser import BrowserManager

    print(f"[swarm] Starting parallel research on: {', '.join(assets)}\n")

    async def research_one(asset: str):
        print(f"[swarm/{asset}] Starting...")
        result = await run_research(asset, query=f"research {asset}")
        report = synthesize(result)
        out = Path(f"report_{asset}.md")
        out.write_text(report, encoding="utf-8")
        print(f"[swarm/{asset}] Complete → {out.name}")
        return asset, report

    results = await asyncio.gather(*[research_one(a.upper()) for a in assets])

    print("\n" + "=" * 70)
    for asset, report in results:
        print(f"\n── {asset} ──")
        # print first 500 chars of each
        print(report[:500] + "...\n")
    print("=" * 70)

    mgr = BrowserManager._instance
    if mgr:
        await mgr.close()


def start_server():
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    print(f"[server] Starting web UI at http://localhost:{port}")
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)


if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        print(__doc__)
        sys.exit(0)

    if args[0] == "--server":
        start_server()
    elif args[0] == "--swarm":
        if len(args) < 2:
            print("Usage: python main.py --swarm BTC ETH HYPE")
            sys.exit(1)
        asyncio.run(run_swarm(args[1:]))
    else:
        query = " ".join(args)
        asyncio.run(run_cli(query))
