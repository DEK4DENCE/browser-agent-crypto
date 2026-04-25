import asyncio
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

load_dotenv()

from orchestrator import run_research, parse_asset_from_query
from synthesizer import synthesize
from browser import BrowserManager

app = FastAPI(title="Crypto Browser Agent")

STATIC_DIR = Path(__file__).parent / "static"


@app.get("/")
async def index():
    html_path = STATIC_DIR / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/research/stream")
async def stream_research(query: str = "HYPE"):
    """
    SSE endpoint — streams agent log in real-time, then emits the final report.
    Usage: GET /research/stream?query=should+I+ape+HYPE
    """

    async def event_generator():
        log_queue: asyncio.Queue = asyncio.Queue()

        # Parse asset from natural language query
        try:
            if any(c.isalpha() for c in query) and len(query) > 6:
                asset = parse_asset_from_query(query)
            else:
                asset = query.strip().upper().replace("$", "")
        except Exception:
            asset = query.strip().upper().replace("$", "")

        yield {
            "event": "start",
            "data": json.dumps({"asset": asset, "query": query}),
        }

        result_holder: dict = {}
        error_holder: dict = {}

        async def run():
            try:
                result = await run_research(asset, query=query, log_queue=log_queue)
                await log_queue.put({"type": "synthesizing", "message": "Synthesizing research with Claude..."})
                report = synthesize(result)
                result_holder["result"] = result
                result_holder["report"] = report
                await log_queue.put({"type": "complete"})
            except Exception as e:
                error_holder["error"] = str(e)
                await log_queue.put({"type": "error", "message": str(e)})

        task = asyncio.create_task(run())

        while True:
            try:
                msg = await asyncio.wait_for(log_queue.get(), timeout=120.0)
            except asyncio.TimeoutError:
                yield {"event": "heartbeat", "data": "{}"}
                continue

            msg_type = msg.get("type", "log")

            if msg_type in ("log", "synthesizing"):
                yield {"event": "log", "data": json.dumps({"message": msg.get("message", "")})}

            elif msg_type == "complete":
                res = result_holder.get("result")
                report = result_holder.get("report", "")
                payload = {
                    "report": report,
                    "asset": asset,
                    "long_bias_pct": res.long_bias_pct if res else 50,
                    "avg_funding_8h": res.avg_funding_8h if res else 0,
                    "whale_signal": res.whale_signal if res else "UNKNOWN",
                    "kol_bullish_pct": res.kol_bullish_pct if res else 50,
                    "num_traders": len(res.top_traders) if res else 0,
                    "num_liq_zones": len(res.liquidation_zones) if res else 0,
                    "token_metrics": res.token_metrics.model_dump() if res else {},
                }
                yield {"event": "complete", "data": json.dumps(payload)}
                # Save report to file
                try:
                    report_path = Path(f"report_{asset}.md")
                    report_path.write_text(report, encoding="utf-8")
                except Exception:
                    pass
                break

            elif msg_type == "error":
                yield {"event": "error", "data": json.dumps({"message": msg.get("message", "")})}
                break

        if not task.done():
            task.cancel()

    return EventSourceResponse(event_generator())


@app.on_event("shutdown")
async def shutdown():
    mgr = BrowserManager._instance
    if mgr:
        await mgr.close()


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
