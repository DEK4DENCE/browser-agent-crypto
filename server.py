import asyncio
import json
import os
import time
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel

load_dotenv()

RUNS_DIR = Path(__file__).parent / "runs"
RUNS_DIR.mkdir(exist_ok=True)

from orchestrator import run_research, parse_asset_from_query
from synthesizer import synthesize
from browser import BrowserManager

app = FastAPI(title="Crypto Browser Agent")

# In-memory key store — populated from .env on startup, overridable via UI
_runtime_keys: dict[str, str] = {
    "GROQ_API_KEY":       os.getenv("GROQ_API_KEY", ""),
    "DUNE_API_KEY":       os.getenv("DUNE_API_KEY", ""),
    "BINANCE_API_KEY":    os.getenv("BINANCE_API_KEY", ""),
    "ANTHROPIC_API_KEY":  os.getenv("ANTHROPIC_API_KEY", ""),
    "GOOGLE_API_KEY":     os.getenv("GOOGLE_API_KEY", ""),
}


def get_key(name: str) -> str:
    """Read a key — runtime store takes priority over env."""
    return _runtime_keys.get(name) or os.getenv(name, "")


class KeysPayload(BaseModel):
    groq_api_key:      str | None = None
    dune_api_key:      str | None = None
    binance_api_key:   str | None = None
    anthropic_api_key: str | None = None
    google_api_key:    str | None = None

STATIC_DIR = Path(__file__).parent / "static"


STATIC_DIR = Path(__file__).parent / "static"


@app.get("/")
async def index():
    html_path = STATIC_DIR / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.post("/api/keys")
async def set_keys(payload: KeysPayload):
    """Receive API keys from the UI and store them in the runtime key store."""
    mapping = {
        "GROQ_API_KEY":      payload.groq_api_key,
        "DUNE_API_KEY":      payload.dune_api_key,
        "BINANCE_API_KEY":   payload.binance_api_key,
        "ANTHROPIC_API_KEY": payload.anthropic_api_key,
        "GOOGLE_API_KEY":    payload.google_api_key,
    }
    updated = []
    for env_name, value in mapping.items():
        if value and not value.startswith("•"):  # ignore masked placeholders
            _runtime_keys[env_name] = value
            # Also push into os.environ so llm.py picks it up
            os.environ[env_name] = value
            updated.append(env_name)

    # Reload llm provider if Groq key changed
    if "GROQ_API_KEY" in updated or "ANTHROPIC_API_KEY" in updated:
        import importlib, llm
        importlib.reload(llm)

    return JSONResponse({"ok": True, "updated": updated})


@app.get("/api/keys/status")
async def keys_status():
    """Report which keys are currently set (masked)."""
    return JSONResponse({
        k: ("set" if v else "missing")
        for k, v in _runtime_keys.items()
    })


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
                # Save report + run history
                try:
                    report_path = Path(f"report_{asset}.md")
                    report_path.write_text(report, encoding="utf-8")
                except Exception:
                    pass
                try:
                    from orchestrator import _save_run
                    _save_run(asset, query, report, res, [], 0)
                except Exception:
                    pass
                break

            elif msg_type == "error":
                yield {"event": "error", "data": json.dumps({"message": msg.get("message", "")})}
                break

        if not task.done():
            task.cancel()

    return EventSourceResponse(event_generator())


@app.get("/api/history")
async def get_history():
    """Return list of past runs (summary only, newest first)."""
    runs = []
    for f in sorted(RUNS_DIR.glob("run_*.json"), reverse=True)[:50]:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            runs.append({
                "id": data.get("id"),
                "timestamp": data.get("timestamp"),
                "asset": data.get("asset"),
                "query": data.get("query"),
                "duration_secs": data.get("duration_secs"),
                "error_count": len(data.get("errors", [])),
                "metrics": data.get("metrics", {}),
            })
        except Exception:
            pass
    return JSONResponse(runs)


@app.get("/api/history/{run_id}")
async def get_run(run_id: str):
    """Return full data for a specific past run."""
    path = RUNS_DIR / f"run_{run_id}.json"
    if not path.exists():
        return JSONResponse({"error": "not found"}, status_code=404)
    try:
        return JSONResponse(json.loads(path.read_text(encoding="utf-8")))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.on_event("shutdown")
async def shutdown():
    mgr = BrowserManager._instance
    if mgr:
        await mgr.close()


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
