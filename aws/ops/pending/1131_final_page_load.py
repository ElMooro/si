"""1131 — final page-load aggregate after items 1+2+3.

Hits the production worker URL with browser-pattern Accept-Encoding to
measure REAL wire size and latency for everything index.html loads.
"""
import json, pathlib, time, urllib.request
from datetime import datetime, timezone

REPORT = "aws/ops/reports/1131_final_page_load.json"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"

# Exact 13 SOURCES that index.html loads (12 critical + 1 ppSummary)
SOURCES = [
    ("intel",       "/intelligence-report.json"),
    ("edge",        "/edge-data.json"),
    ("liq",         "/liquidity-data.json"),
    ("flow",        "/flow-data.json"),
    ("crypto",      "/crypto-intel.json"),
    ("regime",      "/regime/current.json"),
    ("div",         "/divergence/current.json"),
    ("cot",         "/cot/extremes/current.json"),
    ("risk",        "/risk/recommendations.json"),
    ("setups",      "/opportunities/asymmetric-equity.json"),
    ("pnl",         "/portfolio/pnl-daily.json"),
    ("signalBoard", "/data/signal-board.json"),
    ("ppSummary",   "/data/pump-radar-summary.json"),
]


def fetch_one(key, path):
    url = PROXY + path
    req = urllib.request.Request(url)
    req.add_header("Accept-Encoding", "gzip, br, deflate")
    req.add_header("User-Agent", "Mozilla/5.0 (justhodl-final-test/1.0)")
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read()
            return {
                "key":              key,
                "path":             path,
                "wire_size_bytes":  len(body),
                "elapsed_ms":       round((time.time() - t0) * 1000, 1),
                "content_encoding": r.headers.get("Content-Encoding"),
                "x_edge_cache":     r.headers.get("X-Edge-Cache"),
                "status":           r.status,
            }
    except Exception as e:
        return {"key": key, "path": path, "error": str(e)[:200],
                "elapsed_ms": round((time.time() - t0) * 1000, 1)}


def main():
    rows = [fetch_one(k, p) for k, p in SOURCES]
    ok_rows = [r for r in rows if "wire_size_bytes" in r]
    total_wire = sum(r["wire_size_bytes"] for r in ok_rows)
    max_ms = max((r["elapsed_ms"] for r in ok_rows), default=0)
    sum_ms = sum(r["elapsed_ms"] for r in ok_rows)

    out = {
        "started":              datetime.now(timezone.utc).isoformat(),
        "n_files":              len(SOURCES),
        "n_ok":                 len(ok_rows),
        "n_errors":             len(rows) - len(ok_rows),
        "total_wire_kb":        round(total_wire/1024, 1),
        "max_ms_parallel":      max_ms,
        "sum_ms_sequential":    sum_ms,
        "rows":                 rows,
        "proxy_url":            PROXY,
    }
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1131] DONE")


if __name__ == "__main__":
    main()
