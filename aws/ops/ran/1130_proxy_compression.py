"""1130 — measure proxy compression efficacy.

For each heavy file index.html loads, compare:
  - direct S3 size (no gzip)
  - through worker with Accept-Encoding: gzip (browser pattern)
  - through worker with no Accept-Encoding

We need this to confirm the worker is actually compressing. Hits
the production worker URL (justhodl-data-proxy.raafouis.workers.dev)
which means this verification works from anywhere — including the
sandbox, which can reach workers.dev.
"""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import urllib.request

REPORT = "aws/ops/reports/1130_proxy_compression.json"

PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com"

TARGETS = [
    "crypto-intel.json",
    "flow-data.json",
    "opportunities/asymmetric-equity.json",
    "risk/recommendations.json",
    "data/signal-board.json",
    "data/pump-radar-summary.json",
    "data/pump-positioning.json",
    "data/catalysts.json",
    "edge-data.json",
    "liquidity-data.json",
]


def fetch_with(url, accept_encoding=None, timeout=15):
    """Fetch URL and return (status, content_encoding, response_size_bytes, elapsed_ms)."""
    req = urllib.request.Request(url)
    if accept_encoding:
        req.add_header("Accept-Encoding", accept_encoding)
    req.add_header("User-Agent", "justhodl-perf-test/1.0")
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            ms = round((time.time() - t0) * 1000, 1)
            return {
                "status":           r.status,
                "content_encoding": r.headers.get("Content-Encoding"),
                "content_length":   r.headers.get("Content-Length"),
                "content_type":     r.headers.get("Content-Type"),
                "x_edge_cache":     r.headers.get("X-Edge-Cache"),
                "x_edge_ttl":       r.headers.get("X-Edge-TTL"),
                "body_size_bytes":  len(body),
                "elapsed_ms":       ms,
            }
    except Exception as e:
        return {"error": str(e)[:200], "elapsed_ms": round((time.time() - t0) * 1000, 1)}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "comparisons": []}

    for path in TARGETS:
        row = {"path": path}
        # Direct S3
        row["direct_s3"] = fetch_with(f"{S3}/{path}")
        # Through proxy WITHOUT requesting gzip (worker returns plain)
        row["proxy_plain"] = fetch_with(f"{PROXY}/{path}", accept_encoding="identity")
        # Through proxy WITH gzip (browser pattern)
        row["proxy_gzip"] = fetch_with(f"{PROXY}/{path}", accept_encoding="gzip, br, deflate")
        out["comparisons"].append(row)
        time.sleep(0.1)

    # Compute totals
    total_direct = sum(r["direct_s3"].get("body_size_bytes", 0) for r in out["comparisons"]
                         if "body_size_bytes" in r["direct_s3"])
    total_gzip = sum(r["proxy_gzip"].get("body_size_bytes", 0) for r in out["comparisons"]
                       if "body_size_bytes" in r["proxy_gzip"])
    out["totals"] = {
        "direct_s3_total_kb":  round(total_direct/1024, 1),
        "proxy_gzip_total_kb": round(total_gzip/1024, 1),
        "savings_kb":          round((total_direct - total_gzip)/1024, 1),
        "savings_pct":         round(100 * (1 - total_gzip / max(1, total_direct)), 1),
    }

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1130] DONE")


if __name__ == "__main__":
    main()
