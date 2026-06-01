"""1138 — smoke test peer comparison with FRESH ticker (UBER, no cache)."""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import urllib.request

REPORT = "aws/ops/reports/1138_peer_compare_smoke.json"
LAMBDA_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR",
                                "error": str(e)[:300],
                                "traceback": traceback.format_exc()[:800]})


def smoke(ticker, refresh=False):
    url = f"{LAMBDA_URL}?ticker={ticker}" + ("&refresh=1" if refresh else "")
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1138/1.0"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=180) as r:
        body = r.read()
        elapsed = round(time.time() - t0, 1)
    d = json.loads(body)
    pc = d.get("peer_comparison") or {}
    rows = pc.get("rows") or []
    summary = pc.get("summary") or {}
    relative = pc.get("relative") or {}
    return {
        "ticker":              ticker,
        "elapsed_s":           elapsed,
        "size_kb":             round(len(body)/1024, 1),
        "from_cache":          d.get("from_cache"),
        "company":             (d.get("company") or {}).get("name"),
        "sector":              (d.get("company") or {}).get("sector"),
        "industry":            (d.get("company") or {}).get("industry"),
        "rating":              (d.get("verdict") or {}).get("rating"),
        "conviction":          (d.get("verdict") or {}).get("conviction_grade"),
        "ai_peer_assessment":  (d.get("peer_comparison_assessment") or "")[:400],
        "pc_sector":           pc.get("sector"),
        "pc_industry":         pc.get("industry"),
        "n_rows":              len(rows),
        "subject_row":         next((r for r in rows if r.get("is_subject")), None),
        "peer_rows_summary":   [
            {"sym": r.get("symbol"), "name": r.get("name"), "pe": r.get("pe"),
             "ps": r.get("ps"), "roe": r.get("roe_pct")}
            for r in rows if not r.get("is_subject")
        ],
        "summary":             summary,
        "relative":            relative,
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}
    # UBER not in cache from earlier tests — should trigger full fetch + peer compare
    phase(out, "uber_fresh", lambda: smoke("UBER", refresh=False))
    # And force-refresh AAPL to test the new path on a name we already know
    phase(out, "aapl_refresh", lambda: smoke("AAPL", refresh=True))
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1138] DONE")


if __name__ == "__main__":
    main()
