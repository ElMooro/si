"""1117 — probe FMP endpoints to design the squeeze + NLP + pairs Lambdas."""
import json, pathlib, urllib.request
from datetime import datetime, timezone

REPORT = "aws/ops/reports/1117_fmp_endpoints.json"
KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

ENDPOINTS = [
    ("profile",                      f"/stable/profile?symbol=PLTR&apikey={KEY}"),
    ("share-float",                  f"/stable/share-float?symbol=PLTR&apikey={KEY}"),
    ("historical-share-count",       f"/stable/historical-share-count?symbol=PLTR&apikey={KEY}"),
    ("short-interest",               f"/stable/short-interest?symbol=PLTR&apikey={KEY}"),
    ("fail-to-deliver",              f"/stable/fail-to-deliver?symbol=PLTR&apikey={KEY}"),
    ("stock-peers",                  f"/stable/stock-peers?symbol=PLTR&apikey={KEY}"),
    ("peers",                        f"/stable/peers?symbol=PLTR&apikey={KEY}"),
    ("earning-call-transcript",      f"/stable/earning-call-transcript?symbol=PLTR&year=2025&quarter=3&apikey={KEY}"),
    ("earning-call-transcripts-list",f"/stable/earning-call-transcripts-list?symbol=PLTR&apikey={KEY}"),
    ("ratios",                       f"/stable/ratios?symbol=PLTR&apikey={KEY}"),
    ("key-metrics",                  f"/stable/key-metrics?symbol=PLTR&apikey={KEY}"),
    ("options-chain",                f"/stable/options-chain?symbol=PLTR&apikey={KEY}"),
    ("historical-options",           f"/stable/historical-options?symbol=PLTR&apikey={KEY}"),
    ("institutional-ownership",      f"/stable/institutional-ownership?symbol=PLTR&apikey={KEY}"),
    ("etf-sector-weightings",        f"/stable/etf-sector-weightings?symbol=XLK&apikey={KEY}"),
    ("market-cap",                   f"/stable/market-capitalization?symbol=PLTR&apikey={KEY}"),
]

def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "endpoints": {}}
    for name, path in ENDPOINTS:
        url = "https://financialmodelingprep.com" + path
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/probe"})
            r = urllib.request.urlopen(req, timeout=12)
            body = r.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(body)
                if isinstance(data, list) and data:
                    sample = data[0] if isinstance(data[0], dict) else None
                    out["endpoints"][name] = {
                        "ok": True, "type": "list", "n": len(data),
                        "first_keys": list(sample.keys())[:14] if sample else None,
                        "sample_values": {k: str(v)[:120] for k, v in (sample or {}).items() if k in (list(sample.keys())[:6] if sample else [])},
                    }
                elif isinstance(data, dict):
                    if data.get("Error Message") or data.get("error"):
                        out["endpoints"][name] = {"ok": False, "msg": (data.get("Error Message") or data.get("error"))[:200]}
                    else:
                        out["endpoints"][name] = {"ok": True, "type": "dict", "keys": list(data.keys())[:12]}
                else:
                    out["endpoints"][name] = {"ok": True, "type": "primitive", "raw": str(data)[:80]}
            except json.JSONDecodeError:
                out["endpoints"][name] = {"ok": False, "raw": body[:200]}
        except Exception as e:
            out["endpoints"][name] = {"ok": False, "err": str(e)[:160]}
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1117] DONE")

if __name__ == "__main__":
    main()
