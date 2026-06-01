"""1137 — verify BRK-B fix, then probe FMP for peer/industry P/E endpoints."""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import urllib.request, urllib.parse

REPORT = "aws/ops/reports/1137_brkb_peer_probe.json"
LAMBDA_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
FMP = "https://financialmodelingprep.com/stable"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR",
                                "error": str(e)[:400],
                                "traceback": traceback.format_exc()[:1000]})


def get(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1137/1.0"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
    return {"status": r.status, "elapsed_ms": round((time.time()-t0)*1000, 0),
              "size_bytes": len(body), "data": json.loads(body) if body else None}


def fmp_get(path, **params):
    q = dict(params); q["apikey"] = FMP_KEY
    return get(f"{FMP}/{path}?{urllib.parse.urlencode(q)}")


def smoke_brkb():
    """Verify the BRK-B fix landed."""
    url = f"{LAMBDA_URL}?ticker=BRK-B"
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1137/1.0"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            body = r.read()
            elapsed = round(time.time() - t0, 1)
        d = json.loads(body)
        return {
            "ok":         True,
            "elapsed_s":  elapsed,
            "ticker":     d.get("ticker"),
            "company":    (d.get("company") or {}).get("name"),
            "rating":     (d.get("verdict") or {}).get("rating"),
            "conviction": (d.get("verdict") or {}).get("conviction_grade"),
            "income_yrs": len((d.get("statements") or {}).get("income_annual") or []),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def probe_endpoints():
    """Test FMP endpoints we'll need for peer/industry comparison."""
    # Sector P/E
    r1 = fmp_get("sector-pe-snapshot", date="2026-05-30")
    r2 = fmp_get("industry-pe-snapshot", date="2026-05-30")
    # Batched quote (multiple tickers in one call)
    r3 = fmp_get("quote", symbol="AAPL,MSFT,NVDA,GOOGL,META")
    r4 = fmp_get("ratios-ttm", symbol="AAPL,MSFT,NVDA")
    # Peers list
    r5 = fmp_get("stock-peers", symbol="AAPL")
    return {
        "sector_pe_snapshot": {
            "size": r1.get("size_bytes"),
            "sample": (r1.get("data") or [])[:3],
        },
        "industry_pe_snapshot": {
            "size": r2.get("size_bytes"),
            "sample": (r2.get("data") or [])[:3],
        },
        "batched_quote": {
            "size": r3.get("size_bytes"),
            "n_returned": len(r3.get("data") or []),
            "tickers_returned": [q.get("symbol") for q in (r3.get("data") or [])],
        },
        "batched_ratios": {
            "size": r4.get("size_bytes"),
            "n_returned": len(r4.get("data") or []),
            "tickers_returned": [q.get("symbol") for q in (r4.get("data") or [])],
        },
        "peers_for_aapl": {
            "size": r5.get("size_bytes"),
            "sample": r5.get("data"),
        },
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}
    phase(out, "smoke_brkb",       smoke_brkb)
    phase(out, "probe_endpoints",  probe_endpoints)
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1137] DONE")


if __name__ == "__main__":
    main()
