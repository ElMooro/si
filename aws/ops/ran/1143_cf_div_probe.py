"""1143 — what does FMP actually return for cash flow dividends?"""
import json, pathlib, time
from datetime import datetime, timezone
import urllib.request, urllib.parse

REPORT = "aws/ops/reports/1143_cf_div_probe.json"
FMP = "https://financialmodelingprep.com/stable"
KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"


def fetch(path, **params):
    q = dict(params); q["apikey"] = KEY
    url = f"{FMP}/{path}?{urllib.parse.urlencode(q)}"
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1143/1.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "results": {}}
    for ticker in ["AAPL", "KO"]:
        r = fetch("cash-flow-statement", symbol=ticker, period="annual", limit=2)
        if isinstance(r, list) and r:
            latest = r[0]
            # Find ALL fields that mention "div" or "common"
            div_related = {k: v for k, v in latest.items()
                            if any(s in k.lower() for s in ("div", "common", "buyback", "repurchas", "stock"))}
            out["results"][ticker] = {
                "date":          latest.get("date"),
                "n_fields":      len(latest),
                "all_keys":      sorted(latest.keys()),
                "div_related":   div_related,
            }
        else:
            out["results"][ticker] = {"error": "no data"}
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1143] DONE")


if __name__ == "__main__":
    main()
