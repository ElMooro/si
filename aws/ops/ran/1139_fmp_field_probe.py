"""1139 — probe field names returned by FMP /stable/ratios-ttm and key-metrics-ttm."""
import json, pathlib, time
from datetime import datetime, timezone
import urllib.request, urllib.parse

REPORT = "aws/ops/reports/1139_fmp_field_probe.json"
FMP = "https://financialmodelingprep.com/stable"
KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"


def get(path, **params):
    q = dict(params); q["apikey"] = KEY
    url = f"{FMP}/{path}?{urllib.parse.urlencode(q)}"
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1139/1.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "probes": []}

    for endpoint in ["ratios-ttm", "key-metrics-ttm", "ratios"]:
        for ticker in ["AAPL", "MSFT"]:
            try:
                params = {"symbol": ticker}
                if endpoint == "ratios": params["period"] = "annual"; params["limit"] = 1
                r = get(endpoint, **params)
                first = r[0] if isinstance(r, list) and r else r
                # Pull only PE/ROE-related fields
                pe_roe = {k: v for k, v in (first or {}).items()
                            if any(s in k.lower() for s in ('pe', 'price', 'earning', 'returnonequity', 'roe',
                                                                  'roic', 'returnoninvested', 'ebitda', 'enterprisevalue'))}
                out["probes"].append({
                    "endpoint":   endpoint,
                    "ticker":     ticker,
                    "n_keys":     len(first) if isinstance(first, dict) else 0,
                    "all_keys":   sorted(first.keys()) if isinstance(first, dict) else [],
                    "pe_roe_fields": pe_roe,
                })
            except Exception as e:
                out["probes"].append({"endpoint": endpoint, "ticker": ticker, "error": str(e)[:200]})

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1139] DONE")


if __name__ == "__main__":
    main()
