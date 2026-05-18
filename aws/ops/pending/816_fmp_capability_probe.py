"""ops/816 - FMP capability probe for the next two opportunity engines.

merger-arbitrage needs an offer-price-per-share + deal status; an ETF/CEF
catch-up engine needs holdings and (ideally) NAV. FMP's M&A feed may be a
bare announcement RSS with no price terms - this probe finds out exactly
what is available so the engines are built against real fields, not
assumptions (audit-before-build doctrine).
"""
import json, os, urllib.request
from datetime import datetime, timezone

FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"

report = {"ops": 816, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "FMP capability probe - M&A + ETF/CEF"}


def probe(url):
    sep = "&" if "?" in url else "?"
    try:
        with urllib.request.urlopen(url + f"{sep}apikey={FMP}",
                                    timeout=25) as r:
            raw = r.read()
        d = json.loads(raw)
        if isinstance(d, list):
            sample = d[0] if d else {}
            return {"ok": True, "type": "list", "n": len(d),
                    "keys": sorted(sample.keys())
                    if isinstance(sample, dict) else None,
                    "sample": {k: sample[k] for k in list(sample)[:18]}
                    if isinstance(sample, dict) else str(sample)[:240]}
        if isinstance(d, dict):
            return {"ok": True, "type": "dict", "keys": sorted(d.keys()),
                    "sample": {k: d[k] for k in list(d)[:18]}}
        return {"ok": True, "type": type(d).__name__,
                "sample": str(d)[:240]}
    except urllib.error.HTTPError as e:
        return {"ok": False, "http": e.code, "msg": str(e)[:140]}
    except Exception as e:
        return {"ok": False, "err": f"{type(e).__name__}: {str(e)[:140]}"}


probes = {
    # ── merger arbitrage ──
    "ma_latest": f"{BASE}/mergers-acquisitions-latest?page=0&limit=8",
    "ma_search": f"{BASE}/mergers-acquisitions-search?name=Inc",
    # ── ETF holdings / info ──
    "etf_holdings_XLK": f"{BASE}/etf/holdings?symbol=XLK",
    "etf_info_XLK": f"{BASE}/etf/info?symbol=XLK",
    "etf_holdings_SMH": f"{BASE}/etf/holdings?symbol=SMH",
    # ── CEF probes (NAV / discount) ──
    "quote_cef_ADX": f"{BASE}/quote?symbol=ADX",
    "etf_info_cef_ADX": f"{BASE}/etf/info?symbol=ADX",
    # ── price change (already used by beta-laggard, sanity) ──
    "price_change_AAPL": f"{BASE}/stock-price-change?symbol=AAPL",
}
report["probes"] = {k: probe(v) for k, v in probes.items()}

# capability summary for the next builds
ma = report["probes"]["ma_latest"]
ma_keys = set(ma.get("keys") or [])
price_terms = ma_keys & {"price", "offerPrice", "transactionAmount",
                         "dealValue", "pricePerShare", "consideration"}
report["assessment"] = {
    "merger_arb_buildable": ma.get("ok") and bool(price_terms),
    "ma_price_fields_found": sorted(price_terms),
    "ma_note": ("M&A feed carries deal price terms - merger-arb spread "
                "engine is buildable" if price_terms else
                "M&A feed looks like a bare announcement feed (no offer "
                "price per share) - a clean spread engine is NOT buildable "
                "from FMP alone; pivot to announced-deal event tracking or "
                "another concrete play"),
    "etf_holdings_buildable": report["probes"]["etf_holdings_XLK"].get("ok"),
}
report["verdict"] = (
    "PROBE COMPLETE - see assessment{} for what merger-arb / ETF-catchup "
    "can actually be built on.")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/816_fmp_capability_probe.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/816_fmp_capability_probe.json")
