"""
ops/828 - Merger-Arbitrage data-coverage probe (audit-before-build gate).

The platform has justhodl-ma-tracker, which ingests the FMP M&A feed into
screener/ma-latest.json - but that is only an ANNOUNCEMENT feed. It carries
no offer price / exchange ratio, so it cannot price an arbitrage spread.

A real merger-arb desk needs the deal consideration. The M&A feed gives a
direct SEC S-4 'link' for every deal. This probe tests whether we can:
  1. Tell pending deals from closed ones (target still has a live quote).
  2. Pull current target + acquirer prices from FMP /stable/quote.
  3. Parse the merger consideration (cash $/share, stock exchange ratio)
     out of the SEC S-4 the link points at.

If S-4 parsing has a usable hit rate, the next op builds justhodl-merger-arb
as a true spread desk on top of ma-tracker's feed. If not, we pivot.

Read-only probe. Writes aws/ops/reports/828_merger_arb_probe.json.
"""
import json
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP_BASE = "https://financialmodelingprep.com/stable"
SEC_UA = "JustHodl Research raafouis@gmail.com"

s3 = boto3.client("s3", region_name="us-east-1")


def fmp(path, params=""):
    url = f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"_err": str(e)[:160]}


def quote(sym):
    """Return (price, name, mcap) or (None, None, None) if no live quote."""
    d = fmp("quote", f"&symbol={sym}")
    if isinstance(d, list) and d:
        q = d[0]
        return q.get("price"), q.get("name"), q.get("marketCap")
    return None, None, None


def fetch_sec(url, cap_bytes=3_500_000):
    """Fetch an SEC document with a compliant User-Agent. Returns text or None."""
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": SEC_UA,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov",
        })
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read(cap_bytes)
        # SEC may gzip
        if raw[:2] == b"\x1f\x8b":
            import gzip
            raw = gzip.decompress(raw)
        return raw.decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        return f"__HTTP_{e.code}__"
    except Exception as e:
        return f"__ERR_{str(e)[:100]}__"


# ---- merger-consideration extraction patterns (heuristic, to be measured) ----
# Strip HTML tags + collapse whitespace before matching.
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
# all-cash: "$XX.XX in cash" / "$XX.XX per share ... in cash" / "right to receive $XX.XX"
CASH_RE = re.compile(
    r"\$\s?(\d{1,4}(?:\.\d{1,2})?)\s+(?:per share[^.]{0,40}?)?in cash",
    re.IGNORECASE)
CASH_RE2 = re.compile(
    r"right to receive\s+\$\s?(\d{1,4}(?:\.\d{1,2})?)",
    re.IGNORECASE)
# stock: exchange ratio - "X.XXXX shares of [acquirer] common stock"
RATIO_RE = re.compile(
    r"(\d{1,2}\.\d{3,5})\s+(?:fully paid[^.]{0,30}?)?shares of\s+[A-Z]",
    re.IGNORECASE)
RATIO_RE2 = re.compile(
    r"exchange ratio[^.]{0,80}?(\d{1,2}\.\d{3,5})",
    re.IGNORECASE)


def extract_terms(text):
    """Return dict of detected consideration candidates."""
    if not text or text.startswith("__"):
        return {"status": text or "empty", "cash": None, "ratio": None}
    flat = WS_RE.sub(" ", TAG_RE.sub(" ", text))
    cash = None
    m = CASH_RE.search(flat) or CASH_RE2.search(flat)
    if m:
        v = float(m.group(1))
        if 0.5 <= v <= 2000:
            cash = v
    ratio = None
    m = RATIO_RE.search(flat) or RATIO_RE2.search(flat)
    if m:
        v = float(m.group(1))
        if 0.01 <= v <= 50:
            ratio = v
    return {"status": "ok", "cash": cash, "ratio": ratio, "len": len(flat)}


def main():
    rep = {
        "ops": 828,
        "ts": datetime.now(timezone.utc).isoformat(),
        "subject": "Merger-Arbitrage S-4 data-coverage probe (audit-before-build)",
    }

    # 1. Load ma-tracker's feed
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="screener/ma-latest.json")
        feed = json.loads(obj["Body"].read())
    except Exception as e:
        rep["fatal"] = f"could not load screener/ma-latest.json: {e}"
        _write(rep)
        return

    deals = feed.get("deals", [])
    rep["feed_generated_at"] = feed.get("generated_at")
    rep["feed_n_deals"] = len(deals)

    # 2. Pick recent deals that have a target ticker; newest first
    dated = [d for d in deals if d.get("targetedSymbol") and d.get("link")]
    dated.sort(key=lambda d: d.get("acceptedDate", ""), reverse=True)
    sample = dated[:9]
    rep["sample_size"] = len(sample)

    probes = []
    n_target_live = 0
    n_s4_ok = 0
    n_terms = 0
    for d in sample:
        tsym = d.get("targetedSymbol")
        asym = d.get("symbol")
        tprice, tname, tmcap = quote(tsym)
        aprice, aname, amcap = quote(asym)
        live = tprice is not None
        if live:
            n_target_live += 1

        s4 = fetch_sec(d.get("link", ""))
        terms = extract_terms(s4)
        s4_ok = terms["status"] == "ok"
        if s4_ok:
            n_s4_ok += 1
        got_terms = bool(terms.get("cash") or terms.get("ratio"))
        if got_terms:
            n_terms += 1

        # implied spread preview (only meaningful if pending + terms found)
        spread = None
        deal_value = None
        if live and got_terms:
            if terms.get("cash"):
                deal_value = terms["cash"]
            elif terms.get("ratio") and aprice:
                deal_value = round(terms["ratio"] * aprice, 2)
            if deal_value and tprice:
                spread = round((deal_value - tprice) / tprice * 100, 2)

        probes.append({
            "acquirer": asym, "acquirer_price": aprice,
            "target": tsym, "target_name": tname,
            "target_price": tprice, "target_live": live,
            "transactionDate": d.get("transactionDate"),
            "s4_status": terms["status"],
            "s4_len": terms.get("len"),
            "cash_per_share": terms.get("cash"),
            "exchange_ratio": terms.get("ratio"),
            "implied_deal_value": deal_value,
            "implied_spread_pct": spread,
            "link": d.get("link"),
        })
        time.sleep(0.4)  # SEC politeness

    rep["probes"] = probes
    rep["coverage"] = {
        "target_live_rate": round(n_target_live / max(1, len(sample)), 2),
        "s4_fetch_rate": round(n_s4_ok / max(1, len(sample)), 2),
        "terms_parse_rate": round(n_terms / max(1, len(sample)), 2),
    }
    cov = rep["coverage"]
    if cov["terms_parse_rate"] >= 0.5 and cov["s4_fetch_rate"] >= 0.7:
        rep["verdict"] = ("GO - S-4 parsing viable. Build justhodl-merger-arb "
                          "as a spread desk on ma-tracker's feed.")
    elif cov["s4_fetch_rate"] >= 0.7:
        rep["verdict"] = ("PARTIAL - S-4s fetch but terms patterns need work. "
                          "Build with broader patterns + graceful fallback.")
    else:
        rep["verdict"] = ("PIVOT - S-4 access unreliable. Build acquirer-"
                          "reaction / target-drift screen on price data only.")
    _write(rep)


def _write(rep):
    body = json.dumps(rep, indent=1, default=str)
    s3.put_object(Bucket=S3_BUCKET, Key="ops/reports/828_merger_arb_probe.json",
                  Body=body, ContentType="application/json")
    with open("aws/ops/reports/828_merger_arb_probe.json", "w") as f:
        f.write(body)
    print(body)


if __name__ == "__main__":
    main()
