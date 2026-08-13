"""justhodl-backlog-miner v1.0.1 (ops 4653)

Mines disclosed backlog dollars from primary sources — SEC EDGAR
full-text search over 10-Q/10-K filings — for the stock-buying
candidate set plus a standing industrial anchor list. Emits per
ticker: latest backlog level (USD), QoQ % change (sequential
filings), YoY % change (four filings back), the as-of filing date
and the source URL. Companies that do not disclose backlog are
marked NOT_DISCLOSED — never guessed. Warm-cached 7 days per
ticker; budgeted per run; coverage compounds hourly.
"""
import json
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/backlog-mined.json"
WARM = "data/warm/backlog/"
BUDGET = {"n": 22}
ANCHORS = ["BA", "CAT", "LMT", "GD", "NOC", "RTX", "DE", "ETN",
           "PWR", "GE", "HON", "EMR", "TT", "TDG", "LHX", "J",
           "FLR", "ACM", "VMC", "URI"]
UA = {"User-Agent": "JustHodl research khalid@justhodl.ai"}
s3 = boto3.client("s3", region_name="us-east-1")


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def http_json(url, timeout=20):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return json.loads(h.read())


def http_text(url, timeout=25, cap=1500000):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read(cap).decode("utf-8", "replace")


_CIK = {}


def cik_map():
    global _CIK
    if _CIK:
        return _CIK
    try:
        d = http_json("https://www.sec.gov/files/"
                      "company_tickers.json")
        for v in d.values():
            _CIK[str(v["ticker"]).upper()] = int(v["cik_str"])
    except Exception:
        pass
    return _CIK


AMT = re.compile(
    r"\$ ?([\d,]+(?:\.\d+)?) ?(billion|million|bn|mm)?",
    re.I)


def parse_backlog(text):
    """Find dollar amounts within 220 chars of 'backlog'.
    Returns best (largest plausible) USD value or None."""
    best = None
    low = text.lower()
    for m in re.finditer(r"backlog", low):
        seg = text[max(0, m.start() - 220):m.start() + 220]
        for am in AMT.finditer(seg):
            try:
                v = float(am.group(1).replace(",", ""))
            except Exception:
                continue
            unit = (am.group(2) or "").lower()
            if unit in ("billion", "bn"):
                v *= 1e9
            elif unit in ("million", "mm"):
                v *= 1e6
            else:
                if v > 1e7:
                    pass  # raw dollars in tables
                elif v > 100:
                    v *= 1e6  # bare table numbers in $MM
                else:
                    v *= 1e9  # e.g. "$14.2" near 'billion' word
            if 5e7 <= v <= 8e11:
                if best is None or v > best:
                    best = v
    return best


def filings_for(cik, n=6):
    try:
        d = http_json("https://data.sec.gov/submissions/"
                      "CIK%010d.json" % cik)
        rec = d.get("filings", {}).get("recent", {})
        out = []
        for i, form in enumerate(rec.get("form") or []):
            if form not in ("10-Q", "10-K"):
                continue
            acc = rec["accessionNumber"][i].replace("-", "")
            doc = rec["primaryDocument"][i]
            out.append({
                "form": form,
                "date": rec["filingDate"][i],
                "url": ("https://www.sec.gov/Archives/edgar/"
                        "data/%d/%s/%s" % (cik, acc, doc))})
            if len(out) >= n:
                break
        return out
    except Exception:
        return []


def mine(tk):
    wkey = WARM + tk + ".json"
    c = s3_json(wkey)
    if c:
        try:
            age = (datetime.now(timezone.utc)
                   - datetime.fromisoformat(c["fetched_at"])
                   ).total_seconds()
            if age < 7 * 86400:
                return c
        except Exception:
            pass
    if BUDGET["n"] <= 0:
        return c
    cik = cik_map().get(tk)
    if not cik:
        return c
    BUDGET["n"] -= 1
    time.sleep(0.15)
    fl = filings_for(cik, 6)
    series = []
    for f in fl[:5]:
        try:
            time.sleep(0.15)
            txt = http_text(f["url"])
            txt = re.sub(r"<[^>]+>", " ", txt)
            v = parse_backlog(txt)
            series.append({"date": f["date"], "form": f["form"],
                           "value": v, "src": f["url"]})
        except Exception:
            series.append({"date": f["date"], "form": f["form"],
                           "value": None, "src": f["url"]})
        if len([x for x in series
                if x["value"] is not None]) >= 5:
            break
    vals = [x for x in series if x["value"] is not None]
    if len(vals) >= 3:
        import math as _m
        med = sorted(x["value"] for x in vals)[len(vals) // 2]
        vals = [x for x in vals
                if abs(_m.log10(x["value"]) - _m.log10(med))
                <= 0.7]
    out = {"ticker": tk,
           "fetched_at": datetime.now(timezone.utc).isoformat(
               timespec="seconds"),
           "series": series,
           "status": "MINED" if vals else "NOT_DISCLOSED"}
    if vals:
        out["backlog_usd"] = vals[0]["value"]
        out["asof"] = vals[0]["date"]
        out["src"] = vals[0]["src"]
        if len(vals) >= 2 and vals[1]["value"] \
                and 0.25 <= (vals[0]["value"]
                             / vals[1]["value"]) <= 4.0:
            out["backlog_qoq_pct"] = round(
                (vals[0]["value"] / vals[1]["value"] - 1) * 100,
                1)
        if len(vals) >= 5 and vals[4]["value"] \
                and 0.25 <= (vals[0]["value"]
                             / vals[4]["value"]) <= 4.0:
            out["backlog_yoy_pct"] = round(
                (vals[0]["value"] / vals[4]["value"] - 1) * 100,
                1)
        elif len(vals) >= 4 and vals[3]["value"] \
                and vals[0].get("form") == "10-K":
            out["backlog_yoy_pct"] = round(
                (vals[0]["value"] / vals[3]["value"] - 1) * 100,
                1)
    try:
        s3.put_object(Bucket=BUCKET, Key=wkey,
                      Body=json.dumps(out).encode(),
                      ContentType="application/json")
    except Exception:
        pass
    return out


def lambda_handler(event=None, context=None):
    sb = s3_json("data/stock-buying.json") or {}
    cands = [x.get("symbol") for x in (sb.get("top") or [])
             if (x.get("gates") or {}).get("below_sma")][:30]
    targets = []
    for t in ANCHORS + cands:
        t = str(t or "").upper()
        if t and t not in targets:
            targets.append(t)
    by = {}
    mined = nd = 0
    for tk in targets:
        r = mine(tk)
        if not r:
            continue
        by[tk] = {k: r.get(k) for k in
                  ("status", "backlog_usd", "backlog_qoq_pct",
                   "backlog_yoy_pct", "asof", "src")}
        if r.get("status") == "MINED":
            mined += 1
        else:
            nd += 1
    payload = {
        "engine": "justhodl-backlog-miner",
        "as_of": datetime.now(timezone.utc).isoformat(
            timespec="seconds"),
        "doctrine": "backlog from primary sources only (SEC "
                    "10-Q/10-K text); QoQ = sequential filings, "
                    "YoY = four filings back; NOT_DISCLOSED is "
                    "an honest status, never a guess",
        "n_targets": len(targets), "n_mined": mined,
        "n_not_disclosed": nd,
        "budget_left": BUDGET["n"],
        "by_ticker": by}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload).encode(),
                  ContentType="application/json")
    return {"mined": mined, "targets": len(targets)}
