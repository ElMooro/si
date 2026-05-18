"""
ops/829 - S-4 merger-consideration text DUMP (extractor design diagnostic).

ops/828 proved S-4s fetch 100% reliably but a free-floating regex grabs the
wrong dollar figures (par value, fee tables, examples) -> nonsense spreads.

Real S-4s state the deal terms in a canonical sentence in the front matter,
e.g. "each share of <Target> common stock ... will be converted into the
right to receive ...". This op does NOT try to parse - it locates those
canonical anchors in the first 600KB of each S-4 and dumps the raw ~420-char
window so the extractor can be written against real sentence structure.

Read-only. Writes aws/ops/reports/829_s4_consideration_dump.json.
"""
import json
import re
import time
import urllib.request
import urllib.error
import gzip
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP_BASE = "https://financialmodelingprep.com/stable"
SEC_UA = "JustHodl Research raafouis@gmail.com"

s3 = boto3.client("s3", region_name="us-east-1")

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
NBSP_RE = re.compile(r"&nbsp;|&#160;|&#xa0;", re.IGNORECASE)

# anchors that introduce the canonical consideration sentence
ANCHORS = [
    "right to receive",
    "shall be converted into",
    "will be converted into",
    "be cancelled and converted",
    "Merger Consideration",
]


def fmp(path, params=""):
    url = f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return None


def price(sym):
    d = fmp("quote", f"&symbol={sym}")
    if isinstance(d, list) and d:
        return d[0].get("price")
    return None


def fetch_sec(url, cap=4_000_000):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"})
        with urllib.request.urlopen(req, timeout=35) as r:
            raw = r.read(cap)
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return raw.decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        return f"__HTTP_{e.code}__"
    except Exception as e:
        return f"__ERR_{str(e)[:90]}__"


def windows(text):
    """Return up to 4 cleaned ~420-char windows around the earliest anchors."""
    if not text or text.startswith("__"):
        return {"status": text or "empty", "hits": []}
    # de-HTML for readable windows but keep it positional
    flat = NBSP_RE.sub(" ", text)
    flat = TAG_RE.sub(" ", flat)
    flat = WS_RE.sub(" ", flat)
    hits = []
    seen_pos = []
    for anc in ANCHORS:
        for m in re.finditer(re.escape(anc), flat, re.IGNORECASE):
            pos = m.start()
            # skip if within 600 chars of an already-captured window
            if any(abs(pos - p) < 600 for p in seen_pos):
                continue
            seen_pos.append(pos)
            seg = flat[max(0, pos - 120):pos + 320]
            hits.append({"anchor": anc, "pos": pos, "text": seg})
            if len(hits) >= 4:
                break
        if len(hits) >= 4:
            break
    hits.sort(key=lambda h: h["pos"])
    return {"status": "ok", "doc_len": len(flat), "hits": hits[:4]}


def main():
    rep = {
        "ops": 829,
        "ts": datetime.now(timezone.utc).isoformat(),
        "subject": "S-4 merger-consideration text dump (extractor design)",
    }
    try:
        feed = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="screener/ma-latest.json")["Body"].read())
    except Exception as e:
        rep["fatal"] = str(e)
        _write(rep)
        return

    deals = [d for d in feed.get("deals", [])
             if d.get("targetedSymbol") and d.get("link")]
    deals.sort(key=lambda d: d.get("acceptedDate", ""), reverse=True)
    sample = deals[:9]

    out = []
    for d in sample:
        tsym = d.get("targetedSymbol")
        asym = d.get("symbol")
        s4 = fetch_sec(d.get("link", ""))
        w = windows(s4)
        out.append({
            "acquirer": asym, "acquirer_price": price(asym),
            "target": tsym, "target_price": price(tsym),
            "target_name": d.get("targetedCompanyName"),
            "acquirer_name": d.get("companyName"),
            "transactionDate": d.get("transactionDate"),
            "link": d.get("link"),
            "s4": w,
        })
        time.sleep(0.5)

    rep["dumps"] = out
    _write(rep)


def _write(rep):
    body = json.dumps(rep, indent=1, default=str)
    s3.put_object(Bucket=S3_BUCKET,
                  Key="ops/reports/829_s4_consideration_dump.json",
                  Body=body, ContentType="application/json")
    with open("aws/ops/reports/829_s4_consideration_dump.json", "w") as f:
        f.write(body)
    print("ops 829 done:", len(rep.get("dumps", [])), "deals dumped")


if __name__ == "__main__":
    main()
