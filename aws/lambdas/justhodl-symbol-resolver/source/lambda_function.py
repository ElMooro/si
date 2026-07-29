"""justhodl-symbol-resolver — turn imported TV tickers into FETCHABLE series.

ops 4084 audited the 10,319 tickers imported from Khalid's 491 watchlists:
only 43.6% had any fetch route. The largest block — 4,369 — is macro
(ECONOMICS 3,317 / FRED 765 / COT3 229 / USI 143 / COT 111): tickers that
carry his notes but that the fleet cannot pull.

The mechanism to fix this already exists and is proven: the vault's
ALIASES table maps a bare TV code to a fetch key, e.g.
    "CHINTR": "fred_alias:IRSTCI01CHM156N"
CHINTR is ECONOMICS:CHINTR. That table is hand-curated and a few hundred
long. This engine generates the rest, writing data/symbol-aliases.json,
which the vault reads as a LOWER-precedence layer than the curated dict
(curated always wins — a human decision outranks a generated one).

STEP 1 (this version) — FRED: symbols, the certain case.
  For FRED:MABMM301JPM189S the series id IS the fetch key. Strip the
  prefix, VERIFY it against the FRED API (a series that 404s is not an
  alias, it is a broken promise), emit fred:<id>. No inference anywhere.

Deliberately NOT done yet:
  ECONOMICS: matching needs each ticker's description as the join key,
  and ops 4085 found the harvester discards descriptions when source is
  null — which is every macro symbol. Fixed in extension v1.8.1; the
  matcher lands once real descriptions accumulate. Guessing the mapping
  from a code alone would point Khalid's notes at the wrong series, which
  is worse than leaving it unrouted.

LEDGER: FRED is rate-limited, so verification accretes across runs.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone

import boto3

MARKER = "symbol-resolver v1.0 ops4085 step1-fred"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/symbol-aliases.json"
LEDGER_KEY = "symbol-resolver/ledger.json"
FRED_KEY = os.environ.get("FRED_KEY", "")
MAX_NEW = int(os.environ.get("MAX_NEW", "400"))
SPACING = 0.30

s3 = boto3.client("s3", region_name="us-east-1")


def gj(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def fred_series(sid):
    """Return the series record if FRED actually serves it, else None."""
    url = ("https://api.stlouisfed.org/fred/series?"
           + urllib.parse.urlencode({"series_id": sid, "api_key": FRED_KEY,
                                     "file_type": "json"}))
    try:
        j = json.loads(urllib.request.urlopen(url, timeout=20).read())
        arr = j.get("seriess") or []
        return arr[0] if arr else None
    except Exception:
        return None


def lambda_handler(event, context):
    print(f"[symbol-resolver] {MARKER}")
    now = datetime.now(timezone.utc)

    led = gj(LEDGER_KEY, {}) or {}
    verified = led.get("verified") or {}      # bare code -> alias row
    dead = set(led.get("dead") or [])         # ids FRED does not serve

    wl = gj("data/tv-watchlists.json", {}) or {}
    tickers = set()
    for l in (wl.get("watchlists") or wl.get("lists") or []):
        for x in (l.get("symbols") or []):
            x = str(x).strip().upper()
            if x:
                tickers.add(x)

    fred_tickers = sorted(t for t in tickers if t.startswith("FRED:"))
    todo = [t for t in fred_tickers
            if t.split(":", 1)[1] not in verified
            and t.split(":", 1)[1] not in dead]

    spent = newly = 0
    for t in todo:
        if spent >= MAX_NEW or (context and
                                context.get_remaining_time_in_millis() < 45000):
            break
        sid = t.split(":", 1)[1]
        rec = fred_series(sid)
        spent += 1
        time.sleep(SPACING)
        if not rec:
            dead.add(sid)
            continue
        verified[sid] = {
            "alias": f"fred:{sid}",
            "title": rec.get("title"),
            "units": rec.get("units_short") or rec.get("units"),
            "freq": rec.get("frequency_short") or rec.get("frequency"),
            "last_updated": rec.get("last_updated"),
            "route": "fred-verified",
            "confidence": 1.0,
            "tv_symbol": t,
        }
        newly += 1

    s3.put_object(Bucket=BUCKET, Key=LEDGER_KEY,
                  Body=json.dumps({"verified": verified,
                                   "dead": sorted(dead),
                                   "updated": now.isoformat()}),
                  ContentType="application/json")

    # The artifact the vault consumes: bare TV code -> vault alias string.
    aliases = {code: row["alias"] for code, row in verified.items()}

    out = {
        "generated_at": now.isoformat(),
        "marker": MARKER,
        "aliases": aliases,
        "detail": verified,
        "n_aliases": len(aliases),
        "fred_tickers_total": len(fred_tickers),
        "fred_verified": len(verified),
        "fred_dead": len(dead),
        "verified_this_run": newly,
        "fred_calls_this_run": spent,
        "coverage_pct": round(len(verified) / max(len(fred_tickers), 1) * 100, 1),
        "by_route": dict(Counter(r["route"] for r in verified.values())),
        "note": ("Step 1 only: FRED: tickers, where the series id is the fetch "
                 "key and every id is verified against the FRED API before it "
                 "becomes an alias. ECONOMICS: matching is intentionally "
                 "absent until real descriptions accumulate (extension "
                 "v1.8.1) — mapping a macro code without its description "
                 "would risk charting the wrong series under Khalid's note."),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out),
                  ContentType="application/json", CacheControl="max-age=300")

    print(f"[symbol-resolver] fred_tickers={len(fred_tickers)} "
          f"verified={len(verified)} (+{newly}) dead={len(dead)} calls={spent}")
    return {"statusCode": 200,
            "body": json.dumps({"fred_verified": len(verified),
                                "verified_this_run": newly,
                                "dead": len(dead)})}
