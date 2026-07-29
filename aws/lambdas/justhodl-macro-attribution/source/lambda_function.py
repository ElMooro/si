"""justhodl-macro-attribution — who actually publishes each macro series.

WHY (ops 4079/4081/4082): TradingView returns source=null for every
symbol in its macro namespace, so the browser harvester can never produce
agency attribution no matter how it is tuned.  ops 4082 found the real
route: FRED exposes the publishing institution through its own metadata
chain, series -> release -> sources, which returned "U.S. Bureau of
Economic Analysis" and "National Bureau of Economic Research" on live
probes.  That is attribution from the publisher, not inference.

THREE RESOLUTION ROUTES, in descending order of authority:
  1. FRED:<id>            -> FRED series/release -> release/sources
  2. ECONOMICS:<code> that matches a vault row whose resolved_via is
     fred:<id>            -> same FRED chain, via the vault bridge
  3. ECONOMICS:<code> that matches a vault row whose resolved_via is a
     government adapter (boj:/ecb:/mofjp:/ust:/norges:/bcrp:/bcb:/boe:/
     snb:/imf:/estat:) -> that agency, from the gov-sources registry

WHAT IS DELIBERATELY *NOT* DONE
  ECONOMICS:JPM3 is described as "Japan Money Supply M3".  A country+topic
  heuristic would map it to the Bank of Japan and look authoritative.  It
  is a guess.  This fleet has already shipped one false corroboration that
  way (global-recession v1.2 read "CONFIRMED" off a defaulted 0.0 field),
  so every symbol that would only resolve by inference is written out as
  UNATTRIBUTED with route="none".  A smaller honest number beats a large
  invented one; the unattributed count is published so the gap is visible
  rather than hidden.

LEDGER: FRED is rate-limited, so resolutions accrete into
macro-attribution/ledger.json across runs instead of re-fetching. Release
-> sources is cached separately because a few hundred releases cover
thousands of series.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone

import boto3

MARKER = "macro-attribution v1.0 ops4083"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/macro-attribution.json"
LEDGER_KEY = "macro-attribution/ledger.json"
FRED_KEY = os.environ.get("FRED_KEY", "")

# Budget per run: FRED allows ~120 req/min. Stay well under and let the
# ledger close the gap over successive days rather than risk a ban.
MAX_NEW = int(os.environ.get("MAX_NEW", "320"))
SPACING = 0.34

s3 = boto3.client("s3", region_name="us-east-1")

GOV_PREFIXES = {
    "boj": "BOJ", "ecb": "ECB", "mofjp": "MOF-JAPAN", "ust": "US-TREASURY",
    "norges": "NORGES", "bcrp": "BCRP-PERU", "bcb": "BCB-BRAZIL",
    "boe": "BOE", "snb": "SNB", "imf": "IMF", "estat": "ESTAT-JAPAN",
    "eurostat": "EUROSTAT",
}

# Publisher name -> the family vocabulary source-map already speaks.
FAMILY_HINTS = [
    ("bureau of economic analysis", "BEA"),
    ("bureau of labor", "BLS"),
    ("census bureau", "CENSUS-US"),
    ("board of governors", "FRED"),
    ("federal reserve bank", "FRED"),
    ("department of the treasury", "US-TREASURY"),
    ("energy information", "EIA"),
    ("bank of japan", "BOJ"),
    ("european central bank", "ECB"),
    ("eurostat", "EUROSTAT"),
    ("bank of england", "BOE"),
    ("swiss national bank", "SNB"),
    ("international monetary fund", "IMF"),
    ("organisation for economic", "OECD"),
    ("organization for economic", "OECD"),
    ("world bank", "WORLD-BANK"),
    ("national bureau of economic research", "NBER"),
    ("bureau of transportation", "BTS-US"),
    ("commodity futures", "CFTC"),
]


def gj(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def family_of(pub):
    t = str(pub or "").lower()
    for frag, fam in FAMILY_HINTS:
        if frag in t:
            return fam
    return "OTHER-OFFICIAL"


def fred_get(path, params, tries=2):
    params = dict(params)
    params.update({"api_key": FRED_KEY, "file_type": "json"})
    url = f"https://api.stlouisfed.org/fred/{path}?" + urllib.parse.urlencode(params)
    for a in range(tries):
        try:
            return json.loads(urllib.request.urlopen(url, timeout=20).read())
        except Exception:
            if a + 1 >= tries:
                return None
            time.sleep(1.2)
    return None


def lambda_handler(event, context):
    print(f"[macro-attribution] {MARKER}")
    now = datetime.now(timezone.utc)

    ledger = gj(LEDGER_KEY, {}) or {}
    resolved = ledger.get("resolved") or {}      # symbol -> row
    rel_cache = ledger.get("releases") or {}     # release_id -> [publishers]
    unresolvable = set(ledger.get("unresolvable") or [])

    # ── the symbols we owe an answer for ──
    wl = gj("data/tv-watchlists.json", {}) or {}
    macro = set()
    for l in (wl.get("watchlists") or wl.get("lists") or []):
        for x in (l.get("symbols") or []):
            x = str(x).strip().upper()
            if x.startswith(("ECONOMICS:", "FRED:")):
                macro.add(x)

    # ── vault bridge: bare code -> resolved_via ──
    vault = gj("data/tradingview.json", {}) or {}
    bridge = {}
    for r in (vault.get("symbols") or []):
        sym = str(r.get("symbol") or "").upper()
        rv = str(r.get("resolved_via") or "")
        if sym and rv:
            bridge[sym] = rv

    def fred_id_for(sym):
        """Return the FRED series id this symbol resolves through, or None."""
        if sym.startswith("FRED:"):
            return sym.split(":", 1)[1]
        rv = bridge.get(sym.split(":", 1)[1], "")
        if rv.lower().startswith("fred:"):
            return rv.split(":", 1)[1]
        return None

    def gov_family_for(sym):
        rv = bridge.get(sym.split(":", 1)[1], "").lower()
        for pfx, fam in GOV_PREFIXES.items():
            if rv.startswith(pfx + ":"):
                return fam
        return None

    todo = [s for s in sorted(macro)
            if s not in resolved and s not in unresolvable]
    budget, spent = MAX_NEW, 0
    newly = 0

    for sym in todo:
        if spent >= budget or (context and
                               context.get_remaining_time_in_millis() < 45000):
            break

        # route 3 — government adapter in the vault (free, no API call)
        fam = gov_family_for(sym)
        if fam:
            resolved[sym] = {"publisher": fam, "family": fam,
                             "route": "vault-gov", "release": None}
            newly += 1
            continue

        # routes 1 & 2 — FRED's own metadata
        fid = fred_id_for(sym)
        if not fid or not FRED_KEY:
            unresolvable.add(sym)
            continue

        rel = fred_get("series/release", {"series_id": fid})
        spent += 1
        time.sleep(SPACING)
        rid = None
        rname = None
        if rel and rel.get("releases"):
            rid = rel["releases"][0].get("id")
            rname = rel["releases"][0].get("name")
        if rid is None:
            unresolvable.add(sym)
            continue

        pubs = rel_cache.get(str(rid))
        if pubs is None:
            sr = fred_get("release/sources", {"release_id": rid})
            spent += 1
            time.sleep(SPACING)
            pubs = [s.get("name") for s in ((sr or {}).get("sources") or [])
                    if s.get("name")]
            rel_cache[str(rid)] = pubs
        if not pubs:
            unresolvable.add(sym)
            continue

        pub = pubs[0]
        resolved[sym] = {"publisher": pub, "family": family_of(pub),
                         "route": "fred-metadata" if sym.startswith("FRED:")
                                  else "vault-fred",
                         "release": rname}
        newly += 1

    # ── persist the ledger (accretes across runs) ──
    s3.put_object(Bucket=BUCKET, Key=LEDGER_KEY,
                  Body=json.dumps({"resolved": resolved,
                                   "releases": rel_cache,
                                   "unresolvable": sorted(unresolvable),
                                   "updated": now.isoformat()}),
                  ContentType="application/json")

    # ── publish ──
    by_pub = Counter(v["publisher"] for v in resolved.values())
    by_fam = Counter(v["family"] for v in resolved.values())
    by_route = Counter(v["route"] for v in resolved.values())
    unattributed = sorted(macro - set(resolved))

    out = {
        "generated_at": now.isoformat(),
        "marker": MARKER,
        "macro_symbols": len(macro),
        "attributed": len(resolved),
        "unattributed": len(unattributed),
        "coverage_pct": round(len(resolved) / max(len(macro), 1) * 100, 1),
        "resolved_this_run": newly,
        "fred_calls_this_run": spent,
        "by_publisher": [{"publisher": p, "n_symbols": n,
                          "family": family_of(p)}
                         for p, n in by_pub.most_common(60)],
        "by_family": dict(by_fam),
        "by_route": dict(by_route),
        "attribution": resolved,
        # Published on purpose: the honest gap. These are symbols that
        # would only resolve by country+topic inference, which this engine
        # refuses to do.
        "unattributed_sample": unattributed[:40],
        "note": ("Attribution comes from FRED's own series/release/sources "
                 "metadata or a government adapter in the vault. Symbols "
                 "resolvable only by inferring an agency from a country and "
                 "a topic are reported UNATTRIBUTED, never guessed."),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out),
                  ContentType="application/json", CacheControl="max-age=300")

    print(f"[macro-attribution] macro={len(macro)} attributed={len(resolved)} "
          f"(+{newly}) unattributed={len(unattributed)} calls={spent} "
          f"families={dict(by_fam)}")
    return {"statusCode": 200,
            "body": json.dumps({"attributed": len(resolved),
                                "unattributed": len(unattributed),
                                "resolved_this_run": newly})}
