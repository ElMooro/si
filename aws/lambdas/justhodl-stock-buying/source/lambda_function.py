"""justhodl-stock-buying v1.5.2 (Grok FMP /stable/)

Khalid's flagship screener: hunt the LARGEST POSITIVE CHANGE the
market hasn't priced — not the cheapest stock. Institutional
multi-factor composite over the fleet's own stores:

  fundamental-census-matrix (fundam['damentals'] quarters),
  _ma200/closes (SMA~250d, RS, double-bottom, accumulation),
  industry-boom (booming-industry gate + AI-class catalyst),
  deal-scanner (catalyst tape), FMP surprises/estimates
  (revisions + beat streak; warm-cached, key injected by ops).

HARD GATES: price ≤ long-SMA (accumulation zone), EPS up every
quarter (last 4 q/q), dilution ≤ +4%/yr, margin ≥ industry floor.
PILLARS (Khalid's ranks → five-core weights): revisions 25 ·
accel(EPS+rev) 25 · FCF/share 15 · valuation-vs-growth (fwd PEG,
P/E vs industry, FCF yield) 15 · catalyst+RS+volume 20. Quality
(ROIC, margin expansion, backlog) modulates ±10.
Every row carries pillar scores, gate verdicts with reasons, and
why: why.html?ticker=SYM. Missing inputs mark pillars n/a — never
fabricated. Davis double-play framing in doctrine.
"""
import json
import math
import os
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/stock-buying.json"
WARM = "data/warm/blackswan/"
s3 = boto3.client("s3", region_name="us-east-1")
FMP_KEY = os.environ.get("FMP_API_KEY", "")
FMP_BUDGET = {"n": 120}


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def attach_base_rates(rows, br):
    """ops 4819 (Fusion 1 consumer): additive, NON-GATING odds chips.
    Marker: sb-odds v1.  Joins data/base-rates.json empirical
    beat-SPX odds onto screener rows by `symbol`; quintile cells
    matched by their `q` FIELD, never list position (boom_score
    doctrine).  Only ADDS `base_rate_odds` -- scores, tiers, gates
    and every pre-existing field stay byte-identical; spine absent or
    INSUFFICIENT -> honest None, rows untouched."""
    if not br or br.get("status") != "LIVE" or not isinstance(rows,
                                                              list):
        return None
    asg = br.get("current_assignments") or {}
    c26 = (br.get("cohorts") or {}).get("26w") or {}
    by_q = {c.get("q"): c for c in (c26.get("quintiles") or [])
            if isinstance(c, dict)}
    dd_cells = c26.get("dd_bands") or {}
    n = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        a = asg.get(r.get("symbol"))
        if not a:
            continue
        odds = {"q": a.get("q"), "bucket": a.get("b"),
                "dd_band": a.get("dd"), "mom_6m_pct": a.get("m6"),
                "horizon": "26w",
                "src": "base-rates " + str(br.get("as_of"))}
        cell = by_q.get(a.get("q"))
        if cell:
            odds["beat_spx_pct"] = cell.get("beat_pct")
            odds["lb95_pct"] = cell.get("wilson_lb95_pct")
            odds["median_excess_pp"] = cell.get("median_excess_pp")
            odds["cohort_n"] = cell.get("n")
        band = dd_cells.get(a.get("dd") or "")
        if band:
            odds["dd_beat_spx_pct"] = band.get("beat_pct")
        r["base_rate_odds"] = odds
        n += 1
    return {"as_of": br.get("as_of"),
            "ledger_weeks": ((br.get("diag") or {}).get("feeds")
                             or {}).get("ledger_weeks"),
            "rows_with_odds": n}


def fmp(path, qs=""):
    if not FMP_KEY or FMP_BUDGET["n"] <= 0:
        return None
    FMP_BUDGET["n"] -= 1
    try:
        try:
            from fmp_stable import fmp_url as _fmp_url
            url = _fmp_url(path, qs, FMP_KEY)
        except Exception:
            if "/" in path and not path.startswith("http"):
                head, _, tail = path.partition("/")
                qs2 = (("symbol=%s&" % tail) + (qs + "&" if qs else ""))
                url = ("https://financialmodelingprep.com/stable/%s?%s"
                       "apikey=%s" % (head, qs2, FMP_KEY))
            else:
                url = ("https://financialmodelingprep.com/stable/%s?%s"
                       "apikey=%s" % (path, (qs + "&") if qs else "",
                                      FMP_KEY))
        req = urllib.request.Request(
            url, headers={"User-Agent": "justhodl-fleet"})
        time.sleep(0.12)
        with urllib.request.urlopen(req, timeout=14) as h:
            return json.loads(h.read())
    except Exception:
        return None
