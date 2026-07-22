#!/usr/bin/env python3
"""ops 3728 — PROBE ONLY: U.S. Census International Trade API (imports).

Audit-first per house rule. Before building the HS-code import canary engine
we must learn, from the LIVE endpoint on the runner (sandbox cannot reach
api.census.gov):

  G1  which timeseries endpoints answer at all (hs vs naics, monthly imports)
  G2  the exact variable vocabulary each accepts (GEN_VAL_MO, CTY_CODE, I_COMMODITY, ...)
  G3  the latest available month (release lag — trade data runs ~2 months behind)
  G4  whether HS 6-digit detail is served, or only 2/4-digit chapters
  G5  district-level (port of entry) breakout availability
  G6  a real payload for one semiconductor line (HS 8542) from Taiwan

NOTHING is deployed here. No Lambda, no S3 write. This ops exists to write a
vocabulary report we can build against, so the engine ops does not guess.
"""
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

rep = report("3728_census_trade_probe")
rep.heading("Census International Trade API — vocabulary probe (no deploy)")

KEY = os.environ.get("CENSUS_API_KEY", "8423ffa543d0e95cdba580f2e381649b6772f515")
UA = {"User-Agent": "JustHodl research admin@justhodl.ai"}
CTX = ssl.create_default_context()

fails = []


def get(url, timeout=45):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
        return r.status, r.read().decode("utf-8", "replace")


def try_json(url, label, show=3):
    try:
        st, body = get(url)
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:300]
        rep.warn(f"{label}: HTTP {e.code} — {detail}")
        return None
    except Exception as e:
        rep.warn(f"{label}: ERR {type(e).__name__} {str(e)[:160]}")
        return None
    if st != 200:
        rep.warn(f"{label}: status {st}")
        return None
    try:
        data = json.loads(body)
    except Exception:
        rep.warn(f"{label}: non-JSON, first 200 = {body[:200]!r}")
        return None
    if isinstance(data, list) and data:
        rep.ok(f"{label}: {len(data)} rows, header = {data[0]}")
        for row in data[1:1 + show]:
            rep.log(f"    {row}")
    else:
        rep.ok(f"{label}: {str(data)[:220]}")
    return data


# ── G1 endpoint discovery ────────────────────────────────────────────────
rep.section("G1 — which trade endpoints answer")

BASES = {
    "hs_imports":      "https://api.census.gov/data/timeseries/intltrade/imports/hs",
    "naics_imports":   "https://api.census.gov/data/timeseries/intltrade/imports/naics",
    "porths_imports":  "https://api.census.gov/data/timeseries/intltrade/imports/porths",
    "statehs_imports": "https://api.census.gov/data/timeseries/intltrade/imports/statehs",
    "hs_exports":      "https://api.census.gov/data/timeseries/intltrade/exports/hs",
}

alive = {}
for name, base in BASES.items():
    try:
        st, body = get(base + "/variables.json", timeout=40)
        if st == 200:
            v = json.loads(body).get("variables", {})
            alive[name] = sorted(v.keys())
            rep.ok(f"{name}: LIVE, {len(v)} variables")
        else:
            rep.warn(f"{name}: status {st}")
    except Exception as e:
        rep.warn(f"{name}: unreachable — {type(e).__name__} {str(e)[:120]}")

if not alive:
    rep.fail("G1 FAIL — no Census trade endpoint answered; engine cannot be built on this source")
    fails.append("G1")

# ── G2 variable vocabulary ───────────────────────────────────────────────
rep.section("G2 — variable vocabulary (exact names matter)")
WANT = ("GEN_VAL_MO", "GEN_VAL_YR", "CON_VAL_MO", "GEN_QY1_MO",
        "I_COMMODITY", "I_COMMODITY_LDESC", "E_COMMODITY",
        "CTY_CODE", "CTY_NAME", "DISTRICT", "DIST_NAME",
        "PORT", "PORT_NAME", "NAICS", "SUMMARY_LVL",
        "COMM_LVL", "time", "YEAR", "MONTH")
for name, vlist in alive.items():
    have = [w for w in WANT if w in vlist]
    miss = [w for w in WANT if w not in vlist]
    rep.log(f"  {name}: HAVE {have}")
    rep.log(f"  {name}: MISS {miss}")

# ── G3 latest available month (release lag) ──────────────────────────────
rep.section("G3 — latest available month")
now = datetime.now(timezone.utc)
latest_found = None
if "hs_imports" in alive:
    probe_months = []
    y, m = now.year, now.month
    for _ in range(8):
        m -= 1
        if m == 0:
            m, y = 12, y - 1
        probe_months.append(f"{y}-{m:02d}")
    for ym in probe_months:
        q = {"get": "CTY_CODE,GEN_VAL_MO", "time": ym,
             "COMM_LVL": "HS2", "I_COMMODITY": "85", "key": KEY}
        url = BASES["hs_imports"] + "?" + urllib.parse.urlencode(q)
        try:
            st, body = get(url, timeout=40)
            if st == 200 and json.loads(body):
                latest_found = ym
                rep.ok(f"latest month with data: {ym}")
                break
        except Exception:
            continue
    if not latest_found:
        rep.warn("no month in last 8 returned data for HS2=85 — check COMM_LVL grammar")
else:
    rep.warn("hs_imports not alive; skipping month probe")

rep.kv(latest_month=latest_found or "UNKNOWN")

# ── G4 HS 6-digit detail ─────────────────────────────────────────────────
rep.section("G4 — HS 6-digit granularity (8542 = electronic integrated circuits)")
if "hs_imports" in alive and latest_found:
    for lvl, code in (("HS2", "85"), ("HS4", "8542"), ("HS6", "854231"),
                      ("HS10", None)):
        if code is None:
            continue
        q = {"get": "I_COMMODITY,I_COMMODITY_LDESC,CTY_CODE,CTY_NAME,GEN_VAL_MO",
             "time": latest_found, "COMM_LVL": lvl,
             "I_COMMODITY": code, "key": KEY}
        try_json(BASES["hs_imports"] + "?" + urllib.parse.urlencode(q),
                 f"{lvl} {code}", show=3)
else:
    rep.warn("skipped — need hs_imports + a known-good month")

# ── G5 district / port of entry ──────────────────────────────────────────
rep.section("G5 — district / port-of-entry breakout")
if "porths_imports" in alive and latest_found:
    q = {"get": "DISTRICT,DIST_NAME,I_COMMODITY,GEN_VAL_MO",
         "time": latest_found, "COMM_LVL": "HS4",
         "I_COMMODITY": "8542", "key": KEY}
    try_json(BASES["porths_imports"] + "?" + urllib.parse.urlencode(q),
             "porths HS4 8542 by district", show=5)
else:
    rep.warn("porths not alive or no month — district layer may be unavailable")

# ── G6 country-specific real payload (Taiwan = CTY_CODE 5830) ────────────
rep.section("G6 — Taiwan semiconductor imports (ticker-adjacent proof)")
if "hs_imports" in alive and latest_found:
    q = {"get": "I_COMMODITY,CTY_NAME,GEN_VAL_MO,GEN_VAL_YR",
         "time": latest_found, "COMM_LVL": "HS4",
         "I_COMMODITY": "8542", "CTY_CODE": "5830", "key": KEY}
    d = try_json(BASES["hs_imports"] + "?" + urllib.parse.urlencode(q),
                 "TW HS4 8542", show=3)
    if d and len(d) > 1:
        rep.ok("PROOF: country x HS x month import value is retrievable")
    else:
        rep.warn("TW row empty — verify CTY_CODE for Taiwan")

# ── G7 NAICS path (maps cleanly to your industry league) ─────────────────
rep.section("G7 — NAICS import path (best bridge to industry-boom)")
if "naics_imports" in alive and latest_found:
    q = {"get": "NAICS,NAICS_LDESC,CTY_CODE,CTY_NAME,GEN_VAL_MO",
         "time": latest_found, "COMM_LVL": "NA4", "NAICS": "3344", "key": KEY}
    try_json(BASES["naics_imports"] + "?" + urllib.parse.urlencode(q),
             "NAICS 3344 semiconductors", show=4)
else:
    rep.warn("naics_imports not alive or no month")

# ── verdict ──────────────────────────────────────────────────────────────
rep.section("VERDICT")
rep.kv(endpoints_alive=",".join(sorted(alive)) or "NONE",
       latest_month=latest_found or "UNKNOWN",
       probe_only="true", deployed="nothing")

if fails:
    rep.fail(f"probe failed gates: {fails}")
    sys.exit(1)

rep.ok("PROBE COMPLETE — build engine against the vocabulary above")
sys.exit(0)
