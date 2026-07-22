#!/usr/bin/env python3
"""ops 3729 — PROBE ONLY: U.S. Census International Trade API (imports).

Supersedes 3728, which crashed instantly: report() is a CONTEXT MANAGER and
was called bare, so no report ever flushed. Same gates, correct usage.

Audit-first per house rule. Learn from the LIVE endpoint on the runner
(sandbox cannot reach api.census.gov):

  G1  which timeseries endpoints answer (hs / naics / porths / statehs)
  G2  exact variable vocabulary each accepts
  G3  latest available month (trade data runs ~2 months behind)
  G4  HS 6-digit detail served, or only 2/4-digit
  G5  district / port-of-entry breakout availability
  G6  real payload: HS 8542 (integrated circuits) from Taiwan
  G7  NAICS path (the clean bridge to industry-boom)

NOTHING is deployed. No Lambda, no S3 write. Output is a vocabulary report
the engine ops will be built against, so it does not guess.
"""
import json
import os
import ssl
import sys
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

KEY = os.environ.get("CENSUS_API_KEY", "8423ffa543d0e95cdba580f2e381649b6772f515")
UA = {"User-Agent": "JustHodl research admin@justhodl.ai"}
CTX = ssl.create_default_context()

BASES = {
    "hs_imports":      "https://api.census.gov/data/timeseries/intltrade/imports/hs",
    "naics_imports":   "https://api.census.gov/data/timeseries/intltrade/imports/naics",
    "porths_imports":  "https://api.census.gov/data/timeseries/intltrade/imports/porths",
    "statehs_imports": "https://api.census.gov/data/timeseries/intltrade/imports/statehs",
    "hs_exports":      "https://api.census.gov/data/timeseries/intltrade/exports/hs",
}

WANT = ("GEN_VAL_MO", "GEN_VAL_YR", "CON_VAL_MO", "GEN_QY1_MO",
        "I_COMMODITY", "I_COMMODITY_LDESC", "E_COMMODITY",
        "CTY_CODE", "CTY_NAME", "DISTRICT", "DIST_NAME",
        "PORT", "PORT_NAME", "NAICS", "NAICS_LDESC",
        "SUMMARY_LVL", "COMM_LVL", "time", "YEAR", "MONTH")


with report("3729_census_trade_probe") as rep:
    rep.heading("ops 3729 — Census International Trade API vocabulary probe (no deploy)")
    fails = []
    findings = {"endpoints": {}, "latest_month": None}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3729.json").write_text(json.dumps({"verdict": "STARTED"}))

    try:
        def get(url, timeout=45):
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
                return r.status, r.read().decode("utf-8", "replace")

        def try_json(url, label, show=3):
            try:
                st, body = get(url)
            except urllib.error.HTTPError as e:
                detail = e.read().decode("utf-8", "replace")[:280]
                rep.warn(f"{label}: HTTP {e.code} — {detail}")
                return None
            except Exception as e:
                rep.warn(f"{label}: ERR {type(e).__name__} {str(e)[:150]}")
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
                rep.ok(f"{label}: {len(data)} rows | header={data[0]}")
                for row in data[1:1 + show]:
                    rep.log(f"    {row}")
            else:
                rep.ok(f"{label}: {str(data)[:200]}")
            return data

        # ── G1 endpoint discovery ────────────────────────────────────────
        rep.section("G1 — which trade endpoints answer")
        alive = {}
        for name, base in BASES.items():
            try:
                st, body = get(base + "/variables.json", timeout=40)
                if st == 200:
                    v = json.loads(body).get("variables", {})
                    alive[name] = sorted(v.keys())
                    findings["endpoints"][name] = len(v)
                    rep.ok(f"{name}: LIVE, {len(v)} variables")
                else:
                    rep.warn(f"{name}: status {st}")
            except Exception as e:
                rep.warn(f"{name}: unreachable — {type(e).__name__} {str(e)[:110]}")

        if not alive:
            rep.fail("G1 FAIL — no Census trade endpoint answered")
            fails.append("G1")

        # ── G2 vocabulary ────────────────────────────────────────────────
        rep.section("G2 — variable vocabulary")
        for name, vlist in alive.items():
            have = [w for w in WANT if w in vlist]
            miss = [w for w in WANT if w not in vlist]
            rep.log(f"  {name} HAVE: {have}")
            rep.log(f"  {name} MISS: {miss}")
            findings["endpoints"][name] = {"n_vars": len(vlist), "have": have}

        # ── G3 latest month ──────────────────────────────────────────────
        rep.section("G3 — latest available month")
        now = datetime.now(timezone.utc)
        latest = None
        if "hs_imports" in alive:
            months = []
            y, m = now.year, now.month
            for _ in range(9):
                m -= 1
                if m == 0:
                    m, y = 12, y - 1
                months.append(f"{y}-{m:02d}")
            for ym in months:
                q = {"get": "CTY_CODE,GEN_VAL_MO", "time": ym,
                     "COMM_LVL": "HS2", "I_COMMODITY": "85", "key": KEY}
                try:
                    st, body = get(BASES["hs_imports"] + "?" + urllib.parse.urlencode(q), 40)
                    if st == 200:
                        d = json.loads(body)
                        if isinstance(d, list) and len(d) > 1:
                            latest = ym
                            rep.ok(f"latest month with data: {ym} ({len(d)-1} rows)")
                            break
                except Exception:
                    continue
            if not latest:
                rep.warn("no month in last 9 returned data — COMM_LVL grammar may differ")
        findings["latest_month"] = latest

        # ── G4 HS granularity ────────────────────────────────────────────
        rep.section("G4 — HS granularity (8542 = electronic integrated circuits)")
        if "hs_imports" in alive and latest:
            for lvl, code in (("HS2", "85"), ("HS4", "8542"), ("HS6", "854231")):
                q = {"get": "I_COMMODITY,I_COMMODITY_LDESC,CTY_NAME,GEN_VAL_MO",
                     "time": latest, "COMM_LVL": lvl, "I_COMMODITY": code, "key": KEY}
                try_json(BASES["hs_imports"] + "?" + urllib.parse.urlencode(q),
                         f"{lvl} {code}", show=3)
        else:
            rep.warn("skipped — need hs_imports + known-good month")

        # ── G5 district ──────────────────────────────────────────────────
        rep.section("G5 — district / port-of-entry breakout")
        if "porths_imports" in alive and latest:
            q = {"get": "DISTRICT,DIST_NAME,I_COMMODITY,GEN_VAL_MO",
                 "time": latest, "COMM_LVL": "HS4", "I_COMMODITY": "8542", "key": KEY}
            try_json(BASES["porths_imports"] + "?" + urllib.parse.urlencode(q),
                     "porths HS4 8542 by district", show=5)
        else:
            rep.warn("porths unavailable or no month")

        # ── G6 Taiwan proof ──────────────────────────────────────────────
        rep.section("G6 — Taiwan semiconductor imports (CTY_CODE 5830)")
        if "hs_imports" in alive and latest:
            q = {"get": "I_COMMODITY,CTY_NAME,GEN_VAL_MO,GEN_VAL_YR",
                 "time": latest, "COMM_LVL": "HS4",
                 "I_COMMODITY": "8542", "CTY_CODE": "5830", "key": KEY}
            d = try_json(BASES["hs_imports"] + "?" + urllib.parse.urlencode(q),
                         "TW HS4 8542", show=3)
            if d and len(d) > 1:
                rep.ok("PROOF: country x HS x month import value retrievable")
            else:
                rep.warn("TW row empty — verify CTY_CODE for Taiwan")

        # ── G7 NAICS bridge ──────────────────────────────────────────────
        rep.section("G7 — NAICS import path (bridge to industry-boom)")
        if "naics_imports" in alive and latest:
            for lvl, code in (("NA4", "3344"), ("NA6", "334413")):
                q = {"get": "NAICS,CTY_NAME,GEN_VAL_MO", "time": latest,
                     "COMM_LVL": lvl, "NAICS": code, "key": KEY}
                try_json(BASES["naics_imports"] + "?" + urllib.parse.urlencode(q),
                         f"NAICS {lvl} {code}", show=4)
        else:
            rep.warn("naics_imports unavailable or no month")

        # ── verdict ──────────────────────────────────────────────────────
        rep.section("VERDICT")
        rep.kv(endpoints_alive=",".join(sorted(alive)) or "NONE",
               latest_month=latest or "UNKNOWN",
               probe_only="true", deployed="nothing")
        Path("aws/ops/reports/3729.json").write_text(
            json.dumps({"verdict": "PASS" if not fails else "FAIL",
                        "findings": findings}, indent=2))

        if fails:
            rep.fail(f"probe failed gates: {fails}")
            sys.exit(1)
        rep.ok("PROBE COMPLETE — engine will be built against this vocabulary")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3729.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
