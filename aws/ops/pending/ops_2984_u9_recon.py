#!/usr/bin/env python3
"""ops 2984 v2 -- RECON for U9 + allocator bridge + M7/M10/M21.
Crash-proof: the report ALWAYS lands, carrying any traceback.
A. Orphan roster (wiring orphan_detail x audit families) -> report.
B. Compass universe tickers (bridge sleeve mapping).
C. Live dealer-gex keys (0DTE presence).
D. SIFMA + Truflation probes.  E. Allocator live shape.
Read-only. [skip-deploy]
"""
import json
import sys
import traceback
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def probe(url, timeout=25):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(4000)
            return {"status": r.status,
                    "ctype": r.headers.get("Content-Type", "")[:60],
                    "head": body[:180].decode("utf-8", "replace")}
    except Exception as e:
        return {"status": "ERR", "err": str(e)[:120]}


def recon(rep, fails, hl):
    rep.section("A. Orphan roster")
    wiring = s3_json("data/engine-wiring.json") or {}
    audit = s3_json("data/fleet-audit.json") or {}
    fam, outs = {}, {}
    for r in (audit.get("rows") or audit.get("engines") or []):
        n = r.get("name") or r.get("engine")
        if n:
            fam[n] = r.get("family") or r.get("category") or "?"
            o = r.get("outs") or r.get("out_keys") or []
            outs[n] = o if isinstance(o, list) else [o]
    roster = []
    for o in (wiring.get("orphan_detail") or []):
        n = o.get("name")
        roster.append({"name": n, "family": fam.get(n) or "?",
                       "out": (o.get("freshest_key")
                               or (outs.get(n) or [None])[0]),
                       "age_h": o.get("age_h")
                       or o.get("freshest_age_h"),
                       "status": o.get("status") or o.get("state")})
    fresh = [r for r in roster
             if str(r.get("status") or "").upper().startswith("FRESH")
             or (isinstance(r.get("age_h"), (int, float))
                 and r["age_h"] <= 72)]
    rep.kv(orphans_total=len(roster), orphans_fresh=len(fresh),
           families=json.dumps(sorted({str(r["family"])
                                       for r in fresh})))
    hl["orphan_roster"] = roster
    hl["orphans_fresh_n"] = len(fresh)
    hl["wiring_keys"] = sorted(wiring.keys())
    if len(roster) < 50:
        fails.append("orphan roster small: %d (wiring keys: %s)"
                     % (len(roster), sorted(wiring.keys())[:12]))

    rep.section("B. Compass universe")
    comp = s3_json("data/asset-compass.json") or {}
    tks = [a.get("ticker") for a in (comp.get("assets") or [])]
    er_ok = sum(1 for a in (comp.get("assets") or [])
                if a.get("er_1y_pct") is not None)
    hl["compass_tickers"] = tks
    rep.kv(tickers=json.dumps(tks), modeled=er_ok)
    if er_ok < 15:
        fails.append("compass modeled thin: %d" % er_ok)

    rep.section("C. dealer-gex live keys")
    dg = s3_json("data/dealer-gex.json") or {}
    keys = sorted(dg.keys())
    odte = {k: dg[k] for k in keys if "dte" in k.lower()}
    sub_keys = []
    for k in ("underlyings", "symbols", "boards", "gex", "by_symbol"):
        v = dg.get(k)
        if isinstance(v, dict) and v:
            first = list(v.values())[0]
            if isinstance(first, dict):
                sub_keys = sorted(first.keys())
                odte.update({("%s.%s" % (k, sk)): 1 for sk in sub_keys
                             if "dte" in sk.lower()})
            break
        if isinstance(v, list) and v and isinstance(v[0], dict):
            sub_keys = sorted(v[0].keys())
            odte.update({("%s[0].%s" % (k, sk)): 1 for sk in sub_keys
                         if "dte" in sk.lower()})
            break
    rep.kv(top_keys=json.dumps(keys)[:400],
           odte_fields=json.dumps(odte)[:300],
           sub_keys=json.dumps(sub_keys)[:400])
    hl["dealer_gex_keys"] = keys
    hl["dealer_gex_odte"] = odte
    hl["dealer_gex_sub"] = sub_keys

    rep.section("D. SIFMA + Truflation probes")
    probes = {
        "sifma_stats": probe("https://www.sifma.org/resources/"
                             "research/statistics/"),
        "sifma_corp": probe("https://www.sifma.org/resources/research/"
                            "statistics/us-corporate-bonds-statistics/"),
        "sifma_fi": probe("https://www.sifma.org/resources/research/"
                          "statistics/fixed-income-chart/"),
        "truflation_nokey": probe("https://api.truflation.com/"
                                  "current?format=json"),
    }
    for k, v in probes.items():
        rep.kv(**{k: json.dumps(v)[:220]})
    hl["probes"] = probes

    rep.section("E. Allocator live shape")
    ma = s3_json("data/master-allocation.json") or {}
    hl["allocator_keys"] = sorted(ma.keys())
    hl["allocator_summary"] = ma.get("summary")
    rep.kv(alloc_keys=json.dumps(sorted(ma.keys()))[:300],
           summary=json.dumps(ma.get("summary"))[:250])
    if not ma:
        fails.append("master-allocation.json missing")


def main():
    fails, hl = [], {}
    with report("2984_u9_recon") as rep:
        try:
            recon(rep, fails, hl)
        except Exception:
            fails.append("CRASH: " + traceback.format_exc()[-700:])
        out = {"ops": 2984, "fails": fails,
               "verdict": "PASS" if not fails else "FAIL",
               "ts": datetime.now(timezone.utc).isoformat()}
        out.update(hl)
        rp = AWS_DIR / "ops" / "reports" / "2984.json"
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text(json.dumps(out, indent=1))
        rep.log("FAILS=%d" % len(fails))
        if fails:
            sys.exit(1)


main()
sys.exit(0)
