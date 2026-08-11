"""ops 4605 — v2.1.0: day-over-day shock layer + NY Fed URL fix.

Khalid: repo rate, reverse repo, and their DAY-TO-DAY %-change must be
first-class — a huge one-day move is a financial-crisis alarm (Sept 17
2019: SOFR +282bp/+116% in one session, Fed injected $53B from zero).
v2.1.0 adds 5 scored shock series + 3 hard canaries (repo_rate_shock,
rrp_swing, repo_ops_surge) and fixes TGCR/BGCR to the proven
search.json NY Fed pattern (the last/N form 400s past the cap — the
root of all five 4604 reds). Supersedes 4604's failure stamp.
"""
import io
import json
import os
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-plumbing-aggregator"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4605"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def nyfed_full(rate_id):
    d = json.loads(http_get(
        "https://markets.newyorkfed.org/api/rates/secured/"
        "%s/search.json?startDate=2014-08-01" % rate_id.lower(), 60))
    obs = [{"date": o["effectiveDate"], "value": float(o["percentRate"])}
           for o in d.get("refRates", [])
           if o.get("effectiveDate") and o.get("percentRate") is not None]
    obs.sort(key=lambda o: o["date"])
    return obs


def main():
    misses = 0
    with report("4605_plumbing_shock") as r:
        r.heading("ops 4605 — v2.1.0 DoD shock layer (supersedes 4604)")

        r.section("deploy-settle on the v2.1.0 marker")
        settled = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "v2.1.0" in src and "l0_shock_indicators" in src:
                    settled = True
                    r.log("zip carries v2.1.0 (attempt %d)" % (att + 1))
                    break
            except Exception as e:
                r.log("attempt %d: %s" % (att + 1, str(e)[:100]))
            time.sleep(30)
        misses += contract(r, "deploy", settled,
                           "deployed zip carries v2.1.0 shock layer")
        if not settled:
            sys.exit(1)

        r.section("warm gap-fill: TGCR/BGCR via search.json")
        filled = []
        for sid in ("TGCR", "BGCR"):
            try:
                obs = nyfed_full(sid)
                if not obs:
                    raise RuntimeError("0 observations")
                s3.put_object(
                    Bucket=B,
                    Key="data/warm/fred-scoped/Plumbing_L0/%s.json" % sid,
                    Body=json.dumps({
                        "series_id": sid, "observations": obs,
                        "n_obs": len(obs),
                        "fetched_at": datetime.now(
                            timezone.utc).isoformat(timespec="seconds"),
                        "source": "ops-4605-nyfed-gapfill"}).encode(),
                    ContentType="application/json")
                filled.append("%s(%d)" % (sid, len(obs)))
            except Exception as e:
                r.warn("%s: %s" % (sid, str(e)[:90]))
        r.log("banked: %s" % (", ".join(filled) or "none"))
        misses += contract(r, "warm", len(filled) == 2,
                           "TGCR + BGCR banked — data.html carries all "
                           "17 L0 series")

        r.section("invoke v2.1.0 + shock contracts")
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
        raw = inv["Payload"].read().decode("utf-8", "replace")
        ok = False
        try:
            body = json.loads(json.loads(raw).get("body") or "{}")
            ok = bool(body.get("ok"))
        except Exception as e:
            r.warn("parse: %s · %s" % (e, raw[:120]))
        misses += contract(r, "invoke",
                           inv.get("StatusCode") == 200 and ok,
                           "engine ok:true")

        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/plumbing-stress.json")["Body"].read())
        l0 = (pl.get("layers") or {}).get("L0") or {}
        ri = pl.get("raw_indicators") or {}
        fc = ((pl.get("enrichment") or {}).get("four_canary")
              or {}).get("canaries") or {}
        comp = pl.get("composite_score")
        misses += contract(r, "schema", pl.get("schema_version") == "2.0",
                           "schema 2.0")
        for sid in ("TGCR", "BGCR", "SOFR_TGCR_BP", "SOFR_DOD_BP",
                    "RRP_DOD_PCT", "REPO_OPS_DOD_BN"):
            v = (ri.get(sid) or {}).get("value")
            misses += contract(r, sid, v is not None,
                               "%s carrying data (latest=%s)" % (sid, v))
        misses += contract(r, "L0-cov", (l0.get("n_with_data") or 0) >= 21,
                           "L0 coverage >=21 (got %s/%s)"
                           % (l0.get("n_with_data"), l0.get("n_indicators")))
        shock_states = {}
        for cn in ("repo_rate_shock", "rrp_swing", "repo_ops_surge"):
            present = cn in fc
            shock_states[cn] = (fc.get(cn) or {}).get("state")
            misses += contract(r, "canary-" + cn, present,
                               "%s canary live (state=%s · %s)"
                               % (cn, shock_states[cn],
                                  json.dumps({k: v for k, v in
                                              (fc.get(cn) or {}).items()
                                              if k.startswith("value")
                                              or k == "rate"
                                              or k == "day_change_bn"})))
        for lid in ("L1", "L2", "L3", "L4"):
            sc = ((pl.get("layers") or {}).get(lid) or {}).get("score")
            misses += contract(r, lid + "-regress", sc is not None,
                               "%s still scoring (%s)" % (lid, sc))
        misses += contract(r, "composite",
                           comp is not None and 0 <= comp <= 100,
                           "composite %s (%s)"
                           % (comp, pl.get("composite_label")))
        r.kv(l0_score=l0.get("score"),
             sofr_dod_bp=(ri.get("SOFR_DOD_BP") or {}).get("value"),
             rrp_dod_pct=(ri.get("RRP_DOD_PCT") or {}).get("value"),
             shock_states=json.dumps(shock_states))

        r.section("edge freshness")
        edge_ok = False
        for att in range(12):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/plumbing-stress.json?cb=%d"
                    % time.time()))
                cane = ((jd.get("enrichment") or {}).get("four_canary")
                        or {}).get("canaries") or {}
                if "repo_rate_shock" in cane:
                    edge_ok = True
                    r.log("edge carries repo_rate_shock (attempt %d)"
                          % (att + 1))
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:80]))
            time.sleep(30)
        misses += contract(r, "edge", edge_ok,
                           "served payload carries the DoD shock canaries")

        r.section("verdict")
        if misses:
            r.fail("shock layer: %d red" % misses)
            sys.exit(1)
        r.ok("Day-over-day shock layer LIVE — repo/RRP daily %%-change "
             "scored + hard crisis canaries, TGCR/BGCR flowing, composite="
             "%s (%s), edge fresh. KHALID items open: rotate "
             "CLOUDFLARE_API_TOKEN (401) + leaked FRED key."
             % (comp, pl.get("composite_label")))


if __name__ == "__main__":
    main()
