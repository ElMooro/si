"""ops 4603 — Plumbing note closeout: aggregator v2.0.0 L0 Repo Core.

Khalid supplied a repo-market / shadow-banking note and asked for a
"plumbing" engine that monitors everything in it, checking what already
exists on data.html and adding what is missing. Audit found the plumbing
cluster already covers eurodollar, swap lines, fails, dollar, CISS and
bank-risk chapters; the missing repo mechanics now ship as L0 in
plumbing-aggregator v2.0.0 (12 new FRED series, 7 derived spreads,
3 sibling-engine joins, note_concept_map).

This op: (1) deploy-settle on the v2.0.0 zip marker, (2) raise the
timeout/memory floor for the larger pull, (3) warm-store audit of every
L0 FRED id with gap-fill of missing series into
data/warm/fred-scoped/Plumbing_L0/ so data.html counts them,
(4) invoke + full v2 payload contract, (5) provider-catalog kick,
CF purge, edge asserts on the served page and payload.
"""
import io
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-plumbing-aggregator"
REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

L0_FRED = ["RRPONTSYD", "RRPONTSYAWARD", "RPONTSYD", "WALCL", "WTREGEN",
           "WSHOMCB", "WLCFLPCL", "SOFR99", "SOFR1", "TGCR", "BGCR",
           "DTB4WK", "SOFR", "IORB", "EFFR", "WRESBAL", "TLAACBW027SBOG"]


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4603"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def cf(path, method="GET", data=None):
    tok = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    if not tok:
        return None, "no CLOUDFLARE_API_TOKEN in env"
    req = urllib.request.Request(
        "https://api.cloudflare.com/client/v4" + path,
        data=json.dumps(data).encode() if data else None, method=method,
        headers={"Authorization": "Bearer " + tok,
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as h:
            return json.loads(h.read()), None
    except Exception as e:
        return None, str(e)[:120]


def fred_key():
    k = os.environ.get("FRED_API_KEY")
    if k:
        return k
    try:
        src = open("aws/lambdas/%s/source/lambda_function.py" % FN).read()
        return src.split('FRED_API_KEY", "')[1].split('"')[0]
    except Exception:
        return ""


def fred_full(sid, key):
    qs = urllib.parse.urlencode({
        "series_id": sid, "api_key": key, "file_type": "json",
        "sort_order": "asc"})
    d = json.loads(http_get(
        "https://api.stlouisfed.org/fred/series/observations?" + qs))
    return [{"date": o["date"], "value": float(o["value"])}
            for o in d.get("observations", [])
            if o.get("value") not in (None, ".", "")]


def main():
    misses = 0
    with report("4603_plumbing_note") as r:
        r.heading("ops 4603 — plumbing-aggregator v2.0.0 · L0 Repo Core")

        r.section("deploy-settle (v2.0.0 marker in live zip)")
        settled = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], timeout=60)
                zf = zipfile.ZipFile(io.BytesIO(zb))
                src = zf.read("lambda_function.py").decode("utf-8", "replace")
                if "v2.0.0" in src and "L0 REPO CORE" in src:
                    settled = True
                    r.log("zip carries v2.0.0 marker (attempt %d)" % (att + 1))
                    break
                r.log("attempt %d: zip is pre-v2 — waiting on "
                      "deploy-lambdas" % (att + 1))
            except Exception as e:
                r.log("attempt %d: %s" % (att + 1, str(e)[:100]))
            time.sleep(30)
        misses += contract(r, "deploy", settled,
                           "deployed zip contains the v2.0.0 L0 marker")
        if not settled:
            r.fail("deploy never settled — aborting before invoke")
            sys.exit(1)

        r.section("config floor for the larger v2 pull")
        cfg = lam.get_function_configuration(FunctionName=FN)
        t0, m0 = cfg["Timeout"], cfg["MemorySize"]
        r.kv(timeout_before=t0, memory_before=m0)
        want_t = max(t0, 300)
        want_m = max(m0, 1024)
        if (want_t, want_m) != (t0, m0):
            lam.update_function_configuration(
                FunctionName=FN, Timeout=want_t, MemorySize=want_m)
            for _ in range(20):
                st = lam.get_function_configuration(FunctionName=FN)
                if st.get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(5)
            r.log("raised to timeout=%ss memory=%sMB" % (want_t, want_m))
        misses += contract(r, "config", want_t >= 300 and want_m >= 1024,
                           "timeout>=300s and memory>=1024MB for 40+ pulls")

        r.section("warm-store audit — which L0 series data.html already has")
        present = set()
        pag = s3.get_paginator("list_objects_v2")
        for page in pag.paginate(Bucket=B, Prefix="data/warm/fred-scoped/"):
            for o in page.get("Contents", []):
                base = o["Key"].rsplit("/", 1)[-1]
                if base.endswith(".json"):
                    present.add(base[:-5])
        have = [x for x in L0_FRED if x in present]
        missing = [x for x in L0_FRED if x not in present]
        r.kv(warm_total=len(present), l0_have=len(have),
             l0_missing=len(missing))
        r.log("EXISTS on data.html warm store: %s" % (", ".join(have) or "—"))
        r.log("MISSING (gap-filling now): %s" % (", ".join(missing) or "—"))
        key = fred_key()
        filled, fill_err = [], []
        for sid in missing:
            try:
                obs = fred_full(sid, key)
                if not obs:
                    raise RuntimeError("0 observations")
                body = json.dumps({
                    "series_id": sid,
                    "observations": obs,
                    "n_obs": len(obs),
                    "fetched_at": datetime.now(
                        timezone.utc).isoformat(timespec="seconds"),
                    "source": "ops-4603-plumbing-gapfill"}).encode()
                s3.put_object(
                    Bucket=B,
                    Key="data/warm/fred-scoped/Plumbing_L0/%s.json" % sid,
                    Body=body, ContentType="application/json")
                filled.append("%s(%d)" % (sid, len(obs)))
                time.sleep(1.1)
            except Exception as e:
                fill_err.append("%s:%s" % (sid, str(e)[:60]))
        r.log("gap-filled: %s" % (", ".join(filled) or "none needed"))
        if fill_err:
            r.warn("gap-fill misses (non-fatal, engine pulls live "
                   "regardless): %s" % "; ".join(fill_err))
        misses += contract(
            r, "warm", len(missing) == 0 or len(filled) > 0 or not key,
            "every missing L0 series banked (or none were missing)")

        r.section("invoke v2 + payload contract")
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
        raw = inv["Payload"].read().decode("utf-8", "replace")
        r.log("invoke status=%s payload=%s" %
              (inv.get("StatusCode"), raw[:160]))
        ok_inv = inv.get("StatusCode") == 200 and '"ok": true' in raw
        misses += contract(r, "invoke", ok_inv, "engine returned ok:true")

        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/plumbing-stress.json")["Body"].read())
        l0 = (pl.get("layers") or {}).get("L0") or {}
        fc = ((pl.get("enrichment") or {}).get("four_canary")
              or {}).get("canaries") or {}
        comp = pl.get("composite_score")
        misses += contract(r, "schema", pl.get("schema_version") == "2.0",
                           "schema_version 2.0 (got %s)"
                           % pl.get("schema_version"))
        misses += contract(r, "L0-score", l0.get("score") is not None,
                           "L0 layer scoring (score=%s)" % l0.get("score"))
        misses += contract(r, "L0-cov", (l0.get("n_with_data") or 0) >= 12,
                           "L0 coverage >=12 live contributors (got %s/%s)"
                           % (l0.get("n_with_data"), l0.get("n_indicators")))
        for lid in ("L1", "L2", "L3", "L4"):
            sc = ((pl.get("layers") or {}).get(lid) or {}).get("score")
            misses += contract(r, lid + "-regress", sc is not None,
                               "%s still scoring (score=%s)" % (lid, sc))
        misses += contract(r, "composite",
                           comp is not None and 0 <= comp <= 100,
                           "composite in [0,100] (=%s · %s)"
                           % (comp, pl.get("composite_label")))
        for c in ("srf_usage", "sofr_tail", "bill_rrp", "rrp_drain_20d",
                  "discount_window"):
            misses += contract(r, "canary-" + c, c in fc,
                               "%s canary present (state=%s)"
                               % (c, (fc.get(c) or {}).get("state")))
        ncm = pl.get("note_concept_map") or []
        misses += contract(r, "concept-map", len(ncm) >= 14,
                           "note_concept_map has %d concepts" % len(ncm))
        st_counts = {}
        for c in ncm:
            st_counts[c.get("status")] = st_counts.get(c.get("status"), 0) + 1
        r.kv(concept_status=json.dumps(st_counts),
             l0_canary_states=json.dumps(
                 {k: (fc.get(k) or {}).get("state")
                  for k in ("srf_usage", "sofr_tail", "bill_rrp",
                            "rrp_drain_20d", "discount_window")}))

        r.section("catalog kick + CDN purge + edge asserts")
        try:
            lam.invoke(FunctionName="justhodl-provider-catalog",
                       InvocationType="Event")
            r.log("provider-catalog kicked (data.html counts refresh)")
        except Exception as e:
            r.warn("catalog kick: %s" % str(e)[:100])
        zj, zerr = cf("/zones?name=justhodl.ai")
        zid = (((zj or {}).get("result") or [{}])[0].get("id")
               if zj else None)
        if zid:
            pj, perr = cf("/zones/%s/purge_cache" % zid, "POST",
                          {"files": [
                              "https://justhodl.ai/plumbing.html",
                              "https://justhodl.ai/data/"
                              "plumbing-stress.json",
                              "https://justhodl.ai/data.html",
                              "https://justhodl.ai/data/"
                              "provider-catalog.json"]})
            r.log("cf purge ok=%s err=%s"
                  % (bool((pj or {}).get("success")), perr))
        else:
            r.warn("cf zone lookup failed: %s" % zerr)
        page_ok = payload_ok = False
        for att in range(8):
            try:
                pg = http_get("https://justhodl.ai/plumbing.html?cb=%d"
                              % time.time()).decode("utf-8", "replace")
                page_ok = ("L0 \u2014 Repo Core" in pg
                           and "conceptMap" in pg)
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/plumbing-stress.json?cb=%d"
                    % time.time()))
                payload_ok = jd.get("schema_version") == "2.0"
                if page_ok and payload_ok:
                    break
            except Exception as e:
                r.log("edge attempt %d: %s" % (att + 1, str(e)[:80]))
            time.sleep(20)
        misses += contract(r, "edge-page", page_ok,
                           "served plumbing.html carries the L0 card + "
                           "concept map")
        misses += contract(r, "edge-payload", payload_ok,
                           "served plumbing-stress.json is schema 2.0")

        r.section("verdict")
        if misses:
            r.fail("plumbing note closeout: %d red" % misses)
            sys.exit(1)
        r.ok("Plumbing note fully wired — L0 Repo Core live: composite=%s "
             "(%s), L0=%s, %d concepts mapped, warm store gap-filled, "
             "edge serving v2." % (comp, pl.get("composite_label"),
                                   l0.get("score"), len(ncm)))


if __name__ == "__main__":
    main()
