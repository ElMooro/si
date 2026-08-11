"""ops 4604 — plumbing v2.0.1 regate; supersedes the 4603 failure stamp.

4603 reds, root causes, fixes:
1. [warm] TGCR/BGCR 400 on FRED — they are NY Fed markets-API series,
   not FRED ids. v2.0.1 moves them to a keyless NYFED_RATES source and
   this op gap-fills the warm store from the same endpoint.
2. [invoke] false negative — the Lambda body is a JSON-encoded string,
   so the '"ok": true' substring never matches the escaped form. Parse
   properly.
3. [edge-payload] the CF purge 401s (CLOUDFLARE_API_TOKEN in the runner
   is dead — KHALID action item, same class as the FRED key rotation)
   and the first edge check ran inside the 600s CacheControl window.
   Recheck across a window longer than the TTL remnant.
"""
import json
import os
import sys
import time
import urllib.request
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


def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4604"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def nyfed_full(rate_id):
    d = json.loads(http_get(
        "https://markets.newyorkfed.org/api/rates/secured/"
        "%s/last/9000.json" % rate_id.lower(), timeout=60))
    obs = [{"date": o["effectiveDate"], "value": float(o["percentRate"])}
           for o in d.get("refRates", [])
           if o.get("effectiveDate") and o.get("percentRate") is not None]
    obs.sort(key=lambda o: o["date"])
    return obs


def main():
    misses = 0
    with report("4604_plumbing_regate") as r:
        r.heading("ops 4604 — plumbing v2.0.1 regate (supersedes 4603)")

        r.section("deploy-settle on the v2.0.1 marker")
        settled = False
        for att in range(16):
            try:
                import io
                import zipfile
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], timeout=60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "v2.0.1" in src and "NYFED_RATES" in src:
                    settled = True
                    r.log("zip carries v2.0.1 (attempt %d)" % (att + 1))
                    break
            except Exception as e:
                r.log("attempt %d: %s" % (att + 1, str(e)[:100]))
            time.sleep(30)
        misses += contract(r, "deploy", settled,
                           "deployed zip carries v2.0.1 + NYFED_RATES")
        if not settled:
            sys.exit(1)

        r.section("warm gap-fill: TGCR/BGCR from the NY Fed (keyless)")
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
                        "source": "ops-4604-nyfed-gapfill"}).encode(),
                    ContentType="application/json")
                filled.append("%s(%d)" % (sid, len(obs)))
            except Exception as e:
                r.warn("%s: %s" % (sid, str(e)[:80]))
        r.log("banked: %s" % (", ".join(filled) or "none"))
        misses += contract(r, "warm", len(filled) == 2,
                           "TGCR + BGCR banked to the warm store "
                           "(data.html now carries all 17 L0 series)")

        r.section("invoke v2.0.1 + payload contract (proper parse)")
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
        raw = inv["Payload"].read().decode("utf-8", "replace")
        ok = False
        try:
            outer = json.loads(raw)
            body = json.loads(outer.get("body") or "{}")
            ok = bool(body.get("ok"))
            r.kv(invoke_composite=body.get("composite_score"),
                 invoke_label=body.get("composite_label"),
                 invoke_n_with_data=body.get("n_with_data"))
        except Exception as e:
            r.warn("invoke parse: %s · raw=%s" % (e, raw[:120]))
        misses += contract(r, "invoke",
                           inv.get("StatusCode") == 200 and ok,
                           "engine returned ok:true (parsed)")

        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/plumbing-stress.json")["Body"].read())
        l0 = (pl.get("layers") or {}).get("L0") or {}
        ri = pl.get("raw_indicators") or {}
        comp = pl.get("composite_score")
        misses += contract(r, "schema", pl.get("schema_version") == "2.0",
                           "schema 2.0")
        misses += contract(r, "L0-cov", (l0.get("n_with_data") or 0) >= 18,
                           "L0 coverage >=18 with TGCR/BGCR live "
                           "(got %s/%s)" % (l0.get("n_with_data"),
                                            l0.get("n_indicators")))
        for sid in ("TGCR", "BGCR", "SOFR_TGCR_BP"):
            v = (ri.get(sid) or {}).get("value")
            misses += contract(r, sid, v is not None,
                               "%s carrying data (value=%s)" % (sid, v))
        for lid in ("L1", "L2", "L3", "L4"):
            sc = ((pl.get("layers") or {}).get(lid) or {}).get("score")
            misses += contract(r, lid + "-regress", sc is not None,
                               "%s still scoring (%s)" % (lid, sc))
        misses += contract(r, "composite",
                           comp is not None and 0 <= comp <= 100,
                           "composite %s (%s)"
                           % (comp, pl.get("composite_label")))

        r.section("edge: payload freshness past the 600s TTL")
        cf_tok = bool(os.environ.get("CLOUDFLARE_API_TOKEN"))
        if not cf_tok:
            r.warn("CLOUDFLARE_API_TOKEN absent in runner env")
        payload_ok = False
        for att in range(12):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/plumbing-stress.json?cb=%d"
                    % time.time()))
                if jd.get("schema_version") == "2.0":
                    payload_ok = True
                    r.log("edge serving schema 2.0 (attempt %d, as_of=%s)"
                          % (att + 1, jd.get("as_of")))
                    break
            except Exception as e:
                r.log("edge attempt %d: %s" % (att + 1, str(e)[:80]))
            time.sleep(30)
        misses += contract(r, "edge-payload", payload_ok,
                           "served plumbing-stress.json is schema 2.0")
        try:
            pg = http_get("https://justhodl.ai/plumbing.html?cb=%d"
                          % time.time()).decode("utf-8", "replace")
            misses += contract(r, "edge-page",
                               "L0 \u2014 Repo Core" in pg
                               and "conceptMap" in pg,
                               "page still carries L0 card + concept map")
        except Exception as e:
            misses += contract(r, "edge-page", False,
                               "page fetch failed: %s" % str(e)[:80])

        r.section("verdict")
        if misses:
            r.fail("regate: %d red (CF token 401 remains a KHALID "
                   "action item)" % misses)
            sys.exit(1)
        r.ok("Plumbing note closeout GREEN — v2.0.1 live, 17/17 L0 series "
             "on the warm store, TGCR/BGCR + SOFR-TGCR spread carrying "
             "data, composite=%s (%s), edge serving schema 2.0. "
             "Supersedes the 4603 failure stamp. Pending KHALID: rotate "
             "CLOUDFLARE_API_TOKEN in the runner (401) and the leaked "
             "FRED key." % (comp, pl.get("composite_label")))


if __name__ == "__main__":
    main()
