"""ops_4961 -- IMF full-warehouse GRAMMAR PROBE (drain-queue #3 prep)
+ live status of drains #1/#2 (bls-full, worldbank-full).

Doctrine: probe-verified overrides (census) + never pin a constant
the probe didn't test (usd-funding). IMF SDMX has two generations
(legacy dataservices.imf.org SDMX_JSON.svc vs api.imf.org /external/
sdmx/2.1) and hard server-side limits on full-flow pulls; the engine
ships NEXT ops on grammar proven HERE, from the runner:

  P0  drain-status preamble: bls-full + worldbank-full state
  P1  catalog: enumerate dataflows on BOTH generations
  P2  full-pull ladder on {IFS, BOP, DOT, PGI?}: csv + sdmx-json +
      compact, capture byte counts AND refusal bodies verbatim
  P3  slice grammar: one flow, one dimension slice (freq=A) to prove
      the ecb-deep-style ladder works when full-flow refuses
Artifact: data/warm/imf-full/_probe/probe.json  [skip-deploy]
"""
import gzip
import io
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
OUT = "data/warm/imf-full/_probe/probe.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)",
      "Accept": "*/*"}
LEG = "https://dataservices.imf.org/REST/SDMX_JSON.svc"
NEW = "https://api.imf.org/external/sdmx/2.1"

s3 = boto3.client("s3", region_name=REGION)


def gj(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return default


def fetch(url, cap=6_000_000, timeout=90):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(cap)
            more = bool(r.read(1))
            return {"status": r.status, "bytes": len(body),
                    "truncated": more,
                    "ct": r.headers.get("Content-Type"),
                    "head": body[:300].decode("utf-8", "replace"),
                    "s": round(time.time() - t0, 1)}, body
    except urllib.error.HTTPError as e:
        b = b""
        try:
            b = e.read(600)
        except Exception:
            pass
        return {"status": e.code,
                "head": b.decode("utf-8", "replace")[:300],
                "s": round(time.time() - t0, 1)}, b""
    except Exception as e:
        return {"status": 0, "head": str(e)[:200],
                "s": round(time.time() - t0, 1)}, b""


with report("ops_4961_imf_grammar_probe") as R:
    fails = []
    out = {"as_of": datetime.now(timezone.utc).isoformat(
        timespec="seconds"), "ops": 4961, "probes": {}}

    # P0 -- drains #1/#2 status ---------------------------------------
    R.section("P0 drain status (bls-full / worldbank-full)")
    for nm, sk in (("bls-full",
                    "data/warm/bls-full/_state/state.json"),
                   ("worldbank-full",
                    "data/warm/worldbank-full/_state/state.json")):
        st = gj(sk) or {}
        line = ("%s phase=%s files/banked=%s gb=%.2f q=%s fail=%s "
                "as_of=%s" % (
                    nm, st.get("phase"),
                    st.get("n_files") or st.get("n_banked"),
                    (st.get("bytes_total") or 0) / 1e9,
                    len(st.get("queue") or []),
                    len(st.get("failures") or {}),
                    (st.get("as_of") or "")[:19]))
        R.log("  " + line)
        out["probes"]["_status_" + nm] = line

    # P1 -- dataflow catalogs -----------------------------------------
    R.section("P1 dataflow catalogs (both generations)")
    meta, body = fetch(LEG + "/Dataflow")
    n_leg = body.decode("utf-8", "replace").count('"KeyFamilyRef"') \
        if body else 0
    R.log("  legacy Dataflow: %s bytes=%s flows~%d %s" % (
        meta.get("status"), meta.get("bytes"), n_leg,
        meta.get("head", "")[:80].replace("\n", " ")))
    out["probes"]["legacy_dataflow"] = dict(meta, flows=n_leg)
    meta2, body2 = fetch(NEW + "/dataflow?format=sdmx-json")
    n_new = body2.count(b'"id"') if body2 else 0
    R.log("  2.1 dataflow: %s bytes=%s ids~%d" % (
        meta2.get("status"), meta2.get("bytes"), n_new))
    out["probes"]["new_dataflow"] = dict(meta2, ids=n_new)
    if not (n_leg or n_new):
        fails.append("P1 no catalog")

    # P2 -- full-pull ladder on flagship flows ------------------------
    R.section("P2 full-pull ladder (capture refusals verbatim)")
    flows = ["IFS", "BOP", "DOT", "PGI"]
    for fl in flows:
        for label, url in (
                ("leg_compact_all",
                 LEG + "/CompactData/%s" % fl),
                ("new_csv_all",
                 NEW + "/data/%s?format=csv" % fl),
                ("new_json_all",
                 NEW + "/data/%s?format=sdmx-json" % fl)):
            meta, _ = fetch(url, cap=3_000_000, timeout=75)
            key = "%s.%s" % (fl, label)
            out["probes"][key] = meta
            R.log("  %-22s %s bytes=%s trunc=%s %s" % (
                key, meta.get("status"), meta.get("bytes"),
                meta.get("truncated"),
                (meta.get("head") or "")[:70].replace("\n", " ")))
            time.sleep(0.6)

    # P3 -- slice grammar (ecb-deep-style ladder) ---------------------
    R.section("P3 slice grammar (freq=A on IFS)")
    for label, url in (
            ("leg_compact_A",
             LEG + "/CompactData/IFS/A..NGDP_R_SA_XDC"),
            ("leg_compact_A_US",
             LEG + "/CompactData/IFS/A.US.NGDP_R_SA_XDC"
             "?startPeriod=1950"),
            ("new_csv_key",
             NEW + "/data/IFS/A.US.NGDP_R_SA_XDC?format=csv")):
        meta, _ = fetch(url, cap=3_000_000, timeout=75)
        out["probes"]["slice." + label] = meta
        R.log("  %-22s %s bytes=%s %s" % (
            label, meta.get("status"), meta.get("bytes"),
            (meta.get("head") or "")[:80].replace("\n", " ")))
        time.sleep(0.6)

    ok_any = any((v.get("status") == 200 and (v.get("bytes") or 0)
                  > 2000) for k, v in out["probes"].items()
                 if not k.startswith("_status"))
    s3.put_object(Bucket=B, Key=OUT,
                  Body=json.dumps(out, indent=1).encode(),
                  ContentType="application/json")
    R.log("artifact %s (ok_any=%s)" % (OUT, ok_any))
    if fails or not ok_any:
        R.log("ops 4961 RED: %s" % (fails or ["no 200s"]))
        sys.exit(1)
    R.kv(legacy_flows=n_leg, new_ids=n_new,
         probes=len(out["probes"]))
    R.log("ops 4961 GREEN -- IMF grammar captured; imf-full engine "
          "ships next on PROVEN access shapes")
