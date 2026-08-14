"""ops 4664 — ICE BofA history: recover what is recoverable NOW,
probe the routes for the rest (Khalid: data stays on my system forever).

RECOVER (hard contracts): three independently cross-validated GitHub
archives (1996-12-31 -> 2026-04-02, snapshotted days before FRED's
Apr-2026 3y-window truncation) are downloaded in-runner, splice-checked
against our banked overlap, and union-merged into the SAME banked docs
with provenance. Data lands only in the private bucket — never the
repo (ICE internal-use terms).

PROBE (findings, soft): ALFRED pre-truncation vintages · DBnomics FRED
mirror depth · TradingView vault BAML coverage — the candidate lanes
for the other 189 series.
"""
import gzip
import json
import sys
import time
import urllib.request
from datetime import datetime

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=60,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
RECOVER = ["BAMLC0A0CM", "BAMLC0A4CBBB", "BAMLH0A0HYM2"]
RAW = ("https://raw.githubusercontent.com/SimoneFarinelli10/"
       "hierarchical-copula-aggregation/main/%s.csv")


def get(url, to=30):
    req = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai"})
    with urllib.request.urlopen(req, timeout=to) as r:
        return r.read()


def fred_key():
    ssm = boto3.client("ssm", region_name="us-east-1")
    for pn, dec in (("/justhodl/fred-api-key", True),
                    ("/justhodl/fred/api-key", False)):
        try:
            v = ssm.get_parameter(Name=pn, WithDecryption=dec
                                  )["Parameter"]["Value"]
            if v and len(v) >= 16 and v != "PLACEHOLDER":
                return v
        except Exception:
            continue
    return ""


def main():
    with report("4664_ice_recovery") as r:
        r.heading("ops 4664 — ICE BofA recovery + route probes")
        misses = 0

        r.section("1. RECOVER 3 series from verified archives")
        q = json.loads(gzip.decompress(s3.get_object(
            Bucket=B,
            Key="data/_state/fred-queue.json.gz")["Body"].read()))
        rmap = {row[0]: row[2] for row in (q.get("rows") or [])
                if row and len(row) >= 3}
        for sid in RECOVER:
            rname = rmap.get(sid)
            if not rname:
                misses += 1
                r.fail("  [%s] not in queue map — cannot locate doc"
                       % sid)
                continue
            key = ("data/warm/fred-scoped/%s/%s.json"
                   % (rname.replace(" ", "_"), sid))
            doc = json.loads(s3.get_object(
                Bucket=B, Key=key)["Body"].read())
            cur = doc.get("observations") or []
            cur_map = {o.get("date"): str(o.get("value"))
                       for o in cur}
            raw = get(RAW % sid).decode("utf-8", "replace")
            arch = []
            for ln in raw.splitlines()[1:]:
                pt = ln.split(",")
                if len(pt) >= 2 and pt[0][:2] in ("19", "20") \
                        and pt[1].strip() not in ("", "."):
                    arch.append({"date": pt[0].strip(),
                                 "value": pt[1].strip()})
            overlap = [a for a in arch if a["date"] in cur_map]
            mm = 0
            for a in overlap[-120:]:
                try:
                    if abs(float(a["value"])
                           - float(cur_map[a["date"]])) > 0.005:
                        mm += 1
                except Exception:
                    mm += 1
            keep = [a for a in arch if a["date"] not in cur_map]
            merged = sorted(keep + cur,
                            key=lambda o: str(o.get("date")))
            doc["observations"] = merged
            doc["meta"] = dict(
                doc.get("meta") or {},
                recovered_rows=len(keep),
                recovered_source=("github:SimoneFarinelli10/"
                                  "hierarchical-copula-aggregation@"
                                  "main (1996->2026-04 pre-"
                                  "truncation snapshot)"),
                history_floor=(merged[0]["date"] if merged else None),
                merge_v=1)
            s3.put_object(Bucket=B, Key=key,
                          Body=json.dumps(doc, default=str).encode(),
                          ContentType="application/json")
            first = merged[0]["date"] if merged else None
            r.log("  %s: archive=%d overlap=%d splice-mm=%d "
                  "recovered=%d -> doc now %d obs since %s"
                  % (sid, len(arch), len(overlap), mm, len(keep),
                     len(merged), first))
            if not (first == "1996-12-31" and len(merged) >= 7000
                    and mm == 0):
                misses += 1
                r.fail("  [%s] CONTRACT MISS (first=%s n=%d mm=%d)"
                       % (sid, first, len(merged), mm))
            else:
                r.ok("  [%s] full lineage 1996-12-31 -> present, "
                     "splice clean" % sid)

        r.section("2. PROBE — ALFRED pre-truncation vintages")
        k = fred_key()
        if not k:
            r.warn("  no FRED key — ALFRED probe skipped")
        else:
            for vint in ("2025-06-02", "2026-03-02"):
                try:
                    d = json.loads(get(
                        "https://api.stlouisfed.org/fred/series/"
                        "observations?series_id=BAMLH0A0HYM2&api_key="
                        + k + "&file_type=json&observation_start="
                        "1996-01-01&realtime_start=" + vint
                        + "&realtime_end=" + vint + "&limit=5"))
                    ob = d.get("observations") or []
                    fd = ob[0].get("date") if ob else None
                    r.log("  vintage %s: first obs %s (count field "
                          "%s) -> %s"
                          % (vint, fd, d.get("count"),
                             "LOOPHOLE OPEN" if (fd or "9999") < "2020"
                             else "window enforced retroactively"))
                except Exception as e:
                    r.warn("  vintage %s: %s" % (vint, str(e)[:80]))
                time.sleep(0.7)

        r.section("3. PROBE — DBnomics FRED mirror")
        for sid in ("BAMLH0A0HYM2", "BAMLC0A0CM",
                    "BAMLEMRLCRPILAOAS"):
            try:
                d = json.loads(get(
                    "https://api.db.nomics.world/v22/series/FRED/"
                    + sid + "?observations=1"))
                doc0 = ((d.get("series") or {}).get("docs")
                        or [{}])[0]
                per = doc0.get("period") or []
                r.log("  %s: %d obs, %s -> %s"
                      % (sid, len(per),
                         per[0] if per else None,
                         per[-1] if per else None))
            except Exception as e:
                r.warn("  %s: %s" % (sid, str(e)[:80]))
            time.sleep(0.5)

        r.section("4. PROBE — TradingView vault BAML coverage")
        try:
            hub = json.loads(s3.get_object(
                Bucket=B,
                Key="data/provider-catalog.json")["Body"].read())
            tv = next((p for p in hub.get("providers") or []
                       if "TradingView" in str(p.get("name"))), None)
            det = json.loads(s3.get_object(
                Bucket=B, Key="data/providers/%s.json"
                % tv["slug"])["Body"].read()) if tv else {}
            vk = ((det.get("keys") or [{}])[0]).get("key")
            r.log("  vault key: %s" % vk)
            vault = json.loads(s3.get_object(
                Bucket=B, Key=vk)["Body"].read()) if vk else {}
            blob = json.dumps(vault)
            import re as _re
            hits = sorted(set(_re.findall(r"BAML[A-Z0-9]{3,}",
                                          blob)))
            r.log("  BAML symbols in vault: %d (e.g. %s)"
                  % (len(hits), hits[:10]))
        except Exception as e:
            r.warn("  vault probe: %s" % str(e)[:90])

        r.section("verdict")
        if misses:
            r.fail("recovery: %d red" % misses)
            sys.exit(1)
        r.ok("3/3 recovered to 1996 with clean splices; probes on "
             "record for the other 189; engine merge-guard makes the "
             "recovered rows permanent")


if __name__ == "__main__":
    main()
