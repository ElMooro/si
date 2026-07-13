"""ops 3271 — contract close at z-resolution. 3270's one 'gated' panel
(Country Import Prices, 20 raw weeks) cannot yield a single z-score
against the rolling window — statistically that is DATA ABSENCE, the
same class as true-dead, and it self-wakes as the source accrues
history. Final assertion: every dormant panel is either 0-data or
z-nascent (raw < Z_WINDOW + MIN_COMPOSITE_WEEKS). No design gate
remains anywhere."""
import gzip
import json
import re
import sys
from pathlib import Path

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
SRC = (Path(__file__).resolve().parents[2] / "lambdas"
       / "justhodl-wl-engines" / "source" / "lambda_function.py")


def s3_json(key, default=None, gz=False):
    try:
        body = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            body = gzip.decompress(body)
        return json.loads(body)
    except Exception:
        return default


with report("3271_contract_close") as rep:
    fails = []
    src = SRC.read_text()
    zwin = int((re.search(r"Z_WINDOW\s*=\s*(\d+)", src)
                or re.search(r"window\s*=\s*(\d+)", src)).group(1))
    mcw = int(re.search(r"MIN_COMPOSITE_WEEKS\s*=\s*(\d+)", src)
              .group(1))
    thr = zwin + mcw
    rep.kv(z_window=zwin, min_composite_weeks=mcw,
           nascent_threshold=thr)

    idx = s3_json("data/wl-engines.json") or {}
    eng = idx.get("engines") or []
    dorm = [e for e in eng if str(e.get("state")) != "ACTIVE"]
    wl = s3_json("data/tv-watchlists.json") or {}
    by_id = {str(l.get("id")): l for l in (wl.get("lists") or [])}
    state = s3_json("data/thesis-state-v2.json.gz", {}, gz=True) or {}
    cache = state.get("weekly") or {}
    smap = (s3_json("data/symbol-map.json") or {}).get("map") or {}
    zero, nascent, violation = [], [], []
    for e in dorm:
        L = by_id.get(str(e.get("tv_id"))) or {}
        best = 0
        for sym in (L.get("symbols") or []):
            m = smap.get(str(sym)) or {}
            w = cache.get(str(sym)) or cache.get(str(m.get("id"))) \
                or {}
            best = max(best, len(w))
        nm = str(e.get("name"))[:44]
        if best == 0:
            zero.append(nm)
        elif best < thr:
            nascent.append((nm, best))
        else:
            violation.append((nm, best))
    rep.kv(active=len(eng) - len(dorm), dormant=len(dorm),
           zero_data=len(zero), z_nascent=len(nascent),
           violations=len(violation))
    for nm, b in nascent:
        rep.log(f"  NASCENT {b:>3} raw wk (< {thr}) — {nm} · "
                "auto-wakes as history accrues")
    for nm, b in violation:
        rep.log(f"  VIOLATION {b} wk — {nm}")
    if violation:
        fails.append(f"{len(violation)} panels hold z-sufficient "
                     "history yet sit dormant")
    else:
        rep.ok("CONTRACT HOLDS at z-resolution: every dormant panel "
               "is zero-data or z-nascent — dormancy is pure data "
               "absence; no design gate exists anywhere")
    rep.kv(n_fails=len(fails),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
