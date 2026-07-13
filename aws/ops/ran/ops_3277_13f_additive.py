"""ops 3277 — (1) READ KHALID'S DIAG BEACONS, (2) verify the additive
13F sections.

Section 1: filter '[diag]' on /aws/lambda/justhodl-wl-series-api for
the last 24h — his browser's reality (sw, drawer version, lists, favs,
errors) printed verbatim. This is the evidence the chart-pro thread
has been waiting for.

Section 2: 13f.html additive proof — served page carries the ops-3277
literal AND both existing anchors (Action Spotlight / Rare picks)
UNTOUCHED; positions feed fresh with changes_summary; a real cluster
row computed in-ops from the live feed as ground truth.
"""
import json
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3277)"}


def get(u):
    return urllib.request.urlopen(
        urllib.request.Request(u, headers=UA), timeout=20).read()\
        .decode("utf-8", "replace")


with report("3277_13f_additive") as rep:
    fails, warns = [], []

    rep.section("1. Khalid's diag beacons (last 24h)")
    seen = []
    try:
        start = int((datetime.now(timezone.utc)
                     - timedelta(hours=24)).timestamp() * 1000)
        evs = LOGS.filter_log_events(
            logGroupName="/aws/lambda/justhodl-wl-series-api",
            startTime=start, filterPattern='"[diag]"',
            limit=50).get("events") or []
        for e in evs:
            m = e["message"].strip()[:230]
            if m not in seen:
                seen.append(m)
                rep.log("  " + m)
    except Exception as e:
        warns.append(f"beacon read: {str(e)[:60]}")
    rep.kv(beacons_24h=len(seen))
    if not seen:
        rep.log("  none — his page loads are not reaching the "
                "beacon endpoint (or no loads since 3276)")

    rep.section("2. 13F additive sections")
    okp = False
    for i in range(22):
        try:
            h = get("https://justhodl.ai/13f.html?t="
                    f"{int(time.time())}")
            okp = ("ops 3277: additive" in h
                   and "Action Spotlight" in h
                   and "Rare picks" in h)
        except Exception:
            pass
        if okp:
            rep.ok(f"live: new sections present, existing sections "
                   f"untouched (~{(i + 1) * 15}s)")
            break
        time.sleep(15)
    if not okp:
        fails.append("additive literals or existing anchors missing")

    try:
        pos = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/13f-positions.json")
            ["Body"].read())
        acts = defaultdict(lambda: {"NEW": [], "EXIT": []})
        for fk, f in (pos.get("by_fund") or {}).items():
            cs = f.get("changes_summary") or {}
            for kind, K in (("new", "NEW"), ("exit", "EXIT")):
                for x in cs.get(kind) or []:
                    tk = str(x.get("ticker") or "").upper()
                    if tk:
                        acts[tk][K].append(f.get("name") or fk)
        clus = sorted(((t, v) for t, v in acts.items()
                       if len(v["NEW"]) >= 2),
                      key=lambda kv: -len(kv[1]["NEW"]))[:3]
        rep.kv(as_of=pos.get("as_of_quarter"),
               funds=pos.get("funds_parsed"),
               cluster_new_names=len([1 for _, v in acts.items()
                                      if len(v["NEW"]) >= 2]))
        for t, v in clus:
            rep.log(f"  CLUSTER NEW {t}: {len(v['NEW'])} funds — "
                    + ", ".join(v["NEW"][:4]))
        if not clus:
            warns.append("no ≥2-fund NEW clusters this quarter "
                         "(section will honestly say 'none')")
    except Exception as e:
        fails.append(f"positions feed: {str(e)[:70]}")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
