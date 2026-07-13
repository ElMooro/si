"""ops 3253 — EVERY engine on panels.html audited: for all ~207 rows in
the index, its detail feed must exist and parse with the keys the
drawer reads. Missing/invalid docs are repaired inline from the index
row (thin doc, named reason). A missing ACTIVE doc would indicate a
scorer problem and FAILS loudly rather than being papered over. Then
end-to-end public proof over the CDN for the two engines Khalid
reported plus samples of each class."""
import json
import random
import sys
import time
import urllib.request
from datetime import datetime, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3253)"}
REPORTED = ("wl-10-yr-high-quality-market-hqm-pred",
            "wl-bond-global-high-yield-ex-usa-glob")


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


with report("3253_all_engines_audit") as rep:
    fails, warns = [], []
    rep.heading("ops 3253 — all engines on panels.html: audit + repair "
                "+ public proof")

    idx = s3_json("data/wl-engines.json") or {}
    eng = idx.get("engines") or []
    rep.kv(engines_in_index=len(eng))
    now = datetime.now(timezone.utc).isoformat()

    rep.section("1. Every detail feed: exists + parses")
    ok = 0
    repaired_dormant, missing_active, invalid = [], [], []
    for e in eng:
        eid = e.get("engine_id")
        doc = s3_json(f"data/engines/{eid}.json")
        good = isinstance(doc, dict) and doc.get("engine_id") == eid \
            and doc.get("state")
        if good:
            ok += 1
            continue
        if str(e.get("state")) == "ACTIVE":
            missing_active.append(eid)
            continue
        S3.put_object(Bucket=BUCKET, Key=f"data/engines/{eid}.json",
                      Body=json.dumps({**e, "generated_at": now,
                                       "detail_level": "dormant-min",
                                       "event_study": {},
                                       "lit_indicators": [],
                                       "all_members": [],
                                       "fusion_targets": []}),
                      ContentType="application/json")
        (invalid if doc is not None else repaired_dormant).append(eid)
    rep.kv(ok_already=ok, repaired=len(repaired_dormant) + len(invalid),
           missing_active=len(missing_active))
    for eid in (repaired_dormant + invalid)[:10]:
        rep.log(f"  repaired: {eid}")
    for eid in missing_active:
        fails.append(f"ACTIVE doc missing (scorer problem): {eid}")

    rep.section("2. Public CDN proof (reported + samples)")
    dorm = [e["engine_id"] for e in eng
            if str(e.get("state")) != "ACTIVE"
            and e["engine_id"] not in REPORTED]
    act = [e["engine_id"] for e in eng
           if str(e.get("state")) == "ACTIVE"]
    random.seed(3253)
    sample = list(REPORTED) + random.sample(dorm, min(4, len(dorm))) \
        + random.sample(act, min(2, len(act)))
    pub_ok = 0
    for eid in sample:
        got = None
        for _ in range(2):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    f"https://justhodl.ai/data/engines/{eid}.json"
                    f"?t={int(time.time() * 1000)}", headers=UA),
                    timeout=15)
                got = json.loads(h.read())
                break
            except Exception:
                time.sleep(6)
        if isinstance(got, dict) and got.get("state"):
            pub_ok += 1
            tag = "REPORTED" if eid in REPORTED else got.get("state")
            rep.ok(f"200 {eid} [{tag}]"
                   + (f" reason='{str(got.get('reason'))[:44]}'"
                      if got.get("reason") else ""))
        else:
            fails.append(f"public fetch failed: {eid}")
    rep.kv(public_verified=pub_ok, of=len(sample))

    for w in warns:
        rep.warn(w)
    if not fails:
        rep.ok(f"ALL {len(eng)} engines on panels.html have working "
               "detail feeds")
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
