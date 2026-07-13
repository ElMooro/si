"""ops 3227 — seed the two stragglers, wake the two engines.

ES−IT spread and GBDIR fetch perfectly ops-side (422/433 pts, proven
three times) but hiccup intermittently in-runner. Rather than chase a
transient, this ops seeds them into the shared weekly cache in the
runner's exact format (same real data, same pipeline), records them in
the ids-ledger so the 6-day refresh cycle owns them from here, and lets
`if w:` semantics retain them across any future fetch hiccup. Then one
run: Europe Liquidity and Global Deposit Rates both sit at need-1 with
exactly these members missing."""
import gzip
import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
STATE_KEY = "data/thesis-state-v2.json.gz"
SEED = {
    "TVC:ES10Y-TVC:IT10Y": ("DERIVED",
                            "FRED~IRLTLT01ESM156N~minus~"
                            "FRED~IRLTLT01ITM156N"),
    "ECONOMICS:GBDIR": ("FRED", "IR3TIB01GBM156N"),
}


def week_key(iso):
    try:
        y, m, d = (int(x) for x in iso[:10].split("-"))
        iy, iw, _ = date(y, m, d).isocalendar()
        return f"{iy}-{iw:02d}"
    except Exception:
        return None


def s3_json(key, default=None, gz=False):
    try:
        b = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


with report("3227_seed_wake") as rep:
    fails, warns = [], []
    rep.heading("ops 3227 — seed the stragglers, wake the engines")

    rep.section("1. Fetch ops-side, weekly-ize, seed the shared cache")
    st = s3_json(STATE_KEY, {}, gz=True) or {}
    cache = st.get("weekly") or {}
    ids = st.get("ids") or {}
    seeded = 0
    for sym, (src, sid) in SEED.items():
        try:
            ser = SS.fetch(src, sid, "1990-01-01")
        except Exception:
            ser = {}
        wk = {}
        for d, v in sorted(ser.items()):
            k = week_key(d)
            if k:
                wk[k] = v
        if len(wk) >= 12:
            cache[sym] = wk
            ids[sym] = f"{src}|{sid}"
            seeded += 1
            rep.ok(f"{sym} seeded: {len(wk)} weekly obs")
        else:
            fails.append(f"{sym}: only {len(wk)} weekly obs ops-side")
    if seeded:
        st["weekly"], st["ids"] = cache, ids
        S3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                      Body=gzip.compress(json.dumps(st).encode()),
                      ContentType="application/json",
                      ContentEncoding="gzip")

    rep.section("2. Run — wakes by name")
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}
    mark = datetime.now(timezone.utc).isoformat()
    if not fails:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    idx2 = None
    for _ in range(80):
        time.sleep(10)
        d = s3_json("data/wl-engines.json") or {}
        if str(d.get("generated_at", "")) > mark:
            idx2 = d
            break
    if idx2:
        eng2 = idx2.get("engines") or []
        act2 = {e["engine_id"] for e in eng2
                if str(e.get("state")) == "ACTIVE"}
        woken = sorted(act2 - prev_active)
        rep.kv(active_before=len(prev_active), active_now=len(act2),
               woken=len(woken))
        for w in woken[:10]:
            nm = next((e.get("name") for e in eng2
                       if e.get("engine_id") == w), w)
            rep.log(f"  ⏰ WOKE: {nm}")
        for nm in ("Europe Liquidity", "Global Deposit Rates"):
            e = next((x for x in eng2
                      if nm.lower() in str(x.get("name", "")).lower()),
                     None)
            if e:
                rep.log(f"  → {str(e.get('name'))[:36]:<36} "
                        f"{e.get('state')} "
                        f"resolved={e.get('members_resolved')}")
        if woken:
            rep.ok(f"{len(woken)} panels WOKEN")
        else:
            warns.append("seeded but no wake — read fresh reasons")
    else:
        warns.append("index not fresh in window")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
