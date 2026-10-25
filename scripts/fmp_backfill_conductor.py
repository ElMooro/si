#!/usr/bin/env python3
"""Bounded conductor for the FMP warehouse backfill (run by .github/workflows/fmp-backfill.yml, once a day).

For each engine still in phase BACKFILL: wait for a free lease, invoke once (Event), wait for its state write, read the
manifest, repeat - alternating between engines - until every engine is STEADY, max_kicks per engine is reached or the
wall clock is spent.  Prints one line per kick and a final verdict; `ALL_STEADY` on its own line when the backfill is
done (the workflow disables itself on that token).  Exit 0 always on bounded completion; 2 when an engine reports an
unauthorized key (nothing to conduct until the key is fixed).

Doctrine: no self-chaining inside AWS; the only re-kicks are these, from a runner, lease-guarded, counted and bounded.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

LIVE = "justhodl-dashboard-live"
ENGINES = {
    "justhodl-fmp-bank": {"state": "data/warm/fmp-bank/_state/state.json", "manifest": "data/warm/fmp-bank/manifest.json", "wait_s": 16 * 60},
    "justhodl-fmp-walker": {"state": "data/warm/fmp/_state/state.json", "manifest": "data/warm/fmp/manifest.json", "wait_s": 16 * 60},
    "justhodl-polygon-bank": {"state": "data/warm/polygon-bank/_state/state.json", "manifest": "data/warm/polygon-bank/manifest.json", "wait_s": 16 * 60},
    "justhodl-provider-bank": {"state": "data/warm/provider-bank/_state/state.json", "manifest": "data/warm/provider-bank/manifest.json", "wait_s": 16 * 60},
    "justhodl-eia-bank": {"state": "data/warm/eia-bank/_state/state.json", "manifest": "data/warm/eia-bank/manifest.json", "wait_s": 16 * 60},
}
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def jget(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engines", default="")
    ap.add_argument("--max-minutes", type=int, default=320)
    ap.add_argument("--max-kicks", type=int, default=12)
    a = ap.parse_args()
    names = [n for n in a.engines.split() if n] or list(ENGINES)
    for n in names:
        if n not in ENGINES:
            print("unknown engine %s" % n)
            return 1
    s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
    lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
    t_end = time.time() + a.max_minutes * 60
    kicks = {n: 0 for n in names}
    steady = {n: False for n in names}
    unauthorized = False
    print("%s conductor start: engines=%s max_minutes=%d max_kicks=%d" % (now(), names, a.max_minutes, a.max_kicks))
    while time.time() < t_end and not all(steady.values()) and any(kicks[n] < a.max_kicks for n in names if not steady[n]):
        progressed = False
        for n in names:
            if steady[n] or kicks[n] >= a.max_kicks or time.time() > t_end - 60:
                continue
            e = ENGINES[n]
            st = jget(s3, e["state"])
            man = jget(s3, e["manifest"])
            if (man.get("phase") or st.get("phase")) == "STEADY":
                steady[n] = True
                print("%s %s STEADY (objects %s, %.1f MB)" % (now(), n, man.get("objects"), (man.get("bytes") or 0) / 1e6))
                continue
            if st.get("key_status") == "unauthorized":
                print("%s %s key_status unauthorized - stop" % (now(), n))
                unauthorized = True
                steady[n] = True
                continue
            if float(st.get("lease_until") or 0) > time.time():
                print("%s %s lease held until %s - skip this turn" % (now(), n, datetime.fromtimestamp(float(st["lease_until"]), timezone.utc).strftime("%H:%M:%S")))
                continue
            prev = st.get("updated_at")
            resp = lam.invoke(FunctionName=n, InvocationType="Event", Payload=b"{}")
            kicks[n] += 1
            progressed = True
            print("%s %s kick %d -> %s" % (now(), n, kicks[n], resp.get("StatusCode")))
            deadline = time.time() + e["wait_s"]
            while time.time() < deadline:
                time.sleep(20)
                st2 = jget(s3, e["state"])
                if st2.get("updated_at") and st2.get("updated_at") != prev and not float(st2.get("lease_until") or 0) > time.time():
                    run = st2.get("last_run") or {}
                    man2 = jget(s3, e["manifest"])
                    print("%s %s done: phase %s, calls %s, puts %s, unchanged %s, %.0fs; bank %s objects %.1f MB" % (
                        now(), n, st2.get("phase"), run.get("calls"), run.get("puts", run.get("steps") and "-"), run.get("unchanged", "-"), run.get("seconds") or 0,
                        man2.get("objects"), (man2.get("bytes") or 0) / 1e6))
                    break
            else:
                print("%s %s no state write within %d min - leaving it; read CloudWatch /aws/lambda/%s" % (now(), n, e["wait_s"] // 60, n))
        if not progressed:
            time.sleep(60)
    for n in names:
        man = jget(s3, ENGINES[n]["manifest"])
        print("%s %s final: phase %s kicks %d objects %s %.1f MB" % (now(), n, man.get("phase"), kicks[n], man.get("objects"), (man.get("bytes") or 0) / 1e6))
    if all(steady.values()) and not unauthorized:
        print("ALL_STEADY")
    return 2 if unauthorized else 0


if __name__ == "__main__":
    sys.exit(main())
