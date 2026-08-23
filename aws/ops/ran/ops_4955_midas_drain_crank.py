"""ops_4955 -- MIDAS drain crank (Khalid: "why did it stop?").

It didn't stop -- it paused. The 4954 kick resurrected sec-midas from
its weekly cron and the engine's self-chain (PER_RUN=2 quarterly zips
per 820s link, chain_depth<=40) banked +12 files / +1.03GB in minutes,
then the chain ended: either depth-cap burned on failing quarters
(the pattern-probe inventory can include never-published quarters ->
404 tries mount) or a link died mid-stream on a 700MB zip. Budget
unconstrained -> this ops restarts chains until the drain is DONE:

  G0  state read: inventory_n / n_have / n_missing / failures
  G1  crank: kick on lease-expiry whenever n_missing>0 and any
      missing quarter has failures.tries < 3; poll; stall-restart
  G2  verdict: n_missing==0 = FULL; else every residual missing
      quarter must be a >=3-tries NAMED failure (inventory overreach
      stated, never silent) -- anything else is RED
  G3  bytes/keys census of data/warm/sec-midas/ (the GB truth)
Pure ops, [skip-deploy]; parallel session owns ops_4954 -- untouched.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-sec-midas"
STATE_KEY = "data/_state/sec-midas.json"
PREFIX = "data/warm/sec-midas/"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def gj(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return default


def kick():
    try:
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        return True
    except Exception:
        return False


def snap():
    st = gj(STATE_KEY) or {}
    return st, st.get("inventory_n") or 0, st.get("n_have") or 0, \
        st.get("n_missing") or 0, st.get("failures") or {}


with report("ops_4955_midas_drain_crank") as R:
    fails = []

    R.section("G0 state")
    st, inv_n, have0, miss0, fl = snap()
    R.log("G0 inventory=%s have=%s missing=%s failures=%s lease=%s"
          % (inv_n, have0, miss0, len(fl),
             "held" if float(st.get("lease_until") or 0) > time.time()
             else "free"))
    for qid, f in sorted(fl.items())[:10]:
        R.log("  fail %-9s tries=%s err=%s" % (
            qid, f.get("tries"), str(f.get("err"))[:70]))

    def unresolved(miss_n, st_):
        # missing quarters not yet 3-strike named
        have = st_.get("have") or {}
        flz = st_.get("failures") or {}
        hard = sum(1 for q, f in flz.items()
                   if q not in have and (f.get("tries") or 0) >= 3)
        return max(0, miss_n - hard)

    R.section("G1 crank")
    t0, kicks, last_have, last_move = time.time(), 0, have0, \
        time.time()
    ok1 = miss0 == 0
    while time.time() - t0 < 22 * 60 and not ok1:
        st, inv_n, have_n, miss_n, fl = snap()
        if have_n != last_have or miss_n == 0:
            last_have, last_move = have_n, time.time()
            R.log("  t+%4ds have=%s missing=%s failures=%s" % (
                time.time() - t0, have_n, miss_n, len(fl)))
        if miss_n == 0:
            ok1 = True
            break
        if unresolved(miss_n, st) == 0:
            R.log("  residual missing are all >=3-tries named -- "
                  "terminal")
            break
        lease_free = float(st.get("lease_until") or 0) <= time.time()
        stalled = time.time() - last_move > 300
        if lease_free and (kicks == 0 or stalled) and kicks < 8:
            kicks += 1
            kick()
            last_move = time.time()
            R.log("  chain restart kick #%d" % kicks)
        time.sleep(25)
    st, inv_n, have_n, miss_n, fl = snap()
    R.log("G1 done have=%s/%s missing=%s kicks=%d elapsed=%ds" % (
        have_n, inv_n, miss_n, kicks, time.time() - t0))

    R.section("G2 verdict")
    have = st.get("have") or {}
    residual = [q for q in
                (set("%s" % k for k in (st.get("failures") or {}))
                 | set())
                if q not in have]
    hard_named = all((fl.get(q) or {}).get("tries", 0) >= 3
                     for q in residual) if residual else True
    # recompute residual from inventory identity, not failures alone
    ok2 = miss_n == 0 or (
        miss_n == len([q for q, f in fl.items()
                       if q not in have and (f.get("tries") or 0) >= 3])
        and hard_named)
    if miss_n == 0:
        R.log("G2 PASS drain FULL: every inventoried quarter banked")
    elif ok2:
        R.log("G2 PASS terminal: %d quarters are >=3-tries named "
              "failures (pattern-probe inventory overreach -- "
              "stated):" % miss_n)
        for q, f in sorted(fl.items()):
            if q not in have:
                R.log("    %-9s tries=%s %s" % (
                    q, f.get("tries"), str(f.get("err"))[:70]))
    else:
        R.log("G2 FAIL missing=%d not fully drained nor fully named"
              % miss_n)
        fails.append("G2")

    R.section("G3 bytes/keys census")
    tot, nk, tok = 0, 0, None
    while True:
        kw = dict(Bucket=B, Prefix=PREFIX, MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r_ = s3.list_objects_v2(**kw)
        for o in r_.get("Contents", []):
            tot += o["Size"]
            nk += 1
        if not r_.get("IsTruncated"):
            break
        tok = r_.get("NextContinuationToken")
    gb = tot / 1e9
    grew = have_n > have0 or miss0 == 0
    ok3 = nk >= have_n and grew
    R.log("G3 %s keys=%d %.2fGB (have %d -> %d this crank)" % (
        "PASS" if ok3 else "FAIL", nk, gb, have0, have_n))
    if not ok3:
        fails.append("G3")

    if fails:
        R.log("ops 4955 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(inventory=inv_n, have=have_n, missing=miss_n,
         named_failures=len([q for q in fl if q not in have]),
         keys=nk, gb=round(gb, 2), kicks=kicks,
         banked_this_crank=have_n - have0)
    R.log("ops 4955 GREEN -- MIDAS drained to terminal: the pause was "
          "the chain ending, not a failure; 6h schedule keeps it "
          "topped up from here")
