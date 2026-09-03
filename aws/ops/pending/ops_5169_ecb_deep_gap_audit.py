"""ops_5169 -- ecb-deep completeness audit (READ-ONLY).

ops 5164 saw the lane in refresh mode with 58/58 flows "complete" -- but
completeness there is defined by _flow_done, which treats exhausted-error
windows as terminal (ops 4911). 189 err:HTTP + 13 err:Time windows were
abandoned that way. Before deciding between a rearm (tries reset under the
slow-window guard, the engine's own healing switch) and a sub-slicing fix,
this op reads the state and answers: which flows, which windows, which
HTTP codes, how many tries, and whether the failed windows sit at the tail
of a flow (no data there -- benign) or in the middle (a real gap).
"""
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def jget(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


with report("ops_5169_ecb_deep_gap_audit") as R:
    R.heading("ops 5169 -- ecb-deep completeness audit (read-only)")
    st = jget("data/_state/ecb-deep.json") or {}
    if not st:
        R.fail("state missing")
        sys.exit(1)
    flows = st.get("flows") or {}
    R.log("mode=%s flows=%d rearmed=%s resynced=%d" % (st.get("mode"), len(flows), st.get("rearmed"), len(st.get("resynced") or [])))
    by_status = Counter()
    err_by_flow = defaultdict(list)
    codes = Counter()
    tries = Counter()
    bytes_done = 0
    for f, fl in flows.items():
        wins = fl.get("windows") or {}
        wids = sorted(wins, key=lambda w: w.split("_")[0])
        last_ok = None
        for w in wids:
            v = wins[w] or {}
            s_ = str(v.get("status", ""))
            by_status[s_[:12]] += 1
            if s_ == "done":
                bytes_done += int(v.get("raw_bytes") or 0)
                last_ok = w
        for w in wids:
            v = wins[w] or {}
            s_ = str(v.get("status", ""))
            if s_.startswith("err"):
                codes[s_] += 1
                tries[int(v.get("tries") or 0)] += 1
                # tail = every later window of the flow is empty/err (no data past here)
                later = [wins[x] for x in wids if x > w]
                tail = all(str((x or {}).get("status", "")) in ("empty", "") or str((x or {}).get("status", "")).startswith("err") for x in later)
                err_by_flow[f].append((w, s_, int(v.get("tries") or 0), "tail" if tail else "GAP", last_ok))
    R.log("windows by status: %s" % dict(by_status))
    R.log("error codes: %s" % dict(codes))
    R.log("tries distribution among err windows: %s" % dict(sorted(tries.items())))
    R.log("banked raw bytes across done windows: %.1f GB" % (bytes_done / 1e9))
    R.section("err windows by flow")
    n_gap = n_tail = 0
    for f in sorted(err_by_flow, key=lambda x: -len(err_by_flow[x])):
        items = err_by_flow[f]
        gaps = [i for i in items if i[3] == "GAP"]
        n_gap += len(gaps)
        n_tail += len(items) - len(gaps)
        done_n = sum(1 for v in (flows[f].get("windows") or {}).values() if (v or {}).get("status") == "done")
        R.log("   %-14s err %2d (gap %2d, tail %2d)  done %2d  last-ok %s  e.g. %s"
              % (f, len(items), len(gaps), len(items) - len(gaps), done_n, items[0][4],
                 ", ".join("%s:%s/t%d" % (i[0], i[1].replace("err:", ""), i[2]) for i in items[:4])))
        R.kv(section="err_flows", flow=f, err=len(items), gap=len(gaps), tail=len(items) - len(gaps), done=done_n)
    R.section("verdict")
    R.log("   %d err windows are real GAPS (data exists on both sides), %d are tail windows (nothing later in the flow)" % (n_gap, n_tail))
    R.log("   engine has a one-shot healing switch: invoke with {\"rearm_errs\": true} -> tries reset, retried under the slow-window guard")
    R.ok("ops 5169 complete -- read-only")
