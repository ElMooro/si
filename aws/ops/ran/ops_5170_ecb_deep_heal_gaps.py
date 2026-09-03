"""ops_5170 -- ecb-deep: heal the two REAL gaps found by ops 5169.

Audit result: of 202 abandoned err windows, 187 are HTTP400 on 17 flows
that the ECB data endpoint does not serve at all (ESTAT:*, IMF:*,
EUROSTAT:*, ECB.DISS:* dataflows referenced from the ECB registry -- those
agencies' data live in their own lanes); 13 are CSEC monthly windows in
the future (timeouts on months that do not exist yet). Neither is a gap.

Two windows are gaps with data on both sides:
  * PTN  2020_2022  HTTP504 x3 -- a 3-year payments-statistics window the
    server cannot assemble in time. Re-sliced here into 2020, 2021, 2022
    (the engine streams arbitrary sp_ep windows; slow/oversize years
    already fall through to month slicing on their own).
  * CSEC 1900_1979  HTTP502 x3 -- transient upstream error; retried.

The state document is DATA (S3), so the surgery is done there under the
engine's own lease discipline, then the engine is invoked synchronously to
process the pending windows and chain until nothing is pending, at which
point it flips itself back to refresh mode. A replaced window is kept in
state["ops5170"] for the record.
"""
import json
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
STATE_KEY = "data/_state/ecb-deep.json"
FN = "justhodl-ecb-deep"
CFG = Config(retries={"max_attempts": 4, "mode": "adaptive"}, read_timeout=920)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
FAILS = []


def jget(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def snapshot(st, flow, wids):
    fl = (st.get("flows") or {}).get(flow) or {}
    return {w: {k: (fl.get("windows") or {}).get(w, {}).get(k) for k in ("status", "tries", "raw_bytes", "gz_bytes")}
            for w in wids}


with report("ops_5170_ecb_deep_heal_gaps") as R:
    R.heading("ops 5170 -- ecb-deep: heal PTN 2020-2022 (re-sliced yearly) and CSEC 1900-1979")
    st = jget(STATE_KEY)
    lease = float(st.get("lease_until") or 0)
    if lease > time.time():
        R.log("   engine lease active until %s -- waiting" % time.strftime("%H:%M:%SZ", time.gmtime(lease)))
        time.sleep(min(max(lease - time.time() + 5, 0), 900))
        st = jget(STATE_KEY)
        if float(st.get("lease_until") or 0) > time.time():
            R.fail("lease still held; not touching state")
            sys.exit(1)
    flows = st["flows"]
    ptn, csec = flows.get("PTN"), flows.get("CSEC")
    if not ptn or not csec:
        R.fail("PTN/CSEC flows missing from state")
        sys.exit(1)
    R.log("   before PTN : %s" % json.dumps(snapshot(st, "PTN", ["2020_2022"])))
    R.log("   before CSEC: %s" % json.dumps(snapshot(st, "CSEC", ["1900_1979"])))
    st["ops5170"] = {"at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                     "ptn_replaced": {"2020_2022": ptn["windows"].get("2020_2022")},
                     "csec_rearmed": {"1900_1979": csec["windows"].get("1900_1979")}}
    # PTN: replace the 3-year window with yearly ones
    ptn["windows"].pop("2020_2022", None)
    for y in ("2020", "2021", "2022"):
        ptn["windows"]["%s_%s" % (y, y)] = {"status": "pending", "tries": 0, "sliced_from": "2020_2022", "by": "ops5170"}
    ptn["complete"] = False
    ptn.pop("completed_at", None)
    # CSEC: rearm the 502 window only
    csec["windows"]["1900_1979"] = {"status": "pending", "tries": 0, "rearmed_by": "ops5170"}
    csec["complete"] = False
    csec.pop("completed_at", None)
    st["mode"] = "backfill"
    s3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps(st, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    R.ok("   state written: PTN 2020/2021/2022 pending, CSEC 1900_1979 pending, mode=backfill")

    R.section("run the engine (synchronous, up to ~14 min; it chains on its own if more is pending)")
    t0 = time.time()
    try:
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        out = json.loads(resp["Payload"].read() or b"{}")
        R.log("   engine -> %s (%.0fs)" % (json.dumps(out)[:400], time.time() - t0))
    except Exception as e:
        FAILS.append("invoke: %s" % str(e)[:160])
    time.sleep(45)
    st2 = jget(STATE_KEY)
    R.log("   after PTN : %s" % json.dumps(snapshot(st2, "PTN", ["2020_2020", "2021_2021", "2022_2022"])))
    R.log("   after CSEC: %s" % json.dumps(snapshot(st2, "CSEC", ["1900_1979"])))
    R.log("   mode=%s lease_until=%s n_complete=%s" % (st2.get("mode"), st2.get("lease_until"), st2.get("n_complete")))
    pend = [(f, w) for f, fl in st2["flows"].items() for w, v in (fl.get("windows") or {}).items()
            if v.get("status") == "pending" or (str(v.get("status", "")).startswith("err") and int(v.get("tries") or 0) < 3)]
    if pend:
        R.log("   still pending/retrying: %s (the engine chains until none remain, then flips to refresh)" % pend[:8])
    else:
        R.ok("   nothing pending -- lane back in refresh mode with the gaps banked or terminal")
    for f in ("PTN", "CSEC"):
        wins = st2["flows"][f]["windows"]
        R.kv(section="result", flow=f,
             done=sum(1 for v in wins.values() if v.get("status") == "done"),
             pending=sum(1 for v in wins.values() if v.get("status") == "pending"),
             err=sum(1 for v in wins.values() if str(v.get("status", "")).startswith("err")))
    if FAILS:
        for f in FAILS:
            R.fail(f)
        sys.exit(1)
    R.ok("ops 5170 complete")
