"""ops 6706 - READ-ONLY verify of spend-governor v1.5.3 (a budget figure that cannot belong to this month is ignored at
rollover, issue #131), daily health run 2026-11-01.

Three-rung ladder (AUTONOMY.md): the deployed artifact carries the marker from the LAST-EDITED region
(`def budget_actual_is_last_months`), the function is Active / LastUpdateStatus Successful, then two read-only actions:
  {"action":"status"}          -> version 1.5.3; thresholds.sources shows budget_actual / ce_mtd and, while the Budget
                                  still answers October's total, budget_actual_ignored; the stage is reported, not changed;
  {"action":"storage_selftest"} -> v1.5.2's six scenarios (that version never reached the account before this deploy).
Nothing is held, released, written or scheduled - the Nov 1 FREEZE stays until Khalid releases it.
"""
from datetime import datetime, timezone
from pathlib import Path
import io
import json
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
GOV = "justhodl-spend-governor"
MARKER = "def budget_actual_is_last_months("
WANT = "1.5.3"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def invoke(lam, r, action):
    for attempt in range(6):   # reserved concurrency 1: a running tick answers TooManyRequests
        try:
            res = lam.invoke(FunctionName=GOV, InvocationType="RequestResponse", Payload=json.dumps({"action": action}).encode())
            body = json.loads(res["Payload"].read() or b"{}")
            if res.get("FunctionError"):
                r.fail("%s raised: %s" % (action, json.dumps(body)[:600]))
                sys.exit(1)
            return body
        except lam.exceptions.TooManyRequestsException:
            r.warn("governor busy (attempt %d) - waiting 30 s" % (attempt + 1))
            time.sleep(30)
    r.fail("%s: governor busy after 6 attempts" % action)
    sys.exit(1)


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    now = datetime.now(timezone.utc)
    with report("ops_6706_governor_v153_verify") as r:
        r.heading("ops 6706 - spend-governor v1.5.3 verify (read-only), %s" % now.strftime("%Y-%m-%d %H:%M UTC"))

        r.section("rung 2+3: artifact carries the marker, function ready")
        ready, has_marker, ver = False, False, "?"
        for attempt in range(24):
            f = lam.get_function(FunctionName=GOV)
            cfg = f["Configuration"]
            ready = cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful"
            blob = urllib.request.urlopen(f["Code"]["Location"], timeout=60).read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                names = z.namelist()
                core = z.read("governor_core.py").decode("utf-8", "replace") if "governor_core.py" in names else ""
                has_marker = MARKER in core
                ver = next((ln.strip() for ln in core.splitlines() if ln.startswith("VERSION")), "?")
                lf = z.read("lambda_function.py").decode("utf-8", "replace") if "lambda_function.py" in names else ""
                wired = "def month_to_date(" in lf and "month_to_date(ctx, b_actual, ce, now, baseline, cfg)" in lf
            r.log("attempt %d: state %s / %s, last modified %s, marker in governor_core.py: %s, tick wired: %s, %s, %d files, "
                  "description: %s" % (attempt, cfg.get("State"), cfg.get("LastUpdateStatus"), cfg.get("LastModified"),
                                        has_marker, wired, ver, len(names), (cfg.get("Description") or "")[:60]))
            if ready and has_marker and wired:
                break
            time.sleep(20)
        if not (ready and has_marker):
            r.fail("deploy not landed: ready=%s marker=%s version=%s" % (ready, has_marker, ver))
            sys.exit(1)
        time.sleep(5)

        r.section("status (read-only)")
        st = invoke(lam, r, "status")
        th = st.get("thresholds") or {}
        r.log("version %s mode %s stage %s since %s" % (st.get("version"), st.get("mode"), st.get("stage"), st.get("stage_since")))
        r.log("budget actual $%s forecast $%s | MTD used $%s | sources %s" % (
            st.get("budget_actual"), st.get("budget_forecast"), th.get("mtd"), json.dumps(th.get("sources"))))
        r.log("held: %s | capped: %s" % (json.dumps(st.get("held") or {}), st.get("capped")))
        ignored = (th.get("sources") or {}).get("budget_actual_ignored")
        if ignored:
            r.log("the stale October figure is now ignored: MTD $%s instead of $%s" % (th.get("mtd"), st.get("budget_actual")))
        else:
            r.log("no budget figure ignored (Budget already shows this month, or day > 2): MTD $%s" % th.get("mtd"))

        r.section("storage_selftest (read-only; v1.5.2's scenarios, first time on the account)")
        body = invoke(lam, r, "storage_selftest")
        r.log("version %s, ok %s, scenarios %s" % (body.get("version"), body.get("ok"), json.dumps(body.get("scenarios"))[:800]))
        live = body.get("live") or {}
        r.log("live partial: %s | versioning %s" % (json.dumps(live.get("partial"), default=str)[:600], live.get("versioning")))

        r.kv(version=st.get("version"), stage=st.get("stage"), mtd=th.get("mtd"), budget_actual=st.get("budget_actual"),
             budget_actual_ignored=bool(ignored), storage_selftest_ok=body.get("ok"), held=json.dumps(st.get("held") or {}))
        if st.get("version") != WANT or not body.get("ok"):
            r.fail("expected version %s with a passing storage selftest (got %s / %s)" % (WANT, st.get("version"), body.get("ok")))
            sys.exit(1)
        r.ok("v%s live; stage %s left exactly as found (release is Khalid's)" % (WANT, st.get("stage")))
    sys.exit(0)


if __name__ == "__main__":
    main()
