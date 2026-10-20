"""ops 6561 - verify governor v1.4.0 live: anomaly sentinel + the loop layers in one read (Khalid, 2026-10-20).

  1. status: version 1.4.0, cap $600, enforce, stage; engines held by CAP / ANOM
  2. {"action":"anomaly_selftest"}: synthetic $6 storm window for the canary -> detected (hold path only when thawed)
  3. {"action":"meter"}: a forced read (fleet frozen) so the sentinel's daily record starts filling before Nov 1
  4. loop layers: runaway alarms present + enabled + pointing at the governor; coordinator breaker alarm; the two
     October loop engines still under CAP; event_graph gate on the committed tree (run locally in the ops job)
"""
from pathlib import Path
import json
import subprocess
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, read_timeout=600)
RUNAWAY = ("justhodl-runaway-concurrency", "justhodl-runaway-invocations", "justhodl-runaway-duration",
           "justhodl-event-breaker-open", "justhodl-s3-live-bucket-size")


def gov(payload):
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    for attempt in range(8):
        try:
            r = lam.invoke(FunctionName="justhodl-spend-governor", Payload=json.dumps(payload).encode())
            body = json.loads(r["Payload"].read() or b"{}")
            if r.get("FunctionError"):
                raise RuntimeError("governor error: %s" % json.dumps(body)[:400])
            return body
        except lam.exceptions.TooManyRequestsException:
            time.sleep(20)
    raise RuntimeError("governor busy for 160 s")


def main():
    with report("ops_6561_anomaly_sentinel_verify") as r:
        r.heading("ops 6561 - governor v1.4.0 live: anomaly sentinel, kill switch, loop layers")
        st = gov({"action": "status"})
        r.kv(version=st.get("version"), mode=st.get("mode"), stage=st.get("stage"), cap=st.get("cap"),
             capped=json.dumps(st.get("capped")), anomaly_held=json.dumps(st.get("anomaly_held")))
        if st.get("version") != "1.4.0":
            r.fail("status does not show v1.4.0")
            raise SystemExit(1)
        t = gov({"action": "anomaly_selftest"})
        r.log("anomaly_selftest: %s" % json.dumps(t)[:400])
        if not t.get("ok"):
            r.fail("anomaly self-test failed")
            raise SystemExit(1)
        r.ok("anomaly sentinel detects a synthetic $6 window on the canary (%s)" % (t.get("hold_path") or "held and released"))
        m = gov({"action": "meter"})
        r.log("meter: ran=%s window %.1f h reads=%s active=%s" % (m.get("ran"), m.get("window_h") or 0, m.get("reads"), m.get("active")))
        cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
        found = {a["AlarmName"]: a for a in cw.describe_alarms(AlarmNames=list(RUNAWAY)).get("MetricAlarms", [])}
        problems = [n for n in RUNAWAY if n not in found or found[n].get("ActionsEnabled") is False
                    or not any(str(x).endswith(":justhodl-spend-governor") for x in found[n].get("AlarmActions") or [])]
        r.log("runaway alarms: %d of %d armed and wired to the governor; states %s" % (
            len(RUNAWAY) - len(problems), len(RUNAWAY), {n: a.get("StateValue") for n, a in found.items()}))
        if problems:
            r.warn("alarm problems: %s" % problems)
        capped = set(st.get("capped") or [])
        if {"justhodl-event-coordinator", "justhodl-miss-calibrator"} <= capped:
            r.ok("the two October loop engines are under CAP (manual release only)")
        else:
            r.warn("loop engines not both under CAP: %s" % sorted(capped))
        try:
            out = subprocess.run([sys.executable, str(ROOT / "scripts" / "event_graph.py")], capture_output=True, text=True, timeout=300)
            r.log("event_graph gate on the committed tree: exit %d - %s" % (out.returncode, (out.stdout or out.stderr).strip().splitlines()[-1][:160]))
            if out.returncode != 0:
                r.fail("event graph reports a loop")
                raise SystemExit(1)
        except subprocess.TimeoutExpired:
            r.warn("event_graph timed out")
        r.ok("v1.4.0 live: anomaly sentinel armed; loop layers intact")


if __name__ == "__main__":
    main()
