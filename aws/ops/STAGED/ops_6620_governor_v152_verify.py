"""ops 6620 - READ-ONLY verify of spend-governor v1.5.2 (storage fuse waits for a complete day), daily health run 2026-10-23.

Three-rung ladder (AUTONOMY.md): the deployed artifact carries the marker from the LAST-EDITED region (`partial_reason`),
the function is Active / LastUpdateStatus Successful, then two read-only governor actions:
  {"action":"storage_selftest"} -> version 1.5.2, the six pure-logic scenarios, live.days and live.partial;
  {"action":"storage_check"}    -> dry by default: evaluates today's day (2026-10-22) and records nothing.
Nothing is held, released, written or scheduled.
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
MARKER = "def partial_reason(cov, day):"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    now = datetime.now(timezone.utc)
    with report("ops_6620_governor_v152_verify") as r:
        r.heading("ops 6620 - spend-governor v1.5.2 verify (read-only), %s" % now.strftime("%Y-%m-%d %H:%M UTC"))

        r.section("rung 2+3: artifact carries the marker, function ready")
        ready, has_marker = False, False
        for attempt in range(30):
            f = lam.get_function(FunctionName=GOV)
            cfg = f["Configuration"]
            ready = cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful"
            blob = urllib.request.urlopen(f["Code"]["Location"], timeout=60).read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                names = z.namelist()
                src = z.read("storage_fuse.py").decode("utf-8", "replace") if "storage_fuse.py" in names else ""
                has_marker = MARKER in src
                core = z.read("governor_core.py").decode("utf-8", "replace") if "governor_core.py" in names else ""
                ver = next((ln.strip() for ln in core.splitlines() if ln.startswith("VERSION")), "?")
            r.log("attempt %d: state %s / %s, last modified %s, marker in storage_fuse.py: %s, %s, %d files" % (
                attempt, cfg.get("State"), cfg.get("LastUpdateStatus"), cfg.get("LastModified"), has_marker, ver, len(names)))
            if ready and has_marker:
                break
            time.sleep(20)
        if not (ready and has_marker):
            r.fail("deploy not landed: ready=%s marker=%s" % (ready, has_marker))
            sys.exit(1)
        time.sleep(5)

        r.section("storage_selftest (read-only)")
        res = lam.invoke(FunctionName=GOV, InvocationType="RequestResponse", Payload=json.dumps({"action": "storage_selftest"}).encode())
        body = json.loads(res["Payload"].read() or b"{}")
        if res.get("FunctionError"):
            r.fail("storage_selftest raised: %s" % json.dumps(body)[:600])
            sys.exit(1)
        r.log("version %s, ok %s, scenarios %s" % (body.get("version"), body.get("ok"), json.dumps(body.get("scenarios"))))
        live = body.get("live") or {}
        r.log("live days: %s" % json.dumps(live.get("days"), default=str)[:1200])
        r.log("live partial: %s" % json.dumps(live.get("partial"), default=str)[:800])
        r.log("live versioning: %s" % live.get("versioning"))

        r.section("storage_check (dry, read-only)")
        res = lam.invoke(FunctionName=GOV, InvocationType="RequestResponse", Payload=json.dumps({"action": "storage_check"}).encode())
        chk = json.loads(res["Payload"].read() or b"{}")
        if res.get("FunctionError"):
            r.fail("storage_check raised: %s" % json.dumps(chk)[:600])
            sys.exit(1)
        r.log("storage_check: %s" % json.dumps(chk, default=str)[:1500])

        r.kv(version=body.get("version"), selftest_ok=body.get("ok"), partial_days=len(live.get("partial") or {}),
             versioning=live.get("versioning"), check_ran=chk.get("ran"), check_reason=chk.get("reason"),
             check_level=((chk.get("assessment") or {}).get("level") if chk.get("ran") else None))
        if body.get("version") != "1.5.2" or not body.get("ok"):
            r.fail("expected version 1.5.2 with a passing selftest")
            sys.exit(1)
        r.ok("v1.5.2 live and self-test green; storage_check evaluated without recording")
    sys.exit(0)


if __name__ == "__main__":
    main()
