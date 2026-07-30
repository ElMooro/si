"""ops_4124 — streaming phase timeline: settle v3.15.4, invoke, watch the
run's [phase] prints live for 12 minutes. The hog gets a number."""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120, retries={"max_attempts": 1}))
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-tradingview"
MARK = "tradingview-vault v3.15.4 ops4124 phases"


def main():
    with report("4124_phase_stream") as rep:
        rep.heading("ops 4124 — phase stream")
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        assert MARK in src
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
        for att in range(6):
            try:
                lam.update_function_code(FunctionName=FN,
                                         ZipFile=buf.getvalue(), Publish=True)
                rep.ok(f"  update accepted (attempt {att})")
                break
            except Exception as e:
                rep.log(f"  EXC {type(e).__name__}: {str(e)[:100]}")
                time.sleep(10)
        ok = False
        for i in range(35):
            try:
                cfg = lam.get_function_configuration(FunctionName=FN)
                if cfg.get("State") == "Active" and \
                        cfg.get("LastUpdateStatus") == "Successful":
                    dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                        lam.get_function(FunctionName=FN)["Code"]["Location"],
                        timeout=60).read())).read(
                        "lambda_function.py").decode()
                    if MARK in dep:
                        ok = True
                        rep.ok(f"  settled at loop {i}")
                        break
            except Exception:
                pass
            time.sleep(9)
        if not ok:
            rep.fail("never settled")
            sys.exit(1)

        t_inv = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        rep.section("live phase timeline")
        seen = set()
        wrote = False
        for cyc in range(12):
            time.sleep(60)
            try:
                ev = logs.filter_log_events(
                    logGroupName=f"/aws/lambda/{FN}",
                    startTime=int(t_inv.timestamp() * 1000),
                    filterPattern='"[phase]"', limit=200)
                for e in ev.get("events") or []:
                    msg = e["message"].strip()[:120]
                    if msg not in seen:
                        seen.add(msg)
                        rep.log(f"  {msg}")
            except Exception as e2:
                rep.log(f"  logs EXC {type(e2).__name__}")
            try:
                v = json.loads(s3.get_object(
                    Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
                if str(v.get("marker")) == MARK:
                    rep.ok(f"  \u2605 ARTIFACT WROTE v3.15.4 at cycle {cyc}")
                    wrote = True
                    break
            except Exception:
                pass
        rep.ok(f"STREAM DONE — {len(seen)} phase lines, wrote={wrote}")


if __name__ == "__main__":
    main()
