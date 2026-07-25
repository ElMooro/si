"""
ops_3891 — DEPLOY: justhodl-macro-confluence, a brand-new lambda (the macro/
sector-rotation confluence engine — see its own docstring for the full "why").

This is a NEW function, so get_function will 404 on the first several
zip-settle attempts until deploy-lambdas.yml creates it — that's expected,
not a failure, and the loop accounts for it.

The hard gate is the real test: does this engine, reading LIVE data right
now (not my synthetic local test), correctly surface Technology as a
high-convergence bearish theme — the exact pattern Khalid pointed at
(SMH capitulation + crypto leg holding up), which took 8 manual ops to
find by hand? If it can't reproduce that finding from live data
automatically, the build isn't done, regardless of how clean the local
test looked.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-macro-confluence"
BUCKET = "justhodl-dashboard-live"
KEY = "data/macro-confluence.json"
MARKER = "STOCK QUADRANT CLUSTER"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120, retries={"max_attempts": 0}))


def main():
    with report("3891_macro_confluence_deploy") as rep:
        rep.heading("ops 3891 — deploy macro-confluence, hard-gate on LIVE Technology convergence")

        rep.section("1. ZIP-SETTLE — new function, will 404 until deploy-lambdas.yml creates it")
        settled = False
        for attempt in range(1, 31):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                if MARKER in src:
                    rep.ok(f"  new function live on attempt {attempt} ({len(blob):,} zip bytes)")
                    settled = True
                    break
                rep.log(f"  attempt {attempt}: function exists but marker not in zip yet")
            except Exception as e:
                rep.log(f"  attempt {attempt}: {str(e)[:100]} (expected until function is created)")
            time.sleep(15)
        if not settled:
            rep.fail("  function never appeared / marker never matched after 30 attempts")
            sys.exit(1)

        cfg = lam.get_function_configuration(FunctionName=FN)
        for _ in range(20):
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress":
                break
            time.sleep(8)
            cfg = lam.get_function_configuration(FunctionName=FN)
        rep.ok(f"  State={cfg.get('State')} LastUpdateStatus={cfg.get('LastUpdateStatus')}")

        rep.section("2. invoke")
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        payload = json.loads(resp["Payload"].read())
        rep.log(f"  invoke response: {json.dumps(payload, default=str)[:500]}")
        if resp.get("FunctionError"):
            rep.fail(f"  invoke raised FunctionError: {payload}")
            sys.exit(1)

        rep.section("3. THE REAL GATE — does it reproduce Technology's convergence from LIVE data")
        try:
            o = s3.get_object(Bucket=BUCKET, Key=KEY)
            doc = json.loads(o["Body"].read())
        except Exception as e:
            rep.fail(f"  {KEY} unreadable after invoke: {str(e)[:200]}")
            sys.exit(1)

        board = doc.get("board") or []
        by_sector = {r["sector"]: r for r in board}
        tech = by_sector.get("Technology")
        rep.kv(n_sectors=len(board), top_theme=json.dumps(doc.get("top_theme"), default=str)[:300],
               narrative=str(doc.get("narrative"))[:400],
               regime_context=json.dumps(doc.get("regime_context"), default=str))

        checks = [
            ("engine produced a scored board", len(board) >= 8),
            ("Technology sector present in the board", tech is not None),
            ("Technology shows a non-trivial convergence score (>=2/4)",
             tech is not None and tech.get("convergence_score", 0) >= 2),
            ("Technology's theme is bearish (matches the live capitulation cluster + posture)",
             tech is not None and tech.get("theme") == "bearish"),
            ("stock_quadrant_cluster family fired with real evidence (not a stub)",
             tech is not None and tech["families"]["stock_quadrant_cluster"]["direction"] is not None),
            ("narrative is non-empty when convergence is high",
             doc.get("narrative") is not None if by_sector and max(
                 (r.get("convergence_score", 0) for r in board), default=0) >= 3 else True),
        ]
        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")

        if tech:
            rep.log(f"  Technology full record: {json.dumps(tech, default=str, indent=2)}")

        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — macro-confluence correctly surfaces Technology's convergence "
               f"from live data, score={tech.get('convergence_score')}/4")


if __name__ == "__main__":
    main()
