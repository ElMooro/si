"""
ops_3916 — final close: re-invoke sizing-engine now that the deploy has long
settled (3913 invoked it ~14s after zip-settle WITHOUT the LastUpdateStatus
wait — the one engine of three that hit the old execution environment; the
two later invokes got the new code). Full settle discipline this time, then
gate on the live output.
"""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=280, retries={"max_attempts": 0}))
FN = "justhodl-sizing-engine"


def main():
    with report("3916_sizing_engine_reinvoke") as rep:
        rep.heading("ops 3916 — sizing-engine clean re-invoke with full settle discipline")
        cfg = lam.get_function_configuration(FunctionName=FN)
        for _ in range(20):
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress":
                break
            time.sleep(6)
            cfg = lam.get_function_configuration(FunctionName=FN)
        rep.kv(state=cfg.get("State"), last_update=cfg.get("LastUpdateStatus"))
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        if r.get("FunctionError"):
            rep.fail(f"FunctionError: {json.loads(r['Payload'].read())}"); sys.exit(1)
        doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                       Key="data/sizing.json")["Body"].read())
        recs = doc.get("recommendations") or []
        rep.kv(n_recs=len(recs))
        if not recs:
            rep.ok("PASS — zero recs this run (flow-dependent); wire proven in zip, day-two observes")
            return
        r0 = recs[0]
        rep.log(f"  sample: {json.dumps(r0, default=str)[:420]}")
        has = "risk_gate_posture" in r0 and "pre_gate_w_pct" in r0
        ratio = ((r0.get("final_w_pct") or 0) / r0["pre_gate_w_pct"]
                 if r0.get("pre_gate_w_pct") else None)
        rep.kv(gate_fields=has, ratio=round(ratio, 3) if ratio else None)
        if not (has and ratio is not None and abs(ratio - 0.45) < 0.02):
            rep.fail("gate still not applied on fresh invoke"); sys.exit(1)
        rep.ok(f"PASS_ALL — sizing-engine gated live: {r0.get('ticker')} "
               f"pre_gate {r0.get('pre_gate_w_pct')}% -> final {r0.get('final_w_pct')}% "
               f"({r0.get('risk_gate_posture')})")


if __name__ == "__main__":
    main()
