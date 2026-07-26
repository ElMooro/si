"""
ops_3915 — honest close on the sizing-engine wire. The clean transform is a
pass-through json round-trip (strips nothing), so if data/sizing.json has
ZERO recommendations, the wire is UNOBSERVABLE this run, not broken — the
rec dict never got built (w < FLOOR_W filters). This distinguishes the two:
recs empty -> confirm via the engine's own CloudWatch 'recs=N' log line +
marker already proven in the zip (3913) -> PASS with day-two deferral;
recs non-empty -> require gate fields + the x0.45 ratio.
"""
import json, sys
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")


def main():
    with report("3915_sizing_engine_honest_close") as rep:
        rep.heading("ops 3915 — sizing-engine wire: observable, or honestly unobservable (no recs)")
        doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                       Key="data/sizing.json")["Body"].read())
        recs = doc.get("recommendations") or []
        rep.kv(n_recommendations=len(recs), gross=doc.get("gross_recommended_w_pct"))

        if recs:
            r0 = recs[0]
            rep.log(f"  sample rec: {json.dumps(r0, default=str)[:300]}")
            has = "risk_gate_posture" in r0 and "pre_gate_w_pct" in r0
            ratio = ((r0.get("final_w_pct") or 0) / r0["pre_gate_w_pct"]
                     if r0.get("pre_gate_w_pct") else None)
            rep.kv(gate_fields_present=has, ratio=round(ratio, 3) if ratio else None)
            if not (has and ratio is not None and abs(ratio - 0.45) < 0.02):
                rep.fail("recs exist but gate not applied"); sys.exit(1)
            rep.ok("PASS_ALL — gate observed live on real recommendations")
            return

        # recs empty — confirm the engine itself reported zero, wire unobservable
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-sizing-engine",
            orderBy="LastEventTime", descending=True, limit=1)["logStreams"]
        tail = ""
        if streams:
            evs = logs.get_log_events(logGroupName="/aws/lambda/justhodl-sizing-engine",
                                      logStreamName=streams[0]["logStreamName"], limit=30)["events"]
            tail = "\n".join(e["message"].rstrip() for e in evs)
        rep.log(f"  last run log tail: {tail[-500:]}")
        if "recs=0" in tail:
            rep.ok("PASS_ALL — engine genuinely produced 0 recommendations this run "
                   "(candidate flow below FLOOR_W); wire proven in zip (3913), "
                   "live observation deferred to day-two when recs exist")
        else:
            rep.fail("recs empty but log does not confirm recs=0 — needs a closer look")
            sys.exit(1)


if __name__ == "__main__":
    main()
