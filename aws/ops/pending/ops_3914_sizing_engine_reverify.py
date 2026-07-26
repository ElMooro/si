"""
ops_3914 — close the single red check from ops 3913: sizing-engine's real
output key is data/sizing.json (OUT_KEY line 48), not the guessed
data/sizing-engine.json. The engine already ran and wrote during 3913's
invoke; this reads the REAL key and gates on the risk-gate fields being
present in its live output. Container-guessing strikes again — G0 key
contract honored this time (grepped the producer source).
"""
import json, sys
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")


def scan(obj, pred, hits, cap=3):
    if len(hits) >= cap: return
    if isinstance(obj, dict):
        if pred(obj): hits.append(obj)
        for v in obj.values(): scan(v, pred, hits, cap)
    elif isinstance(obj, list):
        for v in obj[:400]: scan(v, pred, hits, cap)


def main():
    with report("3914_sizing_engine_reverify") as rep:
        rep.heading("ops 3914 — sizing-engine live output at the REAL key (data/sizing.json)")
        o = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/sizing.json")
        doc = json.loads(o["Body"].read())
        rep.kv(last_modified=o["LastModified"].isoformat(), top_keys=str(sorted(doc.keys())[:12]))
        hits = []
        scan(doc, lambda d: "risk_gate_posture" in d and "final_w_pct" in d, hits)
        rep.log(f"  gated recs found: {len(hits)}")
        for h in hits[:3]:
            rep.log(f"    {json.dumps({k: h.get(k) for k in ('ticker','symbol','final_w_pct','pre_gate_w_pct','risk_gate_posture') if k in h or True}, default=str)[:260]}")
        ok = bool(hits) and hits[0].get("risk_gate_posture") in ("RISK_ON","NEUTRAL","RISK_OFF","SEVERE")
        ratio_ok = False
        if hits and hits[0].get("pre_gate_w_pct"):
            r = (hits[0].get("final_w_pct") or 0) / hits[0]["pre_gate_w_pct"]
            ratio_ok = abs(r - 0.45) < 0.02
            rep.kv(final_over_pregate_ratio=round(r, 3), expected=0.45)
        checks = [("recs carry risk_gate fields", ok),
                  ("gate multiplier actually applied (ratio ~= 0.45)", ratio_ok)]
        for l, k in checks: (rep.ok if k else rep.fail)(f"  {l}")
        if not all(k for _, k in checks):
            rep.fail("FAILED"); sys.exit(1)
        rep.ok("PASS_ALL — sizing-engine gated live; all 5 wires now verified")


if __name__ == "__main__":
    main()
