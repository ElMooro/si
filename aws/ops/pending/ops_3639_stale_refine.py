"""ops 3639 — stale refinement: reasoned silences for known categories —
quiver-* (upstream dead, Benzinga/Quiver 401 era) · user-* (per-user SaaS,
event-driven pre-launch) · one-time config keys (history-api-url/index,
congress-party-map superseded by congress-direct) · feedback-summary
(feature unscheduled by design). Each override carries a reason in
data/stale-triage.json v2. Residue after registry re-run = the REAL repair
list, surfaced verbatim."""
import json, re, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

S3C = boto3.client("s3", "us-east-1")
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
B = "justhodl-dashboard-live"

RULES = [
    (r"quiver", 9999, "upstream dead (Quiver 401 era); cache writers skip"),
    (r"^data/user-", 9999, "per-user SaaS key — event-driven, not scheduled"),
    (r"history-api-url|history-index", 9999, "one-time config key"),
    (r"congress-party-map", 9999, "superseded by congress-direct"),
    (r"feedback-summary", 9999, "feature unscheduled by design"),
]

with report("3639_stale_refine") as rep:
    rep.heading("ops 3639 — reasoned silences + residue surface")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:660]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:620]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    try:
        reg = json.loads(S3C.get_object(Bucket=B, Key="data/feed-registry.json")["Body"].read())
        stale = reg.get("stale") or []
        n0 = len(stale)
        sla_cfg = json.loads(S3C.get_object(Bucket=B, Key="config/feed-sla.json")["Body"].read())
        tri = json.loads(S3C.get_object(Bucket=B, Key="data/stale-triage.json")["Body"].read())
        silenced = {}
        for row in stale:
            k = row.get("key") or ""
            for pat, hrs, why in RULES:
                if re.search(pat, k):
                    if k not in sla_cfg:
                        sla_cfg[k] = hrs
                    silenced[k] = why
                    break
        tri["v2_silences"] = silenced
        tri["v2_at"] = datetime.now(timezone.utc).isoformat()
        S3C.put_object(Bucket=B, Key="config/feed-sla.json",
                       Body=json.dumps(sla_cfg, indent=2).encode(),
                       ContentType="application/json")
        r = LAM.invoke(FunctionName="justhodl-feed-registry",
                       InvocationType="RequestResponse", Payload=b"{}")
        _ = r["Payload"].read(); time.sleep(2)
        reg2 = json.loads(S3C.get_object(Bucket=B, Key="data/feed-registry.json")["Body"].read())
        residue = [{"key": x.get("key"), "age_h": x.get("age_h"),
                    "sla_h": x.get("sla_h")} for x in (reg2.get("stale") or [])]
        tri["residue_repair_list"] = residue
        S3C.put_object(Bucket=B, Key="data/stale-triage.json",
                       Body=json.dumps(tri, indent=2).encode(),
                       ContentType="application/json")
        n1 = len(residue)
        gate("G1_refined", n1 < n0 and len(silenced) >= 8,
             f"stale {n0} -> {n1} silenced={len(silenced)} "
             f"residue={[r0['key'][:40] for r0 in residue[:14]]}")
        out["residue"] = residue
    except Exception as e:
        gate("G1_refined", False, str(e)[:380])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: G1"
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3639.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
