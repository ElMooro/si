"""
ops 953 -- dump real S3 schemas of edges 6, 8, 9 so ops 952 v3 can align
=========================================================================
Also: diagnose Edge #5 (russell-recon-frontrun) "insufficient universe"
error -- find what input it expects.
"""
import datetime as dt
import json
import os
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

EDGES = [
    ("5", "data/russell-recon-frontrun.json", "justhodl-russell-recon-frontrun"),
    ("6", "data/buyback-scanner.json", "justhodl-buyback-scanner"),
    ("8", "data/opex-calendar.json", "justhodl-opex-calendar"),
    ("9", "data/activist-13d.json", "justhodl-activist-13d"),
]

CHECKS = []


def add(name, passed, detail):
    CHECKS.append({"name": name, "passed": bool(passed), "detail": str(detail)[:500]})


def dump_schema(edge, key):
    try:
        r = s3.get_object(Bucket=BUCKET, Key=key)
        d = json.loads(r["Body"].read())
        top_keys = sorted(d.keys())
        # Trade-ticket-like + state-like fields
        sample = {}
        for k in top_keys[:30]:
            v = d[k]
            if isinstance(v, dict):
                sample[k] = {"_type": "dict", "_keys": list(v.keys())[:10]}
            elif isinstance(v, list):
                sample[k] = {"_type": "list", "_len": len(v),
                              "_first_keys": list(v[0].keys())[:8]
                              if v and isinstance(v[0], dict) else "n/a"}
            elif isinstance(v, str) and len(v) > 80:
                sample[k] = {"_type": "string", "_len": len(v), "_head": v[:80]}
            else:
                sample[k] = v
        add(f"e{edge}.top_keys_count", True, f"{len(top_keys)} keys")
        add(f"e{edge}.schema_dump", True, json.dumps(sample, default=str)[:480])
        # Print which trade-ticket-like and state-like fields exist
        ticket_like = [k for k in top_keys
                       if "trade" in k.lower() or "ticket" in k.lower()
                       or "recommend" in k.lower() or "setup" in k.lower()
                       or "opportun" in k.lower() or "candidate" in k.lower()]
        add(f"e{edge}.trade_like_fields", True, str(ticket_like))
        why_like = [k for k in top_keys if "why" in k.lower() or "explain" in k.lower()]
        add(f"e{edge}.why_like_fields", True, str(why_like))
        # Print state fields
        state_like = [k for k in top_keys if "state" in k.lower()
                      or "phase" in k.lower() or "regime" in k.lower()]
        add(f"e{edge}.state_like_fields", True, str(state_like))
    except ClientError as ex:
        add(f"e{edge}.s3_get", False, str(ex)[:200])


def main():
    print(f"ops 953 -- schema dump at {dt.datetime.utcnow().isoformat()}Z")
    for edge, key, fn in EDGES:
        print(f"\n--- Edge #{edge} ({key}) ---")
        dump_schema(edge, key)

    # Diagnose Edge #5 -- inspect why it returns "insufficient universe"
    try:
        info = lam.get_function(FunctionName="justhodl-russell-recon-frontrun")
        cfg = info.get("Configuration", {})
        env = cfg.get("Environment", {}).get("Variables", {})
        add("e5.env_vars",
            True,
            f"env_keys={list(env.keys())}")
        # Look for the snapshot/universe S3 location it expects
        # The Lambda likely reads from data/russell-* or universe/russell-*
        snapshots = []
        for pref in ("data/russell", "universe/", "snapshots/russell",
                     "data/snapshots", "russell-"):
            try:
                lst = s3.list_objects_v2(Bucket=BUCKET, Prefix=pref, MaxKeys=10)
                for it in lst.get("Contents", []):
                    snapshots.append(f"{it['Key']} ({it['Size']}B)")
            except Exception:
                pass
        add("e5.candidate_universe_objects", True,
            json.dumps(snapshots[:20])[:480])
    except Exception as ex:
        add("e5.diag_error", False, str(ex)[:200])

    report = {
        "ops": 953,
        "title": "dump real S3 schemas + diagnose Edge #5",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS),
                    "passed": sum(1 for c in CHECKS if c["passed"]),
                    "failed": sum(1 for c in CHECKS if not c["passed"])},
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/953_dump_edge_schemas.json", "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nWrote report -- {len(CHECKS)} probes\n")
    for c in CHECKS:
        print(f"  [{'OK' if c['passed'] else 'X '}] {c['name']:32} {c['detail'][:120]}")


if __name__ == "__main__":
    main()
