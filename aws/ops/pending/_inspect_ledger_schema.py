"""Inspect actual decisive-call-history snapshot structure."""
import boto3
import json
from ops_report import report

REGION = "us-east-1"
S3 = boto3.client("s3", region_name=REGION)


def main():
    with report("inspect_ledger_schema") as r:
        r.heading("Snapshot structure for #3 build")
        obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/decisive-call-history.json")
        d = json.loads(obj["Body"].read())
        r.log(f"  v: {d.get('v')}")
        r.log(f"  last_updated: {d.get('last_updated')}")
        r.log(f"  n_snapshots: {d.get('n_snapshots')}")
        r.log("")

        snaps = d.get("snapshots") or []
        r.log(f"  Total snapshots: {len(snaps)}")
        if snaps:
            r.log(f"  First snapshot keys: {list(snaps[0].keys())}")
            r.log("")
            r.log("  All snapshots verb + timestamp:")
            for s in snaps:
                ts = s.get("as_of") or s.get("timestamp") or s.get("generated_at") or "?"
                verb = s.get("verb") or s.get("call_verb") or s.get("call") or "?"
                r.log(f"    {ts}  →  {verb}")
            r.log("")
            r.log("  First snapshot full payload:")
            r.log(json.dumps(snaps[0], indent=2, default=str)[:2000])


if __name__ == "__main__":
    main()
