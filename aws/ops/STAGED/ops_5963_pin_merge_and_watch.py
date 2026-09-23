"""ops 5963 -- re-pin the training bundle so it carries merge_adapter.py, and watch the first DPO generation (Claude, 2026-09-23)."""
import json, subprocess, sys
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
REPO = HERE.parents[3]; PRI = "justhodl-ai-857687956942"


def main():
    s3 = boto3.client("s3", region_name="us-east-1"); sm = boto3.client("sagemaker", region_name="us-east-1")
    with report("5963_pin_merge_and_watch") as r:
        r.heading("ops 5963 -- bundle with merge_adapter.py; the DPO generation")
        pin = json.loads(s3.get_object(Bucket=PRI, Key="factory/training/current.json")["Body"].read())
        p = subprocess.run([sys.executable, "scripts/factory_training_pin.py", "--image", pin["training_image"]] + (["--require-digest"] if pin.get("require_digest") else []),
                           cwd=REPO, capture_output=True, text=True, timeout=300)
        new = json.loads(s3.get_object(Bucket=PRI, Key="factory/training/current.json")["Body"].read())
        (r.ok if p.returncode == 0 else r.fail)("bundle %s -> %s %s" % (str(pin.get("bundle_sha256"))[:12], str(new.get("bundle_sha256"))[:12], (p.stderr or "")[-200:] if p.returncode else ""))
        jobs = sm.list_training_jobs(NameContains="jh-gearb-gen27-", MaxResults=3, SortBy="CreationTime", SortOrder="Descending").get("TrainingJobSummaries", [])
        for j in jobs:
            d = sm.describe_training_job(TrainingJobName=j["TrainingJobName"])
            r.kv(job=j["TrainingJobName"], status=d["TrainingJobStatus"], secondary=d.get("SecondaryStatus"), failure=str(d.get("FailureReason"))[:300], mode=(d.get("HyperParameters") or {}).get("mode"))
            for t in (d.get("SecondaryStatusTransitions") or [])[-4:]:
                r.log("%s %s %s" % (t["StartTime"].strftime("%H:%M:%S"), t["Status"], str(t.get("StatusMessage"))[:160]))
        if p.returncode:
            sys.exit(1)


if __name__ == "__main__":
    main()
