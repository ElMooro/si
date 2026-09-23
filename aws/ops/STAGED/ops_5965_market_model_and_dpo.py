"""ops 5965 -- train + grade the quantitative market model; status of the DPO relaunch (Claude, 2026-09-23). Direct lane."""
import json, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
REPO = HERE.parents[3]; PRI = "justhodl-ai-857687956942"


def main():
    s3 = boto3.client("s3", region_name="us-east-1"); sm = boto3.client("sagemaker", region_name="us-east-1")
    with report("5965_market_model_and_dpo") as r:
        r.heading("ops 5965 -- the market model; the DPO relaunch")
        r.section("1. Market model (FMP history since 2005, crises embargoed, graded on the frozen holdout drills)")
        subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "numpy"], capture_output=True, timeout=300)
        p = subprocess.run([sys.executable, "scripts/factory_market_model.py"], cwd=REPO, capture_output=True, text=True, timeout=1800)
        for line in (p.stdout or "").strip().splitlines()[-3:]:
            r.log(line[:1500])
        (r.ok if p.returncode == 0 else r.fail)("market model rc=%s %s" % (p.returncode, (p.stderr or "")[-500:] if p.returncode else ""))
        r.section("2. Gear B since 15:24")
        for j in sm.list_training_jobs(NameContains="jh-", CreationTimeAfter=datetime(2026, 9, 23, 15, 0, tzinfo=timezone.utc), MaxResults=10, SortBy="CreationTime", SortOrder="Descending").get("TrainingJobSummaries", []):
            d = sm.describe_training_job(TrainingJobName=j["TrainingJobName"])
            r.log("%s %s %s/%s spot=%s mode=%s %s" % (j["CreationTime"].strftime("%H:%M"), j["TrainingJobName"], d["TrainingJobStatus"], d.get("SecondaryStatus"),
                                                     d.get("EnableManagedSpotTraining"), (d.get("HyperParameters") or {}).get("mode"), str(d.get("FailureReason") or "")[:160]))
        try:
            last = json.loads(s3.get_object(Bucket=PRI, Key="factory/gearb/last_tick.json")["Body"].read())
            r.log("last_tick: " + json.dumps({k: last.get(k) for k in ("at", "built", "launched", "refusal", "capacity_stops", "merged")}, default=str)[:1200])
        except Exception as e:  # noqa: BLE001
            r.warn(str(e)[:120])
        if p.returncode:
            sys.exit(1)


if __name__ == "__main__":
    main()
