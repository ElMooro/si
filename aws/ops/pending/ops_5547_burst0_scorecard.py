"""ops 5547 -- burst 0 scorecard (read-only): what the first owned trace burst produced and what the verifier kept.

Reads the burst job description (status, seconds billed), the burst summary written by factory-trace-verify.yml
(candidates seen / passed / failed / timeouts, rows written), counts self_trace rows now under
factory/curriculum/code/verified/, and computes the two numbers that decide compute: pass rate at K and cost per
verified pass (on-demand cap basis and spot-billed basis when SageMaker reports it). Launches nothing.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PRI = "us-east-1", "justhodl-ai-857687956942"
BURST = "jh-burst-gen0-20260914-005825"
VERIFIED = "factory/curriculum/code/verified/"


def _get_json(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=PRI, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("sagemaker", region_name=REGION)
    with report("ops_5547_burst0_scorecard") as R:
        R.heading("ops 5547 -- burst 0 scorecard: job, verifier summary, kept rows, pass rate at K, cost per pass")
        d = sm.describe_training_job(TrainingJobName=BURST)
        billed = d.get("BillableTimeInSeconds"); secs = d.get("TrainingTimeInSeconds")
        R.ok("job %s status=%s training_s=%s billable_s=%s spot_savings=%s%% failure=%s" % (
            BURST, d.get("TrainingJobStatus"), secs, billed, round(100 * (1 - (billed or 0) / secs), 1) if secs and billed else None, (d.get("FailureReason") or "")[:120]))
        rec = _get_json(s3, "factory/bursts/jobs/%s.json" % BURST) or {}
        summaries = [o["Key"] for o in s3.list_objects_v2(Bucket=PRI, Prefix="factory/bursts/%s/summary-" % BURST).get("Contents", [])]
        summary = _get_json(s3, sorted(summaries)[-1]) if summaries else None
        rep = (summary or {}).get("report") or {}
        R.ok("verifier summary: %s" % json.dumps({k: (summary or {}).get(k) for k in ("run_id", "rows_written", "rows_existing", "verdicts")}))
        R.ok("candidates seen=%s passed=%s failed=%s timeouts=%s malformed=%s" % (rep.get("seen"), rep.get("passed"), rep.get("failed"), rep.get("timeouts"), rep.get("malformed")))
        kept, total, token = 0, 0, None
        while True:
            kw = {"Bucket": PRI, "Prefix": VERIFIED, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                total += 1
                doc = _get_json(s3, o["Key"]) or {}
                kept += int(doc.get("kind") == "self_trace")
            token = resp.get("NextContinuationToken")
            if not resp.get("IsTruncated"):
                break
        tasks, k = int(rec.get("tasks") or 0), int(rec.get("samples_per_task") or 0)
        seen, passed = int(rep.get("seen") or 0), int(rep.get("passed") or 0)
        cap = float(rec.get("cap_usd") or 0); hourly = float(rec.get("usd_per_hour") or 0)
        cost_cap = round(hourly * (secs or 0) / 3600.0, 4) if secs else None
        cost_spot = round(hourly * (billed or 0) / 3600.0, 4) if billed else None
        pass_rate = round(passed / seen, 4) if seen else None
        R.ok("rows: self_trace kept=%d of %d verified rows total; tasks=%d K=%d candidates=%d" % (kept, total, tasks, k, seen))
        R.ok("PASS RATE at K=%d: %s (%d/%d); cost on-demand basis $%s, spot-billed basis $%s -> cost per verified pass $%s (spot) / $%s (on-demand)" % (
            k, pass_rate, passed, seen, cost_cap, cost_spot, round(cost_spot / passed, 5) if cost_spot and passed else None, round(cost_cap / passed, 5) if cost_cap and passed else None))
        R.kv(pass_rate=pass_rate, passed=passed, seen=seen, kept_rows=kept, cost_spot_usd=cost_spot)
        R.ok("GREEN -- scorecard recorded")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
