"""
ops_3904 — now that trade-evaluator's fix has produced 1,557 real evaluated
calls (26x alpha-calibrator's MIN_OBS_FOR_WEIGHT_UPDATE=60 gate), manually
invoke alpha-calibrator to see whether it can finally compute a real
factor_attribution_regression and propose reweighted weights - the natural,
immediate payoff of the trade-evaluator fix, rather than waiting for its
weekly cron.
"""
import json
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=280, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("3904_alpha_calibrator_first_real_run") as rep:
        rep.heading("ops 3904 — invoke alpha-calibrator now that real trade-journal data exists")

        rep.section("1. invoke")
        resp = lam.invoke(FunctionName="justhodl-alpha-calibrator",
                          InvocationType="RequestResponse", Payload=b"{}")
        raw = json.loads(resp["Payload"].read())
        body = json.loads(raw["body"]) if isinstance(raw, dict) and "body" in raw else raw
        rep.log(f"  invoke body: {json.dumps(body, default=str)[:800]}")
        if resp.get("FunctionError"):
            rep.fail(f"  invoke raised FunctionError: {raw}")
            sys.exit(1)

        rep.section("2. read the fresh calibration-latest.json")
        try:
            cl = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                          Key="data/calibration-latest.json")["Body"].read())
        except Exception as e:
            rep.fail(f"  unreadable: {str(e)[:200]}")
            sys.exit(1)

        summary = cl.get("summary") or {}
        proposed = cl.get("proposed_weights")
        deltas = cl.get("weight_deltas")
        guardrails = cl.get("guardrails") or {}
        decision = cl.get("deployment_decision") or {}
        rep.kv(n_trades_analyzed=summary.get("n_trades_analyzed"),
               n_evaluated_30d=summary.get("n_evaluated_30d"),
               n_significant_strategies=summary.get("n_significant_strategies"))
        rep.log(f"  guardrails: {json.dumps(guardrails, default=str)}")
        rep.log(f"  current_weights: {json.dumps(cl.get('current_weights'), default=str)}")
        rep.log(f"  proposed_weights: {json.dumps(proposed, default=str)}")
        rep.log(f"  weight_deltas: {json.dumps(deltas, default=str)}")
        rep.log(f"  deployment_decision: {json.dumps(decision, default=str)}")

        attribution = cl.get("factor_attribution") or {}
        rep.log(f"  factor_attribution coefficients: "
                f"{json.dumps(attribution.get('coefficients'), default=str)[:1200]}")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
