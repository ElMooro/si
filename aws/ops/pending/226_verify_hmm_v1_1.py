#!/usr/bin/env python3
"""Step 226 — verify HMM v1.1 (Dirichlet prior) fixed state collapse.

After commit 3eddb24, deploy-lambdas.yml redeploys the Lambda.
This step manually invokes once to validate the new fit, since
EventBridge runs daily and we don't want to wait 24h to see results.

Success criteria:
  - All 4 states have non-zero probability (no 0.0 or 1.0 collapse)
  - Transition matrix has all rows summing to ~1.0
  - is_warming_up still True (training_n=200 < 60 in days, but
    measured in observations — actually 200 > 60, so should be
    False now, which signals the model considers itself trained)
"""
import json, time
from datetime import datetime, timezone
from ops_report import report
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
NEW = "justhodl-regime-anomaly"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)


def fmt_probs(p):
    if not p: return "—"
    return ", ".join(f"{k}={v:.3f}" for k, v in p.items())


with report("verify_hmm_v1_1_prior") as r:
    r.heading("Phase 9.2 v1.1 — verify Dirichlet prior fixed state collapse")

    # Confirm Lambda's last modified timestamp shows the redeploy
    r.section("1. Lambda metadata")
    cfg = lam.get_function_configuration(FunctionName=NEW)
    r.log(f"  CodeSha256:   {cfg['CodeSha256']}")
    r.log(f"  LastModified: {cfg['LastModified']}")
    r.log(f"  Runtime:      {cfg['Runtime']}")
    r.log(f"  Timeout:      {cfg['Timeout']}s")
    r.log(f"  Memory:       {cfg['MemorySize']}MB")

    # Manual invoke
    r.section("2. Manual invoke (forcing fresh fit)")
    t0 = time.time()
    inv = lam.invoke(FunctionName=NEW, InvocationType="RequestResponse",
                     Payload=json.dumps({}))
    elapsed = time.time() - t0
    err = inv.get("FunctionError")
    payload = inv["Payload"].read().decode("utf-8", errors="replace")
    if err:
        r.warn(f"  ✗ err={err} ({elapsed:.1f}s)")
        r.warn(f"  payload: {payload[:600]}")
    else:
        r.log(f"  ✅ OK ({elapsed:.1f}s)")
        r.log(f"  payload summary: {payload[:400]}")

    time.sleep(3)

    # Read S3 output
    r.section("3. Read s3://.../data/regime-anomaly.json")
    obj = s3.get_object(Bucket=BUCKET, Key="data/regime-anomaly.json")
    age_s = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()
    data = json.loads(obj["Body"].read())
    r.log(f"  age={age_s:.1f}s")

    hmm = data.get("hmm", {})
    probs = hmm.get("state_probabilities", {})
    A = hmm.get("transition_matrix", {})
    means = hmm.get("state_means", {})
    stds = hmm.get("state_stds", {})

    r.section("4. State probability sanity")
    r.log(f"  current state: {hmm.get('state_label')}")
    r.log(f"  probabilities: {probs}")
    if probs:
        max_p = max(probs.values())
        n_zero = sum(1 for v in probs.values() if v < 0.001)
        n_total = len(probs)
        if max_p > 0.99 and n_zero == n_total - 1:
            r.warn(f"  ⚠ state collapse persists — one state has {max_p:.3f}, others ~0")
            r.warn(f"  Prior may need to be increased further or training data is genuinely uniform")
        elif n_zero == 0:
            r.log(f"  ✅ all 4 states have positive probability — collapse fixed")
        else:
            r.log(f"  ⚠ {n_zero}/{n_total} states have ~0 probability")

    r.section("5. Transition matrix sanity")
    if A:
        for from_state, to_dict in A.items():
            row_sum = sum(to_dict.values()) if isinstance(to_dict, dict) else 0
            r.log(f"  {from_state}: row_sum={row_sum:.4f}, diag={to_dict.get(from_state, 'N/A'):.3f}")

    r.section("6. State means + std spread")
    if means:
        r.log(f"  means:  {means}")
        r.log(f"  stds:   {stds}")
        # Check that means are NOT all collapsed to global mean
        if len(set(round(v, 1) for v in means.values())) >= 2:
            r.log(f"  ✅ state means show separation")
        else:
            r.warn(f"  ⚠ state means are all equal — model has not differentiated regimes")

    r.section("7. Anomaly engine status")
    anomaly = data.get("anomaly", {})
    r.log(f"  per_signal count: {len(anomaly.get('per_signal', {}))}")
    r.log(f"  n_anomalies:      {anomaly.get('n_anomalies')}")
    r.log(f"  composite score:  {anomaly.get('composite_anomaly_score')}")

    r.section("FINAL")
    r.log(f"  HMM v1.1 deploy: {cfg['LastModified']}")
    r.log(f"  Sample size:      {hmm.get('training_n')}")
    r.log(f"  Warming up:       {hmm.get('is_warming_up')}")
    r.log(f"  Result: {'COLLAPSE FIXED' if probs and max(probs.values()) < 0.99 else 'NEEDS MORE WORK'}")
    r.log("Done")
