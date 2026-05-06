#!/usr/bin/env python3
"""Step 230 — verify HMM v1.2 loader filters zeros and HMM works correctly.

After v1.2:
  - Loader drops any archive with score == 0
  - That removes ~190 of 200 entries (the Mar 9 → Apr 24 corruption)
  - Remaining ~10 entries are all 43 from Apr 25-26
  - HMM with 10 obs should report is_warming_up=True
  - State probabilities should be more spread (not 100% one state),
    since 10 obs of value 43 + Dirichlet prior keeps all states alive
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


with report("verify_hmm_v1_2_loader") as r:
    r.heading("Phase 9.2 v1.2 — verify zero-filter loader + state distribution")

    r.section("1. Lambda metadata (post-redeploy)")
    cfg = lam.get_function_configuration(FunctionName=NEW)
    r.log(f"  CodeSha256:   {cfg['CodeSha256']}")
    r.log(f"  LastModified: {cfg['LastModified']}")

    r.section("2. Manual invoke")
    t0 = time.time()
    inv = lam.invoke(FunctionName=NEW, InvocationType="RequestResponse",
                     Payload=json.dumps({}))
    elapsed = time.time() - t0
    err = inv.get("FunctionError")
    payload = inv["Payload"].read().decode("utf-8")
    if err:
        r.warn(f"  ✗ {err}: {payload[:500]}")
    else:
        r.log(f"  ✅ OK ({elapsed:.1f}s)")
        r.log(f"  payload: {payload[:400]}")

    time.sleep(3)

    r.section("3. Read regime-anomaly.json")
    obj = s3.get_object(Bucket=BUCKET, Key="data/regime-anomaly.json")
    data = json.loads(obj["Body"].read())
    hmm = data.get("hmm", {})
    training = data.get("training_window", {})

    r.log(f"  training_n:        {hmm.get('training_n')}  (was 200 before zero filter)")
    r.log(f"  is_warming_up:     {hmm.get('is_warming_up')}")
    r.log(f"  ka_index_obs:      {training.get('ka_index_observations')}")
    r.log(f"  earliest:          {training.get('earliest')}")
    r.log(f"  latest:            {training.get('latest')}")

    r.log(f"")
    probs = hmm.get("state_probabilities", {})
    r.log(f"  state_label:       {hmm.get('state_label')}")
    r.log(f"  probabilities:")
    for k, v in probs.items():
        bar = "█" * int(v * 30)
        r.log(f"    {k:13s}: {bar} {v:.3f}")

    means = hmm.get("state_means", {})
    stds = hmm.get("state_stds", {})
    r.log(f"")
    r.log(f"  state_means:       {means}")
    r.log(f"  state_stds:        {stds}")

    # Quality checks
    r.section("4. Quality assessment")
    n_zero_prob = sum(1 for v in probs.values() if v < 0.001)
    if n_zero_prob == 0:
        r.log(f"  ✅ all 4 states have positive probability")
    else:
        r.warn(f"  ⚠ {n_zero_prob} states have ~0 probability")

    if hmm.get("is_warming_up"):
        r.log(f"  ✅ is_warming_up=True correctly indicates limited data")
        r.log(f"    Frontend will show the warming-up banner to user")

    if hmm.get("training_n", 0) < 50:
        r.log(f"  ✅ low training_n acknowledged — model is honest about uncertainty")

    r.section("FINAL")
    r.log("Done")
