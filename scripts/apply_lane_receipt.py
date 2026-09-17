#!/usr/bin/env python3
"""Report dispatch evidence honestly. AWS release receipts remain the deployment proof."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path


def build_receipt(env) -> dict:
    lambdas = bool(env.get("TARGETS") or env.get("SHARED"))
    sites = bool(env.get("WORKERS") or env.get("PAGES"))
    ops = bool(env.get("BATCH_OPS"))
    lambda_ok = env.get("LAMBDA_DISPATCH_OUTCOME") == "success" and bool(env.get("DEPLOY_RUN_ID"))
    site_ok = env.get("SITE_DISPATCH_OUTCOME") == "success"
    ops_ok = env.get("OPS_DISPATCH_OUTCOME") == "success"
    dispatched = (lambdas or sites or ops) and (not lambdas or lambda_ok) and (not sites or site_ok) and (not ops or ops_ok)
    status = "not_required" if not (lambdas or sites or ops) else "dispatched_unverified" if dispatched else "dispatch_failed"
    return {
        "schema": "apply-lane-receipt.v2", "status": status,
        "run_id": env.get("GITHUB_RUN_ID"),
        "run_url": f"https://github.com/{env.get('GITHUB_REPOSITORY')}/actions/runs/{env.get('GITHUB_RUN_ID')}",
        "base_sha": env.get("BASE_SHA"), "result_sha": env.get("RESULT_SHA"),
        "patchers": env.get("APPLIED", "").split(), "targets": env.get("TARGETS", "").split(),
        "shared": env.get("SHARED", "").split(), "workers": env.get("WORKERS", "").split(),
        "pages": env.get("PAGES", "").split(),
        "ops_scripts": env.get("BATCH_OPS", "").split(),
        "deploy_dispatched": bool(dispatched), "deployment_verified": False,
        "lambda_dispatch_outcome": env.get("LAMBDA_DISPATCH_OUTCOME"),
        "site_dispatch_outcome": env.get("SITE_DISPATCH_OUTCOME"),
        "ops_dispatch_outcome": env.get("OPS_DISPATCH_OUTCOME"),
        "deploy_run_id": env.get("DEPLOY_RUN_ID"),
        "verification": "Compare data/ops/releases/<function>.json commit to result_sha; site/worker dispatches require their own workflow verification.",
        "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


if __name__ == "__main__":
    receipt = build_receipt(os.environ)
    path = Path("aws/ops/reports/apply-lane") / f"{os.environ['GITHUB_RUN_ID']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(receipt, indent=1) + "\n", encoding="utf-8", newline="\n")
    print(path.read_text(encoding="utf-8"))
