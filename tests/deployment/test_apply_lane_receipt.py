"""Do not turn successful assembly, skipped dispatches or an HTTP 204 into AWS proof."""
import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
build = runpy.run_path(str(ROOT / "scripts/apply_lane_receipt.py"))["build_receipt"]


def test_noop_upload_does_not_claim_dispatch():
    r = build({"RESULT_SHA": "a" * 40})
    assert r["status"] == "not_required" and not r["deploy_dispatched"] and not r["deployment_verified"]


def test_lambda_dispatch_requires_a_matching_run_and_successful_step():
    for outcome, run_id in (("failure", "123"), ("success", ""), ("skipped", "")):
        r = build({"TARGETS": "justhodl-x", "LAMBDA_DISPATCH_OUTCOME": outcome, "DEPLOY_RUN_ID": run_id})
        assert r["status"] == "dispatch_failed" and not r["deploy_dispatched"], r
    r = build({"TARGETS": "justhodl-x", "LAMBDA_DISPATCH_OUTCOME": "success", "DEPLOY_RUN_ID": "123", "RESULT_SHA": "b" * 40})
    assert r["status"] == "dispatched_unverified" and r["deploy_dispatched"] and not r["deployment_verified"]
    assert r["result_sha"] == "b" * 40


def test_partial_multi_surface_dispatch_is_not_reported_as_success():
    r = build({"SHARED": "aws/shared/helpers.py", "PAGES": "calls.html", "LAMBDA_DISPATCH_OUTCOME": "success",
               "DEPLOY_RUN_ID": "123", "SITE_DISPATCH_OUTCOME": "failure"})
    assert r["status"] == "dispatch_failed" and not r["deploy_dispatched"]
    r = build({"PAGES": "calls.html", "SITE_DISPATCH_OUTCOME": "success"})
    assert r["status"] == "dispatched_unverified" and r["deploy_dispatched"] and not r["deployment_verified"]


def test_ops_dispatch_is_reported_and_cannot_disappear_from_a_mixed_release():
    for outcome in ("skipped", "failure"):
        r = build({"BATCH_OPS": "ops_9999_gate.py", "OPS_DISPATCH_OUTCOME": outcome})
        assert r["status"] == "dispatch_failed" and not r["deploy_dispatched"]
    r = build({"BATCH_OPS": "ops_9999_gate.py", "OPS_DISPATCH_OUTCOME": "success"})
    assert r["status"] == "dispatched_unverified" and r["ops_scripts"] == ["ops_9999_gate.py"]
