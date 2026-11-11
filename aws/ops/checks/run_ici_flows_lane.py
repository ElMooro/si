"""Runner entry point for the ICI weekly release lane (.github/workflows/ici-flows-lane.yml); never invokes Lambda."""
from pathlib import Path
import json
import os
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ("aws/shared", "aws/ops/checks")]
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
import ici_flows_lane as lane  # noqa: E402


def main():
    if os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise ValueError("Reviewed main checkout required")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if not re.fullmatch(r"[0-9]{1,20}", run_id):
        raise ValueError("Stable native request identity required")
    client = boto3.client("s3", region_name="us-east-1", config=Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 3}))
    summary = lane.run(client, lane.LIVE, run_id)
    print(json.dumps({"lane": lane.ENGINE, **summary}, default=str), flush=True)
    native = summary.get("native") or {}
    desk = summary.get("desk") or {}
    if not desk.get("weeks"):
        raise ValueError("Desk feed publication incomplete")
    if native.get("published") is not True and native.get("status") != "duplicate_request":
        # the native packet is reported, not required: the desk feed is complete on its own
        print(json.dumps({"warning": "native ici-research.v1 packet not published this run", "native": native}), flush=True)


def known_messages():
    """The fixed literal messages this lane raises: only these are ever printed; anything else stays redacted."""
    texts = []
    for name in ("run_ici_flows_lane.py", "ici_flows_lane.py"):
        texts.append((ROOT / "aws/ops/checks" / name).read_text(encoding="utf-8"))
    for name in ("ici_store.py", "ici_research.py", "verify_ici_research.py"):
        texts.append((ROOT / "aws/lambdas/justhodl-ici-flows/source" / name).read_text(encoding="utf-8"))
    return set(re.findall(r"raise ValueError\(['\"]([^'\"\\]{3,160})['\"]\)", "\n".join(texts)))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        print(json.dumps({"status": "failed", "error_type": type(exc).__name__,
                          "message": message if isinstance(exc, ValueError) and message in known_messages() else "[redacted: not a known literal]",
                          "action": "Inspect the lane log and the native request journal; the next scheduled run retries on its own."}), flush=True)
        sys.exit(1)
