"""Read-only market-history package/control baseline; no credentials or producer data."""
from pathlib import Path
import hashlib
import json
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws/ops"), str(ROOT / "aws/ops/checks")]
from market_runtime_evidence import runtime, BUCKET

FUNCTION = "justhodl-tv-bars"
SOURCE = "aws/lambdas/justhodl-tv-bars/source/lambda_function.py"
SOURCE_SHA256 = "99c94f7e386f61e9aafcef48598aaa7534c3b8df0a05cf86ec5e591150be06b9"


class ReceiptOnly:
    def __init__(self, client):
        self.client = client

    def get_object(self, **kwargs):
        if kwargs != {"Bucket": BUCKET, "Key": "data/ops/releases/" + FUNCTION + ".json"}:
            raise ValueError("Only the named native release receipt is permitted")
        return self.client.get_object(**kwargs)


def normalize(value):
    if not isinstance(value, dict) or value.get("function_name") != FUNCTION:
        raise ValueError("Exact function identity required")
    for field in ("source_files_checked", "handler_bytes", "timeout", "memory_mb", "ephemeral_storage_mb"):
        if type(value.get(field)) is not int or value[field] <= 0:
            raise ValueError("Positive typed native counts required")
    if value["source_files_checked"] != 1:
        raise ValueError("Exact reviewed source population required")
    for field in ("code_sha256", "runtime", "handler", "role"):
        if not isinstance(value.get(field), str) or not value[field]:
            raise ValueError("Native control identity unavailable")
    if not isinstance(value.get("architectures"), list) or not value["architectures"]:
        raise ValueError("Native architectures unavailable")
    if not all(isinstance(x, str) and x for x in value["architectures"]):
        raise ValueError("Native architecture identity unavailable")
    receipt = value.get("receipt")
    if not isinstance(receipt, dict) or receipt.get("status") not in ("matched", "missing_predecessor_receipt"):
        raise ValueError("Receipt inspection unavailable")
    if receipt["status"] == "matched" and (not isinstance(receipt.get("commit"), str) or not receipt["commit"]):
        raise ValueError("Matched receipt requires commit identity")
    rows = value.get("schedules")
    if not isinstance(rows, list):
        raise ValueError("Schedule census unavailable")
    for row in rows:
        if not isinstance(row, dict) or any(not isinstance(row.get(k), str) or not row[k] for k in ("kind", "name", "state")):
            raise ValueError("Native schedule identity unavailable")
        if row["state"] not in ("ENABLED", "DISABLED"):
            raise ValueError("Native schedule state unavailable")
        if type(row.get("native_targets")) is not int or row["native_targets"] < 1:
            raise ValueError("Native schedule binding unavailable")
        if "group" in row and not isinstance(row["group"], str):
            raise ValueError("Native schedule group unavailable")
    return {**value, "schedules": sorted(rows, key=lambda row: (row["kind"], row.get("group", "default"), row["name"]))}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def capture(clients):
    before = normalize(runtime(*clients, FUNCTION))
    after = normalize(runtime(*clients, FUNCTION))
    if encoded(before) != encoded(after):
        raise ValueError("Native producer changed during inspection")
    return before


def main():
    import boto3
    from ops_report import report
    with report("ops_6477_tv_bars_baseline") as output:
        if hashlib.sha256((ROOT / SOURCE).read_bytes()).hexdigest() != SOURCE_SHA256:
            raise ValueError("Reviewed market-history source changed")
        source_commit = subprocess.check_output(["git", "log", "-1", "--format=%H", "--", SOURCE], cwd=ROOT, text=True).strip()
        lam, s3, events, scheduler = [boto3.client(name, region_name="us-east-1") for name in ("lambda", "s3", "events", "scheduler")]
        native = capture((lam, ReceiptOnly(s3), events, scheduler))
        cfg = json.loads((ROOT / "aws/lambdas" / FUNCTION / "config.json").read_bytes())
        declared = {"function_name": cfg["function_name"], "runtime": cfg["runtime"], "handler": cfg["handler"],
                    "timeout": cfg["timeout"], "memory_mb": cfg["memory"], "role": cfg["role"]}
        output.kv(evidence={"status": "native_baseline_captured", "source_commit": source_commit,
          "reviewed_source_sha256": SOURCE_SHA256, "native": native,
          "declared_settings_match": {k: native[k] for k in declared} == declared,
          "observed_enabled_schedules": sum(row["state"] == "ENABLED" for row in native["schedules"]),
          "native_invocations": 0, "provider_requests": 0, "engine_packet_reads": 0,
          "private_reads": 0, "account_reads": 0, "credential_reads": 0,
          "native_writes": 0, "schedule_changes": 0, "application_log_queries": 0,
          "scope": "Exact named Lambda ZIP sources, receipt and selected controls only. No environment values, signed package URL, SSM parameters, provider session, history bank, index, account or log contents returned."})


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)
