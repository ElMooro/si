"""Runner-only Sunday inventory. No vendor calls or schedule mutations."""
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import urllib.request
import zipfile
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws/ops"))
from ops_report import report

B = "justhodl-dashboard-live"
NAMES = ["brief-compiler-plumbing-6h", "brief-compiler-market-tape-6h",
         "brief-compiler-event-6h", "brief-compiler-official-stats-daily",
         "brief-compiler-positioning-daily", "brief-compiler-verdict-3h"]


def main():
    assert os.environ.get("GITHUB_ACTIONS") == "true", "Runner only"
    import boto3
    from botocore.config import Config
    s3 = boto3.client("s3", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=330))
    events = boto3.client("events", region_name="us-east-1")
    scheduler = boto3.client("scheduler", region_name="us-east-1")
    proof = {"at": datetime.now(timezone.utc).isoformat(), "objects": {}, "functions": {}}

    def read(key):
        obj = s3.get_object(Bucket=B, Key=key)
        raw = obj["Body"].read()
        doc = json.loads(gzip.decompress(raw) if key.endswith(".gz") else raw)
        proof["objects"][key] = {"last_modified": obj["LastModified"].isoformat()}
        return doc

    with report("ops_5447_sunday_inventory") as r:
        h = read("data/import-health.json")
        proof["health"] = h
        r.ok("health=%s lanes=%s" % (h.get("overall"), json.dumps([
            {k:p.get(k) for k in ("name", "status", "detail", "age_min", "imported")}
            for p in h.get("pipelines", []) if p.get("name") in ("fred", "dead-lanes")])))
        assert next(p for p in h["pipelines"] if p["name"] == "fred")["status"] == "COMPLETE"
        assert next(p for p in h["pipelines"] if p["name"] == "dead-lanes")["status"] == "OK"
        imf = read("data/warm/imf-full/_state/state.json")
        proof["imf"] = {k:v for k,v in imf.items() if not isinstance(v, (dict, list))}
        proof["imf_collection_counts"] = {k:len(v) for k,v in imf.items() if isinstance(v, (dict, list))}
        catalog = read("data/provider-catalog.json")
        proof["fred_catalog"] = next(p for p in catalog["providers"] if p["slug"] == "fred")
        man = read("config/schedule-manifest.json")
        proof["compiler_manifest"] = [x for k in ("rules", "schedules") for x in man.get(k, []) if x.get("name") in NAMES]
        proof["manifest_counts"] = {k:len(man.get(k, [])) for k in ("rules", "schedules")}
        inv = lam.invoke(FunctionName="justhodl-schedule-reconciler", Payload=b'{"mode":"audit"}')
        assert not inv.get("FunctionError"), "Reconciler audit failed"
        proof["audit_invoke"] = json.loads(inv["Payload"].read())
        proof["drift"] = read("data/schedule-drift.json")
        r.ok("drift=%s split=%s" % (proof["drift"].get("drift_count"), proof["drift"].get("by_class")))
        proof["classic_rules"] = []
        proof["scheduler_schedules"] = []
        for name in NAMES:
            try:
                d = events.describe_rule(Name=name)
                targets = events.list_targets_by_rule(Rule=name)["Targets"]
                proof["classic_rules"].append({"name": name, "state": d["State"], "expr": d.get("ScheduleExpression"), "targets": targets})
            except events.exceptions.ResourceNotFoundException:
                proof["classic_rules"].append({"name": name, "missing": True})
            try:
                d = scheduler.get_schedule(Name=name, GroupName="default")
                proof["scheduler_schedules"].append({k:d.get(k) for k in ("Name", "State", "ScheduleExpression", "ScheduleExpressionTimezone", "Target")})
            except scheduler.exceptions.ResourceNotFoundException:
                proof["scheduler_schedules"].append({"Name": name, "missing": True})
        for fn in ("justhodl-brief-compiler", "justhodl-schedule-reconciler", "justhodl-imf-full"):
            full = lam.get_function(FunctionName=fn)
            c = full["Configuration"]
            item = {k:c.get(k) for k in ("FunctionName", "Role", "Runtime", "Handler", "Timeout", "MemorySize", "LastModified", "CodeSha256", "State", "LastUpdateStatus")}
            if fn != "justhodl-imf-full":
                with urllib.request.urlopen(full["Code"]["Location"], timeout=60) as res:
                    z = zipfile.ZipFile(io.BytesIO(res.read()))
                members = ("lambda_function.py", "brief_compiler.py", "brief_contract.py") if fn.endswith("brief-compiler") else ("lambda_function.py",)
                item["package"] = {}
                for name in members:
                    raw = z.read(name)
                    local = ROOT / ("aws/lambdas/" + fn + "/source/" + name)
                    if not local.exists(): local = ROOT / "aws/shared" / name
                    item["package"][name] = {"sha256": hashlib.sha256(raw).hexdigest(), "matches_checkout": local.exists() and raw == local.read_bytes()}
                if fn.endswith("brief-compiler"):
                    item["by_ticker_parse"] = b"by_ticker" in z.read("brief_compiler.py")
                    assert item["by_ticker_parse"]
            proof["functions"][fn] = item
        for key in ("data/positioning-brief.json", "data/event-brief.json", "data/verdict.json"):
            proof[key] = read(key)
        # Only inspect known event provenance; do not manufacture an adapter.
        signal = read("data/finviz-signals.json")
        proof["event_source_keys"] = sorted(signal)
        proof["event_direction_candidates"] = {k:v for k,v in signal.items() if any(t in k.lower() for t in ("signed", "bull", "bear", "net_", "direction")) and isinstance(v, (int, float, str))}
        proof["ofr_objects"] = []
        for key in ("data/warm/ofr/dataset-repo.json.gz", "data/warm/ofr/dataset-nypd.json.gz", "data/warm/ofr/dataset-mmf.json.gz", "data/warm/ofr/series/REPO-TRI_AR_TOT-P.json.gz"):
            o = s3.head_object(Bucket=B, Key=key)
            proof["ofr_objects"].append({"key": key, "last_modified": o["LastModified"].isoformat(), "bytes": o["ContentLength"]})
        out = ROOT / "aws/ops/reports/5447_sunday_inventory.json"
        out.write_text(json.dumps(proof, indent=2, default=str) + "\n")
        r.ok("Inventory saved; no schedule or importer changes")


if __name__ == "__main__":
    main()
