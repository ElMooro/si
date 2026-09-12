"""Runner-only warehouse search and undeclared-schedule inventory. No schedule writes."""
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
import gzip
import json
import math
import os
from pathlib import Path
import re
import sys
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws/ops"))
from ops_report import report
B = "justhodl-dashboard-live"


def function_name(arn):
    return arn.split(":function:", 1)[1].split(":")[0] if ":lambda:" in arn and ":function:" in arn else None


def timestamp(value):
    try:
        d = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d
    except (ValueError, TypeError):
        return None


def inspect_fields(doc):
    """Find an existing signed event/catalyst field; never subtract two counts.

    Raw positive-only scores, returns, countdowns and news sentiment scores are
    recorded as near misses, not silently promoted to signed event counts.
    """
    native = []
    near = Counter()
    numeric_paths = set()
    def walk(obj, path=""):
        if isinstance(obj, dict):
            for key, value in obj.items():
                p = path + "." + key if path else key
                low = key.lower()
                if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
                    numeric_paths.add(".".join("*" if re.fullmatch(r"[A-Z0-9.-]{1,10}", part) else part for part in p.split(".")))
                    explicit = ("signed" in low and any(w in low for w in ("count", "event", "catalyst"))) or low in (
                        "net_count", "net_events", "net_event_count", "net_catalysts", "net_catalyst_count",
                        "event_balance", "catalyst_balance", "signed_score", "signed_signal") or (
                        low in ("net", "balance", "signed") and any(w in path.lower() for w in ("count", "event", "catalyst")))
                    if explicit:
                        native.append({"field": p, "value": value})
                    elif any(w in low for w in ("bull", "bear", "positive", "negative")) and any(w in low for w in ("count", "_n", "n_")):
                        near["separate_direction_counts"] += 1
                    elif low in ("sentimentscore", "sentiment_score", "overall_sentiment"):
                        near["news_or_earnings_sentiment_score_not_event_count"] += 1
                    elif "score" in low or "strength" in low or "weight" in low:
                        near["score_or_strength_without_signed_count_contract"] += 1
                walk(value, p)
        elif isinstance(obj, list):
            for value in obj: walk(value, path + "[]")
    walk(doc)
    return native, dict(near), sorted(numeric_paths)


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        sys.exit(1)
    import boto3
    from botocore.exceptions import ClientError
    from botocore.config import Config
    cfg = Config(retries={"max_attempts": 8, "mode": "adaptive"})
    s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
    ev = boto3.client("events", region_name="us-east-1", config=cfg)
    sch = boto3.client("scheduler", region_name="us-east-1", config=cfg)
    cw = boto3.client("cloudwatch", region_name="us-east-1", config=cfg)
    now = datetime.now(timezone.utc)
    proof = {"op": 5451, "at": now.isoformat(), "mutations": [], "objects": {}, "catalyst_search": []}
    out = ROOT / "aws/ops/reports/5451_catalyst_drift_inventory.json"

    def read(key):
        obj = s3.get_object(Bucket=B, Key=key)
        raw = obj["Body"].read()
        doc = json.loads(gzip.decompress(raw) if key.endswith(".gz") else raw)
        proof["objects"][key] = {"last_modified": obj["LastModified"].isoformat(), "bytes": obj["ContentLength"]}
        return doc

    with report("ops_5451_catalyst_and_drift_inventory") as r:
        try:
            imf = json.loads((ROOT / "aws/ops/reports/5450_imf_queue_result.json").read_text())
            if imf.get("status") != "PASS_BANKED_OR_NAMED_FAILURE":
                raise RuntimeError("IMF accounting gate must pass first")
            verdict = read("data/verdict.json")
            if verdict.get("shadow_mode") is not True:
                raise RuntimeError("Shadow gate is not true")
            proof["shadow_mode"] = True
            drift = read("data/schedule-drift.json")
            entries = [d for d in drift["drifts"] if d["drift"] == "UNDECLARED"]
            if len({d["key"] for d in entries}) != len(entries):
                raise RuntimeError("Duplicate undeclared drift keys")
            if len(entries) != drift.get("by_class", {}).get("UNDECLARED"):
                raise RuntimeError("Stored drift rows do not contain the complete undeclared inventory")
            proof["drift_snapshot"] = {k:drift.get(k) for k in ("generated_at", "drift_count", "by_class")}
            live = {}
            rules = [row for page in ev.get_paginator("list_rules").paginate() for row in page["Rules"] if row.get("ScheduleExpression")]
            def rule_row(row):
                targets = [t["Arn"] for page in ev.get_paginator("list_targets_by_rule").paginate(Rule=row["Name"]) for t in page["Targets"]]
                return row["Name"], {"name": row["Name"], "kind": "events", "group": "",
                    "state": row["State"], "expression": row["ScheduleExpression"], "target_arns": sorted(targets)}
            with ThreadPoolExecutor(max_workers=4) as pool:
                for key, row in pool.map(rule_row, rules): live[key] = row
            for page in sch.get_paginator("list_schedules").paginate():
                for row in page["Schedules"]:
                    group = row.get("GroupName", "default")
                    key = group + "/" + row["Name"]
                    target = row.get("Target", {}).get("Arn")
                    detail = None
                    if not target or key in {d["key"] for d in entries}:
                        detail = sch.get_schedule(Name=row["Name"], GroupName=group)
                        target = detail["Target"]["Arn"]
                    live[key] = {"name": row["Name"], "kind": "scheduler", "group": group,
                        "state": row["State"], "expression": detail.get("ScheduleExpression") if detail else None,
                        "target_arns": [target]}
            bindings = defaultdict(list)
            for key, row in live.items():
                if row["state"] == "ENABLED":
                    for arn in row["target_arns"]:
                        if function_name(arn): bindings[function_name(arn)].append({"schedule": key, "target_arn": arn})

            # The repository binds producer names to warehouse output keys.
            # Live enabled schedules are required before treating one as a candidate.
            manifest = json.loads((ROOT / "engine-manifest.json").read_text())
            selected = re.compile(r"catalyst|event-study|sec-8k|news-sentiment|earnings-sentiment|event-flow")
            candidates = []
            for engine in manifest["engines"]:
                fn = engine["engine"]
                if selected.search(fn):
                    for key in engine.get("keys", []):
                        if key.endswith((".json", ".json.gz")) and "alert-history" not in key:
                            candidates.append((fn, key))
            proof["search_scope"] = {"producer_pattern": selected.pattern, "candidate_outputs": len(candidates),
                "freshness_rule": "document as_of/generated_at age between 0 and 48 hours; source stale/unavailable flags must not be true",
                "signed_count_required": True, "no_count_subtraction_or_adapter_registration": True}
            found = None
            for fn, key in sorted(set(candidates)):
                item = {"engine": fn, "key": key, "enabled_schedules": bindings.get(fn, [])}
                if not bindings.get(fn):
                    item["result"] = "NO_ENABLED_SCHEDULE"
                    proof["catalyst_search"].append(item)
                    continue
                try:
                    doc = read(key)
                except ClientError as exc:
                    if exc.response["Error"]["Code"] not in ("NoSuchKey", "404"): raise
                    item["result"] = "KEY_MISSING"
                    proof["catalyst_search"].append(item)
                    continue
                as_of = doc.get("as_of") or doc.get("generated_at") or doc.get("updated_at")
                parsed = timestamp(as_of)
                age = (now-parsed).total_seconds()/3600 if parsed else None
                fresh = age is not None and 0 <= age <= 48 and doc.get("stale") is not True and doc.get("data_unavailable") is not True and not doc.get("error") and doc.get("status") not in ("ERROR", "HELD", "UNAVAILABLE") and str(doc.get("freshness", "")).upper() not in ("STALE", "EXPIRED", "UNAVAILABLE")
                native, near, fields = inspect_fields(doc)
                item.update(as_of=as_of, age_hours=age, fresh=fresh, source_freshness=doc.get("freshness"),
                            native_signed_candidates=native[:25], near_misses=near, numeric_field_schema=fields,
                            result="FOUND" if native and fresh else "STALE_OR_UNDATED" if native else "NO_NATIVE_SIGNED_COUNT")
                proof["catalyst_search"].append(item)
                if item["result"] == "FOUND":
                    found = {"key": key, "engine": fn, "as_of": as_of, "age_hours": age,
                             "freshness": "FRESH_WITHIN_48H", "fields": native[:25], "enabled_schedules": bindings[fn]}
                    r.ok("STOP CATALYST SEARCH: existing signed field in " + key)
                    break
            proof["catalyst_result"] = found or "NONE"
            r.ok("CATALYST result=" + json.dumps(proof["catalyst_result"]))

            inventory = []
            for d in sorted(entries, key=lambda d:d["key"]):
                row = dict(live.get(d["key"], {"name": d["key"].split("/")[-1], "kind": "not_found_since_snapshot",
                    "group": "", "state": "NOT_FOUND", "expression": None, "target_arns": []}))
                row["drift_key"] = d["key"]
                inventory.append(row)
            functions = sorted({function_name(a) for row in inventory for a in row["target_arns"] if function_name(a)})
            queries = [{"Id": "m%d" % i, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations",
                "Dimensions": [{"Name": "FunctionName", "Value": fn}]}, "Period": 300, "Stat": "Sum"}, "ReturnData": True} for i,fn in enumerate(functions)]
            metric_results = {}
            if queries:
                token = None
                for _ in range(10):
                    args = {"MetricDataQueries": queries, "StartTime": now-timedelta(hours=48), "EndTime": now,
                            "ScanBy": "TimestampDescending", "MaxDatapoints": 100800}
                    if token: args["NextToken"] = token
                    resp = cw.get_metric_data(**args)
                    for metric in resp.get("MetricDataResults", []):
                        if metric.get("StatusCode") not in ("Complete", "PartialData"):
                            raise RuntimeError("CloudWatch invocation metric query failed")
                        i = int(metric["Id"][1:])
                        ticks = [t for t,v in zip(metric.get("Timestamps", []),metric.get("Values", [])) if v > 0]
                        if ticks:
                            prev = metric_results.get(functions[i])
                            metric_results[functions[i]] = max(ticks+[prev] if prev else ticks)
                    if not resp.get("NextToken"): break
                    token = resp["NextToken"]
                else: raise RuntimeError("CloudWatch pagination limit reached")
            for row in inventory:
                row["last_invocations"] = []
                for arn in row["target_arns"]:
                    fn = function_name(arn)
                    dt = metric_results.get(fn)
                    row["last_invocations"].append({"target_arn": arn, "function": fn,
                        "last_invocation_bucket_utc": dt.isoformat() if dt else None,
                        "age_h": round((now-dt).total_seconds()/3600,3) if dt else None,
                        "basis": "AWS/Lambda Invocations, 300s bucket; function-wide, not per schedule" if dt else "no invocation observed in 48h" if fn else "not a Lambda target"})
            proof["undeclared_inventory"] = inventory
            proof["inventory_count"] = len(inventory)
            proof["by_kind"] = dict(Counter(x["kind"] for x in inventory))
            proof["by_state"] = dict(Counter(x["state"] for x in inventory))
            csv_path = ROOT / "aws/ops/reports/5451_undeclared_schedules.csv"
            columns = ["drift_key", "name", "kind", "group", "state", "expression", "target_arns", "target_invocation_ages_h", "invocation_basis"]
            with csv_path.open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=columns); writer.writeheader()
                for row in inventory:
                    writer.writerow({**{k:row.get(k) for k in columns[:6]}, "target_arns": " | ".join(row["target_arns"]),
                        "target_invocation_ages_h": " | ".join(str(x["age_h"]) if x["age_h"] is not None else x["basis"] for x in row["last_invocations"]),
                        "invocation_basis": "function-wide AWS/Lambda Invocations; 300s buckets; 48h window"})
            proof["csv_path"] = str(csv_path.relative_to(ROOT))
            observed = sum(1 for fn in functions if fn in metric_results)
            overview = [
                "# Undeclared schedules — one-page breakdown", "",
                "Op **5451** · inspected **%s** · inventory only; **zero schedule mutations**." % now.isoformat(), "",
                "The existing drift object was last modified **%s** and contains **%s** total findings. This report covers all **%s undeclared schedule names**; the remaining drift classes are outside this inventory." % (proof["objects"]["data/schedule-drift.json"]["last_modified"], drift["drift_count"], len(inventory)), "",
                "| Breakdown | Count |", "| --- | ---: |",
                *["| Transport: %s | %s |" % (k,v) for k,v in sorted(proof["by_kind"].items())],
                *["| State: %s | %s |" % (k,v) for k,v in sorted(proof["by_state"].items())],
                "| Distinct Lambda targets | %s |" % len(functions),
                "| Lambda targets with an observed invocation in 48h | %s |" % observed, "",
                "**Complete name + target ARN + invocation-age inventory:** [5451_undeclared_schedules.csv](5451_undeclared_schedules.csv). Every undeclared name has one CSV row; multiple target ARNs remain listed in that row. [Structured evidence](5451_catalyst_drift_inventory.json) preserves each target's timestamp and measurement basis.", "",
                "Invocation ages use the latest nonzero **AWS/Lambda Invocations** five-minute bucket in the preceding 48 hours. They are **function-wide**, including other triggers, not evidence that this particular schedule fired. No observation means no metric in that window, not that a function has never run. Non-Lambda targets are labeled separately.", "",
                "**Plan:**", "",
                "1. Match each listed name and target to its producer's current output contract and intended owner. Undeclared does not by itself mean broken or obsolete.",
                "2. Review enabled entries and shared targets for intended payloads, cadence and duplicate triggers; keep input bodies private and preserve current wiring during review.",
                "3. Propose exact manifest additions or retirement candidates with output evidence and rollback. No disable, delete, creation, reattachment or enforcement is authorized by this inventory.", "",
                "No CATALYST adapter or score was written. CATALYST warehouse search evidence appears in the structured report. The six verified compiler schedules and the other residual drift classes were left alone.",
            ]
            (ROOT / "aws/ops/reports/5451_undeclared_schedule_summary.md").write_text("\n".join(overview)+"\n")
            imf_state = read("data/warm/imf-full/_state/state.json")
            proof["imf_final"] = {"phase": imf_state.get("phase"), "banked": len(imf_state.get("have", {})), "queued": len(imf_state.get("queue", []))}
            catalog = read("data/provider-catalog.json")
            proof["imf_card"] = next((p for p in catalog.get("providers", []) if p.get("slug") == "imf"), None)
            health = read("data/import-health.json")
            proof["health"] = {"overall": health.get("overall"), "generated_at": health.get("generated_at")}
            proof["status"] = "PASS"
            r.ok("Inventory=%s undeclared; kinds=%s states=%s; zero schedule mutations" % (len(inventory), proof["by_kind"], proof["by_state"]))
        except Exception as exc:
            proof["status"] = "FAIL"
            proof["error"] = {"type": type(exc).__name__, "message": str(exc)[:350]}
            r.fail(json.dumps(proof["error"]))
            out.write_text(json.dumps(proof, indent=2, default=str)+"\n")
            sys.exit(1)
        out.write_text(json.dumps(proof, indent=2, default=str)+"\n")


if __name__ == "__main__":
    main()
