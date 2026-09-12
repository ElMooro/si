"""Runner-only read: distinguish IMF lease writes from successful drain progress."""
import json
import os
from pathlib import Path
import re
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws/ops"))
from ops_report import report


def main():
    assert os.environ.get("GITHUB_ACTIONS") == "true", "Runner only"
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    logs = boto3.client("logs", region_name="us-east-1")
    cw = boto3.client("cloudwatch", region_name="us-east-1")
    now = datetime.now(timezone.utc)
    start = datetime(2026, 9, 12, 14, 32, tzinfo=timezone.utc)
    proof = {"at": now.isoformat(), "objects": {}, "invocations_sent": 0}

    def read(key):
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        proof["objects"][key] = {"last_modified": obj["LastModified"].isoformat(),
            "age_hours": round((now - obj["LastModified"]).total_seconds() / 3600, 4)}
        return json.loads(obj["Body"].read())

    with report("ops_5449_imf_progress_proof") as r:
        state_key = "data/warm/imf-full/_state/state.json"
        st = read(state_key)
        previous = json.loads((ROOT / "aws/ops/reports/5447_sunday_inventory.json").read_text())
        proof["imf"] = {"phase": st.get("phase"), "as_of": st.get("as_of"),
            "n_banked": st.get("n_banked"), "have_count": len(st.get("have", {})),
            "queue_left": len(st.get("queue", [])), "queued_flow_ids": [row[0] for row in st.get("queue", [])],
            "queue_attempts": [row[1] for row in st.get("queue", [])],
            "failure_count": len(st.get("failures", {})), "last_discover": st.get("last_discover"),
            "lease_until": datetime.fromtimestamp(st.get("lease_until") or 0, timezone.utc).isoformat()}
        proof["imf"]["lease_active"] = (st.get("lease_until") or 0) > now.timestamp()
        proof["progress_since_5447"] = {
            "last_modified_advanced": proof["objects"][state_key]["last_modified"] > previous["objects"][state_key]["last_modified"],
            "banked_delta": len(st.get("have", {})) - previous["imf_collection_counts"]["have"],
            "queue_delta": len(st.get("queue", [])) - previous["imf_collection_counts"]["queue"],
            "as_of_advanced": (st.get("as_of") or "") > (previous["imf"].get("as_of") or ""),
        }
        proof["execution_reports"] = []
        token = None
        for _ in range(10):
            args = {"logGroupName": "/aws/lambda/justhodl-imf-full", "startTime": int(start.timestamp() * 1000),
                    "filterPattern": '"REPORT RequestId:"', "limit": 100}
            if token: args["nextToken"] = token
            page = logs.filter_log_events(**args)
            for ev in page.get("events", []):
                msg = ev["message"]
                report_row = {"at": datetime.fromtimestamp(ev["timestamp"] / 1000, timezone.utc).isoformat()}
                for label in ("Duration", "Billed Duration", "Max Memory Used", "Status", "Error Type"):
                    m = re.search(r"(?:^|\t)" + re.escape(label) + r":\s*([^\t\n]+)", msg)
                    if m: report_row[label] = m.group(1).strip()
                proof["execution_reports"].append(report_row)
            if not page.get("nextToken") or page["nextToken"] == token: break
            token = page["nextToken"]
        proof["lambda_metrics"] = {}
        for metric in ("Invocations", "Errors", "Throttles"):
            d = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": "justhodl-imf-full"}],
                StartTime=start, EndTime=now, Period=60, Statistics=["Sum"])
            proof["lambda_metrics"][metric] = sum(p.get("Sum", 0) for p in d.get("Datapoints", []))
        progress = proof["progress_since_5447"]
        if st.get("phase") == "COMPLETE" and not st.get("queue"):
            proof["drain_status"] = "COMPLETE"
        elif progress["banked_delta"] > 0 or progress["queue_delta"] < 0:
            proof["drain_status"] = "PROGRESS_OBSERVED"
        else:
            proof["drain_status"] = "NO_DRAIN_PROGRESS_OBSERVED"
        proof["kick_threshold_exceeded"] = proof["objects"][state_key]["age_hours"] > 48
        h = read("data/import-health.json")
        proof["health"] = {"overall": h.get("overall"), "generated_at": h.get("generated_at"),
            "pipelines": [p for p in h.get("pipelines", []) if p.get("name") in ("fred", "dead-lanes")]}
        catalog = read("data/provider-catalog.json")
        fred = next(p for p in catalog["providers"] if p["slug"] == "fred")
        proof["fred_stored_series"] = fred.get("series_count")
        event = read("data/event-brief.json")
        proof["event"] = {"source": event.get("source"), "status": event.get("status"), "fields": event.get("fields"), "inputs": event.get("inputs")}
        # Read the known warehouse source. Report only field paths for explicitly
        # signed counters; do not build a score or register an adapter.
        source = read("data/finviz-signals.json")
        signed = []
        def walk(obj, path=""):
            if isinstance(obj, dict):
                for k,v in obj.items():
                    p = path + "." + k if path else k
                    low = k.lower()
                    if isinstance(v, (int, float)) and not isinstance(v, bool) and (
                        "signed" in low or low in ("net_count", "net_catalyst_count", "signed_count", "directional_count")):
                        signed.append(p)
                    walk(v, p)
            elif isinstance(obj, list):
                for i,v in enumerate(obj): walk(v, path + "[%d]" % i)
        walk(source)
        proof["signed_count_candidate_paths"] = signed[:50]
        if signed:
            r.warn("STOP CATALYST WORK: explicit signed-count candidate found in data/finviz-signals.json at " + ", ".join(signed[:10]))
        r.ok("IMF status=%s phase=%s banked=%s queue=%s lm=%s" % (proof["drain_status"], st.get("phase"), len(st.get("have", {})), len(st.get("queue", [])), proof["objects"][state_key]["last_modified"]))
        r.log("IMF execution reports=" + json.dumps(proof["execution_reports"]))
        r.log("No kick sent; state age %.3fh; additional-kick threshold >48h" % proof["objects"][state_key]["age_hours"])
        r.ok("Health=%s; FRED stored=%s; event source=%s confluence=%s" % (h.get("overall"), fred.get("series_count"), event.get("source"), event.get("fields", {}).get("confluence_n")))
        proof["inspection_status"] = "PASS"
        (ROOT / "aws/ops/reports/5449_imf_progress_proof.json").write_text(json.dumps(proof, indent=2) + "\n")


if __name__ == "__main__":
    main()
