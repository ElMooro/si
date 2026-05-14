#!/usr/bin/env python3
"""547 — Retire broken google-trends Lambda + re-verify /insider/ page."""
import io, json, os, urllib.request
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/547_retire_google_trends.json"

lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ─── 1. Retire google-trends Lambda + rule ──────────────────────────────
    target = "justhodl-google-trends"
    rules_to_delete = []
    try:
        resp = events.list_rules(NamePrefix="justhodl-google")
        for r in resp.get("Rules", []):
            try:
                targets = events.list_targets_by_rule(Rule=r["Name"])
                for t in targets.get("Targets", []):
                    if target in t.get("Arn", "") or "google-trends" in r["Name"]:
                        rules_to_delete.append({"rule": r["Name"], "target_id": t["Id"]})
            except Exception: pass
    except Exception as e:
        out["list_rules_err"] = str(e)[:200]
    out["rules_to_delete"] = rules_to_delete

    for r in rules_to_delete:
        try:
            events.remove_targets(Rule=r["rule"], Ids=[r["target_id"]])
            events.delete_rule(Name=r["rule"])
            r["status"] = "deleted"
        except Exception as e:
            r["err"] = str(e)[:120]

    try:
        lam.delete_function(FunctionName=target)
        out["lambda_delete"] = "OK"
    except lam.exceptions.ResourceNotFoundException:
        out["lambda_delete"] = "already_gone"
    except Exception as e:
        out["lambda_delete_err"] = str(e)[:200]

    # ─── 2. Delete the empty sidecar ────────────────────────────────────────
    try:
        s3.delete_object(Bucket="justhodl-dashboard-live", Key="data/google-trends.json")
        out["sidecar_delete"] = "OK"
    except Exception as e:
        out["sidecar_delete_err"] = str(e)[:200]

    # ─── 3. Re-verify /insider/ page (give GH Pages time) ───────────────────
    try:
        req = urllib.request.Request(
            "https://justhodl.ai/insider/",
            headers={"User-Agent": "JustHodl.AI ops/547",
                      "Cache-Control": "no-cache, no-store",
                      "Pragma": "no-cache"},
        )
        with urllib.request.urlopen(req, timeout=12) as r:
            html = r.read().decode("utf-8", "replace")
        out["insider_page"] = {
            "size": len(html),
            "loads_clusters_json": "insider-clusters.json" in html,
            "loads_old_transactions_json": "insider-transactions.json" in html,
            "title_clusters": "Insider Clusters" in html,
            "has_filter_bar": "filter-btn" in html,
            "has_signal_pills": "signal-pill" in html,
            "has_ceo_pill": "ceo_conviction" in html,
            "has_cluster_renderer": "renderCluster" in html,
            "has_methodology_v2": "insider_cluster_scanner_v2" in html.lower() or "Insider Cluster Scanner v2" in html,
        }
    except Exception as e:
        out["insider_page_err"] = str(e)[:200]

    # ─── 4. Final tally of live justhodl-* Lambdas ──────────────────────────
    try:
        paginator = lam.get_paginator("list_functions")
        names = []
        for page in paginator.paginate():
            for f in page.get("Functions", []):
                if f["FunctionName"].startswith("justhodl-"):
                    names.append(f["FunctionName"])
        names.sort()
        out["justhodl_lambda_inventory"] = {"count": len(names), "names": names}
    except Exception as e:
        out["lambda_inventory_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
