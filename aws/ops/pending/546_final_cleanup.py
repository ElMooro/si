#!/usr/bin/env python3
"""546 — Final cleanup:
  1. Delete orphan justhodl-insider-transactions Lambda (no longer used)
  2. Delete any orphan EventBridge rules pointing to it
  3. Audit google-trends Lambda health (was thought 429-blocked but sidecar live)
  4. Verify /insider/index.html schema matches insider-clusters.json by fetching page from GH Pages
"""
import io, json, os, urllib.request
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/546_final_cleanup.json"

lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ─── 1. Retire orphan insider-transactions ──────────────────────────────
    target = "justhodl-insider-transactions"
    try:
        # Find any EventBridge rules with target = this lambda
        rules_to_delete = []
        for prefix in ["justhodl-insider", "justhodl-form4", "insider-trans"]:
            try:
                resp = events.list_rules(NamePrefix=prefix)
                for r in resp.get("Rules", []):
                    try:
                        targets = events.list_targets_by_rule(Rule=r["Name"])
                        for t in targets.get("Targets", []):
                            if target in t.get("Arn", ""):
                                rules_to_delete.append({"rule": r["Name"], "target_id": t["Id"]})
                    except Exception: pass
            except Exception: pass
        out["rules_to_delete"] = rules_to_delete
        for r in rules_to_delete:
            try:
                events.remove_targets(Rule=r["rule"], Ids=[r["target_id"]])
                events.delete_rule(Name=r["rule"])
            except Exception as e:
                r["err"] = str(e)[:120]
        # Delete the Lambda
        try:
            lam.delete_function(FunctionName=target)
            out["lambda_delete"] = "OK"
        except lam.exceptions.ResourceNotFoundException:
            out["lambda_delete"] = "already_gone"
        except Exception as e:
            out["lambda_delete_err"] = str(e)[:200]
    except Exception as e:
        out["cleanup_err"] = str(e)[:300]

    # ─── 2. Audit google-trends ─────────────────────────────────────────────
    try:
        l = lam.get_function(FunctionName="justhodl-google-trends")
        cfg = l["Configuration"]
        out["google_trends"] = {
            "exists": True,
            "memory": cfg.get("MemorySize"),
            "timeout": cfg.get("Timeout"),
            "state": cfg.get("State"),
            "last_modified": cfg.get("LastModified"),
        }
        # Find its schedule rule
        try:
            for prefix in ["justhodl-google", "google-trends"]:
                resp = events.list_rules(NamePrefix=prefix)
                rules_info = []
                for r in resp.get("Rules", []):
                    rules_info.append({
                        "name": r["Name"],
                        "schedule": r.get("ScheduleExpression"),
                        "state": r.get("State"),
                    })
                if rules_info:
                    out["google_trends"]["rules"] = rules_info
                    break
        except Exception: pass
    except Exception as e:
        out["google_trends"] = {"err": str(e)[:200]}

    # Sample google-trends sidecar to confirm real data
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/google-trends.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["google_trends_sidecar_sample"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "source": p.get("source"),
            "n_daily_trending": p.get("n_daily_trending"),
            "daily_trending_us_sample": (p.get("daily_trending_us") or [])[:5],
            "market_fear_index": p.get("market_fear_index"),
            "bull_bear_pulse": p.get("bull_bear_pulse"),
            "indices": p.get("indices"),
        }
    except Exception as e:
        out["google_trends_sidecar_err"] = str(e)[:200]

    # ─── 3. Verify /insider/ page (after deploy) ────────────────────────────
    # GH Pages will reflect the new page in ~30s after commit; try the fetch
    try:
        req = urllib.request.Request(
            "https://justhodl.ai/insider/",
            headers={"User-Agent": "JustHodl.AI ops/546"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", "replace")
        out["insider_page_check"] = {
            "size": len(html),
            "has_v2_url": "insider-clusters.json" in html,
            "has_v1_url": "insider-transactions.json" in html,
            "title_contains_clusters": "Insider Clusters" in html,
            "has_filter_bar": "filter-btn" in html,
            "has_signal_pills": "signal-pill" in html and "ceo_conviction" in html,
            "has_cluster_renderer": "renderCluster" in html,
        }
    except Exception as e:
        out["insider_page_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
