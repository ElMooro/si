"""ops 1099 — recon: what's the current state of all theme/rotation/sympathy Lambdas?

Khalid says cybersecurity ran 25% last month and asks if the system caught the
AI→cyber sympathy. This op reads every relevant Lambda's CURRENT output and
checks:
  - Was cybersecurity in the theme universe?
  - Did it get flagged as ACCELERATING / EXTENDED / EMERGING?
  - Did sympathetic-momentum flag cyber as a sympathy play to AI?
  - Did sector-rotation flag tech sector rotation?
  - What does narrative-density show for cyber?

Verdict: gap or surfacing problem?
"""
import json, os
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

s3 = boto3.client("s3", region_name=REGION)


def fetch(key):
    try:
        r = s3.get_object(Bucket=BUCKET, Key=key)
        return {
            "key": key,
            "size_kb": round(r["ContentLength"] / 1024, 1),
            "last_modified": r["LastModified"].isoformat(),
            "body": json.loads(r["Body"].read()),
        }
    except Exception as e:
        return {"key": key, "err": str(e)[:200]}


def hunt_cyber(obj, max_depth=4):
    """Recursive search for any 'cyber', 'security', 'AI', 'semiconductor' mentions."""
    hits = []
    def walk(node, path="", depth=0):
        if depth > max_depth:
            return
        if isinstance(node, str):
            low = node.lower()
            for kw in ("cyber", "security", "semiconductor", "semis", " ai ", "artificial", "infosec"):
                if kw.strip() in low:
                    hits.append({"path": path, "kw": kw.strip(), "context": node[:120]})
                    return
        elif isinstance(node, dict):
            for k, v in node.items():
                walk(v, f"{path}.{k}", depth + 1)
        elif isinstance(node, list):
            for i, item in enumerate(node[:30]):  # limit
                walk(item, f"{path}[{i}]", depth + 1)
    walk(obj)
    return hits


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    keys = [
        "data/themes-detected.json",
        "data/theme-rotation.json",
        "data/theme-tiers.json",
        "data/sympathetic-momentum.json",
        "data/narrative-density.json",
        "data/pre-pump-signals.json",
        "data/causality-discoveries.json",
        "data/sector-heatmap.json",
        "data/leading-markets.json",
        "data/master-ranker.json",
        "data/best-ideas.json",
        "data/signal-board.json",
    ]

    for k in keys:
        out = fetch(k)
        if "body" in out:
            body = out["body"]
            # Top-level summary
            if isinstance(body, dict):
                out["top_keys"] = list(body.keys())[:15]
                # Hunt for cyber/AI mentions
                out["cyber_mentions"] = hunt_cyber(body)[:8]
            elif isinstance(body, list):
                out["list_len"] = len(body)
                out["sample_first"] = json.dumps(body[0], default=str)[:400] if body else None
                out["cyber_mentions"] = hunt_cyber(body)[:8]
            del out["body"]  # too big for report
        report[k.split("/")[-1]] = out

    out_path = os.path.join(REPO_ROOT, "aws/ops/reports/1099.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:8000])


if __name__ == "__main__":
    main()
