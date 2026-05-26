"""ops 1100 — does causality-scanner find theme-to-theme lead-lag (AI→cyber type)?"""
import json, os, boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
s3 = boto3.client("s3", region_name=REGION)

report = {}

# 1. Causality discoveries
try:
    r = s3.get_object(Bucket=BUCKET, Key="data/causality-discoveries.json")
    d = json.loads(r["Body"].read())
    report["causality"] = {
        "last_modified": r["LastModified"].isoformat(),
        "size_kb": round(r["ContentLength"]/1024, 1),
        "top_keys": list(d.keys()) if isinstance(d, dict) else f"list len {len(d)}",
        "first_5_pairs": d.get("top_pairs", [])[:5] if isinstance(d, dict) else d[:5],
    }
except Exception as e:
    report["causality"] = {"err": str(e)[:200]}

# 2. sector-heatmap (looking for current rotation state)
try:
    r = s3.get_object(Bucket=BUCKET, Key="data/sector-heatmap.json")
    d = json.loads(r["Body"].read())
    report["sector_heatmap"] = {
        "last_modified": r["LastModified"].isoformat(),
        "size_kb": round(r["ContentLength"]/1024, 1),
        "top_keys": list(d.keys()) if isinstance(d, dict) else "list",
    }
    # Show top movers
    if isinstance(d, dict):
        for k in ("rotating_in", "rotating_out", "top_sectors", "rankings"):
            if k in d:
                report["sector_heatmap"][f"sample_{k}"] = str(d[k])[:400]
except Exception as e:
    report["sector_heatmap"] = {"err": str(e)[:120]}

# 3. theme-rotation full structure
try:
    r = s3.get_object(Bucket=BUCKET, Key="data/theme-rotation.json")
    d = json.loads(r["Body"].read())
    # Pull top_10 + breadth + leaders
    summary = d.get("summary", {})
    report["theme_rotation_top10"] = []
    for t in (summary.get("top_10_momentum") or [])[:10]:
        report["theme_rotation_top10"].append({
            "rank": (summary.get("top_10_momentum") or []).index(t) + 1,
            "name": t.get("name"),
            "category": t.get("category"),
            "momentum_4w": t.get("momentum_4w") or t.get("mom_20d") or t.get("ret_20d") or "?",
            "phase": t.get("phase") or t.get("lifecycle"),
            "etf": t.get("etf"),
        })
    report["theme_rotation_breadth"] = summary.get("breadth_summary") or summary.get("breadth")
    report["theme_rotation_leaders_count"] = len(d.get("all_themes", []))
except Exception as e:
    report["theme_rotation"] = {"err": str(e)[:200]}

# 4. Check signal-board for theme synthesis
try:
    r = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")
    d = json.loads(r["Body"].read())
    report["signal_board"] = {
        "last_modified": r["LastModified"].isoformat(),
        "top_keys": list(d.keys())[:10] if isinstance(d, dict) else "list",
        "has_theme_section": any("theme" in str(k).lower() for k in (d.keys() if isinstance(d, dict) else [])),
    }
except Exception as e:
    report["signal_board"] = {"err": str(e)[:120]}

# 5. Daily morning brief — does it cover themes?
try:
    r = s3.get_object(Bucket=BUCKET, Key="data/morning-intelligence.json")
    d = json.loads(r["Body"].read())
    body_text = json.dumps(d, default=str)
    report["morning_brief"] = {
        "last_modified": r["LastModified"].isoformat(),
        "size_kb": round(r["ContentLength"]/1024, 1),
        "mentions_theme": body_text.lower().count("theme"),
        "mentions_cyber": body_text.lower().count("cyber"),
        "mentions_ai_semi": body_text.lower().count("semi") + body_text.lower().count("nvda"),
        "mentions_rotation": body_text.lower().count("rotation"),
    }
except Exception as e:
    report["morning_brief"] = {"err": str(e)[:120]}

# 6. Recent Telegram messages from sector-rotation / theme Lambdas
try:
    fns_with_tg = []
    import re
    for fn in os.listdir(os.path.join(REPO_ROOT, "aws/lambdas")):
        p = os.path.join(REPO_ROOT, "aws/lambdas", fn, "source/lambda_function.py")
        if os.path.isfile(p):
            with open(p) as f:
                content = f.read()
            if ("theme" in fn.lower() or "rotation" in fn.lower() or "sympath" in fn.lower()) and "send_telegram" in content:
                fns_with_tg.append(fn)
    report["theme_lambdas_with_telegram_alerts"] = fns_with_tg
except Exception as e:
    report["telegram_check"] = {"err": str(e)[:120]}

out = os.path.join(REPO_ROOT, "aws/ops/reports/1100.json")
os.makedirs(os.path.dirname(out), exist_ok=True)
with open(out, "w") as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str)[:6000])
