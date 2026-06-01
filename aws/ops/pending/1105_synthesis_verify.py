"""1105 — verify the cross-engine AI synthesis Lambda + widget data."""
import io, json, pathlib, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1105_synthesis_verify.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name="us-east-1")
ebs = boto3.client("scheduler", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # 1. Confirm Lambda exists
    fn = "justhodl-ai-website-synthesis"
    try:
        info = lam.get_function(FunctionName=fn)
        cfg = info["Configuration"]
        out["lambda"] = {
            "state":         cfg["State"],
            "last_modified": cfg["LastModified"],
            "code_size_kb":  round(cfg["CodeSize"]/1024, 1),
            "timeout":       cfg["Timeout"],
            "memory":        cfg["MemorySize"],
            "has_api_key":   bool(cfg.get("Environment",{}).get("Variables",{}).get("ANTHROPIC_API_KEY")),
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:200]
        # Was the Lambda created by the deploy workflow?
        if "ResourceNotFoundException" in str(e):
            out["needs_create"] = True
            # Try to create it via direct boto3
            print("[1105] Lambda not found; creating directly via boto3…")
            # Get config
            cfg_json = json.load(open(f"aws/lambdas/{fn}/config.json"))
            
            # Get API key from a known source
            try:
                src_info = lam.get_function_configuration(FunctionName="justhodl-ai-chat")
                api_key = src_info["Environment"]["Variables"]["ANTHROPIC_API_KEY"]
            except Exception as e2:
                out["api_key_err"] = str(e2)[:200]
                pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
                return
            
            # Build the zip from source/ + aws/shared/
            buf = io.BytesIO()
            seen = set()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                src_dir = pathlib.Path(f"aws/lambdas/{fn}/source")
                for f in sorted(src_dir.iterdir()):
                    if f.is_file():
                        zf.write(f, arcname=f.name); seen.add(f.name)
                sh_dir = pathlib.Path("aws/shared")
                for f in sorted(sh_dir.iterdir()):
                    if f.is_file() and f.suffix == ".py" and f.name not in seen:
                        zf.write(f, arcname=f.name); seen.add(f.name)
            
            resp = lam.create_function(
                FunctionName=fn,
                Runtime=cfg_json["runtime"],
                Role=cfg_json["role_arn"],
                Handler=cfg_json["handler"],
                Code={"ZipFile": buf.getvalue()},
                Timeout=cfg_json["timeout"],
                MemorySize=cfg_json["memory"],
                Description=cfg_json["description"],
                Environment={"Variables": {"ANTHROPIC_API_KEY": api_key}},
            )
            out["created"] = {"arn": resp["FunctionArn"], "state": resp["State"]}
            # Wait for ACTIVE
            for _ in range(30):
                info = lam.get_function(FunctionName=fn)
                if info["Configuration"]["State"] == "Active":
                    break
                time.sleep(1)
            
            # Set up EventBridge Scheduler
            sched_cfg = cfg_json["eventbridge_scheduler"]
            try:
                try:
                    ebs.delete_schedule(Name=sched_cfg["schedule_name"]); time.sleep(1)
                except ebs.exceptions.ResourceNotFoundException: pass
                ebs.create_schedule(
                    Name=sched_cfg["schedule_name"],
                    ScheduleExpression=sched_cfg["cron"],
                    ScheduleExpressionTimezone=sched_cfg["timezone"],
                    Description=sched_cfg.get("description", ""),
                    State="ENABLED",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={
                        "Arn":     resp["FunctionArn"],
                        "RoleArn": sched_cfg["role_arn"],
                        "Input":   json.dumps({"source": "scheduler"}),
                    },
                )
                out["schedule_created"] = sched_cfg["schedule_name"]
            except Exception as e3:
                out["schedule_err"] = str(e3)[:200]
            
            out["lambda"] = {"state": "Active", "newly_created": True}
    
    # 2. Invoke
    print(f"[1105] invoking {fn} (expect 30-60s for full 12-engine read + Claude)…")
    t0 = time.time()
    r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
    out["invoke_elapsed_s"] = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        out["invoke_status_code"] = p.get("statusCode")
        if isinstance(p.get("body"), str):
            try:
                out["invoke_summary"] = json.loads(p["body"])
            except Exception:
                out["body_preview"] = p["body"][:300]
    except Exception:
        out["raw"] = body[:500]
    
    # 3. Read the output
    time.sleep(2)
    print("[1105] reading data/ai-website-synthesis.json…")
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/ai-website-synthesis.json")
        d = json.loads(obj["Body"].read())
        out["output"] = {
            "size_kb":       round(obj["ContentLength"]/1024, 1),
            "last_modified": obj["LastModified"].isoformat(),
            "status":        d.get("status", "ok"),
            "model":         d.get("model"),
            "engines_loaded": d.get("engines_loaded"),
            "engines_total":  d.get("engines_total"),
            "claude_elapsed": d.get("claude_elapsed_sec"),
            "snapshot_age":  d.get("snapshot_age_min"),
        }
        if d.get("status") == "error":
            out["output"]["error"] = d.get("error")
        else:
            syn = d.get("synthesis", {})
            out["synthesis"] = {
                "global_posture":   syn.get("global_posture"),
                "headline":         syn.get("headline"),
                "thesis":           (syn.get("thesis") or "")[:400],
                "decisive_call":    (syn.get("decisive_call") or "")[:300],
                "key_drivers_count":     len(syn.get("key_drivers") or []),
                "key_drivers":           syn.get("key_drivers", [])[:5],
                "key_dissonances_count": len(syn.get("key_dissonances") or []),
                "key_dissonances":       syn.get("key_dissonances", [])[:3],
                "watch_list":            syn.get("watch_list", [])[:5],
                "per_page_focus_keys":   list((syn.get("per_page_focus") or {}).keys()),
                "per_page_focus_sample": (syn.get("per_page_focus") or {}).get("macro-frontrun"),
            }
            out["alert_info"] = d.get("alert_info")
        
        # Save the full output for reference
        with open("aws/ops/reports/1105_synthesis_full.json", "w") as f:
            json.dump(d, f, indent=2, default=str)
    except Exception as e:
        out["output_err"] = str(e)[:200]
    
    # 4. Verify the widget script + a sample page are deployed
    try:
        with urllib.request.urlopen("https://justhodl.ai/jh-ai-insights.js", timeout=15) as r:
            js = r.read().decode("utf-8")
        out["widget_js"] = {
            "size_kb":              round(len(js)/1024, 1),
            "has_JHInsights":       "window.JHInsights" in js,
            "has_DATA_URL":         "ai-website-synthesis.json" in js,
            "has_panel_renderer":   "buildPanel" in js,
        }
    except Exception as e:
        out["widget_js_err"] = str(e)[:150]
    
    try:
        with urllib.request.urlopen("https://justhodl.ai/macro-frontrun.html", timeout=15) as r:
            html = r.read().decode("utf-8")
        out["macro_page"] = {
            "size_kb":            round(len(html)/1024, 1),
            "has_widget_script":  "jh-ai-insights.js" in html,
            "has_macro_kit":      "ai-macro-frontrun-kit.js" in html,
        }
    except Exception as e:
        out["macro_page_err"] = str(e)[:150]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1105] DONE")


if __name__ == "__main__":
    main()
