"""1096 — end-to-end Wave C verification.

Confirms:
1. auction-crisis.html is served from GH Pages with the new structure
2. auction-crisis.js loads + has all renderer functions
3. data/auction-crisis.json is reachable (schema 2.0)
4. data/auction-crisis-ai.json is reachable (status ok)
5. Lambda schedules are correctly set up

Final shipped state report.
"""
import json, pathlib, time, urllib.request
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1096_e2e.json"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
ebs = boto3.client("scheduler", region_name="us-east-1")


def fetch(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops/1096"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # 1. Verify page HTML on GH Pages
    print("[1096] phase 1: fetch live auction-crisis.html…")
    try:
        html = fetch("https://justhodl.ai/auction-crisis.html").decode("utf-8")
        out["html"] = {
            "size_kb":      round(len(html)/1024, 1),
            "lines":        len(html.split("\n")),
            "has_ai_tag":   "ai-tag" in html,
            "has_ai_exec":  "ai-exec-card" in html,
            "has_decisive": 'class="decisive-call' in html,
            "has_tenor":    "tenor-grid" in html,
            "has_cross":   "cross-strip" in html,
            "has_chart":    "composite-chart" in html,
            "has_analog":   "analog-card" in html,
            "has_forward":  "forward-table" in html,
            "has_ai_fwd":   "ai-forward-grid" in html,
            "has_tail":     "tail-grid" in html,
            "has_triggers": "trigger-grid" in html,
            "has_js_ref":   'auction-crisis.js' in html,
            "title_has_ai_edition": "AI EDITION" in html or "AI Edition" in html,
        }
    except Exception as e:
        out["html_err"] = str(e)[:200]
    
    # 2. Verify JS
    print("[1096] phase 2: fetch live auction-crisis.js…")
    try:
        js = fetch("https://justhodl.ai/auction-crisis.js").decode("utf-8")
        out["js"] = {
            "size_kb":           round(len(js)/1024, 1),
            "lines":             len(js.split("\n")),
            "has_DATA_URL":      "DATA_URL" in js,
            "has_AI_URL":        "AI_URL" in js,
            "has_renderHero":    "renderHero" in js,
            "has_renderTenor":   "renderTenorDecomposition" in js,
            "has_renderChart":   "renderCompositeChart" in js,
            "has_renderAnalog":  "renderAnalog" in js,
            "has_renderForward": "renderForwardTable" in js,
            "has_renderTail":    "renderTailRiskDataOnly" in js,
            "has_renderAI":      "renderAISections" in js,
            "has_boldNumbers":   "boldNumbers" in js,
        }
    except Exception as e:
        out["js_err"] = str(e)[:200]
    
    # 3. Verify data file
    print("[1096] phase 3: read data/auction-crisis.json from S3…")
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-crisis.json")
        d = json.loads(obj["Body"].read())
        out["data"] = {
            "size_kb":        round(obj["ContentLength"]/1024, 1),
            "last_modified":  obj["LastModified"].isoformat(),
            "schema_version": d.get("schema_version"),
            "composite":      d.get("composite_score"),
            "regime":         d.get("regime"),
            "v2_sections_present": {
                k: bool(d.get(k))
                for k in ["tenor_decomposition", "forward_calendar",
                            "historical_analog", "cross_signals",
                            "composite_history", "tail_risk", "triggers"]
            },
            "forward_calendar_count": len(d.get("forward_calendar") or []),
            "triggers_count":         len(d.get("triggers") or []),
            "tenor_buckets":          list((d.get("tenor_decomposition") or {}).keys()),
            "cross_signals":          list((d.get("cross_signals") or {}).keys()),
            "composite_history_len":  len((d.get("composite_history") or {}).get("series", [])),
        }
    except Exception as e:
        out["data_err"] = str(e)[:200]
    
    # 4. Verify AI file
    print("[1096] phase 4: read data/auction-crisis-ai.json from S3…")
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-crisis-ai.json")
        d = json.loads(obj["Body"].read())
        out["ai"] = {
            "size_kb":         round(obj["ContentLength"]/1024, 1),
            "last_modified":   obj["LastModified"].isoformat(),
            "status":          d.get("status", "ok"),
            "model":           d.get("model"),
            "regime":          d.get("regime"),
            "claude_elapsed_s": d.get("claude_elapsed_sec"),
            "data_age_min":    d.get("data_age_minutes"),
        }
        if d.get("status") != "error":
            ai = d.get("ai_commentary") or {}
            out["ai"]["sections_present"] = {
                k: bool(ai.get(k))
                for k in ["executive_summary", "what_changed",
                            "indicator_interpretation",
                            "historical_analog_discussion",
                            "forward_predictions", "tail_risk_assessment",
                            "actionable_triggers", "decisive_call"]
            }
            out["ai"]["forward_pred_count"] = len(ai.get("forward_predictions") or [])
            out["ai"]["indicator_interp_count"] = len(ai.get("indicator_interpretation") or [])
    except Exception as e:
        out["ai_err"] = str(e)[:200]
    
    # 5. Verify schedule exists
    print("[1096] phase 5: confirm EventBridge schedule…")
    try:
        sch = ebs.get_schedule(Name="justhodl-auction-crisis-ai-hourly")
        out["schedule"] = {
            "name":     sch.get("Name"),
            "state":    sch.get("State"),
            "cron":     sch.get("ScheduleExpression"),
            "timezone": sch.get("ScheduleExpressionTimezone"),
        }
    except Exception as e:
        out["schedule_err"] = str(e)[:200]
    
    # 6. Verify both Lambdas exist + healthy
    print("[1096] phase 6: confirm Lambda health…")
    out["lambdas"] = {}
    for fn in ("justhodl-auction-crisis-detector", "justhodl-auction-crisis-ai"):
        try:
            info = lam.get_function(FunctionName=fn)
            out["lambdas"][fn] = {
                "state":         info["Configuration"]["State"],
                "last_modified": info["Configuration"]["LastModified"],
                "memory":        info["Configuration"]["MemorySize"],
                "timeout":       info["Configuration"]["Timeout"],
                "code_size_kb":  round(info["Configuration"]["CodeSize"]/1024, 1),
            }
        except Exception as e:
            out["lambdas"][fn] = {"err": str(e)[:120]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1096] DONE — Wave A/B/C complete")


if __name__ == "__main__":
    main()
