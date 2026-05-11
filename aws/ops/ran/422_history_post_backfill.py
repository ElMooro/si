#!/usr/bin/env python3
"""Step 422 — Final verify of Stage 8 with full backfilled history.
Confirms history.json has 30 days, real trend metrics, populated
top_5_rising/falling, and page renders correctly."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/422_history_post_backfill.json"
NAME = "justhodl-tmp-422-final"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, time
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. history.json overview
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/history.json")
        body = obj["Body"].read()
        h = json.loads(body)
        bs = h.get("by_symbol", {})
        # Trend distribution by counting from by_symbol
        trends = {}
        chg7d_pop = chg30d_pop = slope_pop = 0
        for sym, d in bs.items():
            t = d.get("score_trend") or "unknown"
            trends[t] = trends.get(t, 0) + 1
            if d.get("score_7d_chg") is not None: chg7d_pop += 1
            if d.get("score_30d_chg") is not None: chg30d_pop += 1
            if d.get("score_slope") is not None: slope_pop += 1
        summary = h.get("summary", {})
        out["history"] = {
            "size_kb": round(len(body) / 1024, 1),
            "generated_at": h.get("generated_at"),
            "n_days": h.get("n_days"),
            "n_symbols": h.get("n_symbols"),
            "dates_span": f"{(h.get('dates') or [None])[0]} → {(h.get('dates') or [None])[-1]}",
            "trend_distribution": trends,
            "coverage": {
                "score_7d_chg": chg7d_pop,
                "score_30d_chg": chg30d_pop,
                "score_slope": slope_pop,
            },
            "summary": summary,
        }

        # Pull detailed records for top movers
        top_rising = summary.get("top_5_rising") or []
        top_falling = summary.get("top_5_falling") or []
        def detail(sym):
            r = bs.get(sym) or {}
            return {
                "sym": sym,
                "name": (r.get("name") or "")[:25],
                "sector": r.get("sector"),
                "score_now": r.get("score_now"),
                "chg_7d": r.get("score_7d_chg"),
                "chg_14d": r.get("score_14d_chg"),
                "chg_30d": r.get("score_30d_chg"),
                "slope": r.get("score_slope"),
                "trend": r.get("score_trend"),
            }
        out["top_5_rising_detail"]  = [detail(s) for s in top_rising]
        out["top_5_falling_detail"] = [detail(s) for s in top_falling]

        # Spot checks for famous tickers
        for ticker in ("AAPL","NVDA","META","GOOGL","TSLA","CF","NEM","EQT","INCY"):
            if ticker in bs:
                d = bs[ticker]
                scores = d.get("scores") or []
                non_null = sum(1 for v in scores if v is not None)
                out["spot_" + ticker] = {
                    "score_now": d.get("score_now"),
                    "chg_7d": d.get("score_7d_chg"),
                    "chg_30d": d.get("score_30d_chg"),
                    "slope": d.get("score_slope"),
                    "trend": d.get("score_trend"),
                    "non_null_days": non_null,
                    "first_score": next((v for v in scores if v is not None), None),
                    "last_score": next((v for v in reversed(scores) if v is not None), None),
                }
    except Exception as e:
        out["history_err"] = str(e)[:300]

    # 2. Snapshots in S3
    try:
        all_keys = []
        pag = s3.get_paginator("list_objects_v2")
        for page in pag.paginate(Bucket="justhodl-dashboard-live",
                                   Prefix="screener/snapshots/"):
            for o in page.get("Contents") or []:
                all_keys.append(o["Key"])
        all_keys.sort()
        out["snapshots"] = {
            "total_count": len(all_keys),
            "oldest": all_keys[0] if all_keys else None,
            "newest": all_keys[-1] if all_keys else None,
        }
    except Exception as e:
        out["snap_err"] = str(e)[:200]

    # 3. Page deployment check
    try:
        url = "https://justhodl.ai/screener/?cb=" + str(int(time.time()))
        req = urllib.request.Request(url, headers={"User-Agent":"JH/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            page = r.read().decode("utf-8", errors="replace")
            status = r.status
        out["page"] = {
            "status": status, "size_kb": round(len(page)/1024, 1),
            "has_rising_tab":     "RISING" in page,
            "has_fading_tab":     "FADING" in page,
            "has_history_modal":  "historyModal" in page,
            "has_score_spark":    "renderScoreSparkline" in page,
            "has_trend_badge":    "trend_badge" in page,
            "has_history_url":    "HISTORY_URL" in page,
            "has_load_history":   "loadHistory" in page,
            "has_open_history":   "function openHistory" in page,
            "has_draw_chart":     "drawHistoryChart" in page,
            "has_30d_trend_col":  "30d Trend" in page,
        }
    except Exception as e:
        out["page_err"] = str(e)[:200]

    # 4. CloudWatch log tail — confirm history wrote in recent run
    try:
        lg = "/aws/lambda/justhodl-stock-screener"
        sts = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                          descending=True, limit=2)
        lines = []
        for st in sts.get("logStreams", []):
            ev = logs.get_log_events(logGroupName=lg, logStreamName=st["logStreamName"],
                                       startFromHead=False, limit=80)
            for e in ev.get("events", []):
                m = e["message"].strip()
                if "[history]" in m or "[just-crossed]" in m or "DONE:" in m:
                    lines.append((e["timestamp"], m))
        lines.sort()
        out["log_relevant"] = [{"ts": ts, "msg": m[:200]} for ts, m in lines[-20:]]
    except Exception as e:
        out["log_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=90, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
