#!/usr/bin/env python3
"""Step 420 — Final Stage 8 verify: history.json wrote correctly, page
has all new UI elements, sample trend metrics look right."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/420_history_final.json"
NAME = "justhodl-tmp-hist-final"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, time
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")

def fetch(url, t=15):
    req = urllib.request.Request(url, headers={"User-Agent":"JH/1.0"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read().decode("utf-8", errors="replace"), r.status

def lambda_handler(event, context):
    out = {}

    # 1. Read screener/history.json
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                             Key="screener/history.json")
        body = obj["Body"].read()
        h = json.loads(body)
        out["history"] = {
            "size_kb": round(len(body)/1024, 1),
            "generated_at": h.get("generated_at"),
            "n_days": h.get("n_days"),
            "n_symbols": h.get("n_symbols"),
            "dates": h.get("dates"),
            "summary": h.get("summary"),
        }
        # Top rising — pull from summary
        by_sym = h.get("by_symbol") or {}
        rising_syms = (h.get("summary") or {}).get("top_5_rising") or []
        falling_syms = (h.get("summary") or {}).get("top_5_falling") or []
        out["top_rising_detail"] = []
        for sym in rising_syms[:10]:
            d = by_sym.get(sym) or {}
            out["top_rising_detail"].append({
                "sym": sym, "name": (d.get("name") or "")[:25],
                "sector": d.get("sector"),
                "score_now": d.get("score_now"),
                "score_7d_chg": d.get("score_7d_chg"),
                "score_30d_chg": d.get("score_30d_chg"),
                "slope": d.get("score_slope"),
                "trend": d.get("score_trend"),
                "scores": d.get("scores"),
            })
        out["top_falling_detail"] = []
        for sym in falling_syms[:10]:
            d = by_sym.get(sym) or {}
            out["top_falling_detail"].append({
                "sym": sym, "name": (d.get("name") or "")[:25],
                "sector": d.get("sector"),
                "score_now": d.get("score_now"),
                "score_7d_chg": d.get("score_7d_chg"),
                "trend": d.get("score_trend"),
                "scores": d.get("scores"),
            })
        # Sample known tickers
        for ticker in ("AAPL","NVDA","META","GOOGL","TSLA","CF","NEM"):
            d = by_sym.get(ticker)
            if d:
                out["sample_" + ticker] = {
                    "score_now": d.get("score_now"),
                    "score_7d_chg": d.get("score_7d_chg"),
                    "score_30d_chg": d.get("score_30d_chg"),
                    "slope": d.get("score_slope"),
                    "trend": d.get("score_trend"),
                    "scores_len": len([v for v in (d.get("scores") or []) if v is not None]),
                }
    except Exception as e:
        out["history_err"] = str(e)[:300]

    # 2. Page deployment check
    try:
        page, status = fetch("https://justhodl.ai/screener/?cb=" + str(int(time.time())))
        out["page"] = {
            "status": status, "size": len(page),
            "has_rising_tab":          "📈 RISING" in page or "RISING" in page,
            "has_fading_tab":          "📉 FADING" in page or "FADING" in page,
            "has_history_modal":       "historyModal" in page,
            "has_score_spark_col":     "30d Trend" in page,
            "has_7d_delta_col":        "7d Δ" in page,
            "has_load_history":        "function loadHistory" in page,
            "has_open_history":        "function openHistory" in page,
            "has_draw_chart":          "function drawHistoryChart" in page,
            "has_render_sparkline":    "function renderScoreSparkline" in page,
            "has_history_url":         "HISTORY_URL" in page,
            "has_augment":             "augmentStocksWithHistory" in page,
            "has_trend_badge_fmt":     "trend_badge" in page,
            "has_score_spark_fmt":     "scoreSpark" in page,
            "has_threshold_lines":     "QUALITY" in page and "PREMIUM" in page and "STEAL" in page,
        }
    except Exception as e:
        out["page_err"] = str(e)[:200]

    # 3. Snapshots listing (just to confirm pool size)
    try:
        listing = s3.list_objects_v2(Bucket="justhodl-dashboard-live",
                                       Prefix="screener/snapshots/")
        objs = listing.get("Contents") or []
        out["snapshots_count"] = len(objs)
        out["snapshots_dates"] = sorted([o["Key"].rsplit("/",1)[-1].replace(".json","")
                                            for o in objs])[-7:]
    except Exception as e:
        out["snap_err"] = str(e)[:200]

    # 4. Lambda log tail — confirm history wrote
    try:
        lg = "/aws/lambda/justhodl-stock-screener"
        sts = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                          descending=True, limit=2)
        lines = []
        for st in sts.get("logStreams", []):
            ev = logs.get_log_events(logGroupName=lg, logStreamName=st["logStreamName"],
                                       startFromHead=False, limit=40)
            for e in ev.get("events", []):
                msg = e["message"].strip()
                if any(t in msg.lower() for t in ("history", "snapshot", "events written", "done:")):
                    lines.append((e["timestamp"], msg))
        lines.sort()
        out["log_tail"] = [{"ts": ts, "msg": m[:200]} for ts, m in lines[-20:]]
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
                            MemorySize=256, Timeout=60, Code={"ZipFile": zb})
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
