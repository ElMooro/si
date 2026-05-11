#!/usr/bin/env python3
"""Step 406 — Verify Stages 1-5 of screener overhaul shipped live."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/406_screener_stages_1_to_5.json"
NAME = "justhodl-tmp-screener-v"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, time
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

def fetch(url, t=20):
    req = urllib.request.Request(url, headers={"User-Agent":"JH/1.0"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read().decode("utf-8", errors="replace"), r.status

def lambda_handler(event, context):
    out = {}
    cfg = lam.get_function_configuration(FunctionName="justhodl-stock-screener")
    out["lambda"] = {"last_modified": cfg["LastModified"], "code_size": cfg["CodeSize"]}

    # Read S3 data.json — Stages 1-3 produce new fields
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
        body = obj["Body"].read()
        data = json.loads(body)
        stocks = data.get("stocks") or []
        out["data"] = {
            "generated_at": data.get("generated_at"),
            "size_kb": round(len(body) / 1024, 1),
            "n_stocks": len(stocks),
        }

        # Find a sample stock that has Phase 1 fields
        sample = next((s for s in stocks if s.get("revenue") is not None), stocks[0] if stocks else {})
        out["sample_symbol"] = sample.get("symbol")

        # ── Stage 1 fields ──
        out["stage1_fields"] = {
            "revenue": sample.get("revenue"),
            "netIncome": sample.get("netIncome"),
            "operatingIncome": sample.get("operatingIncome"),
            "freeCashFlow": sample.get("freeCashFlow"),
            "fcfYieldCalc": sample.get("fcfYieldCalc"),
            "buybackYield": sample.get("buybackYield"),
            "rev3yCAGR": sample.get("rev3yCAGR"),
            "sustainable3y": sample.get("sustainable3y"),
        }

        # ── Stage 2 fields ──
        out["stage2_fields"] = {
            "instOwnershipPct": sample.get("instOwnershipPct"),
            "instHoldersN": sample.get("instHoldersN"),
            "instQoQChgPct": sample.get("instQoQChgPct"),
            "insiderNet90dUsd": sample.get("insiderNet90dUsd"),
            "insiderSignal": sample.get("insiderSignal"),
            "beatStreak": sample.get("beatStreak"),
        }

        # ── Stage 3: Steal Score distribution ──
        scored = [s for s in stocks if s.get("stealScore") is not None]
        if scored:
            scores = sorted([s["stealScore"] for s in scored], reverse=True)
            top5 = sorted(scored, key=lambda x: -x["stealScore"])[:10]
            out["stage3_stealscore"] = {
                "n_scored": len(scored),
                "ge_90": sum(1 for s in scores if s >= 90),
                "ge_80": sum(1 for s in scores if s >= 80),
                "ge_70": sum(1 for s in scores if s >= 70),
                "mean": round(sum(scores) / len(scores), 1) if scores else None,
                "median": scores[len(scores)//2] if scores else None,
                "max": max(scores) if scores else None,
                "min": min(scores) if scores else None,
                "top10": [{"symbol": s["symbol"], "name": s.get("name","")[:30],
                            "sector": s.get("sector"), "score": s["stealScore"],
                            "bucket": s.get("stealBucket"),
                            "pe": s.get("peRatio"), "revGrow": s.get("revenueGrowth"),
                            "opMargin": s.get("operatingMargin")} for s in top5],
            }
        else:
            out["stage3_stealscore"] = {"n_scored": 0, "note": "Lambda may not have re-run yet"}
    except Exception as e:
        out["data"] = {"error": str(e)[:300]}

    # ── Stages 4-5: Page deployment check ──
    try:
        page, status = fetch("https://justhodl.ai/screener/?cb=" + str(int(time.time())))
        out["page"] = {
            "status": status, "size": len(page),
            # Stage 1
            "has_money_machines":   "MONEY MACHINES" in page,
            "has_sustainable":      "SUSTAINABLE PROFITS" in page,
            "has_margins":          "HIGHEST MARGINS" in page,
            "has_rev_growth":       "REVENUE GROWTH KINGS" in page,
            "has_cash_gen":         "CASH GENERATORS" in page,
            "has_buyback":          "BUYBACK ACTIVE" in page,
            # Stage 2
            "has_hedge_funds":      "HEDGE FUND BOUGHT" in page,
            "has_institutions":     "INSTITUTION-ACCUMULATED" in page,
            "has_insider_buys":     "INSIDER CONVICTION" in page,
            "has_earnings_beats":   "EARNINGS BEAT STREAK" in page,
            "has_retail_fav":       "RETAIL FAVORITES" in page,
            # Stage 3
            "has_steal_board":      "THE STEAL BOARD" in page,
            "has_steal_score":      "stealScore" in page,
            "has_gold_pulse":       "goldPulse" in page,
            # Stage 4
            "has_heatmap":          "toggleHeatmap" in page,
            "has_sparkline":        "toggleSparkline" in page,
            "has_csv_export":       "function exportCSV" in page,
            # Stage 5
            "has_macro_context":    "macroContext" in page,
            "has_cycle_right_tab":  "CYCLE-RIGHT" in page,
            "has_sector_tilt_map":  "SECTOR_TILT_MAP" in page,
            "has_phase_tilt_map":   "PHASE_TILT_MAP" in page,
        }
    except Exception as e:
        out["page"] = {"error": str(e)[:200]}

    # Macro feeds availability
    for k, key in [("lce", "data/liquidity-conditions-engine.json"),
                    ("gbc", "data/global-business-cycle.json")]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            d = json.loads(obj["Body"].read())
            if k == "lce":
                out["lce_state"] = {
                    "posture": d.get("posture") or d.get("regime") or (d.get("summary",{}) or {}).get("posture"),
                    "size_kb": round(len(json.dumps(d)) / 1024, 1),
                }
            else:
                out["gbc_state"] = {
                    "phase": d.get("global_phase") or d.get("phase"),
                    "size_kb": round(len(json.dumps(d)) / 1024, 1),
                }
        except Exception as e:
            out[k + "_err"] = str(e)[:150]

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
                            MemorySize=256, Timeout=120, Code={"ZipFile": zb})
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
