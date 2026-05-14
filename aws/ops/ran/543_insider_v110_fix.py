#!/usr/bin/env python3
"""543 — Deploy insider-transactions v1.1.0 (raw XML fix) + invoke + audit."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/543_insider_v110_fix.json"
NAME = "justhodl-insider-transactions"
SOURCE = "aws/lambdas/justhodl-insider-transactions/source/lambda_function.py"

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def zip_source(path):
    with open(path, "rb") as f: code = f.read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    try:
        zb = zip_source(SOURCE)
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        out["update"] = "ok"
    except Exception as e:
        out["update_err"] = str(e)[:300]
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str); return

    _time.sleep(3)
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if p.get("body") else p
        except: out["raw"] = body[:1500]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-4000:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    # Audit sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/insider-transactions.json")
        body = obj["Body"].read()
        p = json.loads(body)
        comp = p.get("composite") or {}
        bt = p.get("by_ticker") or {}
        # Aggregate code distribution across all tickers
        agg_codes = {}
        for tkr, info in bt.items():
            for c, n in (info.get("code_distribution_30d") or {}).items():
                agg_codes[c] = agg_codes.get(c, 0) + n
        # Find tickers with buys
        with_buys = sorted([(t, i.get("n_buys_30d", 0), i.get("buy_value_30d_usd", 0))
                             for t, i in bt.items() if (i.get("n_buys_30d") or 0) > 0],
                            key=lambda x: -x[2])
        with_sells = sorted([(t, i.get("n_sells_30d", 0), i.get("sell_value_30d_usd", 0))
                              for t, i in bt.items() if (i.get("n_sells_30d") or 0) > 0],
                             key=lambda x: -x[2])
        with_grants = sorted([(t, i.get("n_grants_30d", 0), i.get("grant_value_30d_usd", 0))
                               for t, i in bt.items() if (i.get("n_grants_30d") or 0) > 0],
                              key=lambda x: -x[2])
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "n_tickers": p.get("n_tickers"),
            "n_with_data": p.get("n_with_data"),
            "n_with_err": p.get("n_with_err"),
            "composite_regime": p.get("composite_regime"),
            "composite_signal": p.get("composite_signal"),
            "total_buy_value_30d_usd": comp.get("total_buy_value_30d_usd"),
            "total_sell_value_30d_usd": comp.get("total_sell_value_30d_usd"),
            "n_cluster_buys": comp.get("n_cluster_buys"),
            "n_cluster_sells": comp.get("n_cluster_sells"),
            "n_with_buys": comp.get("n_with_buys"),
            "n_with_sells": comp.get("n_with_sells"),
            "buy_sell_dollar_ratio": comp.get("buy_sell_dollar_ratio"),
            "agg_code_distribution": agg_codes,
            "top_5_buy_value": with_buys[:5],
            "top_5_sell_value": with_sells[:5],
            "top_5_grant_value": with_grants[:5],
        }
        # Sample 4 tickers to show full data
        sample_tkrs = ["NVDA", "AAPL", "META", "WMT"]
        out["per_ticker_sample"] = {
            t: {k: v for k, v in (bt.get(t) or {}).items()
                 if k not in ("recent_txns_top_5",)}
            for t in sample_tkrs
        }
        # Also dump recent_txns_top_5 for AAPL (most active by Form 4 count)
        out["AAPL_recent_txns"] = (bt.get("AAPL") or {}).get("recent_txns_top_5", [])
        out["META_recent_txns"] = (bt.get("META") or {}).get("recent_txns_top_5", [])
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
