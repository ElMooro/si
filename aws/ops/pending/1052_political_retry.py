#!/usr/bin/env python3
"""1052 — diagnose intermittent fetch failures + retry political-stocks.

1) Wait 90s to clear any rate-limit window
2) Probe Quiver + legislators-current.json from a fresh Lambda
3) Re-invoke political-stocks Lambda  
4) Report final state
"""
import io, json, os, pathlib, time, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1052_political_retry.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)
long_lam = boto3.client("lambda", region_name=REGION,
                          config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Step 1: wait to clear potential rate limit
    print("[1052] waiting 90s for any rate-limit window to clear…")
    time.sleep(90)
    
    # Step 2: probe both upstreams from a fresh tiny Lambda
    probe_code = r"""
import urllib.request, json, time
def probe(url):
    h = {"User-Agent": "Mozilla/5.0 (compatible; JustHodl/1.0; raafouis@gmail.com)",
         "Accept": "application/json"}
    try:
        t0 = time.time()
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read()
            elapsed = time.time() - t0
            result = {"status": r.status, "size": len(body), "elapsed_s": round(elapsed, 2)}
            try:
                data = json.loads(body)
                result["json_ok"] = True
                if isinstance(data, list):
                    result["n_items"] = len(data)
                    if data and isinstance(data[0], dict):
                        result["first_keys"] = list(data[0].keys())[:8]
                elif isinstance(data, dict):
                    result["top_keys"] = list(data.keys())[:8]
            except Exception as je:
                result["parse_err"] = str(je)[:100]
            return result
    except Exception as e:
        return {"err": str(e)[:200]}

results = {}
results["quiver_live"] = probe("https://api.quiverquant.com/beta/live/congresstrading")
time.sleep(1)
results["legislators"] = probe("https://theunitedstates.io/congress-legislators/legislators-current.json")
print("__PROBE__" + json.dumps(results, default=str) + "__END__")
"""
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", f"""
def lambda_handler(event, context):
{chr(10).join('    ' + line for line in probe_code.strip().split(chr(10)))}
    return {{"ok": True}}
""")
    
    tmp_name = "justhodl-tmp-1052-probe"
    try:
        lam.delete_function(FunctionName=tmp_name)
        time.sleep(2)
    except Exception:
        pass
    
    try:
        lam.create_function(
            FunctionName=tmp_name,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": buf.getvalue()},
            Timeout=60, MemorySize=256, Publish=False,
        )
        lam.get_waiter("function_active").wait(FunctionName=tmp_name)
        r = long_lam.invoke(FunctionName=tmp_name,
                              InvocationType="RequestResponse", Payload=b"{}",
                              LogType="Tail")
        log_tail = base64.b64decode(r["LogResult"]).decode("utf-8", errors="replace") if "LogResult" in r else ""
        start = log_tail.find("__PROBE__")
        end = log_tail.find("__END__")
        if start >= 0 and end > start:
            out["probe"] = json.loads(log_tail[start + 9:end])
        else:
            out["log_tail"] = log_tail[-1500:]
    except Exception as e:
        out["probe_err"] = str(e)[:300]
    finally:
        try:
            lam.delete_function(FunctionName=tmp_name)
        except Exception:
            pass
    
    # Step 3: re-invoke political-stocks Lambda after the wait
    print("[1052] re-invoking political-stocks…")
    time.sleep(3)
    try:
        r = long_lam.invoke(FunctionName="justhodl-political-stocks",
                              InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            out["invoke"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["invoke_raw"] = body[:400]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]
    
    time.sleep(2)
    
    # Step 4: verify S3 output is now populated
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/political-stocks.json")
        ps = json.loads(obj["Body"].read().decode("utf-8"))
        c = ps.get("congress") or {}
        out["final"] = {
            "schema":             ps.get("schema_version"),
            "generated_at":       ps.get("generated_at"),
            "n_trades_total":     c.get("n_trades_total"),
            "n_trades_house":     c.get("n_trades_house"),
            "n_trades_senate":    c.get("n_trades_senate"),
            "n_tickers":          c.get("n_tickers"),
            "n_party_map":        c.get("n_party_map"),
            "n_top_buys":         len(c.get("top_buys") or []),
            "n_clusters":         len(c.get("clusters") or []),
            "n_bipartisan":       len(c.get("bipartisan_buys") or []),
            "top_5_buys": [{
                "ticker": r["ticker"], "score": r["score"],
                "n_buys": r["n_buys"], "n_pols": r["n_politicians"],
                "parties": r["parties"],
                "bipartisan": r["bipartisan"],
            } for r in (c.get("top_buys") or [])[:5]],
            "bipartisan_top_5": [{
                "ticker": r["ticker"], "score": r["score"],
                "parties": r["parties"],
            } for r in (c.get("bipartisan_buys") or [])[:5]],
        }
    except Exception as e:
        out["final_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
