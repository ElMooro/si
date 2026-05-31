#!/usr/bin/env python3
"""1047 — diagnose Congress feed failures.

Pull CloudWatch logs from political-stocks Lambda to see exact HTTP
responses + any error messages."""
import json, os, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1047_congress_diag.json"
REGION = "us-east-1"

logs = boto3.client("logs", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    lg = "/aws/lambda/justhodl-political-stocks"
    try:
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        
        all_lines = []
        for stream in streams[:2]:
            ev = logs.get_log_events(
                logGroupName=lg, logStreamName=stream["logStreamName"],
                limit=500, startFromHead=True,
            )
            for e in ev.get("events") or []:
                msg = e.get("message", "").strip()
                if not msg or msg.startswith("INIT_") or msg.startswith("END "):
                    continue
                # Keep lines that talk about House/Senate/HTTP
                if "[political]" in msg or "HTTP" in msg or "err" in msg.lower() or "House" in msg or "Senate" in msg:
                    all_lines.append(msg[:300])
        
        out["log_lines"] = all_lines[-30:]
    except Exception as e:
        out["err"] = str(e)[:300]
    
    # Also try probing those URLs from inside a Lambda
    # (sandbox can't reach external; need a Lambda)
    # Quick fix: use an existing Lambda to test
    try:
        lam = boto3.client("lambda", region_name=REGION)
        probe_code = """
import urllib.request, json
urls = [
    "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json",
    "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json",
    "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions_summary.json",
]
results = {}
for url in urls:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Probe/1.0 raafouis@gmail.com"})
        with urllib.request.urlopen(req, timeout=30) as r:
            code = r.status
            body = r.read()
            results[url] = {"status": code, "size": len(body), "first_100": body[:100].decode("utf-8", errors="replace")}
            try:
                data = json.loads(body)
                if isinstance(data, list):
                    results[url]["n_items"] = len(data)
                    if data: results[url]["first_item_keys"] = list(data[0].keys())[:10] if isinstance(data[0], dict) else "not_dict"
                else:
                    results[url]["type"] = type(data).__name__
                    results[url]["keys"] = list(data.keys())[:10] if isinstance(data, dict) else "?"
            except Exception as je:
                results[url]["parse_err"] = str(je)[:100]
    except Exception as e:
        results[url] = {"err": str(e)[:200]}
print(json.dumps(results, default=str))
"""
        # Use existing dex-scanner Lambda or ai-chat as a probe; simpler: deploy a temp
        # Actually let's just create a temp Lambda for this
        import io, zipfile
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("lambda_function.py", f"""
def lambda_handler(event, context):
{chr(10).join('    ' + line for line in probe_code.strip().split(chr(10)))}
    return {{"ok": True}}
""")
        tmp_name = "justhodl-tmp-1047-probe"
        ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
        try:
            lam.delete_function(FunctionName=tmp_name)
        except Exception:
            pass
        lam.create_function(
            FunctionName=tmp_name,
            Runtime="python3.12",
            Role=ROLE,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": buf.getvalue()},
            Timeout=60, MemorySize=256, Publish=False,
        )
        lam.get_waiter("function_active").wait(FunctionName=tmp_name)
        
        # Invoke
        from botocore.config import Config
        long_lam = boto3.client("lambda", region_name=REGION,
                                  config=Config(read_timeout=120))
        r = long_lam.invoke(FunctionName=tmp_name,
                              InvocationType="RequestResponse", Payload=b"{}",
                              LogType="Tail")
        # Get the log output (where our print(json.dumps) went)
        import base64
        if "LogResult" in r:
            log_tail = base64.b64decode(r["LogResult"]).decode("utf-8", errors="replace")
            # Find the JSON line
            for line in log_tail.split("\n"):
                line = line.strip()
                if line.startswith("{") and "size" in line:
                    try:
                        out["probe_results"] = json.loads(line)
                        break
                    except Exception:
                        pass
            out["log_tail_excerpt"] = log_tail[-2000:]
        
        # Cleanup
        try:
            lam.delete_function(FunctionName=tmp_name)
        except Exception:
            pass
    except Exception as e:
        out["probe_err"] = str(e)[:400]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
