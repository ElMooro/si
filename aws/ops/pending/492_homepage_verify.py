#!/usr/bin/env python3
"""Step 492 — Verify the homepage is live with the new pipeline links."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/492_homepage_verify.json"
NAME = "justhodl-tmp-492"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request

def lambda_handler(event, context):
    out = {}
    for url in ["https://justhodl.ai/", "https://justhodl.ai/alpha/",
                "https://justhodl.ai/debate/", "https://justhodl.ai/trades/",
                "https://justhodl.ai/portfolio/", "https://justhodl.ai/sizing/",
                "https://justhodl.ai/catalyst/"]:
        try:
            req = urllib.request.Request(url + "?_t=" + str(int(__import__("time").time())),
                headers={"User-Agent": "JustHodl-Verify/1.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                body = r.read().decode("utf-8", errors="replace")
            o = {"status": r.status, "size_kb": round(len(body)/1024, 1)}
            if url.endswith("/") and url == "https://justhodl.ai/":
                # Check homepage for our new links
                links = {}
                for p in ["alpha", "debate", "trades", "portfolio", "sizing", "catalyst"]:
                    links[p] = body.count(f\'href="/{p}/"\')
                o["new_pipeline_links"] = links
                o["has_intelligence_pipeline_block"] = "INTELLIGENCE PIPELINE" in body
                # Extract title for sanity
                import re
                m = re.search(r"<title>([^<]+)</title>", body)
                o["title"] = m.group(1) if m else None
            else:
                # Sub-pages: confirm they actually render
                import re
                m = re.search(r"<title>([^<]+)</title>", body)
                o["title"] = m.group(1) if m else None
                # Check page is real content not 404
                o["has_h1"] = "<h1" in body
            out[url] = o
        except Exception as e:
            out[url] = {"err": str(e)[:200]}
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
                            MemorySize=512, Timeout=60, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:30000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
