#!/usr/bin/env python3
"""1048 — probe alternative Congress trade data sources.
Test Capitol Trades BFF + a few other endpoints to find a working
replacement for the dead House/Senate Stock Watcher feeds."""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1048_alt_congress_sources.json"
REGION = "us-east-1"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)
long_lam = boto3.client("lambda", region_name=REGION,
                          config=Config(read_timeout=120))


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    probe_code = r"""
import urllib.request, urllib.parse, json, ssl

def probe(label, url, headers=None):
    h = {"User-Agent": "Mozilla/5.0 (compatible; JustHodl/1.0; raafouis@gmail.com)",
         "Accept": "application/json"}
    if headers: h.update(headers)
    try:
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=20) as r:
            code = r.status
            body = r.read()
            result = {"status": code, "size": len(body), "first_300": body[:300].decode("utf-8", errors="replace")}
            try:
                data = json.loads(body)
                result["json_ok"] = True
                if isinstance(data, list):
                    result["n_items"] = len(data)
                    if data: result["sample_keys"] = list(data[0].keys())[:15] if isinstance(data[0], dict) else "not_dict"
                elif isinstance(data, dict):
                    result["top_keys"] = list(data.keys())[:15]
                    if "data" in data and isinstance(data["data"], list):
                        result["n_data_items"] = len(data["data"])
                        if data["data"]: result["data_item_keys"] = list(data["data"][0].keys())[:15] if isinstance(data["data"][0], dict) else "?"
                    if "results" in data and isinstance(data["results"], list):
                        result["n_results"] = len(data["results"])
            except Exception as je:
                result["parse_err"] = str(je)[:150]
            return result
    except Exception as e:
        return {"err": str(e)[:200]}

results = {}

# Capitol Trades — their frontend BFF
results["captrades_recent"] = probe("captrades", "https://bff.capitoltrades.com/trades?perPage=20&page=1&txDate=last7days")
results["captrades_v2"] = probe("captrades-v2", "https://bff.capitoltrades.com/trades?perPage=10")
results["captrades_politicians"] = probe("captrades-pols", "https://bff.capitoltrades.com/politicians?perPage=5")

# Quiver Quantitative no-auth endpoints (some have a public CDN)
results["quiver_congress_aapl"] = probe("quiver-aapl", "https://api.quiverquant.com/beta/historical/congresstrading/AAPL")

# UnusualWhales
results["uw_congress"] = probe("uw", "https://api.unusualwhales.com/api/congress/recent_trades")

# House Clerk official (XML)
results["house_clerk_2025"] = probe("house-2025", "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025FD.ZIP", headers={"Accept": "*/*"})

# Senate eFD search results (likely needs cookie)
results["senate_efd"] = probe("senate-efd", "https://efdsearch.senate.gov/search/")

# Try a python lib mirror? Some folks repost the data
results["github_repo_mirror"] = probe("github-aaron", "https://raw.githubusercontent.com/aaronkjones/congress-stock-research/main/data/recent_trades.json")

print("__RESULTS__" + json.dumps(results, default=str) + "__END__")
"""
    
    # Build temp Lambda
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", f"""
def lambda_handler(event, context):
{chr(10).join('    ' + line for line in probe_code.strip().split(chr(10)))}
    return {{"ok": True}}
""")
    
    tmp_name = "justhodl-tmp-1048-probe"
    try:
        lam.delete_function(FunctionName=tmp_name)
        time.sleep(2)
    except Exception:
        pass
    
    lam.create_function(
        FunctionName=tmp_name,
        Runtime="python3.12",
        Role=ROLE_ARN,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": buf.getvalue()},
        Timeout=120, MemorySize=512, Publish=False,
    )
    lam.get_waiter("function_active").wait(FunctionName=tmp_name)
    
    r = long_lam.invoke(FunctionName=tmp_name,
                          InvocationType="RequestResponse", Payload=b"{}",
                          LogType="Tail")
    
    import base64
    log_tail = base64.b64decode(r["LogResult"]).decode("utf-8", errors="replace") if "LogResult" in r else ""
    
    # Extract our results
    start = log_tail.find("__RESULTS__")
    end = log_tail.find("__END__")
    if start >= 0 and end > start:
        try:
            out["probe"] = json.loads(log_tail[start + 11:end])
        except Exception as e:
            out["parse_err"] = str(e)[:200]
            out["raw"] = log_tail[start + 11:end][:3000]
    else:
        out["log_tail"] = log_tail[-3000:]
    
    # Cleanup
    try:
        lam.delete_function(FunctionName=tmp_name)
    except Exception:
        pass
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
