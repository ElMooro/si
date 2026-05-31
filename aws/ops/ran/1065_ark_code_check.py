#!/usr/bin/env python3
"""1065 — verify deployed ARK Lambda code matches repo version."""
import json, os, pathlib, time
import urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1065_ark_code_check.json"

lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=180))


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Get the Lambda function metadata + download URL
    print("[1065] phase 1: get Lambda metadata…")
    try:
        info = lam.get_function(FunctionName="justhodl-ark-holdings")
        cfg = info["Configuration"]
        out["lambda_meta"] = {
            "last_modified": cfg.get("LastModified"),
            "version":       cfg.get("Version"),
            "code_size":     cfg.get("CodeSize"),
            "code_sha256":   cfg.get("CodeSha256"),
        }
        # Download the deployed code zip
        download_url = info["Code"]["Location"]
        print("[1065] phase 2: download deployed zip…")
        try:
            req = urllib.request.Request(download_url)
            with urllib.request.urlopen(req, timeout=30) as r:
                zip_bytes = r.read()
            out["zip_size"] = len(zip_bytes)
            
            # Extract lambda_function.py
            import zipfile, io
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                files_in_zip = zf.namelist()
                out["files_in_zip"] = files_in_zip
                if "lambda_function.py" in files_in_zip:
                    code = zf.read("lambda_function.py").decode("utf-8")
                    out["deployed_code_size"] = len(code)
                    out["deployed_first_50_lines"] = "\n".join(code.split("\n")[:50])
                    # Check for key markers
                    out["has_arkfunds_io"] = "arkfunds.io" in code
                    out["has_old_csv_url"] = "wp-content/uploads/funds-etf-csv" in code
                    out["has_v2_method"] = "ark_holdings_v2_arkfunds_io" in code
        except Exception as e:
            out["download_err"] = str(e)[:300]
    except Exception as e:
        out["meta_err"] = str(e)[:300]
    
    # Force an update with the current repo code (defensive deploy)
    print("[1065] phase 3: force re-deploy with current repo code…")
    try:
        import zipfile, io
        src = pathlib.Path("aws/lambdas/justhodl-ark-holdings/source")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in src.glob("*.py"):
                zf.writestr(f.name, f.read_bytes())
        zb = buf.getvalue()
        out["force_redeploy"] = {"zip_size": len(zb)}
        
        for attempt in range(3):
            try:
                lam.update_function_code(
                    FunctionName="justhodl-ark-holdings",
                    ZipFile=zb, Publish=False,
                )
                lam.get_waiter("function_updated").wait(FunctionName="justhodl-ark-holdings")
                out["force_redeploy"]["status"] = "ok"
                break
            except Exception as e:
                if "ResourceConflict" in str(e) and attempt < 2:
                    time.sleep(4 * (attempt + 1)); continue
                out["force_redeploy"]["err"] = str(e)[:300]
                raise
    except Exception as e:
        out["force_redeploy_err"] = str(e)[:300]
    
    time.sleep(3)
    
    # Re-invoke
    print("[1065] phase 4: re-invoke after force deploy…")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName="justhodl-ark-holdings",
                        InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            result = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
            out["invoke_after"] = {
                "elapsed_s":   round(time.time() - t0, 1),
                "ok":          result.get("ok"),
                "n_funds":     result.get("n_funds"),
                "n_positions": result.get("n_positions"),
                "n_unique":    result.get("n_unique_tickers"),
                "n_new":       result.get("n_new_positions"),
                "n_adds":      result.get("n_adds"),
            }
        except Exception:
            out["invoke_after"] = {"raw": body[:400]}
    except Exception as e:
        out["invoke_after_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1065] DONE → {REPORT}")


if __name__ == "__main__":
    main()
