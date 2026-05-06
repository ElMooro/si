"""Force redeploy ai-brief + verify call-verb extraction with debug output."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
SOURCE_DIR = "aws/lambdas/justhodl-ai-brief/source"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def make_zip(source_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(source_dir):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, source_dir)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def main():
    with report("force_redeploy_brief") as r:
        # 1. Wait for any update
        r.heading("0) Wait for any in-progress update")
        for attempt in range(20):
            cfg = lam.get_function(FunctionName="justhodl-ai-brief")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.log(f"  ready, current mod={cfg.get('LastModified')}")
                break
            time.sleep(3)

        # 2. Force redeploy
        r.heading("1) Force redeploy")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        try:
            lam.update_function_code(FunctionName="justhodl-ai-brief", ZipFile=zb)
            r.log("  ✓ update_function_code accepted")
        except Exception as e:
            r.log(f"  ✗ {e}")
            return

        for attempt in range(25):
            cfg = lam.get_function(FunctionName="justhodl-ai-brief")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ deployed, mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        # 3. Inspect deployed source for the verb extraction code
        r.heading("2) Inspect deployed Lambda for the new _extract_call_verb")
        try:
            cresp = lam.get_function(FunctionName="justhodl-ai-brief")
            url = cresp["Code"]["Location"]
            import urllib.request
            with urllib.request.urlopen(url, timeout=20) as resp:
                z = zipfile.ZipFile(io.BytesIO(resp.read()))
            for n in z.namelist():
                if n.endswith("lambda_function.py"):
                    src = z.read(n).decode("utf-8", errors="replace")
                    if "tail = md[-2500:]" in src:
                        r.ok("  ✓ NEW _extract_call_verb is deployed")
                    else:
                        r.log("  ✗ OLD code still in place (tail = md[-2500:] not found)")
                    if "calibration_v2" in src:
                        r.ok("  ✓ calibration_v2 enrichment is deployed")
                    if "compress_paper_portfolio" in src:
                        r.ok("  ✓ compress_paper_portfolio is deployed")
                    break
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 4. Re-invoke
        r.heading("3) Re-invoke after force redeploy")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-ai-brief", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  brief_chars: {inner.get('brief_chars')}")
        except Exception as e:
            r.log(f"  parse: {e}")

        # 5. Check call-verb history
        r.heading("4) Verify call_verb in latest history snapshot")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/decisive-call-history.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  n_snapshots: {d.get('n_snapshots')}")
            for snap in (d.get("snapshots") or [])[-3:]:
                r.log(f"  ts={snap.get('timestamp')[:19]}  call={snap.get('call_verb'):20s}  highest={snap.get('highest_weight_signal')}  acc={snap.get('weighted_mean_accuracy')}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 6. Sample brief end (where DECISIVE CALL section is)
        r.heading("5) Last 2000 chars of brief — visual check for verb pattern")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.md")
            md = obj["Body"].read().decode("utf-8")
            r.log(f"  brief size: {len(md):,}b, last 2000 chars:")
            r.log(md[-2000:])
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
