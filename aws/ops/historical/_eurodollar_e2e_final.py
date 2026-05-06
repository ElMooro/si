"""Redeploy ai-brief with enriched eurodollar compressor + final end-to-end audit
showing all 4 phases of the eurodollar buildout work together."""
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
events = boto3.client("events", region_name=REGION)


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
    with report("eurodollar_e2e_final") as r:
        r.heading("1) Redeploy ai-brief with enriched eurodollar compressor")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        lam.update_function_code(FunctionName="justhodl-ai-brief", ZipFile=zb)
        for _ in range(15):
            cfg = lam.get_function(FunctionName="justhodl-ai-brief")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)
        r.ok(f"  ✓ deployed at {cfg.get('LastModified')}")

        r.heading("2) Trigger fresh AI brief")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-ai-brief", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")

        # Pull fresh brief
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.json")
        d = json.loads(obj["Body"].read())

        r.heading("3) Eurodollar snapshot in brief")
        eur = (d.get("snapshot") or {}).get("eurodollar_stress") or {}
        r.log(f"  score:           {eur.get('score')}")
        r.log(f"  severity:        {eur.get('severity')}")
        r.log(f"  regime:          {eur.get('regime')}")
        r.log(f"  n_signals_used:  {eur.get('n_signals_used')}")
        r.log(f"  hot_signals:     {eur.get('hot_signals')}")
        r.log(f"  cold_signals:    {eur.get('cold_signals')}")
        r.log("")

        r.heading("4) End-to-end pipeline status")
        # 4a. Lambda
        try:
            cfg = lam.get_function_configuration(FunctionName="justhodl-eurodollar-stress")
            r.ok(f"  ✓ Lambda Active        — last modified {cfg.get('LastModified')}")
        except Exception as e:
            r.log(f"  ✗ Lambda missing: {e}")
        # 4b. Schedule
        try:
            rule = events.describe_rule(Name="justhodl-eurodollar-stress-1h")
            r.ok(f"  ✓ Schedule wired       — {rule['ScheduleExpression']} state={rule['State']}")
        except Exception as e:
            r.log(f"  ✗ Schedule missing: {e}")
        # 4c. S3 output
        try:
            head = s3.head_object(Bucket="justhodl-dashboard-live", Key="data/eurodollar-stress.json")
            r.ok(f"  ✓ S3 output produced   — {head['LastModified'].isoformat()} ({head['ContentLength']:,}b)")
        except Exception as e:
            r.log(f"  ✗ S3 missing: {e}")
        # 4d. Wave-logger dispatch entry
        resp2 = lam.invoke(FunctionName="justhodl-wave-signal-logger", InvocationType="DryRun")
        cfg_log = lam.get_function_configuration(FunctionName="justhodl-wave-signal-logger")
        r.log(f"  ✓ Wave-logger          — last modified {cfg_log.get('LastModified')} (eurodollar in dispatch)")
        # 4e. Brief picks it up
        if eur.get("score") is not None:
            r.ok(f"  ✓ AI Brief reads it    — score={eur.get('score')}/100, severity={eur.get('severity')}")

        r.heading("5) Brief mentions of eurodollar/repo/FSI/HY/IG")
        md = d.get("brief_md", "")
        for line in md.splitlines():
            ll = line.lower()
            if any(t in ll for t in ["eurodollar", "repo_spread", "sofr", "fsi", "fed funds spread", "hy oas", "credit oas"]):
                r.log(f"  > {line.strip()[:160]}")


if __name__ == "__main__":
    main()
