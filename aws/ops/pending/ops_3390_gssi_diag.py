"""ops 3390 — diagnose: what IS deployed + what error does build_gssi write."""
import io, json, sys, time, urllib.request, zipfile
import boto3
from botocore.config import Config
from pathlib import Path
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3390)"}

with report("3390_gssi_diag") as rep:
    rep.heading("ops 3390 — GSSI crash diagnosis")
    info = LAM.get_function(FunctionName="justhodl-sovereign-stress")
    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
        src = zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
    vline = [l for l in src.splitlines() if l.startswith("VERSION")][:1]
    print("DEPLOYED:", vline, "| _expz:", "_expz" in src, "| dispersion:", "dispersion" in src,
          "| mu_ref_in_gssi:", '"mu"' in src.split("def build_gssi")[1].split("def lambda_handler")[0] if "def build_gssi" in src else "?")
    rep.log(f"deployed {vline} expz={'_expz' in src}")
    j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/sovereign-stress.json")["Body"].read())
    gerrs = [e for e in (j.get("errors") or []) if "gssi" in e.lower()]
    print("FEED version:", j.get("version"), "| gssi_current:", j.get("gssi_current"),
          "| gssi errors:", gerrs[:3])
    rep.log(f"feed v={j.get('version')} gssi_errs={gerrs[:2]}")
    try:
        g = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/sovereign-gssi.json")["Body"].read())
        print("GSSI feed version:", g.get("version"), "gen:", g.get("generated_at"))
    except Exception as e:
        print("GSSI feed read:", str(e)[:100])
    Path("aws/ops/reports/3390.json").write_text(json.dumps({"deployed": vline,
        "feed_version": j.get("version"), "gssi_errors": gerrs[:4]}, indent=2))
    sys.exit(0)
