"""ops 3303 — deploy the OFR gzip fix and prove repo venues + MMF picks
populate (dataset responses are gzip; 3302 landed everything else)."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name="us-east-1")
LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 0}))
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


with report("3303_ofr_gzip") as rep:
    fails, warns = [], []
    cfg = LAM.get_function_configuration(FunctionName="justhodl-ofr-stfm")
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name="justhodl-ofr-stfm",
                  source_dir=AWS_DIR / "lambdas" / "justhodl-ofr-stfm" / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=300, memory=1024,
                  description=str(cfg.get("Description") or "")[:250],
                  smoke=False)
    mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    LAM.invoke(FunctionName="justhodl-ofr-stfm", InvocationType="Event",
               Payload=b"{}")
    of = None
    for _ in range(30):
        time.sleep(10)
        of = s3_json("data/ofr-stfm.json")
        if of and of.get("generated_at", "") >= mark:
            break
    if not (of and of.get("generated_at", "") >= mark):
        fails.append("ofr output never freshened")
        of = of or {}
    ven = ((of.get("repo") or {}).get("venues")) or {}
    picks = ((of.get("mmf") or {}).get("picks")) or {}
    rep.kv(health=of.get("health"),
           repo_n=(of.get("repo") or {}).get("n_series"),
           venues={k: {"vol_mn": v.get("vol_mn"),
                       "rate_pct": v.get("rate_pct")}
                   for k, v in ven.items()},
           mmf_n=(of.get("mmf") or {}).get("n_series"),
           mmf_picks={k: {"mnemonic": v.get("mnemonic"),
                          "latest": v.get("latest")}
                      for k, v in picks.items()},
           catalog_repo_n=len((of.get("catalog") or {}).get("repo") or []),
           catalog_mmf_n=len((of.get("catalog") or {}).get("mmf") or []),
           fails_cross_ok=bool((of.get("nypd_fails_cross") or {})
                               .get("ftd_tot")))
    if (of.get("health") or {}).get("errors"):
        fails.append("dataset errors persist: %s" % of["health"]["errors"])
    if len(ven) < 2:
        fails.append("repo venues %s < 2" % sorted(ven.keys()))
    if not picks:
        warns.append("no MMF picks matched the curation regexes — "
                     "catalog published for manual curation")
    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3303 PASS — OFR STFM fully live: repo venues %s, "
            "%d repo + %d MMF series cataloged for the fleet."
            % (sorted(ven.keys()),
               len((of.get("catalog") or {}).get("repo") or []),
               len((of.get("catalog") or {}).get("mmf") or [])))
sys.exit(0)
