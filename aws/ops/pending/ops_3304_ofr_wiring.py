"""ops 3304 — WIRE OFR STFM INTO THE FUNDING STACK.
[1] ofr-stfm v1.1: MMF curation replaced regex-guessing with mnemonic-
    grammar family grouping (MMF-MMF_{ASSET}_...-M) — every family gets
    a broadest-series pick automatically.
[2] eurodollar-plumbing: us_core gains GCF−Triparty interdealer premium
    (scored, 3/8bp flags), total repo depth, MMF repo pool (info).
    Layers render dynamically, so the page picks these up untouched.
[3] liquidity-inflection: non-scoring onshore_funding context block
    (fingerprint history untouched) for downstream consumers.
Chain: invoke ofr -> fresh -> invoke plumbing + liquidity -> verify."""
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


def redeploy(name, **kw):
    cfg = LAM.get_function_configuration(FunctionName=name)
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name=name,
                  source_dir=AWS_DIR / "lambdas" / name / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=kw.get("timeout", cfg.get("Timeout") or 300),
                  memory=kw.get("memory", cfg.get("MemorySize") or 512),
                  description=str(cfg.get("Description") or "")[:250],
                  smoke=False)


def wait_fresh(key, mark, tries=40, sleep_s=12):
    d = None
    for _ in range(tries):
        time.sleep(sleep_s)
        d = s3_json(key)
        if d and d.get("generated_at", "") >= mark:
            return d
    return None


with report("3304_ofr_wiring") as rep:
    fails, warns = [], []

    rep.section("1. Deploy the three engines")
    redeploy("justhodl-ofr-stfm")
    redeploy("justhodl-eurodollar-plumbing")
    redeploy("justhodl-liquidity-inflection")

    rep.section("2. OFR v1.1 — MMF families")
    mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    LAM.invoke(FunctionName="justhodl-ofr-stfm", InvocationType="Event",
               Payload=b"{}")
    of = wait_fresh("data/ofr-stfm.json", mark, tries=30, sleep_s=10)
    if not of:
        fails.append("ofr-stfm output never freshened")
        of = s3_json("data/ofr-stfm.json") or {}
    famS = ((of.get("mmf") or {}).get("families")) or {}
    picks = ((of.get("mmf") or {}).get("picks")) or {}
    rep.kv(mmf_families={k: {"pick": v.get("pick"),
                             "n": v.get("n_members"),
                             "latest": v.get("latest")}
                         for k, v in famS.items()},
           mmf_pick_keys=sorted(picks.keys()),
           health=of.get("health"))
    if len(famS) < 4:
        fails.append("MMF families %d < 4" % len(famS))
    if (of.get("health") or {}).get("errors"):
        fails.append("OFR errors: %s" % of["health"]["errors"])

    rep.section("3. Plumbing us_core joins")
    m2 = datetime.now(timezone.utc).isoformat(timespec="seconds")
    LAM.invoke(FunctionName="justhodl-eurodollar-plumbing",
               InvocationType="Event", Payload=b"{}")
    LAM.invoke(FunctionName="justhodl-liquidity-inflection",
               InvocationType="Event", Payload=b"{}")
    ed = wait_fresh("data/eurodollar-plumbing.json", m2)
    if not ed:
        fails.append("eurodollar-plumbing never freshened")
        ed = s3_json("data/eurodollar-plumbing.json") or {}
    core = (((ed.get("layers") or {}).get("us_core") or {})
            .get("metrics")) or []
    keys = [m.get("key") for m in core]
    gcf = next((m for m in core if m.get("key") == "gcf_tri"), None)
    depth = next((m for m in core if m.get("key") == "ofr_repo_depth"),
                 None)
    pool = next((m for m in core if m.get("key") == "mmf_repo_pool"), None)
    rep.kv(us_core_keys=keys,
           gcf_tri=gcf, ofr_repo_depth=depth, mmf_repo_pool=pool,
           composite_health=(ed.get("composite") or {}).get("health")
           if isinstance(ed.get("composite"), dict) else ed.get("health"))
    if not gcf:
        fails.append("gcf_tri metric missing from us_core")
    elif not (-50 < float(gcf.get("value") or 0) < 100):
        fails.append("gcf_tri implausible: %s" % gcf.get("value"))
    if not depth:
        fails.append("ofr_repo_depth metric missing")
    elif not (2 < float(depth.get("value") or 0) < 15):
        fails.append("repo depth implausible: %s $tn"
                     % depth.get("value"))
    if not pool:
        warns.append("mmf_repo_pool missing (pick may lack latest)")

    rep.section("4. Liquidity-inflection context block")
    li = wait_fresh("data/liquidity-inflection.json", m2, tries=45)
    if not li:
        fails.append("liquidity-inflection never freshened")
        li = s3_json("data/liquidity-inflection.json") or {}
    onf = li.get("onshore_funding")
    rep.kv(onshore_funding=onf,
           liq_score=(li.get("composite") or {}).get("liquidity_score"))
    if not onf or onf.get("gcf_minus_tri_bp") is None:
        fails.append("onshore_funding block missing/empty: %s" % onf)
    if (li.get("composite") or {}).get("liquidity_score") is None:
        fails.append("liquidity composite regressed")

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3304 PASS — OFR wired into the funding stack: plumbing "
            "scores the interdealer premium, liquidity carries onshore "
            "context, MMF curation is grammar-driven (%d families)."
            % len(famS))
sys.exit(0)
