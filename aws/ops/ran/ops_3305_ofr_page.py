"""ops 3305 — OFR page enrichment + display fixes. (rerun b: all-series stats)
[1] ofr-stfm v1.2: publish stats for ALL 160 repo + 42 MMF series
    (browser-grade; caps dropped).
[2] ofr.html: new additive Short-Term Funding tab — venue tiles with
    interdealer premium, MMF family cards, settlement-fails table
    (regime + per-class FtD/FtR + spikes), full series browser sorted
    by |z|, link to the dealers desk.
[3] primary-dealers.html: MMF tiles were printing raw dollars —
    shared jhUsd auto-scaler; fails regime pill read the wrong key
    (signal.regime)."""
import json
import sys
import time
import urllib.request
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


def http(url, timeout=25):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "justhodl-ops-3305"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return ""


with report("3305_ofr_page") as rep:
    fails, warns = [], []

    rep.section("1. ofr-stfm v1.2 (full series)")
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
        fails.append("ofr-stfm never freshened")
        of = of or {}
    n_repo = len(((of.get("repo") or {}).get("series")) or {})
    n_mmf = len(((of.get("mmf") or {}).get("series")) or {})
    rep.kv(version=of.get("version"), repo_series_published=n_repo,
           mmf_series_published=n_mmf,
           health=of.get("health"))
    if n_repo < 100:
        fails.append("repo series published %d < 100" % n_repo)
    if n_mmf < 30:
        fails.append("mmf series published %d < 30" % n_mmf)

    rep.section("2. Fails signal schema sanity")
    sf = s3_json("data/settlement-fails.json") or {}
    sg = sf.get("signal") or {}
    rep.kv(signal_regime=sg.get("regime"), signal_score=sg.get("score"),
           n_classes=len(sf.get("classes") or []))
    if not sg.get("regime"):
        fails.append("settlement-fails signal.regime missing")

    rep.section("3. Live page markers")
    time.sleep(60)  # pages deploy
    for url, marks in (
        ("https://justhodl.ai/ofr.html?ops=3305",
         ("data-tab=\"stfm\"", "stfmBrowser", "Short-Term Funding")),
        ("https://justhodl.ai/primary-dealers.html?ops=3305",
         ("jhUsd", "SF.signal"))):
        raw = http(url)
        ok = all(m in raw for m in marks)
        if not ok:
            for _ in range(8):
                time.sleep(30)
                raw = http(url + "&r=%d" % time.time())
                if all(m in raw for m in marks):
                    ok = True
                    break
        rep.kv(**{url.split("/")[-1].split("?")[0].replace(".", "_")
                  + "_markers": ok})
        if not ok:
            fails.append("markers missing on %s" % url)

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3305 PASS — OFR page carries the Short-Term Funding tab "
            "(venues, MMF families, fails table, %d-series browser); "
            "dealer-desk display fixed." % (n_repo + n_mmf))
sys.exit(0)
