"""ops 3300 — Pillar 6 verdict fix (Khalid: BMNR chart goes vertical but
badge said STABLE). Root cause: verdict graded ONLY fiscal-year
weighted-average shares (c1), blind to an in-progress explosion the
quarterly series (which the page charts) and the live float already
show. Fix: engine grades the WORST of fy_1y / qtr_yoy / qtr_6m_ann /
live-float-vs-FY (corroboration-gated to filter split artifacts) and
exposes dilution_key_pct + source; page adds a verdict guard computed
from the SAME series it plots (stale 24h caches self-correct
instantly) and the verdict pill next to the chart FLASHES RED via
.jh-flashred when risky."""
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

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-equity-research"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION,
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
            "User-Agent": "justhodl-ops-3300"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return ""


with report("3300_dilution_verdict_fix") as rep:
    fails, warns = [], []

    live = LAM.get_function_configuration(FunctionName=FN)
    env = (live.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=int(live.get("Timeout") or 300),
                  memory=int(live.get("MemorySize") or 1024),
                  description=str(live.get("Description") or "")[:250],
                  smoke=False)
    mark = datetime.now(timezone.utc)

    rep.section("2. Force-refresh BMNR + AAPL through the fixed grader")
    got = {}
    for tk in ("BMNR", "AAPL"):
        try:
            LAM.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps({"_internal": "1", "ticker": tk,
                                           "force_refresh": True}).encode())
        except Exception as e:
            warns.append("invoke %s: %s (falling back to cache poll)"
                         % (tk, str(e)[:120]))
        key = "equity-research/%s.json" % tk
        doc = None
        for _ in range(25):
            try:
                h = S3.head_object(Bucket=BUCKET, Key=key)
                if h["LastModified"].replace(tzinfo=timezone.utc) >= mark:
                    doc = s3_json(key)
                    break
            except Exception:
                pass
            time.sleep(12)
        if not doc:
            fails.append("%s cache never freshened" % tk)
            continue
        got[tk] = (doc.get("dilution") or {})

    b = got.get("BMNR") or {}
    rep.kv(bmnr_verdict=b.get("verdict"), bmnr_risk=b.get("risk_flag"),
           bmnr_key=b.get("dilution_key_pct"),
           bmnr_key_source=b.get("dilution_key_source"),
           bmnr_qtr_yoy=b.get("sh_qtr_yoy_pct"),
           bmnr_live_vs_fy=b.get("live_float_vs_fy_pct"))
    if b.get("verdict") not in ("HEAVY_DILUTION", "DEATH_SPIRAL"):
        fails.append("BMNR verdict %s — expected HEAVY/DEATH"
                     % b.get("verdict"))
    if not b.get("risk_flag"):
        fails.append("BMNR risk_flag not set")

    a = got.get("AAPL") or {}
    rep.kv(aapl_verdict=a.get("verdict"), aapl_risk=a.get("risk_flag"),
           aapl_key=a.get("dilution_key_pct"))
    if a.get("verdict") not in ("SHRINKING", "STABLE"):
        fails.append("AAPL regression: verdict %s" % a.get("verdict"))
    if a.get("risk_flag"):
        fails.append("AAPL regression: risk_flag set")

    rep.section("3. Page guard markers")
    raw = http("https://raw.githubusercontent.com/ElMooro/si/main/why.html")
    for mk in ("ops 3300", "verdict guard", "T[comp]"):
        if mk not in raw:
            fails.append("marker %r missing from repo why.html" % mk)
    if raw.count("jh-flashred") < 5:
        fails.append("flashred pill wiring missing (count=%d)"
                     % raw.count("jh-flashred"))
    live_ok = False
    for _ in range(16):
        pg = http("https://justhodl.ai/why.html?ops=3300")
        if "verdict guard" in pg:
            live_ok = True
            break
        time.sleep(30)
    rep.kv(live_page_marker=live_ok)
    if not live_ok:
        warns.append("live why.html not showing guard yet (CDN lag) — "
                     "repo copy verified")

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3300 PASS — BMNR now flagged %s (key %.1f%% via %s); "
            "badge can never contradict the chart again."
            % (b.get("verdict"), b.get("dilution_key_pct") or 0,
               b.get("dilution_key_source")))
sys.exit(0)
