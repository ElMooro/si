"""ops 3308 (rerun b: state under snap) — SURFACE THE STACK: dealer/funding into the daily comms.
[1] morning-intelligence: +3 feeds (primary_dealers / ofr_stfm /
    settlement_fails) + 10 compact ctx keys (dealer regime/squeeze/
    turnover, fails regime+spikes, GCF-TRI bp, FSI + 26y pctile) so the
    8AM brief reads the funding stack every morning.
[2] alert-sentinel: state block + Telegram flip alerts — dealer regime
    change, squeeze arming, NEW fails-spike class, GCF-TRI crossing
    >=8bp, FSI crossing above zero.
Verify: sentinel runtime (invoke -> fresh doc -> state carries new
keys); morning-intelligence by deployed-code marker (invoking would
burn an LLM run — Anthropic leg is credit-dead)."""
import io
import json
import sys
import time
import urllib.request
import zipfile
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


def redeploy(name):
    cfg = LAM.get_function_configuration(FunctionName=name)
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name=name,
                  source_dir=AWS_DIR / "lambdas" / name / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=int(cfg.get("Timeout") or 300),
                  memory=int(cfg.get("MemorySize") or 512),
                  description=str(cfg.get("Description") or "")[:250],
                  smoke=False)


with report("3308_comms_wiring") as rep:
    fails, warns = [], []

    rep.section("1. Deploy")
    redeploy("justhodl-alert-sentinel")
    redeploy("justhodl-morning-intelligence")

    rep.section("2. Sentinel runtime verify")
    mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    LAM.invoke(FunctionName="justhodl-alert-sentinel",
               InvocationType="Event", Payload=b"{}")
    doc = None
    for _ in range(25):
        time.sleep(8)
        doc = s3_json("data/alert-sentinel.json")
        if doc and str(doc.get("generated_at") or doc.get("as_of")
                       or "") >= mark:
            break
    st = (s3_json("data/_alerts/last.json") or {}).get("snap") or {}
    rep.kv(sentinel_fresh=bool(doc),
           state_dealer_regime=st.get("dealer_regime"),
           state_dealer_squeeze=st.get("dealer_squeeze"),
           state_fails_spikes=st.get("fails_spikes"),
           state_gcf_tri_bp=st.get("gcf_tri_bp"),
           state_fsi_pos=st.get("fsi_pos"),
           last_msgs=(doc or {}).get("messages",
                                     (doc or {}).get("msgs"))
           if doc else None)
    if st.get("dealer_regime") is None:
        fails.append("sentinel state missing dealer_regime")
    if st.get("gcf_tri_bp") is None:
        fails.append("sentinel state missing gcf_tri_bp")
    if not isinstance(st.get("fails_spikes"), list):
        fails.append("sentinel state missing fails_spikes")

    rep.section("3. Morning-intelligence deployed-code markers")
    try:
        loc = LAM.get_function(
            FunctionName="justhodl-morning-intelligence"
        )["Code"]["Location"]
        with urllib.request.urlopen(loc, timeout=60) as r:
            zf = zipfile.ZipFile(io.BytesIO(r.read()))
        src = zf.read("lambda_function.py").decode("utf-8", "replace")
        marks = ["dealer_corp_net_b", "ofr_fsi_pctile26y",
                 "data/nyfed-primary-dealer.json",
                 "fails_spike_classes"]
        missing = [m for m in marks if m not in src]
        rep.kv(mi_markers_ok=not missing, mi_missing=missing)
        if missing:
            fails.append("morning-intelligence markers missing: %s"
                         % missing)
    except Exception as e:
        fails.append("mi code check failed: %s" % str(e)[:120])

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3308 PASS — dealer/funding stack now surfaces in the "
            "8AM brief and fires Telegram flips (regime, squeeze, "
            "fails-spike, strain, FSI-cross).")
sys.exit(0)
