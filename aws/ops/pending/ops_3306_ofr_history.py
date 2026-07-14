"""ops 3306 — OFR HISTORY LAYER (Khalid: every series charted vs history,
from 1990 where the source reaches).
[1] ofr-stfm v1.3: OFR Financial Stress Index CSV — FULL history since
    2000 + 5 components (credit/equity/funding/safe/vol) into doc.fsi;
    NEW charts shard data/history/ofr-stfm-charts.json packing every
    repo/MMF/NYPD series ('d' recent native + 'w' monthly deep tail;
    fails weekly since 1990).
[2] ofr.html STFM tab (additive): FSI hero chart since 2000 + component
    tiles, Treasury-fails chart since 1990 (FtD vs FtR), venue-volume +
    MMF-family history charts, and click-any-row-to-chart in the
    202-series browser.
Also flags: the legacy 42-tile Overview hits a dead API GW and falls
back to Math.random mock data — surfaced to Khalid for a rebuild call."""
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
            "User-Agent": "justhodl-ops-3306"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return ""


with report("3306_ofr_history") as rep:
    fails, warns = [], []

    rep.section("1. Deploy + run ofr-stfm v1.3")
    cfg = LAM.get_function_configuration(FunctionName="justhodl-ofr-stfm")
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name="justhodl-ofr-stfm",
                  source_dir=AWS_DIR / "lambdas" / "justhodl-ofr-stfm" / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=300, memory=1536,
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

    rep.section("2. Verify FSI block")
    fsi = of.get("fsi") or {}
    rep.kv(version=of.get("version"), fsi_latest=fsi.get("latest"),
           fsi_start=fsi.get("start"), fsi_n=fsi.get("n_obs"),
           fsi_pctile_full=fsi.get("pctile_full"),
           fsi_components=fsi.get("components"),
           fails_cross_start={k: (v or {}).get("start")
                              for k, v in (of.get("nypd_fails_cross")
                                           or {}).items()
                              if isinstance(v, dict)})
    if not fsi.get("latest"):
        fails.append("fsi block missing")
    elif str(fsi.get("start", "9999")) > "2001-01-01":
        fails.append("fsi history starts too late: %s" % fsi.get("start"))
    if len(fsi.get("components") or {}) < 4:
        fails.append("fsi components %s" % fsi.get("components"))

    rep.section("3. Verify charts shard")
    ch = s3_json("data/history/ofr-stfm-charts.json") or {}
    ser = ch.get("series") or {}
    fd = ser.get("NYPD-PD_AFtD_TOT-A") or {}
    fd_all = dict(list((fd.get("w") or {}).items())
                  + list((fd.get("d") or {}).items()))
    fd_start = min(fd_all) if fd_all else None
    fsi_pack = ser.get("FSI") or {}
    fsi_all = dict(list((fsi_pack.get("w") or {}).items())
                   + list((fsi_pack.get("d") or {}).items()))
    rep.kv(shard_n_series=ch.get("n_series"),
           fails_chart_start=fd_start, fails_chart_pts=len(fd_all),
           fsi_chart_pts=len(fsi_all),
           shard_has_fsi_components=sum(1 for k in ser
                                        if k.startswith("FSI_")))
    if (ch.get("n_series") or 0) < 150:
        fails.append("charts shard series %s < 150" % ch.get("n_series"))
    if not fd_start or fd_start > "1996-01-01":
        fails.append("fails history start %s (expected early 1990s)"
                     % fd_start)
    if len(fsi_all) < 400:
        fails.append("FSI chart pts %d < 400" % len(fsi_all))

    rep.section("4. Live page markers")
    time.sleep(50)
    marks = ("stfmFsiChart", "stfmFailsChart", "ofr-stfm-charts.json",
             "data-mn=")
    raw = http("https://justhodl.ai/ofr.html?ops=3306")
    ok = all(m in raw for m in marks)
    if not ok:
        for _ in range(8):
            time.sleep(30)
            raw = http("https://justhodl.ai/ofr.html?r=%d" % time.time())
            if all(m in raw for m in marks):
                ok = True
                break
    rep.kv(ofr_page_markers=ok)
    if not ok:
        fails.append("history-chart markers missing on live ofr.html")

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3306 PASS — FSI charted since %s, fails since %s, "
            "%s series clickable-to-chart."
            % (fsi.get("start"), fd_start, ch.get("n_series")))
sys.exit(0)
