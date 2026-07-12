"""ops 3136 — Alpha Compass v2 polish: regime label + track-record quotes.

3135 landed the desk sheet (PASS) with two blemishes visible in its own
gates:
  1. regime headline resolved to a stringified DICT (RORO `posture` is an
     object; regime-composite's real fields are meta_regime /
     composite_score) → page showed a JSON-ish blob.
  2. Lambda-side FMP quotes returned empty (batch quirk) → track record
     never seeded (quotes_available=False, history 0 entries).

Lambda fixes already committed:
  • _lbl() dict-digger applied to every regime source + correct
    regime-composite candidates (meta_regime, composite_score)
  • fmp_quotes(): comma-safe encoding + per-symbol fallback

THIS OP redeploys (with aws/shared shim injection), invokes, and gates
STRICTLY: label must be a clean string; quotes must come back; history
must seed.
"""

import json
import shutil
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-alpha-compass"
OUT_KEY = "data/alpha-compass.json"
HIST_KEY = "data/alpha-compass-history.json"

HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]
SRC = AWS_DIR / "lambdas" / FN / "source"
CFG = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
DONOR = AWS_DIR / "lambdas" / "justhodl-buyback-engine" / "config.json"

S3 = boto3.client("s3", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3136_alpha_compass_polish") as rep:
    fails, warns = [], []

    def _fin():
        for w in warns:
            rep.warn(w)
        for f in fails:
            rep.fail(f)
        rep.kv(n_fails=len(fails), n_warns=len(warns),
               verdict="PASS" if not fails else "FAIL")

    t0 = datetime.now(timezone.utc)
    rep.heading("ops 3136 — Alpha Compass polish (regime label + quotes)")

    rep.section("1. Deploy (shim-injected)")
    fmp = (json.loads(DONOR.read_text()).get("environment") or {}) \
        .get("FMP_API_KEY", "")
    shim = AWS_DIR / "shared" / "_sentry_lite.py"
    if shim.exists():
        shutil.copy(shim, SRC / "_sentry_lite.py")
        rep.log("injected aws/shared/_sentry_lite.py")
    sched = CFG.get("schedule") or {}
    try:
        deploy_lambda(
            report=rep, function_name=FN, source_dir=SRC,
            env_vars={"FMP_API_KEY": fmp} if fmp else {},
            eb_rule_name=sched.get("rule_name"),
            eb_schedule=sched.get("cron"),
            timeout=CFG.get("timeout", 240), memory=CFG.get("memory", 512),
            description=CFG.get("description", ""),
        )
    except Exception as e:
        fails.append(f"deploy failed: {str(e)[:200]}")
        _fin()
        sys.exit(1)

    rep.section("2. Fresh output")
    doc = None
    deadline = time.time() + 240
    while time.time() < deadline:
        try:
            d = s3_json(OUT_KEY)
            if datetime.fromisoformat(d["generated_at"]) >= t0 \
                    and d.get("schema_version") == "2.0":
                doc = d
                break
        except Exception:
            pass
        time.sleep(8)
    if doc is None:
        fails.append("v2 output never freshened")
        _fin()
        sys.exit(1)
    rep.ok(f"fresh doc {doc['generated_at']}")

    rep.section("3. Strict gates")
    reg = doc.get("regime") or {}
    lbl = reg.get("label")
    if not isinstance(lbl, str) or not lbl or lbl == "Unknown" \
            or "{" in lbl or len(lbl) > 60:
        fails.append(f"regime label malformed: {str(lbl)[:90]!r}")
    else:
        rep.ok(f"regime label clean: {lbl!r} "
               f"(score={reg.get('score')}, "
               f"sources={len(reg.get('sources') or [])})")
    for s in reg.get("sources") or []:
        v = s.get("value")
        if not isinstance(v, (str, type(None))):
            fails.append(f"source {s.get('k')} value not a string: "
                         f"{str(v)[:60]!r}")
        else:
            rep.log(f"  · {s.get('label')}: {v} "
                    f"{'(' + str(s.get('score')) + ')' if s.get('score') is not None else ''}")

    tr = doc.get("track_record") or {}
    rep.kv(quotes_available=tr.get("quotes_available"),
           open_calls=tr.get("open_calls"))
    if tr.get("quotes_available"):
        rep.ok("Lambda-side FMP quotes live")
        try:
            h = s3_json(HIST_KEY)
            n = len(h.get("entries") or [])
            if n >= 1:
                rep.ok(f"track-record history seeded — {n} entries "
                       f"({tr.get('open_calls')} open)")
            else:
                warns.append("quotes live but history has 0 entries — check "
                             "primary-ticker resolution next run")
        except Exception as e:
            fails.append(f"history object unreadable: {e}")
    else:
        fails.append("FMP quotes still empty from Lambda — batch+singles "
                     "both failed; inspect CloudWatch next op")

    for c in doc.get("top_calls") or []:
        rep.log(f"  #{c['rank']} {c.get('subject')} conv={c.get('conviction')}"
                f" tier={(c.get('stats') or {}).get('source')}"
                f" kelly={(c.get('sizing') or {}).get('kelly_pct')}%"
                f" primary={(c.get('express') or {}).get('primary')}")

    _fin()
    sys.exit(0 if not fails else 1)
