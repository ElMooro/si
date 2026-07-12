"""ops 3135 — Alpha Compass v2.0: land the desk sheet.

WHY
───
alpha-compass.html rendered a dash-wall: regime "Unknown" (read a key no
engine produces), every stat null (joins used family tokens against
signal_type/ticker vocabularies). v2 lambda fixes every join against real
schemas and adds regime fusion, expression layer, stats ladder, theme
quarter-Kelly, self-grading track record, run deltas. Page rebuilt.

THIS OP
───────
  1. Deploy justhodl-alpha-compass v2 via house helpers (+FMP env read
     from an existing repo config — no new key copies anywhere).
  2. Ensure the 3h EventBridge rule.
  3. Smoke invoke → poll S3 for fresh schema-2.0 output.
  4. QUALITY GATES on the actual payload (regime resolved, expression on
     every top call, stats tier + sizing on every card, track/changes
     blocks live, history object created).
  5. CDN check on the rebuilt page (warn-only).
"""

import json
import os
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
PAGE_URL = "https://justhodl.ai/alpha-compass.html"

HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]
SRC = AWS_DIR / "lambdas" / FN / "source"
CFG = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
DONOR = AWS_DIR / "lambdas" / "justhodl-buyback-engine" / "config.json"

S3 = boto3.client("s3", region_name=REGION)

rep = report(3135, "alpha_compass_v2")
fails, warns = [], []


def kvrow(**kw):
    rep.kv(**kw)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


def _fin():
    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    kvrow(n_fails=len(fails), n_warns=len(warns),
          verdict="PASS" if not fails else "FAIL")


t0 = datetime.now(timezone.utc)
rep.heading("ops 3135 — Alpha Compass v2.0 (desk sheet)")

# ── 1. FMP key from existing repo config (no new copies) ───────────────
rep.section("1. FMP key sourcing + deploy")
fmp = ""
try:
    fmp = (json.loads(DONOR.read_text()).get("environment") or {}) \
        .get("FMP_API_KEY", "")
except Exception as e:
    warns.append(f"donor config unreadable: {e}")
if fmp:
    try:
        u = ("https://financialmodelingprep.com/stable/quote?symbol=SPY"
             f"&apikey={fmp}")
        with urllib.request.urlopen(
                urllib.request.Request(u, headers={"User-Agent": "ops-3135"}),
                timeout=10) as r:
            rows = json.loads(r.read().decode())
        px = rows[0].get("price") if isinstance(rows, list) and rows else None
        if px:
            rep.ok(f"FMP key live (SPY={px})")
        else:
            warns.append("FMP probe returned no price — track record will "
                         "run price-less until key fixed")
    except Exception as e:
        warns.append(f"FMP probe failed ({str(e)[:80]}) — env set anyway, "
                     "lambda degrades gracefully")
else:
    warns.append("no FMP key found in donor config — track record disabled")

env_vars = {"FMP_API_KEY": fmp} if fmp else {}
sched = CFG.get("schedule") or {}
try:
    deploy_lambda(
        report=rep, function_name=FN, source_dir=SRC,
        env_vars=env_vars,
        eb_rule_name=sched.get("rule_name"),
        eb_schedule=(f"cron({sched['cron'].split('(', 1)[1]}"
                     if sched.get("cron", "").startswith("cron(")
                     else sched.get("cron")),
        timeout=CFG.get("timeout", 240), memory=CFG.get("memory", 512),
        description=CFG.get("description", ""),
    )
except Exception as e:
    fails.append(f"deploy failed: {str(e)[:200]}")
    _fin()
    sys.exit(1)

# ── 2. Poll for fresh schema-2.0 output ─────────────────────────────────
rep.section("2. Poll S3 for fresh v2 output")
doc = None
deadline = time.time() + 300
while time.time() < deadline:
    try:
        d = s3_json(OUT_KEY)
        gen = datetime.fromisoformat(d["generated_at"])
        if gen >= t0 and d.get("schema_version") == "2.0":
            doc = d
            break
        # smoke invoke ran inside deploy; re-invoke once if stale v1 remains
    except Exception:
        pass
    time.sleep(10)
if doc is None:
    fails.append("v2 output never freshened in S3")
    _fin()
    sys.exit(1)
rep.ok(f"fresh v2 doc generated_at={doc['generated_at']} "
       f"elapsed_s={doc.get('elapsed_s')}")

# ── 3. Quality gates on the payload ─────────────────────────────────────
rep.section("3. Quality gates")
reg = doc.get("regime") or {}
n_src = len(reg.get("sources") or [])
if not reg.get("label") or reg["label"] == "Unknown":
    fails.append("regime still Unknown — fusion failed")
elif n_src == 0:
    fails.append("regime has zero sources")
else:
    rep.ok(f"regime={reg['label']} sources={n_src} "
           f"risk_mult={reg.get('risk_multiplier')}")
    if n_src < 3:
        warns.append(f"only {n_src} regime sources resolved — check "
                     "upstream freshness")

tops = doc.get("top_calls") or []
watch = doc.get("watchlist") or []
cards = tops + watch
if not tops:
    fails.append("zero top calls — conviction upstream empty?")
kvrow(top_calls=len(tops), watchlist=len(watch))

tiers = {}
for c in cards:
    st = (c.get("stats") or {})
    src = st.get("source")
    tiers[src] = tiers.get(src, 0) + 1
    if not src:
        fails.append(f"card '{c.get('subject')}' missing stats tier")
    sz = c.get("sizing") or {}
    if sz.get("kelly_pct") is None:
        fails.append(f"card '{c.get('subject')}' missing sizing")
kvrow(tiers=json.dumps(tiers))
if tiers and set(tiers) == {"prior"}:
    warns.append("ALL cards at prior tier — magdist/scorecard vocab "
                 "produced zero matches; investigate stacks next op")

for c in tops:
    ex = c.get("express") or {}
    if not (ex.get("vehicles") or ex.get("names")):
        warns.append(f"top call '{c.get('subject')}' has no expression "
                     "(vehicles/names empty)")
    else:
        rep.log(f"  #{c['rank']} {c.get('subject')} conv={c.get('conviction')}"
                f" tier={(c.get('stats') or {}).get('source')}"
                f" kelly={(c.get('sizing') or {}).get('kelly_pct')}%"
                f" primary={ex.get('primary')}")

for blk in ("track_record", "changes", "coverage", "source_feeds"):
    if blk not in doc:
        fails.append(f"payload missing block: {blk}")
tr = doc.get("track_record") or {}
kvrow(track_open=tr.get("open_calls"),
      track_quotes=tr.get("quotes_available"),
      graded_this_run=tr.get("graded_this_run"))
if tr.get("quotes_available") is False and fmp:
    warns.append("quotes unavailable at runtime despite key — check FMP "
                 "from Lambda network")

try:
    h = s3_json(HIST_KEY)
    rep.ok(f"history object live — {len(h.get('entries') or [])} entries")
except Exception as e:
    fails.append(f"history object missing: {e}")

miss_feeds = [k for k, m in (doc.get("source_feeds") or {}).items()
              if not (m or {}).get("present")]
if miss_feeds:
    warns.append("upstream feeds absent: " + ", ".join(miss_feeds))

# ── 4. Page + CDN (warn-only) ───────────────────────────────────────────
rep.section("4. Page cutover (CDN, warn-only)")
try:
    req = urllib.request.Request(
        PAGE_URL + f"?t={int(time.time())}",
        headers={"User-Agent": "Mozilla/5.0 ops-3135",
                 "Cache-Control": "no-cache"})
    html = urllib.request.urlopen(req, timeout=15).read().decode(
        "utf-8", "replace")
    if "AC_SCHEMA_2.0" in html:
        rep.ok("justhodl.ai/alpha-compass.html serves the v2 page")
    else:
        warns.append("CDN still serving old page — GH Pages max-age=600 "
                     "self-heals within ~10 min")
except Exception as e:
    warns.append(f"page fetch failed: {str(e)[:80]}")

_fin()
sys.exit(0 if not fails else 1)
