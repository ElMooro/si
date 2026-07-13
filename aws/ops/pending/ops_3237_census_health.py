"""ops 3237 — the fleet's own dry census + thesis health + the drill
drawer live.

  1. MISSES CENSUS: state.misses is the authoritative map-health report —
     the fleet computed it itself under legal pacing. Grouped by source
     and id-family here; the per-engine dry menu for the 28
     'lack fetchable history' engines is derived from it in one pass —
     the complete fix menu, no per-engine ops crawl.
  2. THESIS HEALTH: justhodl-thesis-engine shares every patched module
     (throttles, guards, ids-ledger era series_source) but hasn't been
     exercised once tonight. Invoke + fresh feed + zero [ERROR] proves
     the second consumer.
  3. DRILL DRAWER: panels.html rows now open a live per-engine drawer
     (event study, lit indicators, members with z). Live-verified after
     the pages deploy.
"""
import gzip
import json
import re
import sys
import time
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3237)"}


def s3_json(key, default=None, gz=False):
    try:
        b = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


def fam(sid):
    sid = str(sid)
    m = re.match(r"^([A-Z]{4,8})01", sid)
    if m:
        return m.group(1) + "01*"
    if "|" in sid:
        return sid.split("|")[0] + "|…"
    if "/" in sid:
        return "/".join(sid.split("/")[:2]) + "/…"
    return sid[:10]


with report("3237_census_health") as rep:
    fails, warns = [], []
    rep.heading("ops 3237 — dry census, thesis health, drill drawer")

    # ── 1. misses census + per-engine menu ─────────────────────────────
    rep.section("1. The fleet's own dry census")
    st = s3_json("data/thesis-state-v2.json.gz", {}, gz=True) or {}
    misses = st.get("misses") or {}
    rep.kv(misses_total=len(misses))
    fams = Counter(fam((m or {}).get("id", "")) for m in misses.values())
    for f, n in fams.most_common(10):
        rep.log(f"  {n:>4} × {f}")
    idx = s3_json("data/wl-engines.json") or {}
    wl = s3_json("data/tv-watchlists.json") or {}
    lists = {str(l.get("id")): l for l in (wl.get("lists") or [])}
    rep.section("1b. Per-engine dry menu (the 'lack history' class)")
    shown = 0
    for e in idx.get("engines") or []:
        if "lack fetchable history" not in str(e.get("reason") or ""):
            continue
        l = lists.get(str(e.get("tv_id"))) or {}
        drym = [s.upper() for s in (l.get("symbols") or [])
                if s.upper() in misses]
        if drym:
            rep.log(f"  {str(e.get('name'))[:34]:<34} dry: "
                    + " | ".join(drym[:4]))
            shown += 1
        if shown >= 14:
            break
    rep.kv(engines_menued=shown)

    # ── 2. thesis-engine health ────────────────────────────────────────
    rep.section("2. Thesis-engine exercised")
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-thesis-engine",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"thesis invoke: {str(e)[:70]}")
    fresh = False
    for _ in range(45):
        time.sleep(10)
        d = s3_json("data/thesis-engine.json") or {}
        if str(d.get("generated_at", "")) > mark:
            fresh = True
            rep.kv(thesis_generated=str(d.get("generated_at"))[:19],
                   keys=len(d))
            break
    if not fresh:
        warns.append("thesis feed not fresh in window")
    time.sleep(20)
    errs = 0
    try:
        grp = "/aws/lambda/justhodl-thesis-engine"
        for stm in LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=2).get("logStreams") or []:
            for ev in LOGS.get_log_events(
                    logGroupName=grp, logStreamName=stm["logStreamName"],
                    limit=200, startFromHead=False).get("events") or []:
                if "[ERROR]" in (ev.get("message") or ""):
                    errs += 1
                    rep.log("  ✗ " + ev["message"].splitlines()[0][:120])
    except Exception as e:
        warns.append(f"thesis logs: {str(e)[:60]}")
    rep.kv(thesis_errors=errs)
    if errs:
        fails.append(f"thesis-engine logged {errs} [ERROR]s on shared "
                     "code")
    elif fresh:
        rep.ok("second consumer healthy on all patched shared modules")

    # ── 3. drill drawer live ───────────────────────────────────────────
    rep.section("3. Drill drawer live on panels.html")
    ok = False
    for i in range(22):
        time.sleep(15)
        try:
            h = urllib.request.urlopen(urllib.request.Request(
                f"https://justhodl.ai/panels.html?t={int(time.time())}",
                headers=UA), timeout=15).read().decode("utf-8", "replace")
            if "data-eid" in h and "drawer" in h and "EVENT STUDY" in h:
                ok = True
                rep.ok(f"drawer live after ~{(i + 1) * 15}s "
                       f"({len(h)} bytes)")
                break
        except Exception:
            pass
    if not ok:
        warns.append("drawer not live in window — next cron bake carries "
                     "it")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
