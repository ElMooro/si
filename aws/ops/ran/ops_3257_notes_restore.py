"""ops 3257 — TV notes restored end-to-end.

  1. Deploy the crawler WITH the never-shrink guard first (nothing can
     re-clobber during recovery).
  2. RESTORE the mirror from the BRAIN: every tv-provenance note
     (id 'tv-*' / text '[TV:…]') merged by id with the current mirror —
     the brain kept everything even while the mirror sat at 10.
  3. LIVE CRAWL: invoke the crawler now — if the SSM session cookie is
     alive it pulls yesterday's NEW notes too; its log tail is printed
     either way (cookie state = the real cadence answer).
  4. notes-intel re-run → notes-index/notes-themes rebuilt on the full
     corpus; counts proven.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]
MIRROR = "data/tradingview-notes.json"


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def tail(fn, want, limit=200):
    out = []
    try:
        grp = f"/aws/lambda/{fn}"
        for stm in LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=2).get("logStreams") or []:
            for ev in LOGS.get_log_events(
                    logGroupName=grp, logStreamName=stm["logStreamName"],
                    limit=limit, startFromHead=False).get("events") or []:
                m = (ev.get("message") or "").strip()
                if any(k in m for k in want):
                    out.append(m[:140])
    except Exception:
        pass
    return out[-8:]


with report("3257_notes_restore") as rep:
    fails, warns = [], []
    rep.heading("ops 3257 — notes restored: guard → brain-restore → "
                "live crawl → intel rebuild")

    rep.section("1. Guarded crawler deployed first")
    fn = "justhodl-tv-notes-crawler"
    cfg = {}
    pc = AWS_DIR / "lambdas" / fn / "config.json"
    if pc.exists():
        cfg = json.loads(pc.read_text())
    sch = cfg.get("schedule")
    rule, cron = (sch.get("rule_name"), sch.get("cron")) \
        if isinstance(sch, dict) else (None, None)
    live = (LAM.get_function_configuration(FunctionName=fn)
            .get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=AWS_DIR / "lambdas" / fn / "source",
                      env_vars=live, eb_rule_name=rule, eb_schedule=cron,
                      timeout=cfg.get("timeout", 900),
                      memory=cfg.get("memory", 1024),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=fn, WaiterConfig={"Delay": 2, "MaxAttempts": 30})
    except Exception as e:
        fails.append(f"deploy crawler: {str(e)[:80]}")

    rep.section("2. Restore from the brain")
    cur = s3_json(MIRROR) or {"notes": []}
    cur_n = len(cur.get("notes") or [])
    brain = s3_json("data/brain.json") or {}
    bn = brain.get("notes") if isinstance(brain, dict) else brain
    bn = bn or []
    tv = [n for n in bn if isinstance(n, dict)
          and (str(n.get("id", "")).startswith("tv-")
               or str(n.get("text", "")).startswith("[TV:"))]
    rep.kv(mirror_before=cur_n, brain_notes=len(bn),
           brain_tv_notes=len(tv))
    if not fails and tv:
        by_id = {n.get("id"): n for n in (cur.get("notes") or [])
                 if n.get("id")}
        added = 0
        for n in tv:
            if n.get("id") not in by_id:
                by_id[n["id"]] = n
                added += 1
        merged = sorted(by_id.values(),
                        key=lambda x: x.get("created") or 0)
        S3.put_object(Bucket=BUCKET, Key=MIRROR,
                      Body=json.dumps(
                          {"generated_at":
                           datetime.now(timezone.utc).isoformat(),
                           "source": "brain-restore (ops 3257)",
                           "n": len(merged), "notes": merged},
                          ensure_ascii=False, default=str)
                      .encode("utf-8"),
                      ContentType="application/json; charset=utf-8")
        rep.ok(f"mirror restored: {cur_n} → {len(merged)} "
               f"(+{added} from brain)")
    elif not tv:
        warns.append("brain carries no tv-provenance notes at "
                     "data/brain.json — restore skipped")

    rep.section("3. Live crawl (cookie state = cadence)")
    LAM.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
    time.sleep(100)
    for ln in tail(fn, ("[crawler]", "403", "cookie", "Cloudflare",
                        "error", "ERROR")):
        rep.log("  " + ln)
    m2 = s3_json(MIRROR) or {}
    rep.kv(mirror_after_crawl=len(m2.get("notes") or []),
           mirror_source=str(m2.get("source"))[:40])

    rep.section("4. notes-intel rebuild")
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-notes-intel",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        warns.append(f"notes-intel invoke: {str(e)[:60]}")
    ni = None
    for _ in range(30):
        time.sleep(8)
        d = s3_json("data/notes-index.json") or {}
        if str(d.get("generated_at", "")) > mark:
            ni = d
            break
    if ni:
        nt = ni.get("tickers") or ni.get("index") or {}
        rep.ok(f"notes-index rebuilt: {len(nt)} tickers, "
               f"n_notes={ni.get('n_notes')}")
    else:
        warns.append("notes-index not fresh in window")

    for w in warns:
        rep.warn(w)
    final = len((s3_json(MIRROR) or {}).get("notes") or [])
    if final < 1000:
        fails.append(f"mirror still small after restore+crawl: {final}")
    rep.kv(mirror_final=final, n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
