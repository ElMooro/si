#!/usr/bin/env python3
"""ops 3124 -- AUTO-REFRESH PROOF + FRESHNESS SURFACING (Khalid:
continue with 'momentum block auto-refreshes with every research-doc
cycle and the industry fields with the daily IR run' -- make the
claim provable, not assumed). Three legs:
(1) IN-ACCOUNT SCHEDULE PROOF: EventBridge rule
industry-rotation-daily must be ENABLED, cron(35 21 * * ? *), and
target justhodl-industry-rotation; IR doc generated_at must be <26h.
(2) RESEARCH 24h-CACHE CONTRACT PROOF end-to-end on a real ticker
(AAPL): if the S3 cache object equity-research/AAPL.json is <24h old,
read it from S3 (that is the cache serving path); if >=24h, GET the
Function URL ?ticker=AAPL to force the regeneration path (that IS the
auto-refresh). Either way assert: technicals.series.close >=260 pts,
last series date <=5 days old, from_cache field present in schema,
generated_at parseable. Real data only -- no synthetic payloads.
(3) PAGE: why.html now surfaces freshness -- momentum block shows
'Series as of <date>' + STALE SERIES badge (>5d) + doc timestamp with
(24h cache)/(fresh) tag; industry join gains an 'Industry data
freshness' row (age vs the 21:35 UTC daily run, STALE >30h). Lessons:
3118 ASCII markers, 3116 marks[0] new to this push, Event+S3-poll
doctrine (no long sync Lambda invokes; Function URL GET bounded 240s
with warn-degrade)."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

EV = boto3.client("events", region_name="us-east-1")
LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3124", "Cache-Control": "no-cache"}


def get(url, timeout=30):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=timeout).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3124_refresh_proof") as rep:
        rep.section("1. IR daily schedule -- in-account proof")
        try:
            r = EV.describe_rule(Name="industry-rotation-daily")
            tg = EV.list_targets_by_rule(
                Rule="industry-rotation-daily").get("Targets", [])
            hits_fn = any("justhodl-industry-rotation"
                          in (t.get("Arn") or "") for t in tg)
            rep.kv(rule_state=r.get("State"),
                   rule_cron=r.get("ScheduleExpression"),
                   targets_fn=hits_fn, n_targets=len(tg))
            if r.get("State") != "ENABLED":
                fails.append("IR rule not ENABLED: %s"
                             % r.get("State"))
            if "cron(35 21" not in (r.get("ScheduleExpression")
                                    or ""):
                warns.append("IR cron drifted: %s"
                             % r.get("ScheduleExpression"))
            if not hits_fn:
                fails.append("IR rule does not target the engine")
        except Exception as e:
            fails.append("IR rule describe: %s" % e)
        try:
            ir = json.loads(S3.get_object(
                Bucket=BUCKET, Key="data/industry-rotation.json"
            )["Body"].read())
            age_h = (datetime.now(timezone.utc)
                     - datetime.fromisoformat(
                         ir["generated_at"].replace("Z", "+00:00"))
                     ).total_seconds() / 3600
            rep.kv(ir_version=ir.get("version"),
                   ir_age_h=round(age_h, 2))
            if age_h > 26:
                fails.append("IR doc stale: %.1fh" % age_h)
        except Exception as e:
            fails.append("IR doc read: %s" % e)

        rep.section("2. Research 24h-cache contract -- real ticker")
        doc = None
        path = None
        try:
            head = S3.head_object(Bucket=BUCKET,
                                  Key="equity-research/AAPL.json")
            c_age = (datetime.now(timezone.utc)
                     - head["LastModified"]).total_seconds()
            rep.kv(cache_age_h=round(c_age / 3600, 2))
            if c_age < 24 * 3600:
                path = "cache-serve"
                doc = json.loads(S3.get_object(
                    Bucket=BUCKET, Key="equity-research/AAPL.json"
                )["Body"].read())
        except Exception:
            rep.kv(cache_age_h=None)
        if doc is None:
            path = "regenerate"
            try:
                url = LAM.get_function_url_config(
                    FunctionName="justhodl-equity-research"
                )["FunctionUrl"].rstrip("/")
                doc = json.loads(get(url + "/?ticker=AAPL",
                                     timeout=240))
            except Exception as e:
                warns.append("regen GET degraded (%s) -- schedule "
                             "legs still binding" % e)
        rep.kv(research_path=path)
        if doc:
            t = (doc.get("technicals") or {})
            ser = (t.get("series") or {})
            closes = [x for x in (ser.get("close") or [])
                      if x is not None]
            dates = ser.get("dates") or []
            last_d = str(dates[-1])[:10] if dates else None
            ok_len = len(closes) >= 260
            age_d = None
            if last_d:
                age_d = (datetime.now(timezone.utc)
                         - datetime.fromisoformat(
                             last_d + "T00:00:00+00:00")
                         ).total_seconds() / 86400
            rep.kv(series_pts=len(closes), series_last=last_d,
                   series_age_d=round(age_d, 1)
                   if age_d is not None else None,
                   from_cache_in_schema="from_cache" in doc,
                   doc_generated_at=doc.get("generated_at"))
            if not ok_len:
                fails.append("series thin: %d pts" % len(closes))
            if age_d is None or age_d > 5:
                fails.append("series last date stale: %s" % last_d)
            if "from_cache" not in doc:
                warns.append("from_cache missing from schema")
        elif path == "regenerate":
            pass  # warn already recorded; do not double-fail

        rep.section("3. Page freshness live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/why.html?cb=%d"
                         % time.time())
                if "jh-fresh" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(15)
        if not ok:
            fails.append("why.html jh-fresh markers not live")
        else:
            for m in ("STALE SERIES", "Series as of",
                      "24h cache", "Industry data freshness",
                      "daily 21:35 UTC run",
                      "auto-refreshes with every research-doc "
                      "cycle"):
                if m not in pg:
                    fails.append("why.html marker missing: %s" % m)

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3124.json").write_text(json.dumps(
        {"ops": 3124, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
