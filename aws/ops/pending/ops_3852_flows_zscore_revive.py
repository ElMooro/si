"""
ops_3852 — flows.html: sector heatmap + top-10 inflows + top-10 outflows +
full-universe are all blank. Diagnose -> auto-fix the safe branch -> hard gate.

CONTRACT (established by repo grep, G0 style, not from memory):
  producer justhodl-etf-fund-flows  -> etf-flows/daily.json = {**meta, "metrics":[...]}
  consumer flows.html
      :649 heatmap       = daily.metrics filtered to 11 SPDR tickers
      :660 allMetrics    = daily.metrics.filter(m => m.flow_zscore_90d != null)
      :661 top/bottom 10 = sorts of allMetrics
      :683 full universe = sort of allMetrics
  => all four dead sections share ONE dependency: metrics[].flow_zscore_90d.
     The composite gauges above them read composite.json and render first,
     which is exactly why only these four died.

  producer path to that field (lambda_function.py:604-613):
      flow_zscore_90d stays None unless daily_flow is not None AND len(history)>=30
      history comes ONLY from api.polygon.io/etf-global/v1/fund-flows
      on ANY upstream failure the row degrades to
      {"ticker","error", signal_label:"DATA_MISSING"} - no z, no category,
      no flow_5d_usd. Silent. Page renders blanks instead of "broken".

BRANCHES:
  HEALTHY  S3 has z-scores and is fresh -> the break is downstream; this ops
           then compares the S3 artifact against what the CDN actually serves
           (page reads the worker, not S3) and reports the delta.
  A        Polygon alive but feed stale/empty -> re-arm rule, invoke, hard gate.
  B        Polygon entitlement dead -> STOP. Do NOT patch a fallback in.
           justhodl-etf-true-flows keeps only 25 days
           (etf-true-flows/source/lambda_function.py:143 `days = days[-25:]`)
           and the z gate needs >=30, so a 90d z CANNOT be honestly rebuilt
           from shares-deltas. That fix needs a page change (rank by %AUM with
           a visible banner) which must be a NORMAL sandbox commit, never a
           runner auto-commit ([skip-deploy] suppresses pages.yml).

This ops writes NO S3 data and fabricates NOTHING.
"""
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-etf-fund-flows"
BUCKET = "justhodl-dashboard-live"
DAILY = "etf-flows/daily.json"
COMPOSITE = "etf-flows/composite.json"
RULE = "justhodl-etf-fund-flows-daily"
CDN = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=890, retries={"max_attempts": 0}))
events = boto3.client("events", region_name="us-east-1")
cw = boto3.client("cloudwatch", region_name="us-east-1")

SPDR = ["XLK", "XLF", "XLE", "XLV", "XLP", "XLY", "XLI", "XLB", "XLU", "XLRE", "XLC"]
PAGE_KEYS = ["ticker", "category", "subcategory", "ref_sector", "smart_money",
             "signal_label", "flow_zscore_90d", "daily_flow_usd", "flow_5d_usd",
             "flow_21d_usd", "pct_aum_5d", "persistence_days"]


def s3_json(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def summarize(d):
    m = d.get("metrics") or []
    z = [r for r in m if r.get("flow_zscore_90d") is not None]
    spdr_ok = [t for t in SPDR
               if any(r.get("ticker") == t and r.get("flow_zscore_90d") is not None
                      for r in m)]
    return m, z, spdr_ok


def main():
    with report("3852_flows_zscore_revive") as rep:
        rep.heading("ops 3852 - flows.html z-score feed: diagnose + auto-fix + gate")
        fails = []

        # ── 1. the feed ─────────────────────────────────────────────────────
        rep.section("1. etf-flows/daily.json - freshness + z-score coverage")
        try:
            daily, lm = s3_json(DAILY)
        except Exception as e:
            rep.fail(f"  daily.json unreadable: {str(e)[:200]}")
            sys.exit(1)

        age_h = (datetime.now(timezone.utc) - lm).total_seconds() / 3600
        metrics, with_z, spdr_ok = summarize(daily)
        errs = [r for r in metrics if r.get("error")]
        hist = {}
        for r in errs:
            k = str(r.get("error"))[:70]
            hist[k] = hist.get(k, 0) + 1

        rep.kv(s3_last_modified=lm.isoformat(), age_hours=round(age_h, 1),
               generated_at=str(daily.get("generated_at")),
               n_ok=daily.get("n_ok"), universe_size=daily.get("universe_size"),
               n_metrics=len(metrics), n_with_zscore=len(with_z),
               n_error_rows=len(errs), spdr_with_z=len(spdr_ok))
        (rep.ok if with_z else rep.fail)(
            f"  {len(with_z)}/{len(metrics)} rows carry flow_zscore_90d "
            f"(page needs >0) - SPDR {len(spdr_ok)}/11")
        if hist:
            rep.log("  error histogram (root cause when z-count is 0):")
            for k, v in sorted(hist.items(), key=lambda x: -x[1]):
                rep.log(f"    {v:>4}x  {k}")
            for r in errs[:2]:
                rep.log(f"  sample error row: {json.dumps(r)[:350]}")

        rep.section("2. field-coverage - keys flows.html reads vs rows")
        for k in PAGE_KEYS:
            n = sum(1 for r in metrics if r.get(k) is not None)
            (rep.ok if n else rep.fail)(f"  {k:<20} {n}/{len(metrics)}")

        rep.section("3. composite.json (drives the gauges that still render)")
        try:
            comp, lm2 = s3_json(COMPOSITE)
            ch = (datetime.now(timezone.utc) - lm2).total_seconds() / 3600
            filled = sum(1 for v in (comp.get("composite") or {}).values()
                         if isinstance(v, dict) and v.get("score") is not None)
            rep.ok(f"  composite.json {ch:.1f}h old - {filled} gauges with a score")
        except Exception as e:
            rep.fail(f"  composite.json: {str(e)[:150]}")

        # ── 2. schedule + run history ───────────────────────────────────────
        rep.section("4. schedule - declared is not live")
        rule_state = "MISSING"
        try:
            r = events.describe_rule(Name=RULE)
            rule_state = r.get("State")
            tg = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
            (rep.ok if rule_state == "ENABLED" else rep.fail)(
                f"  {RULE}: {rule_state} {r.get('ScheduleExpression')} - {len(tg)} target(s)")
        except Exception as e:
            rep.fail(f"  {RULE}: {str(e)[:160]}")
        try:
            sch = boto3.client("scheduler", region_name="us-east-1")
            names = [x["Name"] for x in sch.list_schedules(MaxResults=100).get("Schedules", [])
                     if "etf-fund-flow" in x["Name"]]
            rep.log(f"  EventBridge Scheduler entries: {names or 'none'}")
        except Exception as e:
            rep.log(f"  scheduler probe: {str(e)[:100]}")

        rep.section("5. CloudWatch - did it actually run (7d)")
        inv7 = 0
        for metric in ("Invocations", "Errors"):
            pts = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": FN}],
                StartTime=datetime.now(timezone.utc) - timedelta(days=7),
                EndTime=datetime.now(timezone.utc), Period=86400, Statistics=["Sum"])
            tot = sum(p["Sum"] for p in pts.get("Datapoints", []))
            if metric == "Invocations":
                inv7 = tot
            rep.log(f"  {metric} 7d = {tot:.0f}")

        # ── 3. HEALTHY branch: S3 is fine, so check what the EDGE serves ────
        if with_z and age_h < 36:
            rep.section("6. S3 IS HEALTHY -> compare against what the CDN serves")
            rep.ok("  S3 carries z-scores and is fresh; the engine is not the problem")
            try:
                req = urllib.request.Request(
                    f"{CDN}/{DAILY}?v={int(time.time())}",
                    headers={"User-Agent": UA, "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    served = json.loads(resp.read())
                sm, sz, sspdr = summarize(served)
                rep.kv(cdn_status=resp.status, cdn_generated_at=str(served.get("generated_at")),
                       cdn_n_metrics=len(sm), cdn_n_with_zscore=len(sz),
                       cdn_spdr_with_z=len(sspdr))
                if len(sz) == 0:
                    rep.fail("  EDGE serves 0 z-scores while S3 has them -> stale worker "
                             "cache / proxy mapping. Fix belongs in the CF worker, not the engine.")
                    fails.append("cdn_stale")
                elif served.get("generated_at") != daily.get("generated_at"):
                    rep.fail(f"  EDGE generated_at {served.get('generated_at')} != "
                             f"S3 {daily.get('generated_at')} -> cached copy")
                    fails.append("cdn_generation_drift")
                else:
                    rep.ok("  EDGE matches S3 - feed and delivery are both fine; "
                           "the break is in the page render path, next ops reads flows.html served bytes")
            except Exception as e:
                rep.fail(f"  CDN fetch failed: {str(e)[:200]}")
                fails.append("cdn_unreachable")
            rep.kv(branch="HEALTHY_FEED", fails=str(fails))
            if fails:
                sys.exit(1)
            return

        # ── 4. decisive: is the vendor entitlement alive ─────────────────────
        rep.section("6. LIVE Polygon ETF-Global probe (decides branch A vs B)")
        env = lam.get_function_configuration(FunctionName=FN).get(
            "Environment", {}).get("Variables", {})
        key = env.get("POLYGON_KEY")
        rep.log(f"  POLYGON_KEY on {FN}: present={bool(key)} len={len(key or '')}")
        if not key:
            rep.fail("  no POLYGON_KEY on the function - env was blanked by a deploy")
            sys.exit(1)

        end, start = date.today(), date.today() - timedelta(days=140)
        polygon_rows = 0
        polygon_status = None
        for label, url in (
            ("windowed_SPY", f"https://api.polygon.io/etf-global/v1/fund-flows"
                             f"?composite_ticker=SPY&processed_date.gte={start}"
                             f"&processed_date.lte={end}&order=desc&sort=processed_date"
                             f"&limit=5&apiKey={key}"),
            ("bare_SPY", f"https://api.polygon.io/etf-global/v1/fund-flows"
                         f"?composite_ticker=SPY&limit=5&apiKey={key}"),
        ):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Probe/1.0"})
                with urllib.request.urlopen(req, timeout=25) as resp:
                    body = json.loads(resp.read())
                res = body.get("results") or []
                polygon_status = polygon_status or resp.status
                polygon_rows = max(polygon_rows, len(res))
                rep.ok(f"  {label}: HTTP {resp.status} status={body.get('status')} n={len(res)}")
                if res:
                    r0 = res[0]
                    rep.log(f"    latest: date={r0.get('processed_date')} "
                            f"fund_flow={r0.get('fund_flow')} nav={r0.get('nav')} "
                            f"shares={r0.get('shares_outstanding')}")
            except urllib.error.HTTPError as e:
                b = e.read().decode("utf-8", "ignore")[:250]
                polygon_status = polygon_status or e.code
                rep.fail(f"  {label}: HTTP {e.code} - {b}")
            except Exception as e:
                rep.fail(f"  {label}: {str(e)[:180]}")

        # ── BRANCH B: vendor dead -> refuse to fabricate ─────────────────────
        if polygon_rows == 0:
            rep.section("7. BRANCH B - Polygon ETF-Global returns no rows")
            rep.fail(f"  entitlement/endpoint dead (status={polygon_status}). "
                     f"NOT patching a fallback in this ops.")
            rep.log("  WHY: etf-true-flows keeps 25 days of shares history "
                    "(source line 143 `days = days[-25:]`) and the z gate needs >=30, "
                    "so flow_zscore_90d cannot be rebuilt honestly from shares-deltas.")
            rep.log("  HONEST FIX (next ops, two commits): (a) engine gains a "
                    "shares_delta leg filling flow_5d/20d + pct_aum with "
                    "flow_source provenance and NO fabricated z; (b) flows.html "
                    "ranks by %AUM when z is absent, with a visible banner - and "
                    "that page edit must be a NORMAL sandbox commit, since "
                    "[skip-deploy] on runner auto-commits suppresses pages.yml.")
            rep.kv(branch="B_VENDOR_DEAD", polygon_status=str(polygon_status),
                   n_with_zscore=len(with_z))
            sys.exit(1)

        # ── BRANCH A: vendor alive, feed stale/empty -> re-arm + run ─────────
        rep.section("7. BRANCH A - vendor alive, feed stale/empty -> re-arm + invoke")
        if rule_state == "DISABLED":
            events.enable_rule(Name=RULE)
            rep.ok(f"  re-enabled {RULE}")
        elif rule_state == "MISSING":
            rep.fail(f"  {RULE} absent - engine has no trigger; recreate before relying on it")
            fails.append("rule_missing")
        else:
            rep.ok(f"  {RULE} already ENABLED (7d invocations={inv7:.0f})")

        before_gen = daily.get("generated_at")
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        rep.ok("  invoked async (engine is a ~84-ETF fan-out; sync gating would time out)")

        rep.section("8. HARD GATE - poll S3 until the artifact actually moves")
        fresh = None
        for attempt in range(1, 25):          # up to ~10 min
            time.sleep(25)
            try:
                d2, lm2 = s3_json(DAILY)
            except Exception:
                continue
            if d2.get("generated_at") != before_gen:
                fresh = d2
                rep.ok(f"  regenerated on attempt {attempt}: {d2.get('generated_at')}")
                break
        if fresh is None:
            rep.fail("  daily.json never regenerated - the invoke did not complete; "
                     "read CloudWatch logs for the run before writing any fix")
            sys.exit(1)

        m2, z2, spdr2 = summarize(fresh)
        rep.kv(n_metrics=len(m2), n_with_zscore=len(z2), spdr_with_z=len(spdr2),
               n_ok=fresh.get("n_ok"), universe_size=fresh.get("universe_size"))
        checks = [
            ("z-score coverage > 0 (feeds top-10 + full universe)", len(z2) > 0),
            ("z-score coverage >= 40 rows (full universe is usable)", len(z2) >= 40),
            ("SPDR heatmap >= 8/11 numeric", len(spdr2) >= 8),
            ("flow_5d_usd populated on z rows", sum(
                1 for r in z2 if r.get("flow_5d_usd") is not None) >= max(1, len(z2) // 2)),
        ]
        for label, passed in checks:
            (rep.ok if passed else rep.fail)(f"  {label}")
            if not passed:
                fails.append(label)

        rep.section("9. verdict")
        rep.kv(branch="A_RERUN", fails=str(fails))
        if fails:
            rep.fail(f"FAILED {len(fails)}: {fails}")
            sys.exit(1)
        rep.ok(f"PASS_ALL - {len(z2)} z-scored rows, {len(spdr2)}/11 SPDR; "
               f"all four flows.html sections have data again")


if __name__ == "__main__":
    main()
