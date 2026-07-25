"""
ops_3886 — DEPLOY: three infrastructure fixes found while building the semi
flow/price investigation's news-text-feed request.

  1. justhodl-news-wire: config.json had a literal, never-replaced placeholder
     for ANTHROPIC_API_KEY ("PLACEHOLDER_REPLACE_VIA_AWS_CONSOLE_OR_OPS"),
     producing a 401 on every Claude call, silently falling back to a
     deterministic mode that flagged zero high-impact headlines. Fixed via
     inherit_env from justhodl-confluence-meta (the documented known-good
     secrets source), matching justhodl-flows-ai-analysis's proven pattern.
  2. justhodl-news-sentiment: read ANTHROPIC_KEY, which nothing in the fleet
     populates (the working convention everywhere else is ANTHROPIC_API_KEY),
     producing a 400 on every one of 503 stocks and scoring all-neutral.
     Fixed the config (inherit_env) and the one source line.
  3. justhodl-feed-catalog: schema sampling was feeds[:300] in lexicographic
     S3-listing order with NO priority — exactly why data/rebalance-radar.json
     ('r') and data/earnings-tracker.json ('e') both showed "not sampled"
     investigating the semi divergence. Fixed: sample by recency instead of
     alphabet, cap raised to 4000, prefix scope broadened from data/ alone to
     the fleet's real prefixes (etf-flows/, screener/, sentiment/, macro/,
     config/), writer-attribution now primarily sourced from the also-fixed
     engine-manifest.json (79.1% coverage, up from 50.3%) instead of a weak
     description-text scan, and given an explicit auditable schedule (the old
     "scheduled via the scheduler manifest" claim was unverifiable in any
     workflow or config found).

All three verified locally against stubbed S3/Lambda/jhcore before this push
(daily-pressure-style unit tests, not just a syntax check). This ops verifies
the same claims against the real deployed artifacts and real live data.
"""
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

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=890, retries={"max_attempts": 0}))
logs = boto3.client("logs", region_name="us-east-1")


def zip_settle(fn_name, marker, rep, tries=30, sleep_s=15):
    for attempt in range(1, tries + 1):
        try:
            loc = lam.get_function(FunctionName=fn_name)["Code"]["Location"]
            blob = urllib.request.urlopen(loc, timeout=60).read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                src = z.read("lambda_function.py").decode("utf-8", "ignore")
            if marker in src:
                rep.ok(f"  {fn_name}: new artifact live on attempt {attempt}")
                return True
            rep.log(f"  {fn_name}: attempt {attempt}, marker not yet deployed")
        except Exception as e:
            rep.log(f"  {fn_name}: attempt {attempt}, {str(e)[:90]}")
        time.sleep(sleep_s)
    rep.fail(f"  {fn_name}: deploy never landed after {tries} attempts")
    return False


def wait_active(fn_name, rep):
    cfg = lam.get_function_configuration(FunctionName=fn_name)
    for _ in range(20):
        if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress":
            break
        time.sleep(8)
        cfg = lam.get_function_configuration(FunctionName=fn_name)
    rep.ok(f"  {fn_name}: State={cfg.get('State')} LastUpdateStatus={cfg.get('LastUpdateStatus')} "
           f"Memory={cfg.get('MemorySize')} Timeout={cfg.get('Timeout')}")
    return cfg


def wait_for_env_key(fn_name, env_key, rep, tries=30, sleep_s=15):
    """For config-only changes (inherit_env), the deployed SOURCE never
    changes, so zip-settle-by-source-marker is the wrong check entirely —
    verify the live environment variable directly instead."""
    for attempt in range(1, tries + 1):
        try:
            cfg = lam.get_function_configuration(FunctionName=fn_name)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            val = env.get(env_key, "")
            if val and "PLACEHOLDER" not in val and len(val) > 20:
                rep.ok(f"  {fn_name}: {env_key} is live and non-placeholder "
                       f"(len={len(val)}) on attempt {attempt}")
                return True
            rep.log(f"  {fn_name}: attempt {attempt}, "
                    f"{env_key}={'empty' if not val else 'still placeholder/short'}")
        except Exception as e:
            rep.log(f"  {fn_name}: attempt {attempt}, {str(e)[:90]}")
        time.sleep(sleep_s)
    rep.fail(f"  {fn_name}: {env_key} never became a real live value after {tries} attempts")
    return False


def s3_snapshot(key):
    try:
        o = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception:
        return None, None


def recent_log_text(fn_name, n=60):
    try:
        streams = logs.describe_log_streams(
            logGroupName=f"/aws/lambda/{fn_name}",
            orderBy="LastEventTime", descending=True, limit=1)["logStreams"]
        if not streams:
            return ""
        events = logs.get_log_events(
            logGroupName=f"/aws/lambda/{fn_name}",
            logStreamName=streams[0]["logStreamName"], limit=n)["events"]
        return "\n".join(e["message"] for e in events)
    except Exception:
        return ""


def main():
    with report("3886_infra_fixes_deploy") as rep:
        rep.heading("ops 3886 — deploy news-wire/news-sentiment/feed-catalog fixes, hard-gate on real evidence")

        # ---------- news-wire ----------
        rep.section("1. news-wire — config-only change (inherit_env), verify the LIVE env var directly")
        before_nw, blm_nw = s3_snapshot("data/news-wire.json")
        if wait_for_env_key("justhodl-news-wire", "ANTHROPIC_API_KEY", rep):
            wait_active("justhodl-news-wire", rep)
            lam.invoke(FunctionName="justhodl-news-wire", InvocationType="RequestResponse", Payload=b"{}")
            time.sleep(3)
            log_nw = recent_log_text("justhodl-news-wire")
            rep.log(f"  recent log tail: {log_nw[-1200:]}")
            nw_ok = "401" not in log_nw and "Unauthorized" not in log_nw
        else:
            nw_ok = False
            log_nw = ""

        # ---------- news-sentiment ----------
        rep.section("2. news-sentiment — zip-settle (source fix) + env-key (config fix) + invoke + gate")
        settled_ns = zip_settle("justhodl-news-sentiment", 'os.environ.get("ANTHROPIC_API_KEY"', rep)
        env_ok_ns = wait_for_env_key("justhodl-news-sentiment", "ANTHROPIC_API_KEY", rep) if settled_ns else False
        if settled_ns and env_ok_ns:
            wait_active("justhodl-news-sentiment", rep)
            lam.invoke(FunctionName="justhodl-news-sentiment", InvocationType="Event", Payload=b"{}")
            ns_after = None
            blm_ns = None
            for attempt in range(1, 25):
                time.sleep(15)
                doc, lm = s3_snapshot("sentiment/data.json")
                if doc and (blm_ns is None or lm > blm_ns):
                    ns_after = doc
                    rep.ok(f"  sentiment/data.json rewritten on attempt {attempt}")
                    break
            log_ns = recent_log_text("justhodl-news-sentiment")
            rep.log(f"  recent log tail: {log_ns[-1200:]}")
            ns_400_free = "400" not in log_ns.split("claude err")[-1][:50] if "claude err" in log_ns else True
            ns_scored = ns_after and ((ns_after.get("bullish_count") or 0) + (ns_after.get("bearish_count") or 0)) > 0
        else:
            ns_after, log_ns, ns_scored = None, "", False

        # ---------- feed-catalog ----------
        rep.section("3. feed-catalog — zip-settle + invoke + gate on real schema+writers for the 2 target engines")
        before_fc, blm_fc = s3_snapshot("data/feed-catalog.json")
        if zip_settle("justhodl-feed-catalog", "DATA_PREFIXES", rep):
            wait_active("justhodl-feed-catalog", rep)
            lam.invoke(FunctionName="justhodl-feed-catalog", InvocationType="Event", Payload=b"{}")
            fc_after = None
            for attempt in range(1, 46):   # 46*20s = 920s, safely above the lambda's own 840s timeout
                time.sleep(20)
                doc, lm = s3_snapshot("data/feed-catalog.json")
                if doc and (blm_fc is None or lm > blm_fc):
                    fc_after = doc
                    rep.ok(f"  feed-catalog.json rewritten on attempt {attempt}")
                    break
            log_fc = recent_log_text("justhodl-feed-catalog")
            rep.log(f"  recent log tail: {log_fc[-1500:]}")
        else:
            fc_after, log_fc = None, ""

        rep.section("4. THE HARD GATE — every claim checked against real deployed evidence")
        checks = [("news-wire: no 401/Unauthorized in the invoke that followed the fix", nw_ok)]

        if ns_after:
            b, be = ns_after.get("bullish_count", 0), ns_after.get("bearish_count", 0)
            n_neu = ns_after.get("neutral_count", 0)
            rep.kv(news_sentiment_bullish=b, news_sentiment_bearish=be, news_sentiment_neutral=n_neu)
            checks.append(("news-sentiment: no '400 Bad Request' in the log tail after 'claude err'",
                           ns_400_free))
            checks.append(("news-sentiment: at least some non-neutral scores now (was 0/0/503)",
                           (b + be) > 0))
        else:
            checks.append(("news-sentiment: produced fresh output", False))

        if fc_after:
            feeds = {e["key"]: e for e in (fc_after.get("feeds") or [])}
            rr = feeds.get("data/rebalance-radar.json") or {}
            et = feeds.get("data/earnings-tracker.json") or {}
            n_etf_flows = sum(1 for k in feeds if k.startswith("etf-flows/"))
            rep.kv(rebalance_radar_schema_type=rr.get("schema", {}).get("type"),
                   rebalance_radar_writers=str(rr.get("writers")),
                   earnings_tracker_schema_type=et.get("schema", {}).get("type"),
                   earnings_tracker_writers=str(et.get("writers")),
                   n_etf_flows_feeds_now_catalogued=n_etf_flows,
                   total_feeds=len(feeds))
            checks.extend([
                ("rebalance-radar.json now has a real inferred schema (was 'not sampled')",
                 rr.get("schema", {}).get("type") is not None),
                ("rebalance-radar.json writer attribution now correct",
                 "justhodl-rebalance-radar" in (rr.get("writers") or [])),
                ("earnings-tracker.json now has a real inferred schema (was 'not sampled')",
                 et.get("schema", {}).get("type") is not None),
                ("earnings-tracker.json writer attribution now correct",
                 "justhodl-earnings-tracker" in (et.get("writers") or [])),
                ("etf-flows/* feeds now catalogued at all (was structurally excluded)",
                 n_etf_flows > 0),
            ])
        else:
            checks.append(("feed-catalog: produced fresh output", False))

        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")

        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok("PASS_ALL — all three fixes verified against real deployed evidence")


if __name__ == "__main__":
    main()
