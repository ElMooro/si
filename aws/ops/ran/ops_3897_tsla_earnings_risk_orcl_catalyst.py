"""
ops_3897 — PROBE (writes no code): two distinct investigations.

(1) TSLA pre-earnings risk — was ANYTHING flagging elevated miss-risk before
    the July 22 report (-25% EPS surprise, -14.52% 1d reaction)? Checks
    earnings-whisper.json (consensus vs whisper divergence) and
    eps-revision-velocity.json (were analysts cutting numbers into the print).

(2) ORCL non-earnings decline — no earnings report in the last 30 days
    (confirmed prior ops), -27% this month. Checks SEC filings (insider
    selling, unusual 8-Ks), capital-return (buyback/dividend changes),
    hiring-velocity + talent-migration (workforce contraction signals),
    eps-revision-velocity (are analysts cutting ORCL estimates absent an
    earnings trigger — that alone would be a real, dated clue), and
    news-wire's current live state (last checked broken on an Anthropic
    billing block — re-verifying in case that's since been resolved,
    since real headlines would directly answer "why").
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def age_h(lm):
    return round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1)


def find_ticker(container, ticker):
    """container may be a list of dicts, or a dict keyed by ticker."""
    if isinstance(container, dict):
        if ticker in container:
            return container[ticker]
        for v in container.values():
            if isinstance(v, dict) and v.get("ticker") == ticker:
                return v
        return None
    if isinstance(container, list):
        return next((r for r in container if isinstance(r, dict) and
                    (r.get("ticker") == ticker or r.get("symbol") == ticker)), None)
    return None


def main():
    with report("3897_tsla_earnings_risk_orcl_catalyst") as rep:
        rep.heading("ops 3897 — TSLA pre-earnings risk signals + ORCL non-earnings decline drivers")
        failures = []

        rep.section("1. earnings-whisper.json — was TSLA's whisper number below consensus pre-print")
        try:
            ew, ew_lm = get("data/earnings-whisper.json")
            rep.ok(f"  {age_h(ew_lm)}h old")
            rep.log(f"  top-level keys: {sorted(ew.keys())}")
            container = None
            for k in ("whispers", "tickers", "data", "results"):
                if isinstance(ew.get(k), (list, dict)):
                    container = ew[k]
                    break
            tsla_row = find_ticker(container, "TSLA") if container is not None else None
            rep.log(f"  TSLA entry: {json.dumps(tsla_row, default=str)[:500] if tsla_row else 'NOT FOUND'}")
        except Exception as e:
            rep.fail(f"  earnings-whisper.json unreadable: {str(e)[:200]}")
            failures.append("earnings-whisper")

        rep.section("2. eps-revision-velocity.json — TSLA (pre-print) and ORCL (ongoing, no earnings trigger)")
        try:
            erv, erv_lm = get("data/eps-revision-velocity.json")
            rep.ok(f"  {age_h(erv_lm)}h old")
            rep.log(f"  top-level keys: {sorted(erv.keys())}")
            container = None
            for k in ("tickers", "stocks", "data", "results", "revisions"):
                if isinstance(erv.get(k), (list, dict)):
                    container = erv[k]
                    break
            for tk in ("TSLA", "ORCL"):
                row = find_ticker(container, tk) if container is not None else None
                rep.log(f"  {tk} entry: {json.dumps(row, default=str)[:500] if row else 'NOT FOUND'}")
        except Exception as e:
            rep.fail(f"  eps-revision-velocity.json unreadable: {str(e)[:200]}")
            failures.append("eps-revision-velocity")

        rep.section("3. sec-filings-intel.json — ORCL insider activity / unusual 8-Ks")
        try:
            sfi, sfi_lm = get("data/sec-filings-intel.json")
            rep.ok(f"  {age_h(sfi_lm)}h old")
            rep.log(f"  top-level keys: {sorted(sfi.keys())}")
            container = None
            for k in ("filings", "tickers", "data", "alerts", "results"):
                if isinstance(sfi.get(k), (list, dict)):
                    container = sfi[k]
                    break
            if isinstance(container, list):
                orcl_filings = [r for r in container if isinstance(r, dict) and
                               (r.get("ticker") == "ORCL" or r.get("symbol") == "ORCL")]
                rep.log(f"  ORCL filings found: {len(orcl_filings)}")
                for f in orcl_filings[:10]:
                    rep.log(f"    {json.dumps(f, default=str)[:400]}")
            else:
                row = find_ticker(container, "ORCL") if container is not None else None
                rep.log(f"  ORCL entry: {json.dumps(row, default=str)[:500] if row else 'NOT FOUND'}")
        except Exception as e:
            rep.fail(f"  sec-filings-intel.json unreadable: {str(e)[:200]}")
            failures.append("sec-filings-intel")

        rep.section("4. capital-return.json — ORCL buyback/dividend changes")
        try:
            cr, cr_lm = get("data/capital-return.json")
            rep.ok(f"  {age_h(cr_lm)}h old")
            rep.log(f"  top-level keys: {sorted(cr.keys())}")
            container = None
            for k in ("tickers", "stocks", "data", "results"):
                if isinstance(cr.get(k), (list, dict)):
                    container = cr[k]
                    break
            row = find_ticker(container, "ORCL") if container is not None else None
            rep.log(f"  ORCL entry: {json.dumps(row, default=str)[:600] if row else 'NOT FOUND'}")
        except Exception as e:
            rep.fail(f"  capital-return.json unreadable: {str(e)[:200]}")
            failures.append("capital-return")

        rep.section("5. hiring-velocity + talent-migration — ORCL workforce signals")
        for key, name in (("data/hiring-velocity.json", "hiring-velocity"),
                          ("data/talent-migration.json", "talent-migration")):
            try:
                doc, lm = get(key)
                rep.ok(f"  {name}: {age_h(lm)}h old, top-level keys: {sorted(doc.keys())}")
                container = None
                for k in ("tickers", "stocks", "data", "results", "companies"):
                    if isinstance(doc.get(k), (list, dict)):
                        container = doc[k]
                        break
                row = find_ticker(container, "ORCL") if container is not None else None
                rep.log(f"  ORCL entry: {json.dumps(row, default=str)[:500] if row else 'NOT FOUND'}")
            except Exception as e:
                rep.fail(f"  {name} unreadable: {str(e)[:200]}")
                failures.append(name)

        rep.section("6. news-wire — is it still credit-blocked, or did the billing fix land")
        try:
            streams_ok = True
            logs = boto3.client("logs", region_name="us-east-1")
            streams = logs.describe_log_streams(
                logGroupName="/aws/lambda/justhodl-news-wire",
                orderBy="LastEventTime", descending=True, limit=1)["logStreams"]
            if streams:
                events = logs.get_log_events(
                    logGroupName="/aws/lambda/justhodl-news-wire",
                    logStreamName=streams[0]["logStreamName"], limit=30)["events"]
                tail = "\n".join(e["message"] for e in events)
                rep.log(f"  recent log tail: {tail[-800:]}")
                if "400" in tail or "credit" in tail.lower():
                    rep.log("  STILL credit-blocked as of the most recent run")
                elif "done" in tail.lower():
                    rep.ok("  appears to be running without the 400 error now")
        except Exception as e:
            rep.log(f"  log check skipped: {str(e)[:150]}")

        try:
            nw, nw_lm = get("data/news-wire.json")
            items = nw.get("recent_30") or []
            orcl_items = [it for it in items if "ORCL" in (it.get("tickers") or [])]
            tsla_items = [it for it in items if "TSLA" in (it.get("tickers") or [])]
            rep.kv(news_wire_age_h=age_h(nw_lm), n_recent_30=len(items),
                   n_orcl_headlines=len(orcl_items), n_tsla_headlines=len(tsla_items))
            for it in orcl_items[:5]:
                rep.log(f"    ORCL headline: {json.dumps(it, default=str)[:400]}")
        except Exception as e:
            rep.fail(f"  news-wire.json unreadable: {str(e)[:200]}")
            failures.append("news-wire")

        rep.section("verdict")
        rep.kv(failures=str(failures))
        if len(failures) >= 5:
            rep.fail(f"most feeds unreadable: {failures}")
            sys.exit(1)
        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
