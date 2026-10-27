"""ops 6687 - read-only: the first scheduled day after the 2026-10-26 fixes (Khalid: "fix these ... erroring on their own
schedules").  Fleet lane FLEET3 (the point-scan rule, 291 bank readers) finished 23:35 UTC and the bank-factors v1.0.6b
lane (FINRA from the research-head shards) 23:39 UTC; every engine below has since run on its OWN schedule.  This probe:

  A. REPORT lines since 23:40 UTC for the five fleet-health engines, the FINRA chain (short-interest, bank-factors), the
     why.html desk (equity-research, user traffic) and the five reviewed-contract engines: ok / error / timeout counts,
     max memory, the last lines;
  B. the factor book as the 02:50 UTC join published it: version, coverage.finra_short, sources.finra_short, as_of, and
     the finra_* fields of AAPL / GME / TSLA / SOFI / JPM;
  C. the fleet error census (ops 6664's own code) for the same window.
Nothing is written except the reports.  No key involved.
"""
from pathlib import Path
import gzip
import json
import os
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops" / "STAGED"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
BOOK = "data/warm/factor-book/"
SINCE = os.environ.get("SINCE") or "2026-10-26T23:40:00+00:00"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120)
ENGINES = ("justhodl-analytics-snapshot justhodl-earnings-whisper justhodl-event-flow-monitor justhodl-signal-genealogy treasury-auto-updater "
           "justhodl-short-interest justhodl-bank-factors justhodl-equity-research "
           "justhodl-eps-revision-velocity justhodl-momentum-leaders justhodl-earnings-pead justhodl-estimate-revisions justhodl-pump-positioning").split()


def jget(s3, key):
    try:
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        try:
            raw = gzip.decompress(raw)
        except Exception:  # noqa: BLE001
            pass
        return json.loads(raw)
    except Exception as e:  # noqa: BLE001
        return {"_error": type(e).__name__}


def reports_since(logs, fn, since_ms):
    out, token = [], None
    while True:
        kw = dict(logGroupName="/aws/lambda/" + fn, startTime=since_ms, filterPattern='"REPORT RequestId"', limit=400)
        if token:
            kw["nextToken"] = token
        try:
            resp = logs.filter_log_events(**kw)
        except Exception as e:  # noqa: BLE001
            return None, type(e).__name__
        out += resp.get("events", [])
        token = resp.get("nextToken")
        if not token or len(out) >= 2000:
            break
    return sorted(out, key=lambda e: e["timestamp"]), None


def fields(m):
    d = {}
    for p in m.split("\t"):
        p = p.strip()
        for k in ("Duration:", "Max Memory Used:", "Status:", "Error Type:"):
            if p.startswith(k):
                d[k[:-1]] = p[len(k):].strip().split("\n")[0]
    return d


def main():
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    since_ms = int(datetime.fromisoformat(SINCE).timestamp() * 1000)
    with report("ops_6687_first_scheduled_day") as r:
        r.heading("ops 6687 - the first scheduled day after the fixes: REPORT lines since %s, the FINRA book, the census" % SINCE[:16])
        r.section("A. REPORT lines per engine since %s UTC" % SINCE[11:16])
        for fn in ENGINES:
            ev, err = reports_since(logs, fn, since_ms)
            if err:
                r.warn("%s: logs %s" % (fn, err))
                continue
            rows = [(e["timestamp"], fields(e.get("message", ""))) for e in ev]
            bad = [(t, f) for t, f in rows if f.get("Status")]
            mem = [int(f["Max Memory Used"].split()[0]) for _, f in rows if f.get("Max Memory Used", "").split()[:1] and f["Max Memory Used"].split()[0].isdigit()]
            dur = [float(f["Duration"].split()[0]) / 1000 for _, f in rows if f.get("Duration")]
            r.log("%s: %d runs, %d errors/timeouts | max memory %s MB | max duration %s s" % (
                fn, len(rows), len(bad), max(mem) if mem else None, round(max(dur), 1) if dur else None))
            for t, f in (bad[-4:] if bad else rows[-2:]):
                r.log("   %s | %s" % (time.strftime("%m-%d %H:%M:%S", time.gmtime(t / 1000)), " ".join("%s %s" % (k, v) for k, v in f.items())))
        r.section("B. the factor book (02:50 UTC join on v1.0.6b)")
        man = jget(s3, BOOK + "manifest.json")
        r.log("manifest: version %s updated %s | coverage.finra_short %s (polygon short %s) | sources.finra_short %s | as_of.finra-short-interest %s" % (
            man.get("version"), man.get("updated_at"), (man.get("coverage") or {}).get("finra_short"), (man.get("coverage") or {}).get("short"),
            json.dumps((man.get("sources") or {}).get("finra_short")), (man.get("as_of") or {}).get("finra-short-interest")))
        for n in [x for x in (man.get("notes") or []) if "finra" in str(x).lower()][:3]:
            r.log("   note: %s" % str(n)[:300])
        for sym in ("AAPL", "GME", "TSLA", "SOFI", "JPM"):
            doc = jget(s3, BOOK + "symbols/%s.json.gz" % sym)
            d = doc.get("derived") or {}
            r.log("%s: short_source %s short_pct_float %s | polygon SI %s | FINRA SI %s DTC %s change %s%% vs-polygon %s%% settlement %s" % (
                sym, d.get("short_source"), d.get("short_pct_float"), (doc.get("short") or {}).get("short_interest"), d.get("finra_short_interest"),
                d.get("finra_days_to_cover"), d.get("finra_si_change_pct"), d.get("finra_vs_polygon_si_pct"), d.get("finra_settlement_date")))
        r.section("C. the fleet error census for the same window (ops 6664's code)")
        r.ok("sections A-B complete; the census writes its own report (ops_6664_fleet_error_census.md)")
    os.environ["CENSUS_SINCE"] = SINCE
    import ops_6664_fleet_error_census as census  # noqa: E402 - SINCE is read at import
    census.main()


if __name__ == "__main__":
    main()
    sys.exit(0)
