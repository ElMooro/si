"""ops 6587 - READ-ONLY freshness census: are the pullers' files written when scheduled, and how old is the data inside?

Khalid, 2026-10-21 (data-flow audit, suggestion 1): before the providers are treated as pinned writers at their
publication times, confirm the arithmetic on the files themselves.  For every scheduled engine that pulls one of the
aligned providers (scripts/provider_map.py), up to three of its declared `data/*.json` outputs (engine-manifest.json):

  HEAD  -> LastModified: was the file written within 45 min after the engine's trigger minute (the schedule->file
           link holds), and when was it last written at all (a file older than 2 days = the engine does not write it)
  GET   -> (JSON files up to 1.5 MB) the newest date-like value inside (as_of / date / observation dates ...) and the
           engine's own generated_at: the "data age at write" in days - 0 = the file holds the print of the day it
           was written, 1 = the previous day's print

Per provider: files checked, written on schedule, median data age at write, share of files whose data is a day
old or more at write.  Raw rows go gzip+base64 next to the report (ops 6557 pattern) and to the private bucket.
Budget: <= 2,000 HEAD + 1,200 GET, ~$0.01, under 12 minutes.  Nothing is written to the live bucket.
"""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import base64
import concurrent.futures as cf
import gzip
import json
import os
import re
import statistics
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402
import provider_map as pm  # noqa: E402
import pipeline_order as po  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=40)
ALIGNED = ["FRED", "SEC EDGAR", "ECB", "NY Fed", "Treasury", "OFR", "Cboe", "BLS", "BEA", "Census", "Eurostat", "BIS/IMF/WB/OECD", "EIA", "FINRA", "Polygon/Massive", "FMP", "CFTC"]
DATE_KEYS = ("as_of", "asof", "as_of_date", "asOf", "date", "data_date", "latest_date", "last_date", "observation_date", "last_observation",
             "period", "end_date", "latest", "through", "session", "trade_date", "last_updated_data")
RUN_KEYS = ("generated_at", "generated", "updated_at", "updated", "timestamp", "run_at", "last_updated", "fetched_at", "computed_at", "ts")
DATE_RE = re.compile(r"^(20\d{2})-(\d{2})-(\d{2})")
MAX_GET = 1_500_000


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def parse_date(v):
    if isinstance(v, (int, float)) and 1.3e9 < v < 2e9:
        return datetime.fromtimestamp(v, tz=timezone.utc).date()
    if isinstance(v, (int, float)) and 1.3e12 < v < 2e12:
        return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc).date()
    if isinstance(v, str):
        m = DATE_RE.match(v.strip())
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
            except ValueError:
                return None
    return None


def scan_dates(obj, depth=0, found=None):
    """-> (newest data date, newest run stamp) found in the top three levels of the document."""
    if found is None:
        found = {"data": [], "run": []}
    if depth > 3:
        return found
    if isinstance(obj, dict):
        for k, v in list(obj.items())[:400]:
            lk = str(k).lower()
            d = parse_date(v) if isinstance(v, (str, int, float)) else None
            if d and d.year >= 2000:
                is_run = lk in RUN_KEYS or lk.endswith("_at") or lk.endswith("_ts") or lk.startswith("generated")
                (found["run"] if is_run else found["data"]).append(d)
            elif isinstance(v, (dict, list)):
                scan_dates(v, depth + 1, found)
            if isinstance(k, str) and DATE_RE.match(k):                 # {"2026-10-20": ...} series keyed by date
                found["data"].append(parse_date(k))
    elif isinstance(obj, list):
        for v in obj[:200]:
            if isinstance(v, (dict, list)):
                scan_dates(v, depth + 1, found)
            elif isinstance(v, str):
                d = parse_date(v)
                if d:
                    found["data"].append(d)
    return found


def main():
    s3 = c("s3")
    now = datetime.now(timezone.utc)
    provs = pm.build(ROOT)
    man = json.loads((ROOT / "engine-manifest.json").read_text(encoding="utf-8"))
    keys = {e["engine"]: [k for k in e["keys"] if k.endswith(".json") and "{" not in k and "*" not in k] for e in man["engines"]}
    sched = json.loads((ROOT / "config/schedule-manifest.json").read_text(encoding="utf-8"))
    fan = json.loads((ROOT / "config/fanout-manifest.json").read_text(encoding="utf-8"))
    minute = {}
    tick_min = {}
    for sec in ("rules", "schedules"):
        for e in sched[sec]:
            if e.get("state") != "ENABLED":
                continue
            m = po.minute_of(e.get("expr"))
            fns = sorted({po.fn_of(t.get("arn")) for t in e.get("targets") or [] if po.fn_of(t.get("arn"))})
            if "justhodl-scheduler" in fns:
                for t in e["targets"]:
                    mm = re.search(r'"tick":\s*"([^"]+)"', t.get("input") or "")
                    if mm and m is not None:
                        tick_min[mm.group(1)] = m
                continue
            if m is not None:
                for fn in fns:
                    minute.setdefault(fn, []).append(m)
    for tick, members in fan["ticks"].items():
        if tick in tick_min:
            for fn in members:
                minute.setdefault(fn, []).append(tick_min[tick])
    targets = []
    for fn, pl in sorted(provs.items()):
        dp = [p for p in pm.data_providers(pl) if p in ALIGNED]
        if not dp or fn not in minute or not keys.get(fn):
            continue
        for k in keys[fn][:3]:
            targets.append((fn, dp, minute[fn], k))
    targets = targets[:2000]

    def head(t):
        fn, dp, mins, k = t
        try:
            h = s3.head_object(Bucket=LIVE, Key=k)
        except Exception as e:  # noqa: BLE001
            return dict(engine=fn, providers=dp, key=k, err=type(e).__name__)
        lm = h["LastModified"].astimezone(timezone.utc)
        lag = min(((lm.hour * 60 + lm.minute - m) % 1440) for m in mins)
        row = dict(engine=fn, providers=dp, key=k, size=h.get("ContentLength", 0), last_modified=lm.isoformat(), age_h=round((now - lm).total_seconds() / 3600, 1),
                   trigger_min=mins, lag_after_trigger_min=lag, on_schedule=lag <= 45)
        if row["size"] <= MAX_GET and k.endswith(".json") and row["age_h"] <= 72:
            try:
                body = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
                doc = json.loads(body)
                f = scan_dates(doc)
                dd = max(d for d in f["data"] if d and d <= lm.date() + timedelta(days=1)) if any(d for d in f["data"] if d and d <= lm.date() + timedelta(days=1)) else None
                rd = max(d for d in f["run"] if d) if any(f["run"]) else None
                row.update(data_date=dd.isoformat() if dd else None, run_date=rd.isoformat() if rd else None,
                           data_age_days=(lm.date() - dd).days if dd else None)
            except Exception as e:  # noqa: BLE001
                row["get_err"] = type(e).__name__
        return row

    rows = []
    with report("ops_6587_freshness_census") as r:
        r.heading("ops 6587 - freshness census: files vs schedules vs the data inside (read-only)")
        r.log("engines with an aligned provider and declared json outputs: %d; files to check: %d" % (len({t[0] for t in targets}), len(targets)))
        with cf.ThreadPoolExecutor(max_workers=24) as ex:
            for row in ex.map(head, targets):
                rows.append(row)
        ok = [x for x in rows if "err" not in x]
        r.log("HEAD ok %d, missing/err %d (%s)" % (len(ok), len(rows) - len(ok), json.dumps(sorted({x["err"] for x in rows if "err" in x}))))
        r.section("per provider")
        r.log("| provider | engines | files | written on schedule (<=45 min after trigger) | written in the last 2 days | data age at write: median d | a day old or more |")
        summary = {}
        for p in ALIGNED:
            px = [x for x in ok if p in x["providers"]]
            if not px:
                continue
            ages = [x["data_age_days"] for x in px if x.get("data_age_days") is not None and 0 <= x["data_age_days"] <= 60]
            summary[p] = dict(engines=len({x["engine"] for x in px}), files=len(px), on_schedule=sum(1 for x in px if x["on_schedule"]),
                              recent=sum(1 for x in px if x["age_h"] <= 48), dated=len(ages), median_age=(statistics.median(ages) if ages else None),
                              day_old=sum(1 for a in ages if a >= 1))
            s = summary[p]
            r.log("| %s | %d | %d | %d | %d | %s (of %d dated) | %d |" % (p, s["engines"], s["files"], s["on_schedule"], s["recent"], s["median_age"], s["dated"], s["day_old"]))
        stale_engines = sorted({x["engine"] for x in ok if x["age_h"] > 48})
        r.log("files not written in the last 2 days: %d (engines: %s%s)" % (sum(1 for x in ok if x["age_h"] > 48), ", ".join(stale_engines[:25]), " ..." if len(stale_engines) > 25 else ""))
        off = sorted({x["engine"] for x in ok if x["age_h"] <= 48 and not x["on_schedule"]})
        r.log("written recently but not within 45 min of the trigger (self-invoked, chained or event-driven writes): %d engines: %s%s" % (len(off), ", ".join(off[:25]), " ..." if len(off) > 25 else ""))
        blob = gzip.compress(json.dumps({"generated_at": now.isoformat(), "rows": rows, "summary": summary}, separators=(",", ":"), default=str).encode(), 9)
        s3.put_object(Bucket=PRIVATE, Key="audit/freshness-census-%s.json.gz" % now.strftime("%Y%m%dT%H%M%SZ"), Body=blob, ServerSideEncryption="AES256")
        out = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) / "aws" / "ops" / "reports" / "latest" / "ops_6587_freshness_census_data.md"
        b64 = base64.b64encode(blob).decode()
        out.write_text("# ops 6587 freshness census data (gzip+base64 JSON)\n\n```\n" + "\n".join(b64[i:i + 120] for i in range(0, len(b64), 120)) + "\n```\n", encoding="utf-8")
        r.kv(files=len(rows), head_ok=len(ok), on_schedule=sum(1 for x in ok if x["on_schedule"]), recent=sum(1 for x in ok if x["age_h"] <= 48),
             dated=sum(1 for x in ok if x.get("data_age_days") is not None), day_old_or_more=sum(1 for x in ok if (x.get("data_age_days") or 0) >= 1 and x.get("data_age_days") is not None))
        r.ok("census written (%d KB)" % (len(blob) // 1024))


if __name__ == "__main__":
    main()
