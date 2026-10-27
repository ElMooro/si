"""ops 6690 - read-only: data/universe.json, the shared input of ~45 engines.  The fleet census since 23:40 UTC
2026-10-26 (ops 6687) shows earnings-pead and revenue-acceleration raising "No selected universe; preserve previous
publication" and momentum-breakout / volatility-squeeze-hunter "No original-scope membership" - each reads
data/universe.json and selects nothing.  justhodl-universe-builder (v4, 4hourly tick) builds the file from FMP
/company-screener cap-range calls; a failed range is logged and returns [] and the file is written whatever came back,
so an FMP 429 window publishes a shrunken or empty universe over the good one.

  A. the head: size, LastModified, generated_at, stats;
  B. S3 versions of the key (newest 60 parsed): when it shrank, the last good version;
  C. the builder's own log lines since 2026-10-15: per run raw counts per cap bucket, failed ranges, combined, written;
  D. the bank's profile-bulk (data/warm/fmp-bank/profile-bulk/): parts, sizes, pull dates, and the universe the
     builder's own filter yields from it (NYSE/NASDAQ/AMEX, actively trading, not ETF/fund, market cap >= $1M), compared
     symbol-by-symbol with the last good screener universe - is it a faithful fallback?
  E. Lambda Errors / Invocations per day (72 h) of every engine that reads data/universe.json.
Nothing is written.  The builder module is imported for its filter only (FMP_KEY set to a placeholder: no SSM read,
no FMP call).
"""
from pathlib import Path
import csv
import gzip
import io
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
KEY = "data/universe.json"
BANK = "data/warm/fmp-bank/profile-bulk/"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120)
CONSUMERS = ("activist-filings-scanner ai-infra-stack backlog bagger-engine bank-factors base-rates best-setups bottleneck-boom cannibals "
             "credit-before-equity deal-scanner deep-value-screener dislocation-detector earnings earnings-pead eps-revision-velocity "
             "etf-constituents hiring-velocity history-snapshotter ignition industry-boom industry-case insider-industry-cluster "
             "insider-trades master-ranker microcap-float-squeeze miss-calibrator miss-detector momentum-breakout money-flow-state "
             "opportunity-engine options-flow-scanner pead-detector pre-pump-detector readthrough revenue-acceleration "
             "sector-capital-fusion sector-earnings-diffusion short-book spx-beaters tape-reader theme-second-wave "
             "volatility-squeeze-hunter").split()


def builder_module():
    os.environ.setdefault("FMP_KEY", "probe-placeholder-not-a-key")
    sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-universe-builder" / "source"))
    import lambda_function as ub  # noqa: E402
    return ub


def parse(raw):
    try:
        d = json.loads(raw)
        st = d.get("stats") or {}
        return d, st.get("total_stocks"), st.get("by_cap_bucket"), d.get("generated_at")
    except Exception as e:  # noqa: BLE001
        return None, None, None, "unparseable %s" % type(e).__name__


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    with report("ops_6690_universe_probe") as r:
        r.heading("ops 6690 - data/universe.json: state, history, the builder's runs, the bank fallback, the consumers (read-only)")
        r.section("A. the head")
        obj = s3.get_object(Bucket=LIVE, Key=KEY)
        raw = obj["Body"].read()
        head, total, buckets, gen = parse(raw)
        r.log("%s: %d bytes, LastModified %s, VersionId %s | generated_at %s | total_stocks %s | by_cap_bucket %s | method %s | duration_s %s" % (
            KEY, len(raw), obj["LastModified"].isoformat(), obj.get("VersionId"), gen, total, json.dumps(buckets),
            (head or {}).get("method"), (head or {}).get("duration_s")))
        if head:
            r.log("stats: %s" % json.dumps(head.get("stats"))[:600])
        r.section("B. versions")
        ver = s3.get_bucket_versioning(Bucket=LIVE)
        r.log("bucket versioning: %s" % ver.get("Status"))
        versions, kw = [], {"Bucket": LIVE, "Prefix": KEY, "MaxKeys": 200}
        while True:
            resp = s3.list_object_versions(**kw)
            versions += [v for v in resp.get("Versions", []) if v["Key"] == KEY]
            if not resp.get("IsTruncated") or len(versions) >= 400:
                break
            kw.update(KeyMarker=resp["NextKeyMarker"], VersionIdMarker=resp["NextVersionIdMarker"])
        versions.sort(key=lambda v: v["LastModified"], reverse=True)
        r.log("versions listed: %d (newest %s, oldest %s)" % (len(versions), versions[0]["LastModified"].isoformat() if versions else None,
                                                             versions[-1]["LastModified"].isoformat() if versions else None))
        last_good, good_doc = None, None
        for v in versions[:60]:
            body = s3.get_object(Bucket=LIVE, Key=KEY, VersionId=v["VersionId"])["Body"].read() if v["Size"] < 16 << 20 else b""
            d, t, b, g = parse(body)
            r.log("  %s | %8d B | total %s | generated %s | buckets %s%s" % (v["LastModified"].strftime("%m-%d %H:%M"), v["Size"], t, g, json.dumps(b),
                                                                             " | LATEST" if v.get("IsLatest") else ""))
            if last_good is None and isinstance(t, int) and t >= 3000:
                last_good, good_doc = v, d
        if last_good:
            r.ok("last good version: %s (%s, %d stocks, generated %s)" % (last_good["VersionId"], last_good["LastModified"].isoformat(),
                                                                         good_doc["stats"]["total_stocks"], good_doc.get("generated_at")))
        else:
            r.warn("no version with >= 3000 stocks among the newest 60")
        r.section("C. the builder's runs since 2026-10-15")
        fn = lam.get_function_configuration(FunctionName="justhodl-universe-builder")
        r.log("config: %s MB / %s s, LastModified %s, CodeSha256 %s" % (fn.get("MemorySize"), fn.get("Timeout"), fn.get("LastModified"), fn.get("CodeSha256", "")[:12]))
        start = int(datetime(2026, 10, 15, tzinfo=timezone.utc).timestamp() * 1000)
        evs, token = [], None
        while True:
            kw = dict(logGroupName="/aws/lambda/justhodl-universe-builder", startTime=start, limit=1000,
                      filterPattern='?"[universe]" ?"REPORT RequestId" ?"Task timed out" ?ERROR ?Traceback')
            if token:
                kw["nextToken"] = token
            resp = logs.filter_log_events(**kw)
            evs += resp.get("events", [])
            token = resp.get("nextToken")
            if not token or len(evs) >= 20000:
                break
        evs.sort(key=lambda e: e["timestamp"])
        runs, cur = [], None
        for e in evs:
            m = e["message"].strip()
            if "starting" in m and "[universe]" in m:
                cur = {"t": e["timestamp"], "lines": [], "failed": 0}
                runs.append(cur)
            if cur is None:
                continue
            if " failed: " in m:
                cur["failed"] += 1
                if cur["failed"] <= 2:
                    cur["lines"].append(m[:200])
            elif any(k in m for k in ("raw=", "combined:", "valid:", "seed added", "wrote ", "by_bucket", "REPORT", "Task timed out", "ERROR", "Traceback")):
                cur["lines"].append(m[:240].replace("\t", " "))
        r.log("runs found: %d" % len(runs))
        for run in runs[-24:]:
            r.log("run %s | failed ranges %d" % (time.strftime("%m-%d %H:%M", time.gmtime(run["t"] / 1000)), run["failed"]))
            for line in run["lines"][:14]:
                r.log("     %s" % line)
        r.section("D. the bank's profile-bulk as a universe source")
        objs, kw = [], {"Bucket": LIVE, "Prefix": BANK}
        while True:
            resp = s3.list_objects_v2(**kw)
            objs += resp.get("Contents", [])
            if not resp.get("IsTruncated"):
                break
            kw["ContinuationToken"] = resp["NextContinuationToken"]
        latest = sorted([o for o in objs if o["Key"].endswith("/latest.csv.gz") or o["Key"].endswith("latest.csv.gz")], key=lambda o: o["Key"])
        dailies = sorted({o["Key"].rsplit("/", 1)[-1][:10] for o in objs if "/daily/" in o["Key"]})
        r.log("profile-bulk objects %d; latest parts %d; dated copies on %d days (newest %s)" % (len(objs), len(latest), len(dailies), dailies[-3:]))
        rows, t0 = [], time.time()
        for o in latest:
            h = s3.head_object(Bucket=LIVE, Key=o["Key"])
            r.log("  %s %d B LastModified %s meta %s" % (o["Key"], o["Size"], o["LastModified"].isoformat(), json.dumps(h.get("Metadata"))[:200]))
            text = gzip.decompress(s3.get_object(Bucket=LIVE, Key=o["Key"])["Body"].read()).decode("utf-8", "replace")
            rows += list(csv.DictReader(io.StringIO(text)))
        r.log("profile-bulk rows %d decoded in %.1fs; columns %s" % (len(rows), time.time() - t0, list(rows[0].keys())[:40] if rows else None))
        ub = builder_module()

        def num(x):
            try:
                return float(x)
            except (TypeError, ValueError):
                return None

        def yes(x):
            return str(x).strip().lower() in ("true", "1", "yes")
        exch = {}
        for x in rows:
            exch[x.get("exchange")] = exch.get(x.get("exchange"), 0) + 1
        r.log("exchanges (top 12): %s" % sorted(exch.items(), key=lambda kv: -kv[1])[:12])
        cand = []
        for x in rows:
            if (x.get("exchange") or "").upper() not in ("NYSE", "NASDAQ", "AMEX"):
                continue
            if not yes(x.get("isActivelyTrading")) or yes(x.get("isEtf")) or yes(x.get("isFund")):
                continue
            mc = num(x.get("marketCap"))
            if not mc or mc < 1_000_000:
                continue
            s = {"symbol": (x.get("symbol") or "").upper(), "companyName": x.get("companyName"), "marketCap": mc, "price": num(x.get("price")),
                 "exchange": x.get("exchange"), "industry": x.get("industry"), "sector": x.get("sector"), "country": x.get("country")}
            if ub.is_valid_common_stock(s):
                cand.append(s)
        by_b = {}
        for s in cand:
            b = next((n for n, lo, hi in ub.CAP_BUCKETS if lo <= s["marketCap"] < hi), "unknown")
            by_b[b] = by_b.get(b, 0) + 1
        r.log("bank-derived universe (builder filter): %d stocks, by bucket %s" % (len(cand), json.dumps(by_b)))
        if good_doc:
            good = {s["symbol"]: s for s in good_doc.get("stocks", [])}
            bank = {s["symbol"]: s for s in cand}
            both = set(good) & set(bank)
            only_good, only_bank = sorted(set(good) - set(bank)), sorted(set(bank) - set(good))
            r.log("vs last good screener universe (%d): both %d (%.1f%% of screener), only screener %d, only bank %d" % (
                len(good), len(both), 100.0 * len(both) / max(1, len(good)), len(only_good), len(only_bank)))
            r.log("   only screener (first 25): %s" % only_good[:25])
            r.log("   only bank (first 25): %s" % only_bank[:25])
            diffs = sorted(abs(bank[s]["marketCap"] / good[s]["market_cap"] - 1) for s in both if good[s].get("market_cap"))
            if diffs:
                r.log("   market-cap ratio |bank/screener - 1| median %.3f, p90 %.3f" % (diffs[len(diffs) // 2], diffs[int(len(diffs) * 0.9)]))
            same_bucket = sum(1 for s in both if next((n for n, lo, hi in ub.CAP_BUCKETS if lo <= bank[s]["marketCap"] < hi), None) == good[s].get("cap_bucket"))
            r.log("   same cap bucket: %d of %d" % (same_bucket, len(both)))
        r.section("E. consumers: Lambda Errors / Invocations per UTC day, last 72 h")
        end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        begin = end - timedelta(hours=72)
        fns = ["justhodl-" + c for c in CONSUMERS]
        queries = []
        for i, f in enumerate(fns):
            for mname in ("Errors", "Invocations"):
                queries.append({"Id": "m%d%s" % (i, mname[0].lower()), "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": mname,
                                "Dimensions": [{"Name": "FunctionName", "Value": f}]}, "Period": 86400, "Stat": "Sum"}})
        vals = {}
        for i in range(0, len(queries), 400):
            resp = cw.get_metric_data(MetricDataQueries=queries[i:i + 400], StartTime=begin, EndTime=end)
            for res in resp.get("MetricDataResults", []):
                vals[res["Id"]] = sum(res.get("Values", []))
        bad = []
        for i, f in enumerate(fns):
            e, n = vals.get("m%de" % i, 0), vals.get("m%di" % i, 0)
            if e:
                bad.append((f, int(e), int(n)))
        r.log("consumers with errors in 72 h: %d of %d" % (len(bad), len(fns)))
        for f, e, n in sorted(bad, key=lambda x: -x[1]):
            r.log("   %s: %d errors / %d invocations" % (f, e, n))
        r.ok("ops 6690 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
