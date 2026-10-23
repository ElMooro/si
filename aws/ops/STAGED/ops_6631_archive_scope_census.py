"""ops 6631 - READ-ONLY census for narrowing the warehouse archive (Khalid 2026-10-23: archive option B - "protect only
data that can't be re-downloaded, like the provider history on data.html, and skip files your engines rebuild every
day"; promised: measure the saving per folder before changing anything).

The archive (justhodl-warehouse-archive-857687956942) receives every write of the live bucket through one whole-bucket
replication rule: ~84 k Standard-IA copies a day ($0.84/day), each replaced copy billed for IA's 30-day minimum. To
narrow the rule to the irreplaceable data, this op measures, per folder ("family"), from the live bucket's own daily
S3 Inventory (all versions):
  - current objects / bytes / IA-billable bytes (128 KB minimum per object) - what the archive holds for it;
  - writes in the window (every version created), split into NEW keys (history that accumulates) and REWRITES of keys
    that already existed (files rebuilt in place), bytes written, IA-billable bytes written;
  - the share of the family's current objects rewritten in the window ("rebuilt" share);
  - keys at the inventory's version cap (more writes than the inventory can show);
and, from yesterday's S3 server access logs (sampled), which engines write each family (Lambda session = function).
Every top-level data/ file is also listed one by one (history files there are single keys rewritten daily).
Window: the two full post-thaw days before the newest inventory (fleet thawed 2026-10-21 00:30 UTC).
Nothing is written but this report and its data file. Cost: the inventory read (~1.5 GB from the private bucket to the
runner, ~$0.14) + ~6 k small log GETs.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures as cf
import csv
import gzip
import io
import json
import re
import sys
import urllib.parse

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
INV_PREFIX = "inventory/%s/" % LIVE
IA_MIN = 128 * 1024
IA_GB_MONTH = 0.0125
IA_PUT = 0.01 / 1000
THAW = datetime(2026, 10, 21, 1, 0, tzinfo=timezone.utc)
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=300, max_pool_connections=64)
LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
                    r'"([^"]*)" "([^"]*)"')
WRITE_OPS = ("REST.PUT.OBJECT", "REST.COPY.OBJECT", "REST.POST.UPLOAD")


def family(key):
    """The folder a key is judged by: three levels under data/warm, data/providers and audit-private/<stamp>, two
    elsewhere; files sitting directly in a folder are grouped as '<folder>/(files)'."""
    parts = key.split("/")
    if len(parts) == 1:
        return "(root files)"
    if parts[0] == "data" and len(parts) >= 3 and parts[1] in ("warm", "providers"):
        return "/".join(parts[:3]) + "/" if len(parts) > 3 else "data/%s/(files)" % parts[1]
    if parts[0] == "audit-private" and len(parts) >= 4:
        return "/".join(parts[:3]) + "/"
    if len(parts) == 2:
        return parts[0] + "/(files)"
    if re.fullmatch(r"\d{4}-?\d{2}-?\d{2}", parts[1]):
        return parts[0] + "/"                      # dated sub-folders (equity-research-history/2026-10-22/...) are one family
    return "/".join(parts[:2]) + "/"


def ts(v):
    try:
        return datetime.strptime(v[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:  # noqa: BLE001
        return None


def selftest():
    assert family("data/warm/fred-scoped/a/b.json") == "data/warm/fred-scoped/"
    assert family("data/warm/x.json") == "data/warm/(files)"
    assert family("data/best-setups.json") == "data/(files)"
    assert family("data/fundgraph/AAPL.json") == "data/fundgraph/"
    assert family("audit-private/20260909-originals/fx-research/requests/a.json") == "audit-private/20260909-originals/fx-research/"
    assert family("audit-private/20260909-originals/fx-research.json") == "audit-private/20260909-originals/"
    assert family("edge-data.json") == "(root files)"
    assert family("equity-research/NVDA.json") == "equity-research/(files)"
    assert family("equity-research-history/2026-10-22/NVDA.json") == "equity-research-history/"
    print("selftest ok")


def main():
    selftest()
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6631_archive_scope_census") as r:
        r.heading("ops 6631 - archive scope census: what each folder costs the archive and whether it is rebuilt (read-only)")
        mans = []
        token = {}
        while True:
            res = s3.list_objects_v2(Bucket=PRIVATE, Prefix=INV_PREFIX, **token)
            mans += [o["Key"] for o in res.get("Contents", []) if o["Key"].endswith("/manifest.json") and "/hive/" not in o["Key"]]
            if not res.get("IsTruncated"):
                break
            token = {"ContinuationToken": res["NextContinuationToken"]}
        mans.sort()
        if not mans:
            r.fail("no inventory manifest under s3://%s/%s" % (PRIVATE, INV_PREFIX))
            sys.exit(1)
        mkey = mans[-1]
        stamp = mkey.split("/")[-2]
        inv_time = datetime.strptime(stamp, "%Y-%m-%dT%H-%MZ").replace(tzinfo=timezone.utc)
        w_end = inv_time
        w_start = max(THAW, w_end - timedelta(days=2))
        days = (w_end - w_start).total_seconds() / 86400.0
        man = json.loads(s3.get_object(Bucket=PRIVATE, Key=mkey)["Body"].read())
        schema = [x.strip() for x in man["fileSchema"].split(",")]
        ix = {n: i for i, n in enumerate(schema)}
        r.log("inventory %s (%d files), window %s -> %s (%.2f days)" % (stamp, len(man["files"]), w_start.isoformat()[:16], w_end.isoformat()[:16], days))

        min_mod = {}                                   # key -> earliest version time (epoch s)
        win = defaultdict(lambda: [0, 0, 0, 0])        # key -> [versions in window, bytes, billable bytes, versions total]
        cur = defaultdict(lambda: [0, 0, 0])           # family -> [current objects, bytes, billable]
        cur_recent = Counter()                         # family -> current objects last modified inside the window
        latest_size = {}
        allv = Counter()                               # key -> versions in the inventory (capped at 11 by the 10-old-copies rule)
        rows = 0
        ws, we = w_start.timestamp(), w_end.timestamp()
        for n, f in enumerate(man["files"]):
            body = s3.get_object(Bucket=PRIVATE, Key=f["key"])["Body"]
            with gzip.GzipFile(fileobj=body) as gz:
                for row in csv.reader(io.TextIOWrapper(gz, encoding="utf-8", newline="")):
                    rows += 1
                    if row[ix["IsDeleteMarker"]].strip().lower() == "true":
                        continue
                    key = urllib.parse.unquote(row[ix["Key"]])
                    size = int(row[ix["Size"]] or 0)
                    t = ts(row[ix["LastModifiedDate"]])
                    e = t.timestamp() if t else 0
                    if key not in min_mod or e < min_mod[key]:
                        min_mod[key] = e
                    allv[key] += 1
                    if ws <= e < we:
                        w = win[key]
                        w[0] += 1
                        w[1] += size
                        w[2] += max(size, IA_MIN)
                    if row[ix["IsLatest"]].strip().lower() == "true":
                        fam = family(key)
                        c = cur[fam]
                        c[0] += 1
                        c[1] += size
                        c[2] += max(size, IA_MIN)
                        latest_size[key] = size
                        if ws <= e < we:
                            cur_recent[fam] += 1
            r.log("file %d/%d: rows %d, keys %d, keys written in window %d" % (n + 1, len(man["files"]), rows, len(min_mod), len(win)))

        fam_stats = defaultdict(lambda: Counter())
        files_in_data = []
        for key, (wn, wb, wbill, _) in win.items():
            vtot = allv[key]
            fam = family(key)
            is_new = min_mod[key] >= ws
            fs = fam_stats[fam]
            fs["writes"] += wn
            fs["new_keys"] += 1 if is_new else 0
            fs["rewrites"] += wn - (1 if is_new else 0)
            fs["bytes"] += wb
            fs["billable"] += wbill
            fs["keys_written"] += 1
            if vtot >= 11:
                fs["keys_at_cap"] += 1
            if fam in ("data/(files)", "(root files)", "data/warm/(files)", "data/providers/(files)"):
                files_in_data.append((key, wn, is_new, latest_size.get(key, 0)))

        r.section("access logs: who writes each family (yesterday, sampled)")
        writers = defaultdict(Counter)
        try:
            lg = s3.get_bucket_logging(Bucket=LIVE).get("LoggingEnabled") or {}
            tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
            base = "%s%s/%s/%s/%s/" % (tp, ACCOUNT, REGION, LIVE, (w_end - timedelta(days=1)).strftime("%Y/%m/%d"))
            keys, tok = [], None
            while True:
                kw = {"Bucket": tb, "Prefix": base, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                res = s3.list_objects_v2(**kw)
                keys += [o["Key"] for o in res.get("Contents", [])]
                if not res.get("IsTruncated") or len(keys) >= 900000:
                    break
                tok = res.get("NextContinuationToken")
            sample = keys[::max(1, len(keys) // 6000)][:6000]
            scale = len(keys) / float(len(sample) or 1)

            def read(k):
                try:
                    return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
                except Exception:  # noqa: BLE001
                    return ""
            with cf.ThreadPoolExecutor(max_workers=48) as ex:
                for text in ex.map(read, sample):
                    for line in text.splitlines():
                        m = LOG_RE.match(line)
                        if not m or m.group(7) not in WRITE_OPS or not m.group(10).startswith("2"):
                            continue
                        req = m.group(5)
                        who = req.split("/")[-1] if "assumed-role" in req else (req.split("/")[-1] if ":user/" in req else req[:30])
                        writers[family(m.group(8))][who] += scale
            r.log("log objects %d, read %d (x%.1f)" % (len(keys), len(sample), scale))
        except Exception as ex:  # noqa: BLE001
            r.warn("access-log section failed: %s %s" % (type(ex).__name__, str(ex)[:200]))

        out = []
        for fam in set(cur) | set(fam_stats):
            c, fs = cur.get(fam, [0, 0, 0]), fam_stats.get(fam, Counter())
            writes_day = fs["writes"] / days
            rewrites_day = fs["rewrites"] / days
            new_day = fs["new_keys"] / days
            bill_day_gb = fs["billable"] / days / 1e9
            put_month = writes_day * 30 * IA_PUT
            # a rewritten key's archived copies are replaced within days: each is billed IA's 30-day minimum
            min30_month = (fs["billable"] * (fs["rewrites"] / max(1, fs["writes"]))) / days / 1e9 * 30 * IA_GB_MONTH
            growth_gb_month = (fs["billable"] * (fs["new_keys"] / max(1, fs["writes"]))) / days / 1e9 * 30
            out.append({"family": fam, "current_objects": c[0], "current_gb": round(c[1] / 1e9, 3), "archive_gb": round(c[2] / 1e9, 3),
                        "writes_day": round(writes_day), "new_keys_day": round(new_day), "rewrites_day": round(rewrites_day),
                        "written_gb_day": round(fs["bytes"] / days / 1e9, 3), "billable_gb_day": round(bill_day_gb, 3),
                        "rebuilt_share": round(cur_recent.get(fam, 0) / c[0], 3) if c[0] else None,
                        "keys_at_cap": fs["keys_at_cap"],
                        "put_usd_month": round(put_month, 2), "min30_usd_month": round(min30_month, 2),
                        "growth_gb_month": round(growth_gb_month, 2),
                        "writers": [(w, round(v)) for w, v in writers.get(fam, Counter()).most_common(6)]})
        out.sort(key=lambda x: -(x["put_usd_month"] + x["min30_usd_month"] + x["growth_gb_month"] * IA_GB_MONTH))
        r.section("families by monthly archive cost of their writes")
        for x in out[:60]:
            r.log("%-58s writes/day %7d (new %6d, rewrites %6d) | PUT $%5.2f + 30d-min $%5.2f /mo, +%6.2f GB/mo | holds %7.2f GB, rebuilt %s | %s" % (
                x["family"][:58], x["writes_day"], x["new_keys_day"], x["rewrites_day"], x["put_usd_month"], x["min30_usd_month"],
                x["growth_gb_month"], x["archive_gb"], x["rebuilt_share"], x["writers"][:3]))
        files_in_data.sort(key=lambda t: -t[1])
        tot_writes = sum(x["writes_day"] for x in out)
        r.kv(inventory=stamp, window_days=round(days, 2), families=len(out), writes_day=tot_writes,
             put_usd_month=round(sum(x["put_usd_month"] for x in out), 2), min30_usd_month=round(sum(x["min30_usd_month"] for x in out), 2))
        latest = ROOT / "aws" / "ops" / "reports" / "latest"
        latest.mkdir(parents=True, exist_ok=True)
        (latest / "ops_6631_archive_scope_census_data.md").write_text(
            "# ops 6631 raw data (JSON)\n\n```json\n" + json.dumps({"inventory": stamp, "window": [w_start.isoformat(), w_end.isoformat()],
                                                                 "days": days, "families": out,
                                                                 "top_level_files_written": [list(t) for t in files_in_data[:4000]]},
                                                                default=str, separators=(",", ":")) + "\n```\n", encoding="utf-8")
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
