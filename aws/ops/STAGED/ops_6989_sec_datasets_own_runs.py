"""ops 6989 - justhodl-sec-datasets: its own scheduled runs, read from the warehouse (read-only, re-runnable).

The engine went out on 2026-11-27 with one disclosed first-light run (ops 6988). From then on it runs by itself at
22:47 UTC. This op invokes nothing and writes nothing. It reads:
  1. the schedule and the function as they stand (enabled, once a day, no self-retry);
  2. the bank's manifest: every run on record, which were the schedule's own, what each banked and read;
  3. how far the history has come for each of the seven data sets, and every file the engine said it does not read;
  4. the raw files against their recorded sizes;
  5. the two published documents: their dates, their size, their history;
  6. the Lambda's own report lines (seconds billed, memory used) for sizing and cost.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import math
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

NAME = "ops_6989_sec_datasets_own_runs"
REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN, SCHED = "justhodl-sec-datasets", "justhodl-sec-datasets-daily"
MANIFEST, MONEY, HOLD, RAW = "data/warm/sec-datasets/manifest.json", "data/sec-money-funds.json", "data/sec-fund-holdings.json", "sec-datasets/raw/"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=120)
MAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
PRICE_GB_SECOND = 0.0000166667
ALL_GB = 16.9                         # every file the seven pages advertise, by the sizes they show (ops 6987)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def clean(text):
    return MAIL.sub("(address held back)", str(text))


def bn(x):
    return "$%.1fbn" % ((x or 0) / 1e9)


def doc_of(s3, key):
    obj = s3.get_object(Bucket=LIVE, Key=key)
    body = obj["Body"].read()
    return json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body), len(body), obj["LastModified"]


def keys_under(s3, prefix):
    out, token = {}, None
    while True:
        kw = dict(Bucket=LIVE, Prefix=prefix, MaxKeys=1000)
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            out[o["Key"]] = o["Size"]
        token = resp.get("NextContinuationToken")
        if not token:
            return out


def main():
    s3, lam, sch, logs = c("s3"), c("lambda"), c("scheduler"), c("logs")
    with report(NAME) as r:
        say = lambda m: r.log(clean(m))       # noqa: E731
        fails = []

        def check(ok, text):
            (r.ok if ok else r.fail)(clean(text))
            if not ok:
                fails.append(text)

        now = datetime.now(timezone.utc)
        r.heading("ops 6989 - justhodl-sec-datasets: its own runs, read %s" % now.isoformat(timespec="minutes"))

        r.section("1. the schedule and the function")
        live = sch.get_schedule(Name=SCHED, GroupName="default")
        cf = lam.get_function_configuration(FunctionName=FN)
        try:
            retries = lam.get_function_event_invoke_config(FunctionName=FN).get("MaximumRetryAttempts")
        except Exception:                     # noqa: BLE001
            retries = "not set (Lambda's default is 2)"
        say("schedule %s %s | function %s MB, %s s, storage %s MB, modified %s | self-retries %s" % (
            live.get("ScheduleExpression"), live.get("State"), cf.get("MemorySize"), cf.get("Timeout"), (cf.get("EphemeralStorage") or {}).get("Size"), (cf.get("LastModified") or "")[:19], retries))
        check(live.get("State") == "ENABLED" and live.get("ScheduleExpression") == "cron(47 22 * * ? *)" and retries == 0, "enabled, once a day at 22:47 UTC, a failed run is not retried by itself")

        r.section("2. every run on record")
        manifest, size, modified = doc_of(s3, MANIFEST)
        runs = manifest.get("runs") or []
        say("manifest %d bytes, written %s, version %s | runs on record: %d" % (size, modified.isoformat(timespec="seconds"), manifest.get("version"), len(runs)))
        own = []
        for x in runs:
            whose = "first light (ops)" if x.get("trigger") else "the schedule's own"
            if not x.get("trigger"):
                own.append(x)
            say("  %s | %-18s | %5.0fs | banked %3d files %.2f GB | months read %2d | quarters read %d | own memory %s MB | published %s" % (
                x.get("at"), whose, x.get("seconds") or 0, x.get("banked") or 0, (x.get("bytes_banked") or 0) / 1024 ** 3, x.get("months_read") or 0, x.get("quarters_read") or 0,
                x.get("peak_memory_mb", "?"), json.dumps(x.get("published"))))
            for note in (x.get("notes") or [])[:6]:
                say("      note: %s" % note)
        due = now >= datetime(2026, 11, 27, 22, 57, tzinfo=timezone.utc)
        if not own:
            (check if due else (lambda ok, text: say("PENDING: " + text)))(False, "no run of the schedule's own is on record yet (the first is due 2026-11-27 22:47 UTC)")
        else:
            last = own[-1]
            age_h = (now - datetime.fromisoformat(last["at"])).total_seconds() / 3600
            check(age_h <= 26, "the schedule's own last run finished %.1f hours ago" % age_h)
            stamps = sorted(datetime.fromisoformat(x["at"]) for x in own)
            gaps = [(b - a).total_seconds() / 3600 for a, b in zip(stamps, stamps[1:])]
            check(all(g >= 23 for g in gaps), "the schedule's own runs are a day apart (%s)" % (", ".join("%.1fh" % g for g in gaps) or "one run so far"))

        r.section("3. how far each data set has come")
        t = manifest.get("totals") or {}
        for name, x in t.items():
            say("  %-9s advertised %4s | banked %3s (%.2f GB) | newest %s | read into tables %s | waiting %s" % (
                name, x.get("advertised"), x.get("banked"), (x.get("bytes") or 0) / 1024 ** 3, x.get("newest_banked"), x.get("read_into_tables"), x.get("waiting_to_be_read")))
        left = sum((x.get("advertised") or 0) - (x.get("banked") or 0) for x in t.values())
        banked_gb = sum((x.get("bytes") or 0) for x in t.values()) / 1024 ** 3
        say("files still to bank: %d of %d | %.2f of about %.1f GB banked: at 2.44 GB a run the history is complete in about %d more run(s); one quarter of holdings is read a run (%s waiting)" % (
            left, sum(x.get("advertised") or 0 for x in t.values()), banked_gb, ALL_GB, max(0, math.ceil((ALL_GB - banked_gb) / 2.44)), (t.get("nport") or {}).get("waiting_to_be_read")))
        unread = []
        for name, entry in (manifest.get("sets") or {}).items():
            for fname, rec in (entry.get("files") or {}).items():
                why = (rec.get("derived") or {}).get("not_handled")
                if why:
                    unread.append("%s: %s" % (fname, why[:110]))
        say("files the engine said it does not read: %d" % len(unread))
        for line in unread[:25]:
            say("  %s" % line)
        check(all(len(v.get("files") or {}) >= 1 for v in (manifest.get("sets") or {}).values()) and len(manifest.get("sets") or {}) == 7, "all seven data sets are in the bank")

        r.section("4. raw files against their recorded sizes")
        raw = keys_under(s3, RAW)
        wrong, n = [], 0
        for name, entry in (manifest.get("sets") or {}).items():
            for fname, rec in (entry.get("files") or {}).items():
                if "#" in fname or not rec.get("key"):
                    continue
                n += 1
                if raw.get(rec["key"]) != rec["bytes"]:
                    wrong.append(fname)
        say("raw files stored: %d, %.2f GB | on record: %d" % (len(raw), sum(raw.values()) / 1024 ** 3, n))
        check(not wrong and n == len(raw), "every recorded file is stored at its recorded size%s" % ("" if not wrong else ": " + ", ".join(wrong[:5])))

        r.section("5. the published documents")
        money, size, modified = doc_of(s3, MONEY)
        m = money["month"]
        say("%s: as of %s | %d bytes, written %s | %d funds, net assets %s (%s without feeders), repo %s, unsecured %s | history: %s" % (
            MONEY, money["as_of"], size, modified.isoformat(timespec="seconds"), m["funds"], bn(m["net_assets"]), bn(m.get("net_assets_ex_feeders")), bn(m["repo"]["total"]),
            bn(m["unsecured"]["total"]), ", ".join("%s %s" % (h["month"], bn(h["repo"])) for h in money["history"])))
        check(len(money["history"]) >= 13 and money["as_of"] >= "2026-10", "money funds: the newest month and at least thirteen months of history")
        months = [h["month"] for h in money["history"]]
        check(months == sorted(set(months)), "one row a month, in order")
        hold, size, modified = doc_of(s3, HOLD)
        s = hold["summary"]
        say("%s: as of %s from %s | %d bytes, written %s | %s funds, net assets %s | quarters read: %s" % (
            HOLD, hold["as_of"], hold["source"]["file"], size, modified.isoformat(timespec="seconds"), format(s["funds"], ","), bn(s["net_assets"]),
            ", ".join("%s (%s securities)" % (h["as_of"], format(h["securities"], ",")) for h in hold["history"])))
        check(hold["as_of"] >= "2026-06-30" and s["funds"] >= 10000, "fund holdings: the newest quarter stands")

        r.section("6. the Lambda's own report lines")
        events, token = [], None
        try:
            for _ in range(12):               # a filtered read comes back in pages, some of them empty
                kw = dict(logGroupName="/aws/lambda/%s" % FN, startTime=int((time.time() - 3 * 86400) * 1000), filterPattern='"REPORT RequestId"')
                if token:
                    kw["nextToken"] = token
                page = logs.filter_log_events(**kw)
                events += page.get("events", [])
                token = page.get("nextToken")
                if not token:
                    break
        except Exception as exc:              # noqa: BLE001
            say("logs not readable from here: %s" % type(exc).__name__)
        gb_seconds = 0.0
        for e in events:
            msg = e["message"]
            dur = re.search(r"Billed Duration: (\d+) ms", msg)
            mem = re.search(r"Max Memory Used: (\d+) MB", msg)
            size_mb = re.search(r"Memory Size: (\d+) MB", msg)
            if dur and size_mb:
                gb_seconds += int(dur.group(1)) / 1000 * int(size_mb.group(1)) / 1024
            say("  %s | billed %ss | memory used %s MB of %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc).isoformat(timespec="seconds"),
                                                              round(int(dur.group(1)) / 1000, 1) if dur else "?", mem.group(1) if mem else "?", size_mb.group(1) if size_mb else "?"))
        say("billed in the last three days: %.0f GB-seconds = $%.3f" % (gb_seconds, gb_seconds * PRICE_GB_SECOND))
        try:
            trouble = logs.filter_log_events(logGroupName="/aws/lambda/%s" % FN, startTime=int((time.time() - 3 * 86400) * 1000),
                                             filterPattern='?"Task timed out" ?Traceback ?OutOfMemory ?"[ERROR]"').get("events", [])
        except Exception:                     # noqa: BLE001
            trouble = []
        for e in trouble[:10]:
            say("  trouble: %s" % clean(e["message"].strip())[:300])
        check(not trouble, "no timeout, crash or out-of-memory line in three days of logs")

        r.kv(failed_checks=len(fails), own_runs=len(own), raw_files=len(raw))
        if fails:
            r.fail("%d check(s) failed" % len(fails))
            raise SystemExit(1)
        r.ok("read complete: nothing was invoked or written")


if __name__ == "__main__":
    main()
