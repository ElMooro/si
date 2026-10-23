"""ops 6635 - READ-ONLY complete check of archive scope B (applied by ops 6634; Khalid 2026-10-23 "2archive lets do B").

ops 6634 added ReplicationStatus to the live bucket's daily versions inventory. The first inventory taken after the
change therefore lists, for every version written since, whether S3 sent it to the archive. This op reads it and
checks every write made after the change settled (applied_at + 2 min, from ops 6634's report):
  - under an excluded prefix of config/archive-scope.json: no replication status at all (never sent);
  - everywhere else: COMPLETED (PENDING only if written in the last hour before the inventory; FAILED is a problem);
  - the self-test canaries behave the same way;
and counts what that means: archive copies per day before (ops 6632) and now, by excluded prefix and for the rest.
Also reads Cost Explorer's daily Standard-IA PUT requests (the archive's copies) for the days around the change.
Nothing is written but this report and its data file.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import csv
import gzip
import io
import json
import re
import sys
import urllib.parse

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
INV_PREFIX = "inventory/%s/" % LIVE
SCOPE = json.loads((ROOT / "config" / "archive-scope.json").read_text(encoding="utf-8"))
APPLY_REPORT = ROOT / "aws" / "ops" / "reports" / "latest" / "ops_6634_archive_scope_apply.md"
MEASURED = ROOT / "aws" / "ops" / "reports" / "latest" / "ops_6632_archive_scope_savings_data.md"
SETTLE = timedelta(minutes=2)
KEPT = "(kept)"


def applied_at(text):
    """The applied_at value from the Data table of ops 6634's report."""
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("| ") and "applied_at" in line and i + 2 < len(lines):
            head = [c.strip() for c in line.strip().strip("|").split("|")]
            vals = [c.strip() for c in lines[i + 2].strip().strip("|").split("|")]
            row = dict(zip(head, vals))
            if row.get("applied_at"):
                return datetime.fromisoformat(row["applied_at"]).astimezone(timezone.utc)
    return None


def matcher(prefixes):
    rx = re.compile("(?:%s)" % "|".join(re.escape(p) for p in sorted(prefixes, key=len, reverse=True)))

    def match(key):
        m = rx.match(key)
        return m.group(0) if m else KEPT
    return match


def ts(v):
    try:
        return datetime.strptime(v[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:  # noqa: BLE001
        return None


def selftest():
    sample = ("# x\n**Status:** success  \n\n## Data\n\n| applied_at | backup | skip_rules |\n|---|---|---|\n"
              "| 2026-10-23T20:01:02.123456+00:00 | b.json | 23 |\n\n## Log\n")
    assert applied_at(sample) == datetime(2026, 10, 23, 20, 1, 2, 123456, tzinfo=timezone.utc)
    assert applied_at("## Data\n\n| a |\n|---|\n| 1 |\n") is None
    m = matcher([e["prefix"] for e in SCOPE["exclude"]] + [SCOPE["self_test"]["excluded_prefix"]])
    assert m("_health/archive-scope-excluded/canary-x.json") == SCOPE["self_test"]["excluded_prefix"]
    assert m("_health/archive-scope-included/canary-x.json") == KEPT
    assert m("data/fundgraph/vintage/AAPL.json") == KEPT
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=300)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    excluded = [e["prefix"] for e in SCOPE["exclude"]]
    st_ex, st_in = SCOPE["self_test"]["excluded_prefix"], SCOPE["self_test"]["included_prefix"]
    match = matcher(excluded + [st_ex])
    with report("ops_6635_archive_scope_verify") as r:
        r.heading("ops 6635 - archive scope B: every write since the change, checked in the inventory (read-only)")
        t0 = applied_at(APPLY_REPORT.read_text(encoding="utf-8")) if APPLY_REPORT.exists() else None
        if not t0:
            r.fail("ops 6634 report has no applied_at - scope B was not applied")
            sys.exit(1)
        start = t0 + SETTLE
        mans, token = [], {}
        while True:
            res = s3.list_objects_v2(Bucket=PRIVATE, Prefix=INV_PREFIX, **token)
            mans += [o["Key"] for o in res.get("Contents", []) if o["Key"].endswith("/manifest.json") and "/hive/" not in o["Key"]]
            if not res.get("IsTruncated"):
                break
            token = {"ContinuationToken": res["NextContinuationToken"]}
        mans.sort()
        mkey = mans[-1] if mans else None
        inv_time = datetime.strptime(mkey.split("/")[-2], "%Y-%m-%dT%H-%MZ").replace(tzinfo=timezone.utc) if mkey else None
        if not mkey or inv_time <= start + timedelta(hours=1):
            r.fail("no inventory taken at least an hour after the change (applied %s, newest %s) - run again after 01:00 UTC + delivery" % (
                t0.isoformat()[:16], inv_time.isoformat()[:16] if inv_time else None))
            sys.exit(1)
        man = json.loads(s3.get_object(Bucket=PRIVATE, Key=mkey)["Body"].read())
        schema = [x.strip() for x in man["fileSchema"].split(",")]
        if "ReplicationStatus" not in schema:
            r.fail("inventory %s has no ReplicationStatus field (schema %s) - ops 6634 step 3 did not take" % (mkey.split("/")[-2], schema))
            sys.exit(1)
        ix = {n: i for i, n in enumerate(schema)}
        hours = (inv_time - start).total_seconds() / 3600.0
        r.section("inventory")
        r.log("applied %s; checking versions written %s -> %s (%.1f h), inventory %s" % (t0.isoformat()[:19], start.isoformat()[:16],
                                                                                        inv_time.isoformat()[:16], hours, mkey.split("/")[-2]))
        tally = defaultdict(Counter)
        bad = defaultdict(list)
        rows = 0
        for f in man["files"]:
            body = s3.get_object(Bucket=PRIVATE, Key=f["key"])["Body"]
            with gzip.GzipFile(fileobj=body) as gz:
                for row in csv.reader(io.TextIOWrapper(gz, encoding="utf-8", newline="")):
                    rows += 1
                    if row[ix["IsDeleteMarker"]].strip().lower() == "true":
                        continue
                    t = ts(row[ix["LastModifiedDate"]])
                    if not t or t < start or t >= inv_time:
                        continue
                    key = urllib.parse.unquote(row[ix["Key"]])
                    b = match(key)
                    rs = row[ix["ReplicationStatus"]].strip().upper()
                    tally[b][rs or "NONE"] += 1
                    young = t >= inv_time - timedelta(hours=1)
                    if b != KEPT and rs:
                        bad[b].append((key, rs, t.isoformat()[:19]))
                    elif b == KEPT and rs not in ("COMPLETED",) and not (rs == "PENDING" and young):
                        bad[KEPT].append((key, rs or "NONE", t.isoformat()[:19]))
        r.log("%d inventory rows read" % rows)

        r.section("writes since the change, by replication status")
        per_day = 24.0 / hours if hours else 0
        measured = {}
        if MEASURED.exists():
            d = json.loads(re.search(r"```json\n(.*)\n```", MEASURED.read_text(encoding="utf-8"), re.S).group(1))
            measured = {x["prefix"]: x["writes_day"] for x in d["excluded"]}
            measured[KEPT] = d["totals"]["kept"]["writes_day"]
        for b in [KEPT, st_ex] + excluded:
            c = tally.get(b, Counter())
            n = sum(c.values())
            r.log("%-66s %7d writes (%6d/day; before %s/day) %s%s" % (b[:66], n, n * per_day, measured.get(b, "-"), dict(c),
                                                                    "  <-- %d wrong" % len(bad[b]) if bad.get(b) else ""))
        st_kept = tally.get(KEPT, Counter())
        problems = []
        for b, items in bad.items():
            if b == KEPT:
                failed = [x for x in items if x[1] == "FAILED"]
                none = [x for x in items if x[1] == "NONE"]
                if failed or none:
                    problems.append("%d kept writes not archived (FAILED %d, none %d), e.g. %s" % (len(failed) + len(none), len(failed), len(none), (failed + none)[:3]))
                stale = [x for x in items if x[1] == "PENDING"]
                if stale:
                    problems.append("%d kept writes still PENDING after an hour, e.g. %s" % (len(stale), stale[:3]))
            else:
                problems.append("%d writes under excluded %s were sent to the archive, e.g. %s" % (len(items), b, items[:3]))
        archived_day = st_kept.get("COMPLETED", 0) * per_day
        r.kv(applied_at=t0.isoformat()[:19], hours_checked=round(hours, 1), kept_writes_archived_per_day=round(archived_day),
             excluded_writes_per_day=round(sum(sum(tally.get(p, Counter()).values()) for p in excluded) * per_day), problems=len(problems))

        r.section("Cost Explorer: Standard-IA PUT requests per day (the archive's copies)")
        try:
            ce = boto3.client("ce", region_name="us-east-1", config=cfg)
            end = datetime.now(timezone.utc).date()
            res = ce.get_cost_and_usage(TimePeriod={"Start": (end - timedelta(days=6)).isoformat(), "End": end.isoformat()},
                                        Granularity="DAILY", Metrics=["UsageQuantity", "UnblendedCost"],
                                        Filter={"Dimensions": {"Key": "USAGE_TYPE", "Values": ["Requests-SIA-Tier1"]}})
            for day in res["ResultsByTime"]:
                u = float(day["Total"]["UsageQuantity"]["Amount"])
                cst = float(day["Total"]["UnblendedCost"]["Amount"])
                r.log("%s: %9.0f IA PUTs  $%.2f%s" % (day["TimePeriod"]["Start"], u, cst, "  (estimated)" if day.get("Estimated") else ""))
        except Exception as ex:  # noqa: BLE001
            r.warn("Cost Explorer read failed: %s %s" % (type(ex).__name__, str(ex)[:160]))

        latest = ROOT / "aws" / "ops" / "reports" / "latest"
        (latest / "ops_6635_archive_scope_verify_data.md").write_text(
            "# ops 6635 raw data (JSON)\n\n```json\n" + json.dumps({"applied_at": t0.isoformat(), "inventory": mkey.split("/")[-2], "hours": hours,
                                                                 "tally": {k: dict(v) for k, v in tally.items()},
                                                                 "bad_examples": {k: v[:20] for k, v in bad.items()}, "problems": problems},
                                                                default=str, separators=(",", ":")) + "\n```\n", encoding="utf-8")
        if problems:
            for p in problems:
                r.warn(p)
            r.fail("%d problem(s)" % len(problems))
            sys.exit(1)
        r.ok("every write since the change went where the scope says: excluded folders stayed out, everything else was archived")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
