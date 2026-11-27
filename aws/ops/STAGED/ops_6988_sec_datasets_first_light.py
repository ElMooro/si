"""ops 6988 - justhodl-sec-datasets: birth and first light (re-runnable).

Khalid 2026-11-26: "go and add as many files as my system needs especially the ones that would improve my system and
its data exponentially". After deploy-lambdas ships the engine:
  1. the function exists with config.json's runtime / memory / timeout / storage / env; the Scheduler schedule
     justhodl-sec-datasets-daily (22:47 UTC) exists, is ENABLED and targets it; a failed run is not retried by itself;
  2. deployed schedule manifest := committed (additions-only, the added entry live-verified) - the reconciler disables
     what the manifest does not declare;
  3. the Lambda's own dry run: pages read, nothing fetched or written (the warehouse is listed before and after);
  4. first light: the engine once (an Event invoke, watched through the bank's manifest; a first-light invoke through
     ops is the precedent of every engine birth, and it is disclosed) - skipped when a run already read the newest
     month and quarter;
  5. what the warehouse now holds: every raw file against the SEC's own size, its sha256 recomputed for the newest
     N-MFP file, raw files not reachable from outside, the published documents reachable;
  6. the published tables against a second reading on the runner: the same engine code over the banked raw files
     (every count and sum must agree), and no CUSIP or ISIN of the SEC's file in anything under data/.
Writes nothing but the schedule manifest (additions-only, with a backup), the function's retry setting, and what the
engine owns. Identifiers are held in memory for step 6 only and never logged.
"""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import gzip
import hashlib
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-sec-datasets" / "source"))
from ops_report import report  # noqa: E402

sys.modules.setdefault("chain_guard", SimpleNamespace(begin=lambda event, context=None: 0))
import lambda_function as LF  # noqa: E402
import sec_derive as D  # noqa: E402
import sec_sets as S  # noqa: E402

NAME = "ops_6988_sec_datasets_first_light"
REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
FN, SCHED = "justhodl-sec-datasets", "justhodl-sec-datasets-daily"
TRIGGER = "ops_6988_first_light"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=16)
ONCE = Config(retries={"max_attempts": 0}, connect_timeout=10, read_timeout=300)      # an invoke is never repeated by the client
MAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")


def c(name, cfg=CFG):
    return boto3.client(name, region_name=REGION, config=cfg)


def clean(text):
    return MAIL.sub("(address held back)", str(text))


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def bn(x):
    return "$%.1fbn" % ((x or 0) / 1e9)


def doc_of(s3, key):
    body = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
    return json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body), len(body)


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


def outside(url):
    """HTTP status of a plain anonymous request (what a stranger would get)."""
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="GET", headers={"User-Agent": "Mozilla/5.0 jh-ops", "Range": "bytes=0-63"}), timeout=30) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code
    except Exception as exc:                  # noqa: BLE001
        return "no answer (%s)" % type(exc).__name__


def words_of(obj, out=None):
    """Every string in a document: identifiers are looked for in text, never in numbers (a dollar value can equal a CUSIP made of digits)."""
    out = [] if out is None else out
    if isinstance(obj, str):
        out.append(obj)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            out.append(k)
            words_of(v, out)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            words_of(v, out)
    return out


def near(a, b, tol=2):
    """Equal, allowing sums of dollars to differ by a rounding (two readings may add the same rows in another order)."""
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(near(a[k], b[k], tol) for k in a)
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(near(x, y, tol) for x, y in zip(a, b))
    if isinstance(a, bool) or isinstance(b, bool) or a is None or b is None or isinstance(a, str) or isinstance(b, str):
        return a == b
    return abs(a - b) <= tol


def log_tail(logs, minutes=25, pattern=None):
    try:
        kw = dict(logGroupName="/aws/lambda/%s" % FN, startTime=int((time.time() - minutes * 60) * 1000), limit=60)
        if pattern:
            kw["filterPattern"] = pattern
        return [clean(e["message"].strip())[:400] for e in logs.filter_log_events(**kw).get("events", [])]
    except Exception as exc:                  # noqa: BLE001
        return ["(logs not readable from here: %s)" % type(exc).__name__]


def main():
    s3, lam, sch, logs = c("s3"), c("lambda"), c("scheduler"), c("logs")
    lam_once = c("lambda", ONCE)
    with report(NAME) as r:
        say = lambda m: r.log(clean(m))       # noqa: E731
        fails = []

        def check(ok, text):
            (r.ok if ok else r.fail)(clean(text))
            if not ok:
                fails.append(text)

        r.heading("ops 6988 - justhodl-sec-datasets: birth and first light %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. function + schedule")
        cfg = json.loads((ROOT / "aws/lambdas/justhodl-sec-datasets/config.json").read_text(encoding="utf-8"))
        try:
            cf = lam.get_function_configuration(FunctionName=FN)
        except Exception as e:                # noqa: BLE001
            r.fail("%s not deployed yet (%s) - wait for deploy-lambdas, then run again" % (FN, type(e).__name__))
            raise SystemExit(1)
        env = (cf.get("Environment") or {}).get("Variables", {})
        storage = (cf.get("EphemeralStorage") or {}).get("Size")
        say("%s: runtime %s, memory %s, timeout %s, storage %s MB, state %s, modified %s, handler %s, env %s" % (
            FN, cf.get("Runtime"), cf.get("MemorySize"), cf.get("Timeout"), storage, cf.get("State"), (cf.get("LastModified") or "")[:19], cf.get("Handler"),
            json.dumps({k: env[k] for k in sorted(env) if k in cfg["env"]})))
        if cf.get("Runtime") != cfg["runtime"] or cf.get("MemorySize") != cfg["memory"] or cf.get("Timeout") != cfg["timeout"] or cf.get("Handler") != cfg["handler"] \
                or storage != cfg["ephemeral_storage"]:
            r.fail("live configuration differs from config.json")
            raise SystemExit(1)
        diff = {k: (env.get(k), v) for k, v in cfg["env"].items() if env.get(k) != v}
        if diff or cf.get("State") != "Active":
            r.fail("live environment differs from config.json or the function is not Active: %s %s" % (json.dumps(diff), cf.get("State")))
            raise SystemExit(1)
        sc = cfg["eventbridge_scheduler"]
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
        except Exception as e:                # noqa: BLE001
            r.fail("schedule %s missing (%s)" % (SCHED, type(e).__name__))
            raise SystemExit(1)
        say("schedule %s: %s %s tz %s target %s" % (SCHED, live.get("ScheduleExpression"), live.get("State"), live.get("ScheduleExpressionTimezone"), fn_of((live.get("Target") or {}).get("Arn"))))
        if live.get("ScheduleExpression") != sc["cron"] or fn_of((live.get("Target") or {}).get("Arn")) != FN or live.get("State") != "ENABLED":
            r.fail("live schedule differs from config (%s vs %s)" % (live.get("ScheduleExpression"), sc["cron"]))
            raise SystemExit(1)
        try:
            eic = lam.get_function_event_invoke_config(FunctionName=FN)
            attempts = eic.get("MaximumRetryAttempts")
        except Exception:                     # noqa: BLE001 - none set: Lambda's default is two retries
            attempts = None
        if attempts != 0:
            lam.put_function_event_invoke_config(FunctionName=FN, MaximumRetryAttempts=0, MaximumEventAgeInSeconds=900)
            say("a failed run was set to be retried %s time(s) by Lambda itself; now 0: a run that fails waits for the next day" % ("2 (the default)" if attempts is None else attempts))
        r.ok("function and schedule match config.json (one run a day, no self-retry)")

        r.section("2. deployed schedule manifest := committed (additions-only)")
        repo_text = (ROOT / KEY).read_text(encoding="utf-8")
        repo = json.loads(repo_text)
        old_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        a, b = entries(json.loads(old_text)), entries(repo)
        removed = sorted(k for k in a if k not in b)
        added = sorted(k for k in b if k not in a)
        changed = sorted(k for k in a if k in b and {x: a[k][x] for x in ("expr", "state")} != {x: b[k][x] for x in ("expr", "state")})
        say("S3 %d entries, repo %d; added %s removed %d changed %d" % (len(a), len(b), added, len(removed), len(changed)))
        if removed or changed:
            r.fail("not additions-only: removed %s changed %s - the deployed manifest is left as it is" % (removed[:5], changed[:5]))
            raise SystemExit(1)
        for kind, name in added:
            e = b[(kind, name)]
            if kind != "scheduler":
                r.fail("unexpected added entry %s %s" % (kind, name))
                raise SystemExit(1)
            lv = sch.get_schedule(Name=name, GroupName=e.get("group") or "default")
            if not (lv.get("ScheduleExpression") == e["expr"] and lv.get("State") == e["state"] and fn_of((lv.get("Target") or {}).get("Arn")) == fn_of(e["targets"][0]["arn"])):
                r.fail("declared %s does not match live" % name)
                raise SystemExit(1)
        if added:
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s/schedule-manifest.json" % stamp, Body=old_text.encode("utf-8"), ServerSideEncryption="AES256")
            s3.put_object(Bucket=LIVE, Key=KEY, Body=repo_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            r.ok("manifest uploaded (+%d), backup backups/schedule-manifest/%s/" % (len(added), stamp))
        else:
            r.ok("manifest already equal")

        r.section("3. the Lambda's own dry run: nothing fetched, nothing written")
        before = dict(keys_under(s3, LF.RAW_PREFIX), **keys_under(s3, LF.OUT_PREFIX))
        t0 = time.time()
        resp = lam_once.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"dry_run": True, "trigger": "ops_6988_dry_run"}).encode())
        dry = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError") or not dry.get("dry_run"):
            r.fail("dry run failed: %s" % clean(json.dumps(dry))[:900])
            raise SystemExit(1)
        after = dict(keys_under(s3, LF.RAW_PREFIX), **keys_under(s3, LF.OUT_PREFIX))
        say("%.0fs | version %s | advertised %s | already banked %s | waiting to be read %s" % (
            time.time() - t0, dry.get("version"), json.dumps(dry.get("advertised")), json.dumps(dry.get("already_banked")), json.dumps(dry.get("waiting_to_be_read"))))
        say("would bank %d files (%.2f GB shown): %s ..." % (len(dry["would_bank"]), dry["would_bank_bytes_shown"] / 1024 ** 3, ", ".join("%s/%s" % (x[0], x[1]) for x in dry["would_bank"][:9])))
        check(before == after, "the warehouse is unchanged by the dry run (%d objects under the engine's prefixes)" % len(after))
        check(set(dry.get("advertised") or {}) == set(S.SETS), "the Lambda reached all seven data-set pages on sec.gov")

        r.section("4. first light")
        try:
            manifest, _ = doc_of(s3, LF.MANIFEST_KEY)
        except Exception:                     # noqa: BLE001
            manifest = None
        t = (manifest or {}).get("totals") or {}
        have_all = bool(manifest) and (t.get("nmfp") or {}).get("read_into_tables", 0) >= 1 and (t.get("nport") or {}).get("read_into_tables", 0) >= 1
        if have_all:
            say("a run already read the newest month and quarter (last run %s): no invoke" % json.dumps((manifest.get("runs") or [{}])[-1].get("at")))
        else:
            n_runs = len((manifest or {}).get("runs") or [])
            t0 = time.time()
            resp = lam_once.invoke(FunctionName=FN, InvocationType="Event", Payload=json.dumps({"trigger": TRIGGER}).encode())
            say("one-off invoke sent (Event, status %s) at %s - disclosed: this is the engine's first run, outside its 22:47 UTC schedule" % (
                resp.get("StatusCode"), datetime.now(timezone.utc).strftime("%H:%M:%S UTC")))
            done = None
            while time.time() - t0 < 1000:
                time.sleep(20)
                try:
                    m, _ = doc_of(s3, LF.MANIFEST_KEY)
                except Exception:             # noqa: BLE001
                    continue
                runs = m.get("runs") or []
                if len(runs) > n_runs and runs[-1].get("trigger") == TRIGGER:
                    done, manifest = runs[-1], m
                    break
            if not done:
                for line in log_tail(logs)[-25:]:
                    say("  log: %s" % line)
                r.fail("no finished run in the manifest after %.0fs" % (time.time() - t0))
                raise SystemExit(1)
            say("finished after %.0fs wall: %s" % (time.time() - t0, json.dumps({k: done.get(k) for k in ("seconds", "banked", "bytes_banked", "months_read", "quarters_read", "published")})))
            for note in done.get("notes") or []:
                say("  note: %s" % note)
        for line in log_tail(logs, pattern="REPORT")[-4:]:
            say("  lambda: %s" % line[:300])
        t = manifest.get("totals") or {}
        for name in S.SETS:
            x = t.get(name) or {}
            say("  %-9s advertised %4s | banked %3s (%.2f GB) | newest %s | read into tables %s | waiting %s" % (
                name, x.get("advertised"), x.get("banked"), (x.get("bytes") or 0) / 1024 ** 3, x.get("newest_banked"), x.get("read_into_tables"), x.get("waiting_to_be_read")))
        check(all((t.get(n) or {}).get("banked", 0) >= 1 for n in S.SETS), "the newest file of all seven sets is in the warehouse")
        check((t.get("nmfp") or {}).get("read_into_tables", 0) >= 1 and (t.get("nport") or {}).get("read_into_tables", 0) >= 1, "a month of money funds and a quarter of holdings were read into tables")

        r.section("5. what the warehouse holds")
        raw = keys_under(s3, LF.RAW_PREFIX)
        say("raw files: %d, %.2f GB under %s" % (len(raw), sum(raw.values()) / 1024 ** 3, LF.RAW_PREFIX))
        wrong, checked = [], 0
        for name, entry in manifest["sets"].items():
            for fname, rec in entry.get("files", {}).items():
                if "#" in fname or not rec.get("key"):
                    continue
                checked += 1
                if raw.get(rec["key"]) != rec["bytes"]:
                    wrong.append("%s recorded %s, stored %s" % (fname, rec["bytes"], raw.get(rec["key"])))
        check(not wrong and checked == len(raw), "every recorded file is stored at its recorded size (%d files)%s" % (checked, "" if not wrong else ": " + "; ".join(wrong[:3])))
        check(all(k.startswith("sec-datasets/raw/") and not k.startswith("data/") for k in raw), "raw files are outside the site's data/ tree")
        work = tempfile.mkdtemp(prefix="secds-fl-")
        newest_m = manifest["totals"]["nmfp"]["newest_banked"]
        rec_m = manifest["sets"]["nmfp"]["files"][newest_m]
        path_m = os.path.join(work, newest_m)
        s3.download_file(LIVE, rec_m["key"], path_m)
        head = s3.head_object(Bucket=LIVE, Key=rec_m["key"])
        digest = hashlib.sha256(open(path_m, "rb").read()).hexdigest()
        _, page, _ = LF.sec_get(S.SETS["nmfp"]["page"])
        urls = {f["name"]: f["url"] for f in S.advertised(page or b"", S.SETS["nmfp"]["page"], S.SETS["nmfp"]["match"])}
        status, _, headers = LF.sec_get(urls[newest_m], method="HEAD") if newest_m in urls else (0, None, {})
        say("%s: stored sha256 %s... | recomputed %s... | metadata %s | sec.gov now says %s bytes (HTTP %s)" % (
            newest_m, rec_m["sha256"][:16], digest[:16], json.dumps({k: (v[:16] + "..." if k == "sha256" else v) for k, v in (head.get("Metadata") or {}).items()}), headers.get("content-length"), status))
        check(digest == rec_m["sha256"] == (head.get("Metadata") or {}).get("sha256"), "the stored file is byte for byte what was recorded")
        check(status != 200 or int(headers.get("content-length") or 0) == rec_m["bytes"], "and the size the SEC serves today")
        probe = raw and sorted(raw)[0]
        for label, url in (("the site", "https://justhodl.ai/%s" % probe), ("the bucket itself", "https://%s.s3.amazonaws.com/%s" % (LIVE, probe))):
            code = outside(url)
            say("a stranger asking %s for a raw file: %s" % (label, code))
            check(code not in (200, 206), "raw files are not served by %s" % label)
        for key in (LF.MONEY_FUNDS_KEY, LF.HOLDINGS_KEY, LF.MANIFEST_KEY):
            say("a stranger asking the site for %s: %s" % (key, outside("https://justhodl.ai/%s" % key)))

        r.section("6. the published tables against a second reading on the runner")
        money, size = doc_of(s3, LF.MONEY_FUNDS_KEY)
        month = money["month"]
        with zipfile.ZipFile(path_m) as zf:
            again = D.nmfp_month(zf)
        say("money funds %s (%d bytes): %d funds, net assets %s, repo %s to %d counterparties, unsecured %s, %d flow days | history rows %d" % (
            money["as_of"], size, month["funds"], bn(month["net_assets"]), bn(month["repo"]["total"]), month["repo"]["n_counterparties"], bn(month["unsecured"]["total"]),
            len(month["flows"]["days"]), len(money["history"])))
        same = all(near(month[k], again[k]) for k in ("month", "report_dates", "funds", "net_assets", "by_type", "holdings", "repo", "unsecured", "flows"))
        check(same, "the published month equals a second reading of the banked file, figure for figure")
        check(month["funds"] >= 250 and 5e12 <= month["net_assets"] <= 1.1e13, "its size is the industry's: %d funds, %s" % (month["funds"], bn(month["net_assets"])))

        hold, size = doc_of(s3, LF.HOLDINGS_KEY)
        newest_q = hold["source"]["file"]
        rec_q = manifest["sets"]["nport"]["files"][newest_q]
        path_q = os.path.join(work, newest_q)
        t0 = time.time()
        s3.download_file(LIVE, rec_q["key"], path_q)
        second = D.nport_quarter(path_q, work)
        say("second reading of %s on the runner: %.0fs" % (newest_q, time.time() - t0))
        tables = {part: doc_of(s3, key) for part, key in hold["tables"].items()}
        say("published: %s" % ", ".join("%s %.1f MB" % (hold["tables"][p].rsplit("/", 1)[-1], tables[p][1] / 1048576) for p in tables))
        pub = {p: tables[p][0] for p in tables}
        s1, s2 = pub["summary"], second["summary"]
        say("holdings as of %s: %s funds, net assets %s, %s holding rows | %s securities | credit swaps %d positions on %d names, %s | stock on loan %s of %s" % (
            s1["as_of"], format(s1["funds"], ","), bn(s1["net_assets"]), format(s1["holding_rows"], ","), format(pub["bonds"]["totals"]["securities"], ","),
            pub["cds"]["totals"]["positions"], pub["cds"]["totals"]["reference_names"], bn(pub["cds"]["totals"]["notional_usd"]),
            bn(pub["stock_loan"]["totals"]["on_loan"]), bn(pub["stock_loan"]["totals"]["held"])))
        check(all(near(s1[k], s2[k]) for k in ("as_of", "report_dates", "filings_in_file", "funds", "net_assets", "holding_rows", "classes", "flows_by_month")), "summary equals the second reading")
        check(pub["bonds"]["totals"] == second["bonds"]["totals"] and len(pub["bonds"]["rows"]) == len(second["bonds"]["rows"]), "bonds and loans: the same counts %s" % json.dumps(pub["bonds"]["totals"]))
        vi, fi, pi, ni = (pub["bonds"]["cols"].index(k) for k in ("value", "funds", "px", "n_px"))
        sums = lambda rows: (sum(x[vi] or 0 for x in rows), sum(x[fi] for x in rows), sum(x[ni] for x in rows), round(sum(x[pi] or 0 for x in rows), 1))      # noqa: E731
        p1, p2 = sums(pub["bonds"]["rows"]), sums(second["bonds"]["rows"])
        check(abs(p1[0] - p2[0]) <= len(pub["bonds"]["rows"]) and p1[1:3] == p2[1:3] and abs(p1[3] - p2[3]) <= 1, "bonds and loans: the same value, fund counts and marks in total %s" % (p1,))
        by1, by2 = ({x[0]: x[1:8] for x in d["names"]} for d in (pub["cds"], second["cds"]))
        check(near(pub["cds"]["totals"], second["cds"]["totals"]) and by1.keys() == by2.keys() and all(near(by1[k], by2[k]) for k in by1), "credit swaps: every name's row is the same (%d names)" % len(by1))
        check(near(pub["stock_loan"]["totals"], second["stock_loan"]["totals"], tol=len(second["stock_loan"]["rows"])) and len(pub["stock_loan"]["rows"]) == len(second["stock_loan"]["rows"]),
              "stock on loan: the same totals")
        check(s1["funds"] >= 10000 and pub["bonds"]["totals"]["securities"] >= 150000, "its size is the industry's")

        cusips, isins = set(), set()
        with zipfile.ZipFile(path_q) as zf:
            header, it = D.rows_of(zf, D.member(zf, "FUND_REPORTED_HOLDING"))
            ci = header.index("ISSUER_CUSIP")
            for parts in it:
                if len(parts[ci]) == 9 and parts[ci] not in ("000000000", "N/A"):
                    cusips.add(parts[ci])
            info = D.member(zf, "IDENTIFIERS")
            if info is not None:
                header, it = D.rows_of(zf, info)
                ii = header.index("IDENTIFIER_ISIN")
                for parts in it:
                    if len(parts[ii]) == 12:
                        isins.add(parts[ii])
        public = keys_under(s3, "data/warm/sec-datasets/")
        public.update({LF.MONEY_FUNDS_KEY: 0, LF.HOLDINGS_KEY: 0})
        leak9, leak12, scanned = set(), set(), 0
        for key in sorted(public):
            body = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
            text = "\n".join(words_of(json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)))
            scanned += len(text)
            leak9 |= {tok for tok in set(re.findall(r"(?<![A-Za-z0-9])[A-Z0-9]{9}(?![A-Za-z0-9])", text)) & cusips if re.search(r"\d", tok)}
            leak12 |= set(re.findall(r"(?<![A-Za-z0-9])[A-Z0-9]{12}(?![A-Za-z0-9])", text)) & isins
        say("published objects scanned: %d (%.1f MB of names and titles) against %s CUSIPs and %s ISINs of the SEC's file" % (len(public), scanned / 1048576, format(len(cusips), ","), format(len(isins), ",")))
        check(not leak9 and not leak12, "no CUSIP and no ISIN appears in anything under data/ (%d and %d found)" % (len(leak9), len(leak12)))

        r.kv(failed_checks=len(fails), raw_files=len(raw), raw_gb=round(sum(raw.values()) / 1024 ** 3, 2))
        if fails:
            r.fail("%d check(s) failed" % len(fails))
            raise SystemExit(1)
        r.ok("first light complete")


if __name__ == "__main__":
    main()
