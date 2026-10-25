"""ops 6655 - justhodl-polygon-eod birth: declare, first fold + 1 kick, data.html card (Khalid 2026-10-25: "100% pulled and used").

After deploy-lambdas ships justhodl-polygon-eod v1.0.0 (its config creates the Scheduler schedule justhodl-polygon-eod-
daily, 03:10 UTC) and justhodl-polygon-full v1.1.0 (hole repair):
  1. function + schedule exist;
  2. deployed schedule manifest := committed (additions-only, live-verified);
  3. first fold now + one kick: each run folds up to 120 of polygon-full's ~1,300 grouped sessions into the per-ticker
     files data/warm/polygon-full/eod/<T>.json.gz; the bounded conductor (fmp-backfill.yml) and the daily schedule
     finish the backfill; the manifest's `gaps` (weekdays with no session that are not recorded holidays) is read back -
     polygon-full's own 02:29 UTC run with hole repair closes them;
  4. data.html card: data/providers/extra-providers.json gains polygon-eod (count_from the manifest), then
     justhodl-provider-catalog is invoked async;
  5. one ticker's file read back (AAPL: n, first, last, the last row).
Direct lane (STAGED): idempotent, no serial twin; lease-guarded kicks.  No key involved.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import io
import json
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
FN, SCHED = "justhodl-polygon-eod", "justhodl-polygon-eod-daily"
BANK = "data/warm/polygon-full/eod/"
MAN, STATE = BANK + "manifest.json", BANK + "_state/state.json"
EXTRA = "data/providers/extra-providers.json"
MAX_KICKS = 2
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=300)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def jget(s3, key, default=None):
    try:
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if key.endswith(".gz") else raw)
    except Exception:  # noqa: BLE001
        return default


def wait_run(s3, prev_updated, deadline_s):
    t_end = time.time() + deadline_s
    while time.time() < t_end:
        time.sleep(20)
        st = jget(s3, STATE) or {}
        if st.get("updated_at") and st.get("updated_at") != prev_updated and not float(st.get("lease_until") or 0) > time.time():
            return st
    return None


def main():
    s3, lam, sch = c("s3"), c("lambda"), c("scheduler")
    with report("ops_6655_polygon_eod_birth") as r:
        r.heading("ops 6655 - justhodl-polygon-eod: declare, first fold + 1 kick, data.html card")
        r.section("1. function + schedule")
        try:
            cf = lam.get_function_configuration(FunctionName=FN)
        except Exception as e:  # noqa: BLE001
            r.fail("%s not deployed yet (%s) - dispatch deploy-lambdas function=%s, then run again" % (FN, type(e).__name__, FN))
            raise SystemExit(1)
        r.log("%s: runtime %s, memory %s, timeout %s, state %s, modified %s" % (FN, cf.get("Runtime"), cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), (cf.get("LastModified") or "")[:19]))
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            r.log("schedule %s: %s %s tz %s target %s" % (SCHED, live.get("ScheduleExpression"), live.get("State"), live.get("ScheduleExpressionTimezone"), fn_of((live.get("Target") or {}).get("Arn"))))
        except Exception as e:  # noqa: BLE001
            r.fail("schedule %s missing (%s)" % (SCHED, type(e).__name__))
            raise SystemExit(1)
        pf = lam.get_function_configuration(FunctionName="justhodl-polygon-full")
        r.log("justhodl-polygon-full modified %s (v1.1.0 hole repair expected)" % (pf.get("LastModified") or "")[:19])

        r.section("2. deployed schedule manifest := committed (additions-only)")
        repo_text = (ROOT / KEY).read_text(encoding="utf-8")
        repo = json.loads(repo_text)
        old_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        a, b = entries(json.loads(old_text)), entries(repo)
        removed = sorted(k for k in a if k not in b)
        added = sorted(k for k in b if k not in a)
        changed = sorted(k for k in a if k in b and {x: a[k][x] for x in ("expr", "state")} != {x: b[k][x] for x in ("expr", "state")})
        r.log("S3 %d entries, repo %d; added %s removed %d changed %d" % (len(a), len(b), added, len(removed), len(changed)))
        if removed or changed:
            r.fail("not additions-only: removed %s changed %s" % (removed[:5], changed[:5]))
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

        r.section("3. first fold + kick")
        kicks = 0
        budget_end = time.time() + 38 * 60
        while kicks < MAX_KICKS and time.time() < budget_end:
            st = jget(s3, STATE) or {}
            if float(st.get("lease_until") or 0) > time.time():
                r.log("lease held until %s - waiting" % datetime.fromtimestamp(st["lease_until"], timezone.utc).strftime("%H:%M:%S"))
                time.sleep(60)
                continue
            if kicks and st.get("phase") == "STEADY":
                break
            prev = st.get("updated_at")
            resp = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
            kicks += 1
            r.log("kick %d: async invoke %s at %s" % (kicks, resp.get("StatusCode"), datetime.now(timezone.utc).strftime("%H:%M:%S")))
            st = wait_run(s3, prev, 16 * 60)
            if not st:
                r.warn("no state write within 16 min after kick %d - read CloudWatch /aws/lambda/%s" % (kicks, FN))
                break
            run = st.get("last_run") or {}
            r.kv(kick=kicks, phase=st.get("phase"), sessions=len(run.get("sessions") or []), rows=run.get("rows"), tickers_written=run.get("tickers_written"),
                 unchanged=run.get("tickers_unchanged"), steps=json.dumps(run.get("steps"))[:600], errors=len(run.get("errors") or []))
        man = jget(s3, MAN) or {}
        r.log("manifest: phase %s sessions available %s folded %s pending %s tickers %s first %s last %s" % (man.get("phase"), man.get("sessions_available"), man.get("sessions_folded"), man.get("sessions_pending"), man.get("tickers"), man.get("first_session"), man.get("last_session")))
        r.log("gaps (weekdays with no session, not recorded holidays): %s" % (man.get("gaps") or [])[:40])
        r.log("holidays recorded by polygon-full: %s" % (man.get("holidays_recorded") or [])[-12:])
        if not man.get("tickers"):
            r.fail("nothing folded after %d kicks" % kicks)
            raise SystemExit(1)

        r.section("4. data.html card (extra-providers.json, add-only) + catalog refresh")
        extra = jget(s3, EXTRA, {}) or {}
        if isinstance(extra, list):
            extra = {p.get("slug", p.get("id")): p for p in extra if isinstance(p, dict)}
        if "polygon-eod" not in extra:
            extra["polygon-eod"] = {
                "name": "Polygon/Massive — per-ticker daily histories (transposed from the grouped tape)", "api": "none (bank-only: data/warm/polygon-full/grouped)",
                "engines": ["justhodl-polygon-eod", "justhodl-polygon-full"], "prefixes": [],
                "count_from": [MAN, "pages", "pages_bytes"], "hot": [MAN],
                "cadence": "EOD", "label": "Every US ticker's daily OHLCV/VWAP/trades since 2021-08-26 as one file per ticker, folded from justhodl-polygon-full's verbatim grouped sessions (02:29 UTC) at 03:10 UTC — Polygon's own numbers, never LIVE; gaps published in the manifest"}
            s3.put_object(Bucket=LIVE, Key=EXTRA, Body=json.dumps(extra, indent=1).encode(), ContentType="application/json", CacheControl="no-cache")
            r.ok("extra-providers.json += polygon-eod (%d entries)" % len(extra))
        else:
            r.ok("extra-providers.json already carries polygon-eod")
        try:
            lam.invoke(FunctionName="justhodl-provider-catalog", InvocationType="Event", Payload=b"{}")
            r.log("justhodl-provider-catalog invoked async (card lands within ~10 min; daily 06:20 otherwise)")
        except Exception as e:  # noqa: BLE001
            r.warn("provider-catalog invoke: %s" % type(e).__name__)

        r.section("5. one ticker read back")
        doc = jget(s3, BANK + "AAPL.json.gz") or {}
        rows = doc.get("rows") or []
        r.log("AAPL: n %s first %s last %s fields %s last row %s" % (doc.get("n"), doc.get("first"), doc.get("last"), doc.get("fields"), rows[-1] if rows else None))
        r.ok("ops 6655 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
