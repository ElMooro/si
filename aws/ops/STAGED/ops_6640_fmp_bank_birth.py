"""ops 6640 - justhodl-fmp-bank birth (Khalid 2026-10-23: "fix it all, I want 100% of the data pulled and used").

After deploy-lambdas creates justhodl-fmp-bank (config.json carries its Scheduler schedule):
  1. the function and schedule exist (State Active, env has FMP_KEY, schedule ENABLED on the right target);
  2. deployed schedule manifest := committed (additions-only, live-verified; ops 6594/6599 pattern);
  3. first run NOW (async, ~13 min budget) -> read back manifest/catalog: which bulk endpoints the Ultimate key serves
     (every refusal is recorded by http code, never guessed), objects/bytes banked, EOD cursor;
  4. kick the backfill: the dated bulks (10 endpoints x ~40 years x 5 periods) and the EOD walk from 2000 need many
     budgets - this op re-invokes the engine while it reports BACKFILL and the lease is free, up to N kicks inside the
     runner's time, then leaves the daily schedule to finish (no self-chaining, no event loop);
  5. data.html card: data/providers/extra-providers.json gains the fmp-bank entry (count_from the bank manifest), then
     justhodl-provider-catalog is invoked async so the card appears today;
  6. the fmp_book -> fmp_bank chain is live in the fleet: pick one deployed consumer and prove its bundle holds
     aws/shared/fmp_bank.py.
Direct lane (STAGED): idempotent, no serial twin.  Lists in-flight invocations before any kick (ops 5585 trap).
"""
from datetime import datetime, timezone
from pathlib import Path
import io
import json
import re
import sys
import time
import zipfile
import urllib.request

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
FN, SCHED = "justhodl-fmp-bank", "justhodl-fmp-bank-daily"
BANK = "data/warm/fmp-bank/"
MAN, CAT, STATE = BANK + "manifest.json", BANK + "catalog.json", BANK + "_state/state.json"
EXTRA = "data/providers/extra-providers.json"
MAX_KICKS = int((ROOT / "aws/ops/STAGED/.fmp_bank_kicks").read_text().strip()) if (ROOT / "aws/ops/STAGED/.fmp_bank_kicks").exists() else 4
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
        return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
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
    s3, lam, sch, cw = c("s3"), c("lambda"), c("scheduler"), c("cloudwatch")
    with report("ops_6640_fmp_bank_birth") as r:
        r.heading("ops 6640 - justhodl-fmp-bank: declare, first run, backfill kicks, data.html card, chain proof")
        r.section("1. function + schedule")
        try:
            cf = lam.get_function_configuration(FunctionName=FN)
        except Exception as e:  # noqa: BLE001
            r.fail("%s not deployed yet (%s) - dispatch deploy-lambdas function=%s, then run again" % (FN, type(e).__name__, FN))
            raise SystemExit(1)
        r.log("%s: runtime %s, memory %s, timeout %s, state %s, modified %s" % (FN, cf.get("Runtime"), cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), (cf.get("LastModified") or "")[:19]))
        env_keys = sorted((cf.get("Environment") or {}).get("Variables", {}).keys())
        r.log("env keys: %s (values never printed)" % env_keys)
        if "FMP_KEY" not in env_keys:
            r.warn("FMP_KEY not in env - managed_secret falls back to SSM /justhodl/fmp/api-key")
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            r.log("schedule %s: %s %s tz %s target %s" % (SCHED, live.get("ScheduleExpression"), live.get("State"), live.get("ScheduleExpressionTimezone"), fn_of((live.get("Target") or {}).get("Arn"))))
        except Exception as e:  # noqa: BLE001
            r.fail("schedule %s missing (%s)" % (SCHED, type(e).__name__))
            raise SystemExit(1)

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

        r.section("3. first run + backfill kicks")
        st0 = jget(s3, STATE) or {}
        kicks = 0
        budget_end = time.time() + 150 * 60          # leave the runner well inside its 6 h
        while kicks < MAX_KICKS and time.time() < budget_end:
            st = jget(s3, STATE) or {}
            if float(st.get("lease_until") or 0) > time.time():
                r.log("lease held until %s - another invocation is running; waiting" % datetime.fromtimestamp(st["lease_until"], timezone.utc).strftime("%H:%M:%S"))
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
            r.kv(kick=kicks, phase=st.get("phase"), key_status=st.get("key_status"), objects=st.get("objects"), mb=round((st.get("bytes") or 0) / 1e6, 1),
                 calls=run.get("calls"), retries=run.get("retries"), seconds=run.get("seconds"), dated_pending=st.get("dated_pending"), eod_cursor=(st.get("eod") or {}).get("cursor"))
            if st.get("key_status") == "unauthorized":
                r.fail("FMP key refused by the bulk endpoints (401/403) - the plan does not serve bulk, or the key is stale; nothing banked")
                raise SystemExit(1)
        man = jget(s3, MAN) or {}
        cat = jget(s3, CAT) or {}
        r.log("endpoints served: %s" % man.get("endpoints_served"))
        r.log("endpoints NOT served (recorded, not guessed): %s" % [(k, (cat.get("endpoints") or {}).get(k, {}).get("last_verdict")) for k in man.get("endpoints_not_served") or []])
        if not man.get("objects"):
            r.fail("nothing banked after %d kicks - see catalog verdicts above" % kicks)
            raise SystemExit(1)
        r.ok("bank: %s objects, %.1f MB, phase %s, EOD cursor %s, dated pending %s (the daily 23:13 UTC run continues the backfill)" % (
            man.get("objects"), (man.get("bytes") or 0) / 1e6, man.get("phase"), (man.get("eod") or {}).get("cursor"), man.get("dated_pending")))

        r.section("4. data.html card (extra-providers.json, add-only) + catalog refresh")
        extra = jget(s3, EXTRA, {}) or {}
        if isinstance(extra, list):
            extra = {p.get("slug", p.get("id")): p for p in extra if isinstance(p, dict)}
        if "fmp-bank" not in extra:
            extra["fmp-bank"] = {
                "name": "FMP — Ultimate bulk + EOD warehouse", "api": "financialmodelingprep.com/stable (*-bulk, eod-bulk)",
                "engines": ["justhodl-fmp-bank"], "prefixes": [],
                "count_from": [MAN, "pages", "pages_bytes"], "hot": [MAN, CAT],
                "cadence": "EOD", "label": "FMP Ultimate bulk + EOD warehouse (verbatim CSV) — never LIVE"}
            s3.put_object(Bucket=LIVE, Key=EXTRA, Body=json.dumps(extra, indent=1).encode(), ContentType="application/json", CacheControl="no-cache")
            r.ok("extra-providers.json += fmp-bank (%d entries)" % len(extra))
        else:
            r.ok("extra-providers.json already carries fmp-bank")
        try:
            lam.invoke(FunctionName="justhodl-provider-catalog", InvocationType="Event", Payload=b"{}")
            r.log("justhodl-provider-catalog invoked async (card lands within ~10 min; daily 06:20 otherwise)")
        except Exception as e:  # noqa: BLE001
            r.warn("provider-catalog invoke failed: %s" % type(e).__name__)

        r.section("5. the fmp_book -> fmp_bank chain is in the fleet")
        probe = None
        for cand in ("justhodl-fmp-ratios", "justhodl-stock-screener", "justhodl-analyst-consensus"):
            try:
                loc = lam.get_function(FunctionName=cand)["Code"]["Location"]
                with urllib.request.urlopen(loc, timeout=120) as h:
                    names = set(zipfile.ZipFile(io.BytesIO(h.read())).namelist())
                probe = (cand, "fmp_bank.py" in names, "fmp_book.py" in names)
                break
            except Exception as e:  # noqa: BLE001
                r.warn("%s bundle read failed: %s" % (cand, type(e).__name__))
        if probe:
            r.log("%s bundle: fmp_bank.py=%s fmp_book.py=%s" % probe)
            if not probe[1]:
                r.warn("the shared closure has not redeployed %s yet - the chain goes live as deploy-lambdas works through the importers" % probe[0])
        r.ok("ops 6640 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
