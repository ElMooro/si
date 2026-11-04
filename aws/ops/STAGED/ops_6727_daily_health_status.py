"""ops 6727 - READ-ONLY daily health status (scheduled daily health run, 2026-11-04: second full day after the Nov 2 15:44 UTC
release of the false FREEZE; first run after the 13F book birth (justhodl-13f-book, ops 6723-6726) and the Nov 3 Pages
outage (ubuntu-26.04 runner image, fixed by the Python 3.12 pin at 20:18 UTC).

  A. justhodl-spend-governor {"action":"status"}: version, mode, stage, cap, budget actual/forecast, thresholds, held /
     capped / anomaly lists, fuse, cadence sentinel, meter, storage record (last assessment, versioning, per-service
     daily spend);
  B. justhodl-spend-governor {"action":"caps"}: month-to-date top engines by metered Lambda cost;
  C. the governor's own event records since yesterday's health run (ops 6719, 2026-11-03 11:19 UTC): everything on
     20261103 after that stamp and everything on 20261104 - plus the raw describe_budget read, and the relay's marker
     (spend-governor/alerts/github-issues.json): how far behind the newest event record the GitHub-issues relay is
     (governor-alerts.yml is cron '41 * * * *', but GitHub has been running the repo's scheduled workflows 6-7 h late
     since Nov 1 - pages.yml's 05:17 run fired at 12:50 on Nov 3);
  D. freshness: HEAD every declared output in engine-manifest.json (bucket justhodl-dashboard-live) and bucket the ages
     with the fleet-monitor tiers (<30 h fresh, <7 d stale, >=7 d dead). Yesterday's run found 216 engines with every
     output 60-69 h old (last written Fri Oct 31 ~15:00 UTC; the Mon Nov 2 slot fell inside the freeze) and said: "if
     tomorrow's run still shows them at 90 h+, that is the finding". So: engines with ALL outputs >= 88 h and < 7 d old
     have now missed two slots since the restore and are named, with their declared schedule where the cadence
     manifest knows it. Held engines (CAP/ANOM) are marked, never re-invoked. Also: the 13F family after the book's
     birth (data/13f-book.json and its inputs);
  E. FMP bandwidth hold: the walker's and the bank's manifests (bandwidth_hold_until / key_status) - both were
     self-holding until 06:22 / 07:05 UTC today.
Invokes nothing but the governor's read-only actions; releases nothing; writes nothing but this report.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import concurrent.futures as cf
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops" / "checks"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))

REGION = "us-east-1"
ACCOUNT = "857687956942"
LIVE = "justhodl-dashboard-live"
PRIVATE = "justhodl-ops-private-857687956942"
GOVERNOR = "justhodl-spend-governor"
BUDGET_NAME = "JustHodl-Monthly-Hard-Cap-600"
EVENTS_PREFIX = "spend-governor/events/"
RELAY_MARKER = "spend-governor/alerts/github-issues.json"
YESTERDAY_RUN = "20261103T111900"          # ops 6719 finished 2026-11-03T11:19:21Z
FRESH_H, DEAD_H = 30.0, 168.0
TWO_SLOTS_H = 88.0                         # Oct 31 ~15:00 UTC writes are ~92 h old at this run; two daily slots missed
FMP_MANIFESTS = ("data/warm/fmp/manifest.json", "data/warm/fmp-bank/manifest.json")
THIRTEEN_F = ("data/13f-book.json", "data/13f-positions.json", "data/holdings-research.json", "data/13f-clone-alpha.json",
              "data/smart-money-clusters.json", "data/sec-13f.json", "data/capital-flow.json")


def tier(age_h):
    if age_h is None:
        return "missing"
    if age_h < FRESH_H:
        return "fresh"
    if age_h < DEAD_H:
        return "stale"
    return "dead"


def hold_state(man, now_ts):
    """(held, until_iso, note) from a walker/bank manifest - pure, for the selftest."""
    until = float(man.get("bandwidth_hold_until") or 0)
    held = until > now_ts
    until_iso = datetime.fromtimestamp(until, timezone.utc).isoformat(timespec="minutes") if until else None
    return held, until_iso, str(man.get("bandwidth_note") or "")[:160]


def missed_two_slots(ages):
    """True when an engine's every declared output is >= TWO_SLOTS_H and < DEAD_H old (pre-freeze write, two daily slots
    since the restore, not yet dead). Pure, for the selftest. A missing output (None) disqualifies: 'never had one' is a
    different state from 'had one and stopped' and is reported separately."""
    return bool(ages) and all(a is not None and TWO_SLOTS_H <= a < DEAD_H for a in ages)


def event_after(key, stamp):
    """Event record key newer than a compact UTC stamp (YYYYMMDDTHHMMSS) - pure, for the selftest."""
    stem = key.rsplit("/", 1)[-1]
    return stem[:15] > stamp


def selftest():
    assert tier(None) == "missing" and tier(1) == "fresh" and tier(40) == "stale" and tier(200) == "dead"
    assert hold_state({"bandwidth_hold_until": 100}, 50)[0] is True
    assert hold_state({"bandwidth_hold_until": 100}, 150)[0] is False
    assert hold_state({}, 150) == (False, None, "")
    assert missed_two_slots([92.0, 93.5]) is True
    assert missed_two_slots([92.0, 12.0]) is False          # one fresh output: the engine ran
    assert missed_two_slots([92.0, None]) is False          # a missing output is its own finding
    assert missed_two_slots([200.0]) is False               # dead, counted elsewhere
    assert missed_two_slots([]) is False
    assert event_after(EVENTS_PREFIX + "20261103T154447.324976Z-restore.json", YESTERDAY_RUN) is True
    assert event_after(EVENTS_PREFIX + "20261103T062129.769909Z-release-cap.json", YESTERDAY_RUN) is False
    assert event_after(EVENTS_PREFIX + "20261104T000737.000001Z-x.json", YESTERDAY_RUN) is True
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=48)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    with report("ops_6727_daily_health_status") as r:
        r.heading("ops 6727 - daily health status %s (governor status + caps, event records since ops 6719, relay lag, output "
                  "freshness 2 days after the restore, 13F family, FMP bandwidth hold; read-only)" % now.date())

        r.section("A. governor status")
        st = {}
        for attempt in range(6):   # reserved concurrency 1: a running tick answers TooManyRequests
            try:
                resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=b'{"action":"status"}')
                st = json.loads(resp["Payload"].read() or b"{}")
                if resp.get("FunctionError") or "error" in st:
                    r.fail("status invoke failed: %s" % str(st)[:600])
                    sys.exit(1)
                break
            except lam.exceptions.TooManyRequestsException:
                r.warn("governor busy (attempt %d) - waiting 30 s" % (attempt + 1))
                time.sleep(30)
        if not st:
            r.fail("governor status unavailable after 6 attempts")
            sys.exit(1)
        th = st.get("thresholds") or {}
        facts.update(version=st.get("version"), mode=st.get("mode"), stage=st.get("stage"), stage_since=st.get("stage_since"),
                     cap=st.get("cap"), cap_source=st.get("cap_source"), budget_limit=st.get("budget_limit"),
                     budget_actual=st.get("budget_actual"), budget_forecast=st.get("budget_forecast"), mtd=th.get("mtd"),
                     projected_eom=th.get("projected_eom"), baseline_usd_day=th.get("baseline_usd_day"))
        r.log("version %s mode %s stage %s since %s" % (st.get("version"), st.get("mode"), st.get("stage"), st.get("stage_since")))
        r.log("cap $%s (%s) | budget limit $%s actual $%s forecast $%s" % (st.get("cap"), st.get("cap_source"), st.get("budget_limit"),
                                                                             st.get("budget_actual"), st.get("budget_forecast")))
        r.log("thresholds: %s" % json.dumps(th, sort_keys=True, default=str)[:900])
        r.log("held (stage/lever -> count): %s" % json.dumps(st.get("held") or {}, sort_keys=True))
        r.log("capped (CAP, manual release only): %s" % (st.get("capped") or "none"))
        r.log("anomaly held (ANOM): %s" % (st.get("anomaly_held") or "none"))
        r.log("fuse: %s" % json.dumps(st.get("fuse") or {}, default=str)[:600])
        r.log("suppress_until: %s | last tick: %s" % (st.get("suppress_until"), json.dumps(st.get("last") or {}, default=str)[:700]))
        r.log("cadence sentinel: %s | fixed entries: %s" % (json.dumps(st.get("cadence") or {}, default=str)[:500], st.get("cadence_fixed")))
        r.log("anomalies: %s" % json.dumps(st.get("anomalies") or {}, default=str)[:700])
        r.log("meter: %s" % json.dumps(st.get("meter") or {}, default=str)[:700])
        if st.get("freshness"):
            r.log("governor freshness: %s" % json.dumps(st.get("freshness"), default=str)[:900])
        if st.get("trigger_breaches") or st.get("breaches"):
            r.log("trigger breaches: %s" % json.dumps(st.get("trigger_breaches") or st.get("breaches"), default=str)[:600])
        sto = st.get("storage") or {}
        r.log("storage: day %s | versioning %s | warn_days %s | trip_days %s" % (sto.get("day"), sto.get("versioning"),
                                                                                  sto.get("warn_days"), sto.get("trip_days")))
        r.log("storage last assessment: %s" % json.dumps(sto.get("last") or {}, default=str)[:900])
        r.log("S3 $/day (storage.s3_usd): %s" % json.dumps(sto.get("s3_usd") or {}, sort_keys=True))
        svc = sto.get("svc_usd") or {}
        for day in sorted(svc)[-4:]:
            rows = svc[day] if isinstance(svc[day], dict) else {}
            top = sorted(((k, float(v)) for k, v in rows.items() if isinstance(v, (int, float))), key=lambda kv: -kv[1])[:6]
            r.log("service spend %s: total $%.2f | %s" % (day, sum(float(v) for v in rows.values() if isinstance(v, (int, float))),
                                                          ", ".join("%s $%.2f" % (k[:30], v) for k, v in top)))
        facts.update(capped=len(st.get("capped") or []), anomaly_held=len(st.get("anomaly_held") or []),
                     storage_day=sto.get("day"), versioning=sto.get("versioning"),
                     storage_last_gb=(sto.get("last") or {}).get("gb"), storage_last_level=(sto.get("last") or {}).get("level"))
        held_total = sum(int(v) for v in (st.get("held") or {}).values())
        facts["held_total"] = held_total
        if st.get("stage") != "NORMAL":
            r.warn("stage %s on day %d of the month with %d ledger entries held (NORMAL since 2026-11-02 15:44 UTC yesterday - "
                   "a non-NORMAL stage now is new)" % (st.get("stage"), now.day, held_total))
        if st.get("version") != "1.6.1":
            r.warn("governor version %s - 1.6.1 (deployed 2026-11-03 06:27 UTC) expected; a newer version means a deploy this "
                   "run did not know about" % st.get("version"))
        if st.get("anomaly_held"):
            r.warn("ANOM holds present (6 h cooldown each; a third hold in a month becomes CAP): %s" % st.get("anomaly_held"))
        capped_now = sorted(st.get("capped") or [])
        if capped_now != ["justhodl-event-coordinator", "justhodl-miss-calibrator", "ofrapi"]:
            r.warn("CAP list changed since yesterday (was event-coordinator, miss-calibrator, ofrapi): %s" % capped_now)

        r.section("B. meter: month-to-date top engines")
        try:
            resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=b'{"action":"caps"}')
            caps = json.loads(resp["Payload"].read() or b"{}")
        except Exception as exc:  # noqa: BLE001
            caps = {"error": str(exc)[:300]}
        if "error" in caps:
            r.warn("caps invoke failed: %s" % str(caps)[:400])
        else:
            r.log("engine cap $%s/month | overrides %s | held %s" % (caps.get("cap_usd_month"), caps.get("overrides"), caps.get("held")))
            r.log("released: %s" % json.dumps(caps.get("released") or {}, default=str)[:900])
            top = caps.get("month_to_date_top") or []
            r.log("month-to-date top %d: %s" % (len(top), ", ".join("%s $%.2f" % (fn.replace("justhodl-", ""), v) for fn, v in top[:20])))
            facts["meter_top"] = "; ".join("%s $%.2f" % (fn.replace("justhodl-", ""), v) for fn, v in top[:5]) or "(none yet)"
            near = [(fn, v) for fn, v in top if v >= 25.0]
            if near:
                r.warn("engines past half the $50 cap this month: %s" % near)

        r.section("C. governor event records since ops 6719 (2026-11-03 11:19 UTC), the raw budget read, the relay's marker")
        kinds = Counter()
        newest_key = None
        try:
            keys = []
            for day in ("20261103", "20261104"):
                tok = None
                while True:
                    kw = {"Bucket": PRIVATE, "Prefix": EVENTS_PREFIX + day}
                    if tok:
                        kw["ContinuationToken"] = tok
                    res = s3.list_objects_v2(**kw)
                    keys += [o["Key"] for o in res.get("Contents", [])]
                    tok = res.get("NextContinuationToken")
                    if not tok:
                        break
            keys = sorted(keys)
            newest_key = keys[-1] if keys else None
            since = [k for k in keys if event_after(k, YESTERDAY_RUN)]
            r.log("%d event records on Nov 3 + Nov 4; %d since ops 6719 (newest %s)" % (len(keys), len(since), (newest_key or "-").split("/")[-1]))
            for k in since:
                try:
                    ev = json.loads(s3.get_object(Bucket=PRIVATE, Key=k)["Body"].read() or b"{}")
                except Exception as exc:  # noqa: BLE001
                    r.warn("%s unreadable: %s" % (k, str(exc)[:120]))
                    continue
                kind = ev.get("kind") or k.rsplit("-", 1)[-1].replace(".json", "")
                kinds[kind] += 1
                t = ev.get("thresholds") or {}
                extra = ev.get("function") or ev.get("functions") or ev.get("over") or ev.get("engines") or ev.get("held") or ""
                r.log("%s %s | reason %s | mtd %s | counts %s | %s" % (
                    k.split("/")[-1][:28], kind, str(ev.get("reason") or ev.get("stage") or "")[:160], t.get("mtd"),
                    json.dumps(ev.get("counts") or {})[:120], json.dumps(extra, default=str)[:200]))
            facts["events_since_6719"] = json.dumps(dict(kinds), sort_keys=True)
            alerting = {k: v for k, v in kinds.items() if k not in ("fast-fuse-check", "meter-month-close")}
            if alerting:
                r.warn("alert-class event kinds since yesterday's run (each should have a governor-labelled issue): %s" % json.dumps(alerting))
            else:
                r.log("no alert-class event records since yesterday's run (only bookkeeping kinds, if any)")
        except Exception as exc:  # noqa: BLE001
            r.warn("event records unreadable: %s: %s" % (type(exc).__name__, str(exc)[:200]))
        try:
            mk = json.loads(s3.get_object(Bucket=PRIVATE, Key=RELAY_MARKER)["Body"].read() or b"{}")
            last_key = str(mk.get("last_key") or "")
            r.log("relay marker: %s" % json.dumps(mk, default=str)[:400])
            behind = [k for k in keys if k > last_key] if newest_key else []
            alert_behind = [k for k in behind if not any(s in k for s in ("fast-fuse-check", "meter-month-close"))]
            r.log("relay is behind the newest record by %d records (%d alert-class): %s" % (
                len(behind), len(alert_behind), ", ".join(k.split("/")[-1][:40] for k in alert_behind[:10])))
            facts["relay_alert_records_unrelayed"] = len(alert_behind)
            if alert_behind:
                r.warn("alert-class governor records not yet relayed to GitHub issues: %d (governor-alerts.yml cron '41 * * * *'; "
                       "GitHub has run this repo's scheduled workflows 6-7 h late since Nov 1)" % len(alert_behind))
        except Exception as exc:  # noqa: BLE001
            r.warn("relay marker unreadable: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        try:
            b = boto3.client("budgets", region_name="us-east-1", config=cfg).describe_budget(AccountId=ACCOUNT, BudgetName=BUDGET_NAME)["Budget"]
            cs = b.get("CalculatedSpend") or {}
            act = float(cs["ActualSpend"]["Amount"]) if cs.get("ActualSpend") else None
            fc = float(cs["ForecastedSpend"]["Amount"]) if cs.get("ForecastedSpend") else None
            r.log("describe_budget now: limit $%s actual $%s forecast $%s | LastUpdatedTime %s" % (
                b.get("BudgetLimit", {}).get("Amount"), act, fc, b.get("LastUpdatedTime")))
            facts["budget_actual_now"] = act
            facts["budget_last_updated"] = str(b.get("LastUpdatedTime"))
        except Exception as exc:  # noqa: BLE001 - the runner may not hold budgets:ViewBudget; the governor's own read is in A
            r.warn("describe_budget from the runner: %s: %s (the governor's own read is in section A)" % (type(exc).__name__, str(exc)[:160]))
        r.log("MTD sources: %s" % json.dumps(th.get("sources") or {}, sort_keys=True, default=str)[:400])
        sup = {day: rows.get("AWS Business Support+") for day, rows in svc.items() if isinstance(rows, dict) and rows.get("AWS Business Support+")}
        r.log("#138 - 'AWS Business Support+' by day in the governor's 14-day service record: %s" % (json.dumps(sup, sort_keys=True) or "{}"))

        r.section("D. freshness of declared outputs (HEAD, bucket %s) - %.0f h after the Nov 2 15:44 UTC restore" % (
            LIVE, (now - datetime(2026, 11, 2, 15, 44, tzinfo=timezone.utc)).total_seconds() / 3600))
        for key in ("_health/fleet.json", "_health/cadence-manifest.json", "data/_freshness-monitor.json"):
            try:
                h = s3.head_object(Bucket=LIVE, Key=key)
                age = (now - h["LastModified"]).total_seconds() / 3600
                r.log("%s: %.1f h old (%d B) -> %s" % (key, age, h["ContentLength"], tier(age)))
                facts[key] = round(age, 1)
            except Exception as exc:  # noqa: BLE001 - report, do not stop
                r.warn("%s: %s" % (key, str(exc)[:120]))
        manifest = json.loads((ROOT / "engine-manifest.json").read_text(encoding="utf-8"))
        pairs = sorted({(e["engine"], k) for e in manifest["engines"] for k in (e.get("keys") or []) if k.startswith("data/")})
        keys = sorted({k for _, k in pairs})
        r.log("manifest %s: %d engines, %d declared data/ keys" % (manifest.get("generated_at"), len({e for e, _ in pairs}), len(keys)))

        def head(key):
            try:
                h = s3.head_object(Bucket=LIVE, Key=key)
                return key, (now - h["LastModified"]).total_seconds() / 3600
            except Exception:  # noqa: BLE001 - missing objects are a finding, not a crash
                return key, None
        ages = {}
        with cf.ThreadPoolExecutor(max_workers=24) as ex:
            for key, age in ex.map(head, keys):
                ages[key] = age
        tiers = Counter(tier(a) for a in ages.values())
        r.log("outputs by tier: %s (yesterday: fresh 751, stale 328, dead 213, missing 25)" % dict(tiers))
        facts.update(outputs=len(keys), **{"out_" + k: v for k, v in tiers.items()})
        owner = {}
        for e, k in pairs:
            owner.setdefault(k, []).append(e)
        by_engine = {}
        for e, k in pairs:
            by_engine.setdefault(e, []).append(ages.get(k))
        held_names = set(st.get("capped") or []) | {fn for fn, _ in (st.get("anomaly_held") or [])}
        # the declared schedule, where the cadence manifest knows it
        sched = {}
        try:
            cm = json.loads(s3.get_object(Bucket=LIVE, Key="_health/cadence-manifest.json")["Body"].read() or b"{}")
            rows = cm.get("engines") or cm.get("functions") or cm.get("triggers") or cm
            if isinstance(rows, dict):
                for name, v in rows.items():
                    if isinstance(v, dict):
                        sched[name] = str(v.get("schedule") or v.get("cron") or v.get("expression") or v.get("cadence") or "")[:40]
            elif isinstance(rows, list):
                for v in rows:
                    if isinstance(v, dict) and (v.get("function") or v.get("engine") or v.get("name")):
                        sched[v.get("function") or v.get("engine") or v.get("name")] = str(
                            v.get("schedule") or v.get("cron") or v.get("expression") or v.get("cadence") or "")[:40]
            r.log("cadence manifest: %d schedule entries readable" % len(sched))
        except Exception as exc:  # noqa: BLE001
            r.warn("cadence manifest unreadable: %s" % str(exc)[:120])
        two_slots = sorted(e for e, a in by_engine.items() if missed_two_slots(a))
        r.log("THE FINDING from yesterday's note - engines with ALL outputs >= %.0f h and < 7 d old (pre-freeze writes, two daily "
              "slots missed since the restore, not dead): %d" % (TWO_SLOTS_H, len(two_slots)))
        for e in two_slots[:120]:
            a = [x for x in by_engine[e] if x is not None]
            r.log("  %s%s: %d output(s) %.0f-%.0f h | schedule %s" % (e.replace("justhodl-", ""), " [HELD]" if e in held_names else "",
                                                                   len(a), min(a), max(a), sched.get(e) or sched.get(e.replace("justhodl-", "")) or "?"))
        if len(two_slots) > 120:
            r.log("  ... and %d more" % (len(two_slots) - 120))
        facts["engines_missed_two_slots"] = len(two_slots)
        if two_slots:
            r.warn("%d engines have not written since before the Nov 1-2 freeze although two daily slots have passed since the "
                   "restore - these are not the recovery curve any more (held engines marked; nothing re-invoked)" % len(two_slots))
        not_back = sorted(e for e, a in by_engine.items() if a and all(x is not None and 60 <= x < DEAD_H for x in a))
        r.log("engines with ALL outputs 60 h - 7 d old (yesterday 216): %d" % len(not_back))
        facts["engines_not_back_since_restore"] = len(not_back)
        fresh_engines = sorted(e for e, a in by_engine.items() if any(x is not None and x < FRESH_H for x in a))
        r.log("engines with at least one output < 30 h: %d of %d (yesterday 456)" % (len(fresh_engines), len(by_engine)))
        facts["engines_fresh"] = len(fresh_engines)
        stale_keys = sorted((k for k, a in ages.items() if a is not None and FRESH_H <= a < DEAD_H), key=lambda k: ages[k])
        r.log("STALE 30h-7d (%d), youngest 40: %s" % (len(stale_keys), ", ".join("%s %.0fh" % (k.replace("data/", ""), ages[k]) for k in stale_keys[:40])))
        dead = [k for k, a in ages.items() if a is not None and a >= DEAD_H]
        newly_dead = sorted((k for k in dead if ages[k] < DEAD_H + 24), key=lambda k: ages[k])
        oldest = sorted(dead, key=lambda k: -ages[k])[:15]
        r.log("DEAD >=7d (%d); crossed the line in the last day (%d): %s" % (len(dead), len(newly_dead), ", ".join(
            "%s %.0fh<-%s" % (k.replace("data/", ""), ages[k], ",".join(owner[k])[:30]) for k in newly_dead[:40])))
        facts["newly_dead"] = len(newly_dead)
        r.log("DEAD oldest: %s" % ", ".join("%s %.0fd" % (k.replace("data/", ""), ages[k] / 24) for k in oldest))
        missing = [k for k, a in ages.items() if a is None]
        r.log("MISSING objects (%d): %s" % (len(missing), ", ".join("%s<-%s" % (k.replace("data/", ""), ",".join(owner[k])[:40]) for k in missing[:40])))
        fresh = [k for k, a in ages.items() if a is not None and a < FRESH_H]
        r.log("FRESH (<30 h): %d of %d" % (len(fresh), len(keys)))
        r.log("13F family after the book's birth (ops 6723-6726; justhodl-13f-book-daily cron(37 16 * * ? *) UTC):")
        for key in THIRTEEN_F:
            try:
                h = s3.head_object(Bucket=LIVE, Key=key)
                age = (now - h["LastModified"]).total_seconds() / 3600
                r.log("  %s: %.1f h old (%d B) -> %s" % (key, age, h["ContentLength"], tier(age)))
                if key == "data/13f-book.json":
                    facts["13f_book_age_h"] = round(age, 1)
            except Exception as exc:  # noqa: BLE001
                r.log("  %s: %s" % (key, str(exc)[:100]))

        r.section("E. FMP bandwidth hold (walker + bank manifests)")
        now_ts = time.time()
        for key in FMP_MANIFESTS:
            try:
                obj = s3.get_object(Bucket=LIVE, Key=key)
                man = json.loads(obj["Body"].read() or b"{}")
                held, until_iso, note = hold_state(man, now_ts)
                age = (now - obj["LastModified"]).total_seconds() / 3600
                r.log("%s (%.1f h old): version %s phase %s key_status %s | hold %s until %s | %s" % (
                    key, age, man.get("version"), man.get("phase"), man.get("key_status"), "HELD" if held else "clear", until_iso, note))
                facts["fmp_hold_" + key.split("/")[2]] = "held until %s" % until_iso if held else "clear"
            except Exception as exc:  # noqa: BLE001
                r.warn("%s: %s" % (key, str(exc)[:160]))

        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing changed, nothing released")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
