"""ops 6719 - READ-ONLY daily health status (scheduled daily health run, 2026-11-03: first full day after the Nov 2 15:44 UTC
release of the false FREEZE (#131/#135) and after governor v1.6.0/1.6.1 (the trigger cap's shutdown lever, ops 6718).

  A. justhodl-spend-governor {"action":"status"}: version, mode, stage, cap, budget actual/forecast, thresholds (incl.
     v1.5.3's budget_actual_ignored source), held/capped/anomaly lists, cadence sentinel, meter summary, storage record
     (last assessment, versioning, per-service daily spend);
  B. justhodl-spend-governor {"action":"caps"}: month-to-date top engines by metered Lambda cost;
  C. the governor's own event records since yesterday's health run (private bucket, spend-governor/events/20261102* and
     20261103*): the restore, the two release-caps, the service-spend notice (#138: AWS Business Support+ $29 on Nov 1),
     ops 6718's trigger-breach / release-cap on the canary - and anything unexpected. Plus the raw describe_budget read;
  D. freshness: HEAD every declared output in engine-manifest.json (bucket justhodl-dashboard-live) and bucket the ages
     with the fleet-monitor tiers (<30 h fresh, <7 d stale, >=7 d dead) - plus the age of _health/fleet.json,
     _health/cadence-manifest.json and data/_freshness-monitor.json themselves. The fleet was dark 39.6 h (Nov 1 00:07 -
     Nov 2 15:44 UTC): engines whose daily slot fell inside the freeze catch up on today's slot, so "stale 30-54 h" is the
     recovery curve, not a finding, unless an engine is still stale once its slot has passed;
  E. FMP bandwidth hold: the walker's and the bank's manifests (bandwidth_hold_until / key_status).
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
FRESH_H, DEAD_H = 30.0, 168.0
FMP_MANIFESTS = ("data/warm/fmp/manifest.json", "data/warm/fmp-bank/manifest.json")


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


def stale_budget(budget_actual, ce_mtd, day_of_month, baseline_usd_day):
    """True when a budget ActualSpend cannot belong to this month: in the first two days of a month, an actual that is
    more than 3 days of baseline above what Cost Explorer has seen is last month's figure (AWS Budgets rolls its period
    over hours after midnight UTC). Pure, for the selftest; the ops only REPORTS it."""
    if budget_actual is None or day_of_month > 2:
        return False
    seen = float(ce_mtd or 0.0)
    return float(budget_actual) > seen + 3.0 * max(float(baseline_usd_day or 0.0), 5.0)


def selftest():
    assert tier(None) == "missing" and tier(1) == "fresh" and tier(40) == "stale" and tier(200) == "dead"
    assert hold_state({"bandwidth_hold_until": 100}, 50)[0] is True
    assert hold_state({"bandwidth_hold_until": 100}, 150)[0] is False
    assert hold_state({}, 150) == (False, None, "")
    assert stale_budget(1176.94, 0.0, 1, 10.26) is True          # the Nov 1 00:07 tick
    assert stale_budget(1176.94, 0.3, 2, 10.26) is True          # day 2, budget still not rolled over
    assert stale_budget(12.0, 9.5, 1, 10.26) is False            # a plausible first day
    assert stale_budget(1176.94, 0.0, 5, 10.26) is False         # only the first two days are ambiguous
    assert stale_budget(None, 0.0, 1, 10.26) is False
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
    with report("ops_6719_daily_health_status") as r:
        r.heading("ops 6719 - daily health status %s (governor status + caps, event records since yesterday, output "
                  "freshness after the Nov 1-2 freeze, FMP bandwidth hold; read-only)" % now.date())

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
                     budget_actual=st.get("budget_actual"), budget_forecast=st.get("budget_forecast"), mtd=th.get("mtd"))
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
        sto = st.get("storage") or {}
        r.log("storage: day %s | versioning %s | warn_days %s | trip_days %s" % (sto.get("day"), sto.get("versioning"),
                                                                                  sto.get("warn_days"), sto.get("trip_days")))
        r.log("storage last assessment: %s" % json.dumps(sto.get("last") or {}, default=str)[:900])
        r.log("S3 $/day (storage.s3_usd): %s" % json.dumps(sto.get("s3_usd") or {}, sort_keys=True))
        svc = sto.get("svc_usd") or {}
        for day in sorted(svc)[-4:]:
            rows = svc[day] if isinstance(svc[day], dict) else {}
            top = sorted(((k, float(v)) for k, v in rows.items() if isinstance(v, (int, float))), key=lambda kv: -kv[1])[:6]
            r.log("service spend %s: total $%.2f | %s" % (day, sum(v for _, v in top), ", ".join("%s $%.2f" % (k[:30], v) for k, v in top)))
        facts.update(capped=len(st.get("capped") or []), anomaly_held=len(st.get("anomaly_held") or []),
                     storage_day=sto.get("day"), versioning=sto.get("versioning"))
        held_total = sum(int(v) for v in (st.get("held") or {}).values())
        facts["held_total"] = held_total
        if st.get("stage") != "NORMAL":
            r.warn("stage %s on day %d of the month with %d ledger entries held (the fleet was restored 2026-11-02 15:44 UTC, "
                   "ops 6709 - a non-NORMAL stage now is new)" % (st.get("stage"), now.day, held_total))
        if st.get("version") != "1.6.1":
            r.warn("governor version %s - 1.6.1 (trigger-cap lever, deployed 2026-11-03 06:27 UTC) expected" % st.get("version"))
        if st.get("anomaly_held"):
            r.warn("ANOM holds present (6 h cooldown each; a third hold in a month becomes CAP): %s" % st.get("anomaly_held"))

        r.section("B. meter: month-to-date top engines")
        try:
            resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=b'{"action":"caps"}')
            caps = json.loads(resp["Payload"].read() or b"{}")
        except Exception as exc:  # noqa: BLE001
            caps = {"error": str(exc)[:300]}
        if "error" in caps:
            r.warn("caps invoke failed: %s" % str(caps)[:400])
        else:
            r.log("engine cap $%s/month | overrides %s | held %s | released %s" % (caps.get("cap_usd_month"), caps.get("overrides"),
                                                                                    caps.get("held"), caps.get("released")))
            top = caps.get("month_to_date_top") or []
            r.log("month-to-date top %d: %s" % (len(top), ", ".join("%s $%.2f" % (fn.replace("justhodl-", ""), v) for fn, v in top[:15])))
            facts["meter_top"] = "; ".join("%s $%.2f" % (fn.replace("justhodl-", ""), v) for fn, v in top[:5]) or "(none yet)"

        r.section("C. governor event records since yesterday (Nov 2 + Nov 3) + the raw budget read")
        try:
            keys = []
            for day in ("20261102", "20261103"):
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
            r.log("%d event records on Nov 2 + Nov 3" % len(keys))
            kinds = Counter()
            for k in sorted(keys):
                try:
                    ev = json.loads(s3.get_object(Bucket=PRIVATE, Key=k)["Body"].read() or b"{}")
                except Exception as exc:  # noqa: BLE001
                    r.warn("%s unreadable: %s" % (k, str(exc)[:120]))
                    continue
                kind = ev.get("kind") or k.rsplit("-", 1)[-1].replace(".json", "")
                kinds[kind] += 1
                t = ev.get("thresholds") or {}
                extra = ev.get("function") or ev.get("functions") or ev.get("over") or ev.get("engines") or ""
                r.log("%s %s | reason %s | mtd %s sources %s | counts %s | %s" % (
                    k.split("/")[-1][:28], kind, str(ev.get("reason") or ev.get("stage") or "")[:160], t.get("mtd"),
                    json.dumps(t.get("sources")) if t.get("sources") else "-", json.dumps(ev.get("counts") or {})[:120],
                    json.dumps(extra, default=str)[:200]))
            facts["events_nov2_nov3"] = json.dumps(dict(kinds), sort_keys=True)
            unexpected = {k: v for k, v in kinds.items() if k not in ("restore", "release-cap", "service-spend", "trigger-breach", "meter-month-close", "month-close")}
            if unexpected:
                r.warn("event kinds not accounted for by ops 6709/6712/6718 or #138: %s" % json.dumps(unexpected))
        except Exception as exc:  # noqa: BLE001
            r.warn("event records unreadable: %s: %s" % (type(exc).__name__, str(exc)[:200]))
        try:
            b = boto3.client("budgets", region_name="us-east-1", config=cfg).describe_budget(AccountId=ACCOUNT, BudgetName=BUDGET_NAME)["Budget"]
            cs = b.get("CalculatedSpend") or {}
            act = float(cs["ActualSpend"]["Amount"]) if cs.get("ActualSpend") else None
            fc = float(cs["ForecastedSpend"]["Amount"]) if cs.get("ForecastedSpend") else None
            r.log("describe_budget now: limit $%s actual $%s forecast $%s | LastUpdatedTime %s | TimePeriod %s" % (
                b.get("BudgetLimit", {}).get("Amount"), act, fc, b.get("LastUpdatedTime"), json.dumps(b.get("TimePeriod") or {}, default=str)))
            facts["budget_actual_now"] = act
            facts["budget_last_updated"] = str(b.get("LastUpdatedTime"))
        except Exception as exc:  # noqa: BLE001 - the runner may not hold budgets:ViewBudget; the governor's own read is in A
            r.warn("describe_budget from the runner: %s: %s (the governor's own read is in section A)" % (type(exc).__name__, str(exc)[:160]))
        srcs = th.get("sources") or {}
        ce_mtd = srcs.get("ce_mtd")
        stale = stale_budget(st.get("budget_actual"), ce_mtd, now.day, th.get("baseline_usd_day"))
        facts["budget_actual_looks_like_last_month"] = stale
        facts["budget_actual_ignored"] = srcs.get("budget_actual_ignored")
        r.log("MTD sources (v1.5.3): %s" % json.dumps(srcs, sort_keys=True, default=str)[:400])
        if stale and not srcs.get("budget_actual_ignored"):
            r.warn("the governor's MTD $%s still comes from a budget ActualSpend that cannot belong to %s (Cost Explorer MTD %s) "
                   "and v1.5.3 did NOT flag it as ignored - check the deployed version" % (th.get("mtd"), now.strftime("%Y-%m"), ce_mtd))
        # #138: "AWS Business Support+" $29.00 on Nov 1 crossed svc_alert_other ($5). Show what the same service billed on
        # the 1st of October (state.storage.svc_usd keeps 14 days, so only if still present) - a support plan bills monthly.
        sup = {day: rows.get("AWS Business Support+") for day, rows in svc.items() if isinstance(rows, dict) and rows.get("AWS Business Support+")}
        r.log("#138 - 'AWS Business Support+' by day in the governor's 14-day service record: %s" % (json.dumps(sup, sort_keys=True) or "{}"))

        r.section("D. freshness of declared outputs (HEAD, bucket %s) - 43.5 h after the Nov 2 15:44 UTC restore" % LIVE)
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
        r.log("outputs by tier: %s" % dict(tiers))
        facts.update(outputs=len(keys), **{"out_" + k: v for k, v in tiers.items()})
        owner = {}
        for e, k in pairs:
            owner.setdefault(k, []).append(e)
        stale_keys = sorted((k for k, a in ages.items() if a is not None and FRESH_H <= a < DEAD_H), key=lambda k: ages[k])
        r.log("STALE 30h-7d (%d): %s" % (len(stale_keys), ", ".join("%s %.0fh" % (k.replace("data/", ""), ages[k]) for k in stale_keys[:60])))
        newly_stale = [k for k in stale_keys if ages[k] < 54]
        r.log("STALE 30-54 h (%d) - written before the freeze and not yet refreshed after the restore: %s" % (len(newly_stale), ", ".join(
            "%s %.0fh<-%s" % (k.replace("data/", ""), ages[k], ",".join(owner[k])[:30]) for k in newly_stale[:40])))
        # engines with every declared output still >= 60 h old were last written BEFORE the freeze and have had a full day
        # of slots since the restore: these are the ones to look at (not re-invoked here - CAP/FUSE/ANOM holds are honoured)
        by_engine = {}
        for e, k in pairs:
            by_engine.setdefault(e, []).append(ages.get(k))
        held_names = set(st.get("capped") or []) | {fn for fn, _ in (st.get("anomaly_held") or [])}
        not_back = sorted(e for e, a in by_engine.items() if a and all(x is not None and 60 <= x < DEAD_H for x in a))
        r.log("engines with ALL outputs 60 h - 7 d old (pre-freeze writes, a day of slots since the restore, not dead): %d: %s" % (
            len(not_back), ", ".join("%s%s" % (e.replace("justhodl-", ""), " [HELD]" if e in held_names else "") for e in not_back[:80])))
        facts["engines_not_back_since_restore"] = len(not_back)
        fresh_engines = sorted(e for e, a in by_engine.items() if any(x is not None and x < FRESH_H for x in a))
        r.log("engines with at least one output < 30 h: %d of %d" % (len(fresh_engines), len(by_engine)))
        facts["engines_fresh"] = len(fresh_engines)
        dead = [k for k, a in ages.items() if a is not None and a >= DEAD_H]
        newly_dead = sorted((k for k in dead if ages[k] < DEAD_H + 24), key=lambda k: ages[k])
        oldest = sorted(dead, key=lambda k: -ages[k])[:15]
        r.log("DEAD >=7d (%d); crossed the line in the last day (%d): %s" % (len(dead), len(newly_dead), ", ".join(
            "%s %.0fh" % (k.replace("data/", ""), ages[k]) for k in newly_dead[:30])))
        r.log("DEAD oldest: %s" % ", ".join("%s %.0fd" % (k.replace("data/", ""), ages[k] / 24) for k in oldest))
        missing = [k for k, a in ages.items() if a is None]
        r.log("MISSING objects (%d): %s" % (len(missing), ", ".join("%s<-%s" % (k.replace("data/", ""), ",".join(owner[k])[:40]) for k in missing[:40])))
        fresh = [k for k, a in ages.items() if a is not None and a < FRESH_H]
        r.log("FRESH (<30 h): %d of %d" % (len(fresh), len(keys)))

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
