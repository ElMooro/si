"""ops 6705 - READ-ONLY daily health status (scheduled daily health run, 2026-11-01: the first day after the October freeze).

  A. justhodl-spend-governor {"action":"status"}: version, mode, stage, cap, budget actual/forecast, thresholds,
     held/capped/anomaly lists, cadence sentinel, meter summary, storage record (last assessment, versioning,
     per-service daily spend);
  B. justhodl-spend-governor {"action":"caps"}: month-to-date top engines by metered Lambda cost;
  C. the Nov 1 rollover, from the governor's own event records (private bucket, spend-governor/events/20261101*):
     kind, reason and the MTD sources of every record written today - issue #131 says the 00:07 UTC tick restored the
     October freeze and then re-FROZE 904 functions on "MTD $1176.94" while Cost Explorer's November MTD was $0.00, i.e.
     AWS Budgets' ActualSpend had not rolled over to the new period yet. Also the raw describe_budget read (ActualSpend,
     LastUpdatedTime) so the report shows what the budget says NOW, hours later;
  D. freshness: HEAD every declared output in engine-manifest.json (bucket justhodl-dashboard-live) and bucket the ages
     with the fleet-monitor tiers (<30 h fresh, <7 d stale, >=7 d dead) - plus the age of _health/fleet.json and
     _health/cadence-manifest.json themselves;
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
    with report("ops_6705_daily_health_status") as r:
        r.heading("ops 6705 - daily health status %s (governor status + caps, Nov 1 rollover records, output freshness, "
                  "FMP bandwidth hold; read-only)" % now.date())

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
        if st.get("stage") == "FREEZE":
            r.warn("stage FREEZE on day %d of the month with %d ledger entries held - see section C for why" % (now.day, held_total))

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

        r.section("C. the Nov 1 rollover: governor event records written today + the raw budget read")
        try:
            keys = []
            tok = None
            while True:
                kw = {"Bucket": PRIVATE, "Prefix": EVENTS_PREFIX + now.strftime("%Y%m%d")}
                if tok:
                    kw["ContinuationToken"] = tok
                res = s3.list_objects_v2(**kw)
                keys += [o["Key"] for o in res.get("Contents", [])]
                tok = res.get("NextContinuationToken")
                if not tok:
                    break
            r.log("%d event records today" % len(keys))
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
                r.log("%s %s | reason %s | mtd %s sources %s | counts %s" % (
                    k.split("/")[-1][:28], kind, str(ev.get("reason") or ev.get("stage") or "")[:160], t.get("mtd"),
                    json.dumps(t.get("sources")) if t.get("sources") else "-", json.dumps(ev.get("counts") or {})[:120]))
            facts["events_today"] = json.dumps(dict(kinds), sort_keys=True)
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
        ce_mtd = ((th.get("sources") or {}).get("ce_mtd"))
        stale = stale_budget(st.get("budget_actual"), ce_mtd, now.day, th.get("baseline_usd_day"))
        facts["budget_actual_looks_like_last_month"] = stale
        if stale:
            r.warn("the governor's MTD $%s comes from a budget ActualSpend that cannot belong to %s (Cost Explorer MTD %s): "
                   "the FREEZE line was crossed on last month's figure. decide() holds FREEZE until a cap raise or an operator "
                   "release - this ops releases nothing (hard limit); Khalid's call." % (th.get("mtd"), now.strftime("%Y-%m"), ce_mtd))

        r.section("D. freshness of declared outputs (HEAD, bucket %s)" % LIVE)
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
        r.log("STALE that were fresh yesterday (30-54 h, %d): %s" % (len(newly_stale), ", ".join(
            "%s %.0fh<-%s" % (k.replace("data/", ""), ages[k], ",".join(owner[k])[:30]) for k in newly_stale[:40])))
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
