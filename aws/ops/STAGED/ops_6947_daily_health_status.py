"""ops 6947 - READ-ONLY daily health status (scheduled daily health run, 2026-11-25, a Wednesday 11 UTC).

Yesterday's run (ops 6934, Tuesday, issue #163) found: stage NORMAL, MTD $205.84 on day 24, projection $265; CAP holds
unchanged (event-coordinator, miss-calibrator, ofrapi); storage fuse day 2026-11-23 (Nov 22 assessed: 1,275.1 GB, +81,712
objects - a second day near double the usual +42-49k, the Sovereign desk build the likeliest source; Nov 23 is the tiebreak);
Nov 22 S3 line $3.49 (lowest of the month); FMP walkers clear (30-day window rolls ~Nov 24-27); 1,107 / 65 / 157 / 24 outputs
fresh / stale / dead / missing, 17 silent engines; 10kq-filings.json 198 h dead (EDGAR getcurrent atom HTTP 400, a build
item); DLQ 3,237. Since then a day-long build session shipped ops 6935-6946 (Risk Map 2.0.0 = justhodl-regime-map, a heat map
of every asset class on chart.html; Credit Before Equity 2.0.0/2.1 = justhodl-credit-before-equity, every company the bond
market prints, industry heat map, company CDS, a stress rank; all twelve reports end success, 4 direct reds Nov 24 15:00-17:40
UTC were its own iterations) and the scheduled capital-structure refresh went red again at 04:46 UTC (control 'failed' since
2026-10-08, by design: explicit review required).

This run reads, and changes nothing:
  A. justhodl-spend-governor {"action":"status"}: version, mode, stage, cap, budget actual/forecast, thresholds, held /
     capped / anomaly lists, fuse, cadence sentinel, meter, storage record (last assessment, versioning, S3 $/day,
     per-service daily spend) - today's open questions: the Nov 23 object delta (third day above +80k with no build session
     behind it = a per-object writer to find; Nov 23 was a quiet Monday evening apart from ops 6928-6933 page work), and the
     Nov 23 S3 line (a 'high' day by the alternation).
  B. {"action":"caps"}: month-to-date top engines against the $50 per-engine cap - the two new engines ran their first
     scheduled slots last night (regime-map 21:00, credit-before-equity 22:35 UTC).
  C. governor event records written since ops 6934 (Nov 24-25) and the relay marker.
  D. freshness of every output the engine manifest declares (HEAD only): fresh / stale / dead / missing, engines silent
     two daily slots, outputs that crossed the 7-day line since yesterday; held engines marked, nothing re-invoked. The
     manifest was regenerated 2026-11-24 23:29 UTC (921 engines) and now declares the two new engines' outputs.
  E. the FMP walker + bank manifests: bandwidth hold state (the 30-day window rolls ~Nov 24-27 - day two).
  F. justhodl-fleet-error-monitor 1.3.0 - its own latest output (DLQ depth, alerts raised).
  G. the Sovereign desk outputs (ciss-stress) and the Repo desk / OFR bank (ages only).
  H. the capital-structure refresh control (read-only): status, failed_at, error_type - the by-design red workflow.

No release, no hold, no retime, no invoke of any engine. Exit 1 only if the governor status itself cannot be read.
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures as cf
import json
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
PRIVATE = "justhodl-ops-private-857687956942"
GOVERNOR = "justhodl-spend-governor"
EVENTS_PREFIX = "spend-governor/events/"
RELAY_MARKER = "spend-governor/alerts/github-issues.json"
YESTERDAY_RUN = "20261124T111900"          # ops 6934 ran 2026-11-24 ~11:19 UTC
EVENT_DAYS = ("20261124", "20261125")
FRESH_H, DEAD_H, TWO_SLOTS_H = 30.0, 168.0, 54.0
EXPECTED_CAP = ["justhodl-event-coordinator", "justhodl-miss-calibrator", "ofrapi"]
FMP_MANIFESTS = ("data/warm/fmp/manifest.json", "data/warm/fmp-bank/manifest.json")
MONITOR_KEY = "data/_fleet-monitor.json"
CAPSTRUCT_CONTROL = "audit-private/20260909-originals/share-structure-research/refresh-control.json"
WATCH = (   # born or re-made in the last fortnight; the Repo desk set is from the overnight session
    "data/strength.json", "data/strength-wide.json", "data/leaders.json", "data/bottom.json", "data/stock-volatility.json",
    "data/campaign-scan.json", "data/etf-true-flows.json", "data/ici-flows.json", "data/13f-book.json",
    "data/warm/polygon-full/eod/manifest.json", "data/warm/factor-book/manifest.json", "data/warm/history-book/manifest.json",
    "data/corporate-actions/_state.json", "data/_freshness-monitor.json", "_health/fleet.json", "_health/cadence-manifest.json",
    "data/ciss-stress.json", "data/ciss-sovereign.json", "data/ciss-sovereign-world.json", "data/ciss-dollar-shortage.json",
    "data/ciss-sovereign-history.json", "data/10kq-filings.json", "data/insider-trades.json",
    "data/regime-map.json", "data/regime-map-prev.json", "data/credit-before-equity.json", "credit/credit-before-equity-history.json",
)
REPO_DESK_PREFIXES = ("data/ciss-research/sovereign", "data/repo-market", "data/dealer", "data/warm/ofr", "data/ofr")
SECRET_KV = re.compile(r"(?i)\b(api[_-]?key|apikey|access[_-]?key|token|secret|password|passwd|authorization|signature|key)(\s*[=:]\s*|%3D)[^\s&\"',}]+")
LONG_TOKEN = re.compile(r"[A-Za-z0-9_\-+/=]{28,}")


def redact(line):
    line = SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + "***", str(line))
    return LONG_TOKEN.sub(lambda m: m.group(0) if ("-" in m.group(0) or "_" in m.group(0)) and not any(c.isdigit() for c in m.group(0)) else "***", line)


def tier(age_h):
    if age_h is None:
        return "MISSING"
    if age_h < FRESH_H:
        return "FRESH"
    if age_h < DEAD_H:
        return "STALE"
    return "DEAD"


def missed_two_slots(ages):
    a = [x for x in ages if x is not None]
    return bool(a) and all(TWO_SLOTS_H <= x < DEAD_H for x in a)


def selftest():
    assert tier(None) == "MISSING" and tier(1) == "FRESH" and tier(40) == "STALE" and tier(200) == "DEAD"
    assert missed_two_slots([60, 100]) and not missed_two_slots([10, 100]) and not missed_two_slots([None]) and not missed_two_slots([60, 200])
    assert "***" in redact("api_key=ABCDEFGH12345") and "justhodl-spend-governor" in redact("fn justhodl-spend-governor ok")
    assert redact("x=" + "A1" * 20).endswith("***")


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

    def head(key, bucket=LIVE):
        try:
            h = s3.head_object(Bucket=bucket, Key=key)
            return key, (now - h["LastModified"]).total_seconds() / 3600, h["ContentLength"]
        except Exception:  # noqa: BLE001 - a missing object is a finding, not a crash
            return key, None, None

    with report("ops_6947_daily_health_status") as r:
        r.heading("ops 6947 - daily health status %s (governor status + caps + events since ops 6934, output freshness, FMP hold, "
                  "fleet-error-monitor, the Sovereign desk + Repo desk outputs, capital-structure control; Wednesday; read-only)" % now.date())

        r.section("A. governor status")
        st = {}
        for attempt in range(6):   # reserved concurrency 1: a running tick answers TooManyRequests
            try:
                resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=b'{"action":"status"}')
                st = json.loads(resp["Payload"].read() or b"{}")
                if resp.get("FunctionError") or "error" in st:
                    r.fail("status invoke failed: %s" % redact(str(st))[:600])
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
                     cap=st.get("cap"), budget_actual=st.get("budget_actual"), budget_forecast=st.get("budget_forecast"),
                     mtd=th.get("mtd"), projected_eom=th.get("projected_eom"), baseline_usd_day=th.get("baseline_usd_day"))
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
        s3_usd = sto.get("s3_usd") or {}
        r.log("S3 $/day (storage.s3_usd): %s" % json.dumps(s3_usd, sort_keys=True))
        recent = [(d, float(v)) for d, v in sorted(s3_usd.items())[-8:] if isinstance(v, (int, float))]
        if recent:
            r.log("S3 line, last %d days: %s  (Nov 14-22: 3.93 / 7.03 / 3.97 / 7.22 / 4.07 / 7.36 / 4.45 / 6.17 / 3.49; Nov 23 is a 'high' day by "
                  "the alternating pattern, the third day with the history book on write-if-changed)" % (len(recent), ", ".join("%s $%.2f" % kv for kv in recent)))
            facts["s3_usd_recent"] = "; ".join("%s %.2f" % kv for kv in recent[-4:])
            post = [v for d, v in recent if d >= "2026-11-21"]
            if post and max(post) >= 6.5:
                r.warn("the S3 line is still >= $6.50 on a day after the write-if-changed deploy (%s) - the $7 days are not gone" % post)
        svc = sto.get("svc_usd") or {}
        for day in sorted(svc)[-4:]:
            rows = svc[day] if isinstance(svc[day], dict) else {}
            top = sorted(((k, float(v)) for k, v in rows.items() if isinstance(v, (int, float))), key=lambda kv: -kv[1])[:6]
            r.log("service spend %s: total $%.2f | %s" % (day, sum(float(v) for v in rows.values() if isinstance(v, (int, float))),
                                                          ", ".join("%s $%.2f" % (k[:30], v) for k, v in top)))
        facts.update(capped=len(st.get("capped") or []), anomaly_held=len(st.get("anomaly_held") or []), storage_day=sto.get("day"),
                     versioning=sto.get("versioning"), storage_last_gb=(sto.get("last") or {}).get("gb"),
                     storage_last_level=(sto.get("last") or {}).get("level"), held_total=sum(int(v) for v in (st.get("held") or {}).values()))
        if st.get("stage") != "NORMAL":
            r.warn("stage %s (NORMAL since 2026-11-02 15:44 UTC through yesterday's run) - a non-NORMAL stage is new" % st.get("stage"))
        if str(sto.get("day") or "") <= "2026-11-23":
            r.warn("storage fuse has not assessed a day past %s (it was 2026-11-23 at the Nov 24 run)" % sto.get("day"))
        else:
            r.log("storage fuse advanced: day %s (was 2026-11-23 at the Nov 24 run)" % sto.get("day"))
        last_assess = sto.get("last") or {}
        if isinstance(last_assess.get("delta_objects"), (int, float)):
            d_obj = int(last_assess["delta_objects"])
            r.log("object delta for %s: %+d (Nov 17-20 ran +42-49k/day; Nov 21 +99,943 Repo desk build; Nov 22 +81,712 Sovereign desk build)" % (last_assess.get("day"), d_obj))
            facts["delta_objects"] = d_obj
            if d_obj >= 80000:
                r.warn("object delta %+d is a THIRD day near double the usual, and Nov 23 had no desk build behind it - a per-object writer needs finding (list the day's new keys by prefix next)" % d_obj)
        if st.get("version") != "1.6.1":
            r.warn("governor version %s - 1.6.1 (deployed 2026-11-03) expected; a newer version means a deploy this run did not know about" % st.get("version"))
        if st.get("anomaly_held"):
            r.warn("ANOM holds present (6 h cooldown each; a third hold in a month becomes CAP): %s" % st.get("anomaly_held"))
        capped_now = sorted(st.get("capped") or [])
        if capped_now != EXPECTED_CAP:
            r.warn("CAP list changed since Nov 24 (was %s): %s" % (EXPECTED_CAP, capped_now))
        try:   # read-only: pure scenarios + a live read of the bucket metrics and versioning; records nothing
            resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=b'{"action":"storage_selftest"}')
            sst = json.loads(resp["Payload"].read() or b"{}")
            live = sst.get("live") or {}
            r.log("storage_selftest ok %s | scenarios %s | live versioning %s | live error %s" % (
                sst.get("ok"), json.dumps(sst.get("scenarios")), live.get("versioning"), live.get("error")))
            r.log("storage_selftest live days: %s" % json.dumps(live.get("days") or {}, sort_keys=True)[:700])
            r.log("storage_selftest live partial: %s" % json.dumps(live.get("partial") or {}, sort_keys=True)[:700])
            facts["storage_live_partial_days"] = ",".join(sorted(live.get("partial") or {})) or "none"
            if live.get("partial"):
                r.warn("storage_selftest: %d of the last 6 days read as partial (%s)" % (
                    len(live["partial"]), "; ".join("%s: %s" % (d, v) for d, v in sorted(live["partial"].items()))[:500]))
        except Exception as exc:  # noqa: BLE001
            r.warn("storage_selftest invoke: %s: %s" % (type(exc).__name__, redact(str(exc))[:200]))

        r.section("B. meter: month-to-date top engines against the $50 per-engine cap")
        try:
            resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=b'{"action":"caps"}')
            caps = json.loads(resp["Payload"].read() or b"{}")
        except Exception as exc:  # noqa: BLE001
            caps = {"error": redact(str(exc))[:300]}
        if "error" in caps:
            r.warn("caps invoke failed: %s" % str(caps)[:400])
        else:
            r.log("engine cap $%s/month | overrides %s | held %s" % (caps.get("cap_usd_month"), caps.get("overrides"), caps.get("held")))
            r.log("released: %s" % json.dumps(caps.get("released") or {}, default=str)[:900])
            top = caps.get("month_to_date_top") or []
            r.log("month-to-date top %d: %s" % (len(top), ", ".join("%s $%.2f" % (fn.replace("justhodl-", ""), v) for fn, v in top[:20])))
            facts["meter_top"] = "; ".join("%s $%.2f" % (fn.replace("justhodl-", ""), v) for fn, v in top[:6]) or "(none yet)"
            near = [(fn, v) for fn, v in top if v >= 25.0]
            if near:
                r.warn("engines past half the $50 cap this month: %s" % near)

        r.section("C. governor event records since ops 6934 (2026-11-24 11:19 UTC) and the relay marker")
        try:
            kinds, newest, n = Counter(), "", 0
            for day in EVENT_DAYS:
                pg = s3.get_paginator("list_objects_v2")
                for page in pg.paginate(Bucket=PRIVATE, Prefix=EVENTS_PREFIX + day):
                    for o in page.get("Contents") or []:
                        key = o["Key"]
                        stamp = key[len(EVENTS_PREFIX):].split("_")[0].split("/")[0]
                        if stamp[:15] <= YESTERDAY_RUN[:15] and day == EVENT_DAYS[0]:
                            continue
                        n += 1
                        newest = max(newest, key)
                        kinds[re.sub(r"^\d{8}T\d{6}Z?[_-]?", "", key.rsplit("/", 1)[-1]).rsplit(".", 1)[0][:40]] += 1
            r.log("event records since the Nov 24 run: %d | kinds %s | newest %s" % (n, dict(kinds), newest or "-"))
            facts["events_since_6934"] = n
            if n:
                r.warn("%d governor event record(s) written since yesterday's run - the kinds above say which lever moved" % n)
        except Exception as exc:  # noqa: BLE001
            r.warn("event listing failed: %s: %s" % (type(exc).__name__, redact(str(exc))[:200]))
        try:
            obj = s3.get_object(Bucket=PRIVATE, Key=RELAY_MARKER)
            marker = json.loads(obj["Body"].read() or b"{}")
            age = (now - obj["LastModified"]).total_seconds() / 3600
            keys_ = marker if isinstance(marker, dict) else {}
            r.log("relay marker %.1f h old: %d relayed keys | %s" % (age, len(keys_.get("relayed") or keys_.get("seen") or keys_) if isinstance(keys_, dict) else 0,
                                                                  redact(json.dumps({k: v for k, v in keys_.items() if not isinstance(v, (list, dict))}, default=str))[:400]))
            facts["relay_marker_age_h"] = round(age, 1)
        except Exception as exc:  # noqa: BLE001
            r.warn("relay marker unreadable: %s: %s" % (type(exc).__name__, redact(str(exc))[:160]))

        r.section("D. freshness of declared outputs (HEAD, bucket %s)" % LIVE)
        manifest = json.loads((ROOT / "engine-manifest.json").read_text(encoding="utf-8"))
        pairs = sorted({(e["engine"], k) for e in manifest["engines"] for k in (e.get("keys") or []) if k.startswith("data/")})
        keys = sorted({k for _, k in pairs})
        r.log("manifest %s: %d engines, %d declared data/ keys" % (manifest.get("generated_at"), len({e for e, _ in pairs}), len(keys)))
        ages = {}
        with cf.ThreadPoolExecutor(max_workers=24) as ex:
            for key, age, _size in ex.map(head, keys):
                ages[key] = age
        tiers = Counter(tier(a) for a in ages.values())
        r.log("outputs by tier: %s (Tue Nov 24, ops 6934: fresh 1107, stale 65, dead 157, missing 24 of 1,353; Mon Nov 23: 979 / 194 / 156 / 24; Wed Nov 18 read 1081 / 58 / 155 / 25)" % dict(tiers))
        facts.update(outputs=len(keys), **{"out_" + k.lower(): v for k, v in tiers.items()})
        owner, by_engine = {}, {}
        for e, k in pairs:
            owner.setdefault(k, []).append(e)
            by_engine.setdefault(e, []).append(ages.get(k))
        held_names = set(st.get("capped") or []) | {fn for fn, _ in (st.get("anomaly_held") or []) if isinstance(fn, str)}
        two_slots = sorted(e for e, a in by_engine.items() if missed_two_slots(a))
        r.log("SILENT engines - ALL outputs 54 h - 7 d old (Tue: 17, all weekly or event-driven slots; Mon: 85; a Wednesday should read near 15-30): %d" % len(two_slots))
        if len(two_slots) > 45:
            r.warn("%d engines silent two daily slots on a Wednesday (Tuesday read 17) - check the list below for a daily engine that stopped" % len(two_slots))
        for e in sorted(two_slots, key=lambda e: -min(x for x in by_engine[e] if x is not None))[:80]:
            a = [x for x in by_engine[e] if x is not None]
            r.log("  %s%s: %d output(s) %.0f-%.0f h" % (e.replace("justhodl-", ""), " [HELD]" if e in held_names else "", len(a), min(a), max(a)))
        facts["engines_missed_two_slots"] = len(two_slots)
        fresh_engines = sorted(e for e, a in by_engine.items() if any(x is not None and x < FRESH_H for x in a))
        r.log("engines with at least one output < 30 h: %d of %d (Tue: 695 of 784; Mon: 624; Sun: 626)" % (len(fresh_engines), len(by_engine)))
        facts["engines_fresh"] = len(fresh_engines)
        stale_keys = sorted((k for k, a in ages.items() if a is not None and FRESH_H <= a < DEAD_H), key=lambda k: ages[k])
        r.log("STALE 30h-7d (%d), youngest 40: %s" % (len(stale_keys), ", ".join("%s %.0fh" % (k.replace("data/", ""), ages[k]) for k in stale_keys[:40])))
        dead = [k for k, a in ages.items() if a is not None and a >= DEAD_H]
        newly_dead = sorted((k for k in dead if ages[k] < DEAD_H + 24.1), key=lambda k: ages[k])
        r.log("DEAD >=7d (%d); crossed the line since the Nov 24 run (%d): %s" % (len(dead), len(newly_dead), ", ".join(
            "%s %.0fh<-%s" % (k.replace("data/", ""), ages[k], ",".join(owner[k])[:30]) for k in newly_dead[:40])))
        facts["newly_dead"] = len(newly_dead)
        if newly_dead:
            r.warn("%d declared output(s) crossed the 7-day line since yesterday: %s" % (len(newly_dead), ", ".join(
                "%s<-%s" % (k.replace("data/", ""), ",".join(owner[k])[:30]) for k in newly_dead[:20])))
        missing = [k for k, a in ages.items() if a is None]
        r.log("MISSING objects (%d): %s" % (len(missing), ", ".join("%s<-%s" % (k.replace("data/", ""), ",".join(owner[k])[:40]) for k in missing[:40])))
        r.log("watched outputs (desks, banks, monitors):")
        for key, age, size in map(head, WATCH):
            if age is None:
                r.warn("  %s: missing" % key)
            else:
                r.log("  %s: %.1f h old (%d B) -> %s" % (key, age, size, tier(age)))
                facts[key.replace("data/", "").replace("_health/", "").replace("/", "_").replace(".json", "") + "_age_h"] = round(age, 1)

        r.section("E. FMP bandwidth hold (walker + bank manifests) - the 30-day window rolls ~Nov 24-27; today is Nov 25, day two")
        for key in FMP_MANIFESTS:
            try:
                obj = s3.get_object(Bucket=LIVE, Key=key)
                man = json.loads(obj["Body"].read() or b"{}")
                age = (now - obj["LastModified"]).total_seconds() / 3600
                hold = man.get("hold") or man.get("bandwidth_hold") or {}
                until = hold.get("until") if isinstance(hold, dict) else hold
                until_dt = None
                if isinstance(until, (int, float)):
                    until_dt = datetime.fromtimestamp(float(until), tz=timezone.utc)
                elif isinstance(until, str) and until:
                    try:
                        until_dt = datetime.fromisoformat(until.replace("Z", "+00:00"))
                    except ValueError:
                        until_dt = None
                held = bool(until_dt and until_dt > now)
                summary = {k: man.get(k) for k in ("version", "key_status", "last_run", "last_ok", "symbols", "cursor", "written", "refused", "status") if man.get(k) is not None}
                r.log("%s (%.1f h old): held %s until %s | %s" % (key, age, held, until_dt.isoformat() if until_dt else None, redact(json.dumps(summary, default=str))[:500]))
                facts["fmp_hold_" + key.split("/")[2]] = ("held until %s" % until_dt.isoformat()) if held else "clear"
                if held:
                    r.warn("  %s still self-held (FMP 429 Bandwidth Limit Reach) - the 150 GB / 30-day window rolls ~Nov 24-27" % key.split("/")[2])
            except Exception as exc:  # noqa: BLE001
                r.warn("%s unreadable: %s: %s" % (key, type(exc).__name__, redact(str(exc))[:160]))

        r.section("F. justhodl-fleet-error-monitor 1.3.0 - its latest own output (read-only)")
        try:
            obj = s3.get_object(Bucket=LIVE, Key=MONITOR_KEY)
            doc = json.loads(obj["Body"].read() or b"{}")
            age = (now - obj["LastModified"]).total_seconds() / 3600
            dlq = doc.get("dlq") or {}
            alerts = doc.get("alerts") or doc.get("raised") or []
            r.log("%s (%.1f h old): version %s | scanned %s | detected %s | raised %s | dlq %s | sns_sent %s telegram_sent %s" % (
                MONITOR_KEY, age, doc.get("version"), doc.get("scanned") or doc.get("functions_scanned"), doc.get("detected"),
                len(alerts) if isinstance(alerts, list) else alerts, redact(json.dumps(dlq, default=str))[:200], doc.get("sns_sent"), doc.get("telegram_sent")))
            for a in (alerts if isinstance(alerts, list) else [])[:10]:
                r.log("  alert: %s" % redact(json.dumps(a, default=str))[:300])
            facts["monitor_age_h"] = round(age, 1)
            facts["monitor_dlq_visible"] = dlq.get("visible") if isinstance(dlq, dict) else None
            if age > 30.0:
                r.warn("  monitor output is %.1f h old - its daily 03:39 UTC slot should have rewritten it" % age)
        except Exception as exc:  # noqa: BLE001
            r.warn("%s unreadable: %s: %s" % (MONITOR_KEY, type(exc).__name__, redact(str(exc))[:160]))

        r.section("G. the Sovereign desk research state + Repo desk / OFR bank outputs (ages only, first 60 keys per prefix)")
        for prefix in REPO_DESK_PREFIXES:
            try:
                resp = s3.list_objects_v2(Bucket=LIVE, Prefix=prefix, MaxKeys=60)
                objs = resp.get("Contents") or []
                if not objs:
                    r.log("%s: no objects" % prefix)
                    continue
                newest = max(objs, key=lambda o: o["LastModified"])
                oldest = min(objs, key=lambda o: o["LastModified"])
                r.log("%s: %d object(s)%s | newest %s %.1f h | oldest %s %.1f h" % (
                    prefix, len(objs), "+" if resp.get("IsTruncated") else "", newest["Key"].split("/")[-1][:50],
                    (now - newest["LastModified"]).total_seconds() / 3600, oldest["Key"].split("/")[-1][:50], (now - oldest["LastModified"]).total_seconds() / 3600))
                facts["repo_" + prefix.split("/")[-1] + "_newest_h"] = round((now - newest["LastModified"]).total_seconds() / 3600, 1)
            except Exception as exc:  # noqa: BLE001
                r.warn("%s: %s: %s" % (prefix, type(exc).__name__, redact(str(exc))[:160]))

        r.section("H. capital-structure refresh control (read-only; the scheduled workflow is red by design while status is 'failed')")
        try:
            obj = s3.get_object(Bucket=LIVE, Key=CAPSTRUCT_CONTROL)
            ctl = json.loads(obj["Body"].read() or b"{}")
            age_d = (now - obj["LastModified"]).total_seconds() / 86400
            r.log("control %.1f d old: contract %s | status %s | started %s | failed_at %s | error_type %s | active_phase %s | completed %s | review %s" % (
                age_d, ctl.get("contract"), ctl.get("status"), ctl.get("started_at"), ctl.get("failed_at"), ctl.get("error_type"),
                ctl.get("active_phase"), ctl.get("completed_phases"), redact(json.dumps(ctl.get("review") or {}, default=str))[:300]))
            facts["capstruct_status"] = ctl.get("status")
            facts["capstruct_failed_at"] = ctl.get("failed_at")
            if ctl.get("status") == "failed":
                r.warn("capital-structure control still 'failed' (since %s) - every scheduled plan stops at 04:46 UTC until release_failed_control "
                       "runs with an ops number and a review note (Khalid's call; this run does not release it)" % ctl.get("failed_at"))
        except Exception as exc:  # noqa: BLE001
            r.warn("capital-structure control unreadable: %s: %s" % (type(exc).__name__, redact(str(exc))[:160]))

        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing changed, nothing released, nothing re-invoked")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        print("selftest ok")
    else:
        main()
