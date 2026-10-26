"""ops 6671 - READ-ONLY daily health status (scheduled daily health run, 2026-10-26).

  A. justhodl-spend-governor {"action":"status"}: version, mode, stage, cap, budget actual/forecast, thresholds,
     held/capped/anomaly lists, cadence sentinel, meter summary, storage record (last assessment, versioning,
     per-service daily spend);
  B. justhodl-spend-governor {"action":"caps"}: month-to-date top engines by metered Lambda cost;
  C. freshness: HEAD every declared output in engine-manifest.json (bucket justhodl-dashboard-live) and bucket the ages
     with the fleet-monitor tiers (<30 h fresh, <7 d stale, >=7 d dead) - plus the age of _health/fleet.json and
     _health/cadence-manifest.json themselves. The fleet is thawed (stage NORMAL since 2026-10-21) and on the daily
     cadence, so STALE is the interesting set now;
  D. capital-structure refresh control (private bucket, read via the checks module): why the nightly workflow stops.
     ops 6645 could not import the module (its deps live in aws/shared AND aws/ops/checks) - both are on the path now;
  E. FMP bandwidth hold: the walker's and the bank's manifests (bandwidth_hold_until / key_status) - since 2026-10-26
     01:38 UTC every FMP /stable call answers 429 "Bandwidth Limit Reach" (150 GB per rolling 30 days spent, ops 6656/6657),
     which explains FMP-fed outputs going stale until the window ages out.
Invokes nothing but the governor's read-only actions; writes nothing but this report.
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
LIVE = "justhodl-dashboard-live"
GOVERNOR = "justhodl-spend-governor"
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


def selftest():
    assert tier(None) == "missing" and tier(1) == "fresh" and tier(40) == "stale" and tier(200) == "dead"
    assert hold_state({"bandwidth_hold_until": 100}, 50)[0] is True
    assert hold_state({"bandwidth_hold_until": 100}, 150)[0] is False
    assert hold_state({}, 150) == (False, None, "")
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
    with report("ops_6671_daily_health_status") as r:
        r.heading("ops 6671 - daily health status %s (governor status + caps, output freshness, capital-structure control, "
                  "FMP bandwidth hold; read-only)" % now.date())

        r.section("A. governor status")
        resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=b'{"action":"status"}')
        st = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError") or "error" in st:
            r.fail("status invoke failed: %s" % str(st)[:600])
            sys.exit(1)
        th = st.get("thresholds") or {}
        facts.update(version=st.get("version"), mode=st.get("mode"), stage=st.get("stage"), stage_since=st.get("stage_since"),
                     cap=st.get("cap"), cap_source=st.get("cap_source"), budget_limit=st.get("budget_limit"),
                     budget_actual=st.get("budget_actual"), budget_forecast=st.get("budget_forecast"))
        r.log("version %s mode %s stage %s since %s" % (st.get("version"), st.get("mode"), st.get("stage"), st.get("stage_since")))
        r.log("cap $%s (%s) | budget limit $%s actual $%s forecast $%s" % (st.get("cap"), st.get("cap_source"), st.get("budget_limit"),
                                                                             st.get("budget_actual"), st.get("budget_forecast")))
        r.log("thresholds: %s" % json.dumps(th, sort_keys=True, default=str)[:900])
        r.log("held (stage/lever -> count): %s" % json.dumps(st.get("held") or {}, sort_keys=True))
        r.log("capped (CAP, manual release only): %s" % (st.get("capped") or "none"))
        r.log("anomaly held (ANOM): %s" % (st.get("anomaly_held") or "none"))
        r.log("fuse: %s" % json.dumps(st.get("fuse") or {}, default=str)[:600])
        r.log("suppress_until: %s | last tick: %s" % (st.get("suppress_until"), json.dumps(st.get("last") or {}, default=str)[:500]))
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

        r.section("B. meter: month-to-date top engines")
        resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=b'{"action":"caps"}')
        caps = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError") or "error" in caps:
            r.warn("caps invoke failed: %s" % str(caps)[:400])
        else:
            r.log("engine cap $%s/month | overrides %s | held %s | released %s" % (caps.get("cap_usd_month"), caps.get("overrides"),
                                                                                    caps.get("held"), caps.get("released")))
            top = caps.get("month_to_date_top") or []
            r.log("month-to-date top %d: %s" % (len(top), ", ".join("%s $%.2f" % (fn.replace("justhodl-", ""), v) for fn, v in top[:15])))
            facts["meter_top"] = "; ".join("%s $%.2f" % (fn.replace("justhodl-", ""), v) for fn, v in top[:5])

        r.section("C. freshness of declared outputs (HEAD, bucket %s)" % LIVE)
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
        stale = sorted((k for k, a in ages.items() if a is not None and FRESH_H <= a < DEAD_H), key=lambda k: ages[k])
        r.log("STALE 30h-7d (%d): %s" % (len(stale), ", ".join("%s %.0fh" % (k.replace("data/", ""), ages[k]) for k in stale[:60])))
        newly_stale = [k for k in stale if ages[k] < 54]
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

        r.section("D. capital-structure refresh control (why capital-structure-source-refresh.yml stops)")
        try:
            import capital_structure_refresh as csr
            state, _etag, _raw = csr.load_control(s3)
            if not state:
                r.warn("no refresh control object")
            else:
                keep = {k: state.get(k) for k in ("contract", "request_id", "run_id", "status", "started_at", "failed_at", "error_type",
                                                  "error", "active_phase", "completed_phases", "review")}
                r.log("control: %s" % json.dumps(keep, default=str)[:1200])
                facts["capstruct_status"] = state.get("status")
                facts["capstruct_error"] = str(state.get("error_type") or state.get("error") or "")[:80]
                if state.get("status") not in ("complete", "released"):
                    r.warn("status %r since %s - the plan phase refuses every day until release_failed_control(ops, note) is run "
                           "by a reviewed ops (never automatic)" % (state.get("status"), state.get("failed_at") or state.get("started_at")))
        except Exception as exc:  # noqa: BLE001
            r.warn("capital-structure control unreadable: %s: %s" % (type(exc).__name__, str(exc)[:200]))

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
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
