"""ops 6645 - READ-ONLY daily health status (scheduled daily health run, 2026-10-25).

  A. justhodl-spend-governor {"action":"status"}: version, mode, stage, cap, budget actual/forecast, thresholds,
     held/capped/anomaly lists, cadence sentinel, meter summary, storage record (last assessment, versioning,
     per-service daily spend - S3 vs the $10 line behind governor issue #128);
  B. justhodl-spend-governor {"action":"caps"}: month-to-date top engines by metered Lambda cost;
  C. freshness: HEAD every declared output in engine-manifest.json (bucket justhodl-dashboard-live) and bucket the ages
     with the fleet-monitor tiers (<30 h fresh, <7 d stale, >=7 d dead) - plus the age of _health/fleet.json and
     _health/cadence-manifest.json themselves. The fleet is FROZEN until 2026-11-01 (ops 6525), so stale is expected;
     the interesting set is what is FRESH (something other than the frozen fleet writes it);
  D. capital-structure refresh control (private bucket, read via the checks module): why the nightly workflow stops.
Invokes nothing but the governor's read-only actions; writes nothing but this report.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import concurrent.futures as cf
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops" / "checks"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
GOVERNOR = "justhodl-spend-governor"
FRESH_H, DEAD_H = 30.0, 168.0


def tier(age_h):
    if age_h is None:
        return "missing"
    if age_h < FRESH_H:
        return "fresh"
    if age_h < DEAD_H:
        return "stale"
    return "dead"


def selftest():
    assert tier(None) == "missing" and tier(1) == "fresh" and tier(40) == "stale" and tier(200) == "dead"
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
    with report("ops_6645_daily_health_status") as r:
        r.heading("ops 6645 - daily health status %s (governor status + caps, output freshness, capital-structure control; read-only)" % now.date())

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
        fresh = sorted((k for k, a in ages.items() if a is not None and a < FRESH_H), key=lambda k: ages[k])
        r.log("FRESH under the freeze (%d): %s" % (len(fresh), ", ".join("%s %.0fh" % (k.replace("data/", ""), ages[k]) for k in fresh[:60])))
        owner = {}
        for e, k in pairs:
            owner.setdefault(k, []).append(e)
        stale = sorted((k for k, a in ages.items() if a is not None and FRESH_H <= a < DEAD_H), key=lambda k: ages[k])
        r.log("STALE 30h-7d (%d): %s" % (len(stale), ", ".join("%s %.0fh" % (k.replace("data/", ""), ages[k]) for k in stale[:40])))
        dead = [k for k, a in ages.items() if a is not None and a >= DEAD_H]
        oldest = sorted(dead, key=lambda k: -ages[k])[:15]
        r.log("DEAD >=7d (%d); oldest: %s" % (len(dead), ", ".join("%s %.0fd" % (k.replace("data/", ""), ages[k] / 24) for k in oldest)))
        missing = [k for k, a in ages.items() if a is None]
        r.log("MISSING objects (%d): %s" % (len(missing), ", ".join("%s<-%s" % (k.replace("data/", ""), ",".join(owner[k])[:40]) for k in missing[:40])))
        frozen_since = datetime(2026, 10, 16, 22, 53, tzinfo=timezone.utc)
        since_freeze = sum(1 for a in ages.values() if a is not None and a < (now - frozen_since).total_seconds() / 3600)
        r.log("written since the freeze began (%s): %d of %d" % (frozen_since.isoformat(timespec="minutes"), since_freeze, len(keys)))

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
            r.warn("capital-structure control unreadable: %s" % str(exc)[:200])

        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
