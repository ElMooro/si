"""ops_5095 -- justhodl-global-business-cycle v2.1.0: deploy wait, real run, verify.

What v2.1.0 fixes (all found in the ops 5092 forensics of the LIVE feed):
  * USA pinned at CLI 120 every run: the FRED supplement USACSCICP02STSAM is an
    OECD index normalised at 100, but v2.0 blended `value*10*0.25` (~+247
    composite points) -- the 25%-weight economy reported the cap, not a reading.
    v2.1 blends the ANOMALY (value-100, clipped +-3) and only when <= 3 months old.
  * CZE UNKNOWN: PX.PR 404s, the chain stopped at PXTR.PR (a few bars) and
    CEZP.PR was a wrong ticker. v2.1: acceptance gate (>= 252 bars, last bar
    <= 12 days) on EVERY candidate, then equal-weight blue-chip basket, then
    the US-listed country ETF from our own polygon-full warehouse.
  * NOR ^OSEAX dead since 2026-07-17 (2mo stale) -> same chain (OBX.OL, basket).
  * hard clip [80,120] saturating (KOR 120 with -16.7% 3m) -> soft tanh clip.
  * physical layer flipping 34/34 to UNCONFIRMED when portwatch ships an empty
    ports list -> previous run carried forward with provenance.

  S1  BEFORE: the live feed's USA/KOR/CZE/NOR rows and physical counts
  S2  wait for deploy-lambdas.yml: Description carries v2.1.0, update Successful
  S3  async invoke; poll S3 until engine_version == 2.1.0 (<= 16 min)
  S4  per-country table: phase, CLI, source chain, staleness, physical
  S5  history feed regenerated (schema 2.4), portwatch feed shape today
  S6  CloudWatch tail of THIS run ([gbc]/[polygon-lane]/[physical] lines)

Gate (sys.exit(1)): engine_version != 2.1.0 on S3, < 33/34 countries with a
CLI, USA CLI still at the cap, history not regenerated, or a traceback in the
run's log.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-global-business-cycle"
LIVE_KEY = "data/global-business-cycle.json"
HIST_KEY = "data/global-business-cycle-history.json"
PW_KEY = "data/portwatch.json"
WANT = "2.1.0"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=60, retries={"max_attempts": 1}))
logs = boto3.client("logs", region_name=REGION)


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:120]


def row_summary(iso, c):
    return (f"{iso:<4} {str(c.get('phase')):<10} CLI={c.get('cli_level')} 3m={c.get('three_month_change')} "
            f"12m={c.get('yoy_change')} src={c.get('source') or c.get('yahoo_symbol')} "
            f"stale={c.get('days_stale', c.get('months_stale'))}d "
            f"phys={(c.get('physical') or {}).get('state')} "
            f"{'DEGRADED ' + str(c.get('source_degraded')) if c.get('source_degraded') else ''}"
            f"{'reason=' + str(c.get('reason')) if c.get('reason') else ''}")


def main():
    with report("5095-gbc-engine-v21-verify") as r:
        r.heading("ops 5095 -- justhodl-global-business-cycle v2.1.0 deploy + run + verify")
        t_start = datetime.now(timezone.utc)
        r.log("started %s" % t_start.isoformat(timespec="seconds"))
        fails = []

        r.section("S1 BEFORE (live feed as deployed)")
        before, lm = get_json(LIVE_KEY)
        if before:
            bc = before.get("by_country") or {}
            r.log(f"generated_at={before.get('generated_at')} schema={before.get('schema_version')} "
                  f"engine_version={before.get('engine_version')} fresh={before.get('countries_with_fresh_data')}")
            for iso in ("USA", "CHN", "KOR", "CZE", "HUN", "NOR", "GRC"):
                if iso in bc:
                    c = bc[iso]
                    r.log("  " + row_summary(iso, c) + f" supplement={c.get('supplement_value')}@{c.get('supplement_date')}")
            r.log(f"  physical: {json.dumps((before.get('physical_confirmation') or {}).get('counts'))} "
                  f"countries_with_ports={(before.get('physical_confirmation') or {}).get('countries_with_ports')}")
            n120 = [i for i, c in bc.items() if c.get("cli_level") == 120]
            r.log(f"  countries pinned at the 120 cap BEFORE: {n120}")
        else:
            r.warn(f"no live feed: {lm}")

        r.section("S2 wait for deploy")
        deployed = False
        t0 = time.time()
        while time.time() - t0 < 900:
            try:
                cfg = lam.get_function_configuration(FunctionName=FN)
            except Exception as e:  # noqa: BLE001
                r.warn(f"get_function_configuration: {str(e)[:120]}")
                time.sleep(20)
                continue
            desc = cfg.get("Description") or ""
            st = cfg.get("LastUpdateStatus")
            if "v2.1.0" in desc and st == "Successful":
                r.ok(f"deployed: {cfg.get('LastModified')} timeout={cfg.get('Timeout')} mem={cfg.get('MemorySize')} "
                     f"status={st} after {time.time() - t0:.0f}s")
                deployed = True
                break
            r.log(f"  waiting… desc_has_v2.1.0={'v2.1.0' in desc} status={st} last_modified={cfg.get('LastModified')}")
            time.sleep(25)
        if not deployed:
            fails.append("deploy-lambdas.yml did not land v2.1.0 within 15 min")
            r.fail(fails[-1])

        r.section("S3 run the engine")
        t_inv = datetime.now(timezone.utc)
        try:
            resp = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
            r.log(f"async invoke status={resp.get('StatusCode')} at {t_inv.isoformat(timespec='seconds')}")
        except Exception as e:  # noqa: BLE001
            fails.append(f"invoke failed: {str(e)[:200]}")
            r.fail(fails[-1])
        after = None
        t0 = time.time()
        while time.time() - t0 < 960:
            time.sleep(20)
            doc, lm = get_json(LIVE_KEY)
            if doc and doc.get("engine_version") == WANT and (doc.get("generated_at") or "") > t_inv.isoformat():
                after = doc
                r.ok(f"fresh v{WANT} feed on S3: generated_at={doc.get('generated_at')} elapsed={doc.get('elapsed_sec')}s "
                     f"after {time.time() - t0:.0f}s")
                break
            r.log(f"  polling… engine_version={doc.get('engine_version') if doc else None} "
                  f"generated_at={doc.get('generated_at') if doc else lm}")
        if after is None:
            fails.append("no v2.1.0 feed landed on S3 within 16 min of the invoke")
            r.fail(fails[-1])
            after = get_json(LIVE_KEY)[0] or {}

        r.section("S4 per-country")
        bc = after.get("by_country") or {}
        agg = after.get("aggregate") or {}
        r.log(f"global_phase={agg.get('global_phase')} avg_cli={agg.get('global_avg_cli')} "
              f"coverage={agg.get('classification_coverage_pct')}% mix={json.dumps(agg.get('global_phase_mix_pct'))}")
        r.log(f"sources_used={json.dumps(after.get('sources_used'))} polygon_lane={json.dumps(after.get('polygon_lane'))}")
        r.log(f"supplements={json.dumps(after.get('supplements'))[:400]}")
        r.log(f"acceptance_gate={json.dumps(after.get('acceptance_gate'))} fresh={after.get('countries_with_fresh_data')}/{after.get('countries_total')}")
        with_cli, unknown, pinned, degraded = [], [], [], []
        for iso in sorted(bc):
            c = bc[iso]
            r.log("  " + row_summary(iso, c))
            r.kv(iso=iso, phase=c.get("phase"), cli=c.get("cli_level"), src=(c.get("source") or "")[:40],
                 stale_d=c.get("days_stale"), phys=(c.get("physical") or {}).get("state"),
                 degraded=bool(c.get("source_degraded")), attempts=len(c.get("source_attempts") or []))
            if c.get("cli_level") is not None:
                with_cli.append(iso)
            else:
                unknown.append((iso, c.get("reason"), [(a.get("candidate"), a.get("why")) for a in (c.get("source_attempts") or [])][:8]))
            if c.get("cli_level") in (120, 80):
                pinned.append(iso)
            if c.get("source_degraded"):
                degraded.append((iso, c.get("source_degraded")))
        r.log(f"with CLI: {len(with_cli)}/{len(bc)} · UNKNOWN: {json.dumps(unknown)[:900]}")
        r.log(f"pinned at a cap: {pinned} · degraded: {degraded}")
        usa = bc.get("USA") or {}
        r.log(f"USA: cli={usa.get('cli_level')} equity_composite={usa.get('equity_composite_pct')} "
              f"supplement={usa.get('supplement_value')}@{usa.get('supplement_date')} status={usa.get('supplement_status')}")
        phys = after.get("physical_confirmation") or {}
        r.log(f"physical: counts={json.dumps(phys.get('counts'))} countries_with_ports={phys.get('countries_with_ports')} "
              f"carried_from_previous_run={phys.get('carried_from_previous_run')} portwatch_at={phys.get('portwatch_generated_at')}")
        if after.get("engine_version") != WANT:
            fails.append(f"engine_version on S3 is {after.get('engine_version')}, expected {WANT}")
        if len(with_cli) < 33:
            fails.append(f"only {len(with_cli)}/34 countries carry a CLI: {unknown}")
        if usa.get("cli_level") is None or usa.get("cli_level") >= 119.5:
            fails.append(f"USA still at the cap: {usa.get('cli_level')}")
        for iso in ("CZE", "NOR"):
            c = bc.get(iso) or {}
            if c.get("cli_level") is None:
                r.warn(f"{iso} still UNKNOWN: reason={c.get('reason')} attempts={json.dumps(c.get('source_attempts'))[:600]}")

        r.section("S5 history + portwatch")
        hist, hlm = get_json(HIST_KEY)
        if hist:
            r.log(f"history: schema={hist.get('schema_version')} engine_version={hist.get('engine_version')} "
                  f"generated_at={hist.get('generated_at')} countries={hist.get('countries_count')} "
                  f"transitions={hist.get('transitions_count')} cli_transform={hist.get('cli_transform')}")
            if (hist.get("generated_at") or "") < t_inv.isoformat():
                fails.append("history feed was not regenerated by this run")
        else:
            fails.append(f"history feed unreadable: {hlm}")
        pw, plm = get_json(PW_KEY)
        if pw:
            ports = pw.get("ports") if isinstance(pw.get("ports"), list) else []
            with_yoy = sum(1 for p in ports if isinstance(p, dict) and isinstance(p.get("yoy_pct"), (int, float)))
            r.log(f"portwatch: generated_at={pw.get('generated_at')} ports={len(ports)} with_yoy_pct={with_yoy} "
                  f"errors={json.dumps(pw.get('errors'))[:300]}")
        else:
            r.warn(f"portwatch feed unreadable: {plm}")

        r.section("S6 log tail of this run")
        try:
            lg = f"/aws/lambda/{FN}"
            streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime", descending=True, limit=2)
            found_tb = False
            for s in streams.get("logStreams") or []:
                ev = logs.get_log_events(logGroupName=lg, logStreamName=s["logStreamName"],
                                         startTime=int(t_inv.timestamp() * 1000) - 60000, limit=400)
                lines = [e["message"].rstrip() for e in ev.get("events") or []]
                if not lines:
                    continue
                r.log(f"── stream {s['logStreamName'][-24:]} ({len(lines)} events) ──")
                for ln in lines:
                    if any(k in ln for k in ("[gbc]", "[polygon-lane]", "[physical]", "Traceback", "Error", "REPORT")) \
                            and "[gbc-history]" not in ln:
                        r.log("  " + ln[:220])
                    if "Traceback" in ln or "[ERROR]" in ln:
                        found_tb = True
            if found_tb:
                fails.append("traceback in the run's log")
        except Exception as e:  # noqa: BLE001
            r.warn(f"log tail failed: {str(e)[:160]}")

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            r.log("VERDICT: RED -- %d failure(s)" % len(fails))
            sys.exit(1)
        r.ok(f"VERDICT: GREEN -- v{WANT} live: {len(with_cli)}/34 countries, USA CLI {usa.get('cli_level')} "
             f"(was pinned at 120), sources {json.dumps(after.get('sources_used'))}, "
             f"physical {json.dumps(phys.get('counts'))}")


if __name__ == "__main__":
    main()
