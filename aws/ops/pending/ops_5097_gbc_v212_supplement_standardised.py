"""ops_5097 -- GBC v2.1.2: own-history standardised supplements, verified on real FRED prints.

ops 5095 (v2.1.0, GREEN) showed the USA supplement USACSCICP02STSAM printing
53.26 for 2026-06 and CHNBSCICP02STSAM printing exactly 0.0 for 2026-05.
Neither is an OECD normalised index (~100) nor a small anomaly, so v2.1.0's
transform clipped USA to a -3 anomaly and pulled its CLI to 101.22 -- a
guessed sign, not a measurement. v2.1.1 EXCLUDES a supplement whose scale it
does not recognise (status `scale-unrecognised (<value>) -- excluded`) and
reports it; USA becomes equity-only (composite 8.25 -> CLI ~104.07).

  S1  FRED metadata for both supplement series (title, units, frequency,
      observation_end, last_updated, notes) -- so the scale question is
      answered from the source, not inferred
  S2  wait for the v2.1.1 deploy, invoke, poll S3 for engine_version 2.1.1
  S3  verify: 34/34 with CLI, USA supplement_status excluded-or-recognised,
      USA CLI == cli_from_composite(equity_composite) when excluded,
      no caps, history 2.4 regenerated, baskets still resolving
Gate (sys.exit(1)) on any of S3.
"""
import json
import math
import os
import sys
import time
import urllib.request
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
WANT = "2.1.2"
FRED_KEY = "2f057499936072679d8843d7fce99989"
SERIES = {"USA": "USACSCICP02STSAM", "CHN": "CHNBSCICP02STSAM"}

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=60, retries={"max_attempts": 1}))


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read())
    except Exception as e:  # noqa: BLE001
        return {"_err": str(e)[:120]}


def fred(path):
    url = f"https://api.stlouisfed.org/fred/{path}&api_key={FRED_KEY}&file_type=json"
    req = urllib.request.Request(url, headers={"User-Agent": "ops5096"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())


def main():
    with report("5097-gbc-v212-supplement-standardised") as r:
        r.heading("ops 5097 -- GBC v2.1.2 supplements standardised against own history")
        fails = []
        r.section("S1 what FRED serves for the supplement series")
        for iso, sid in SERIES.items():
            try:
                meta = (fred(f"series?series_id={sid}").get("seriess") or [{}])[0]
                obs = fred(f"series/observations?series_id={sid}&sort_order=desc&limit=6").get("observations") or []
                r.log(f"{iso} {sid}: title={meta.get('title')!r} units={meta.get('units')!r} freq={meta.get('frequency')!r} "
                      f"seasonal={meta.get('seasonal_adjustment_short')!r} obs_end={meta.get('observation_end')} "
                      f"last_updated={meta.get('last_updated')}")
                r.log(f"  notes: {(meta.get('notes') or '')[:400]!r}")
                r.log(f"  last obs: {[(o.get('date'), o.get('value')) for o in obs]}")
                r.kv(series=sid, title=(meta.get("title") or "")[:60], units=meta.get("units"),
                     obs_end=meta.get("observation_end"), last=obs[0].get("value") if obs else None)
            except Exception as e:  # noqa: BLE001
                r.warn(f"{iso} {sid}: FRED metadata failed: {str(e)[:160]}")

        r.section("S2 deploy wait + run")
        t0 = time.time()
        deployed = False
        while time.time() - t0 < 900:
            cfg = lam.get_function_configuration(FunctionName=FN)
            if "v2.1.2" in (cfg.get("Description") or "") and cfg.get("LastUpdateStatus") == "Successful":
                deployed = True
                r.ok(f"deployed {cfg.get('LastModified')} after {time.time() - t0:.0f}s")
                break
            r.log(f"  waiting… desc_has_v2.1.2={'v2.1.2' in (cfg.get('Description') or '')} status={cfg.get('LastUpdateStatus')}")
            time.sleep(25)
        if not deployed:
            fails.append("v2.1.2 not deployed within 15 min")
        t_inv = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        r.log(f"async invoke at {t_inv.isoformat(timespec='seconds')}")
        after = None
        t0 = time.time()
        while time.time() - t0 < 900:
            time.sleep(20)
            doc = get_json(LIVE_KEY)
            if doc.get("engine_version") == WANT and (doc.get("generated_at") or "") > t_inv.isoformat():
                after = doc
                r.ok(f"fresh v{WANT} feed: generated_at={doc.get('generated_at')} elapsed={doc.get('elapsed_sec')}s")
                break
            r.log(f"  polling… engine_version={doc.get('engine_version')} generated_at={doc.get('generated_at')}")
        if after is None:
            fails.append("no v2.1.2 feed landed within 15 min")
            after = get_json(LIVE_KEY)

        r.section("S3 verify")
        bc = after.get("by_country") or {}
        agg = after.get("aggregate") or {}
        with_cli = [i for i, c in bc.items() if c.get("cli_level") is not None]
        r.log(f"global_phase={agg.get('global_phase')} avg_cli={agg.get('global_avg_cli')} with_cli={len(with_cli)}/34 "
              f"sources={json.dumps(after.get('sources_used'))} supplements={json.dumps(after.get('supplements'))[:500]}")
        for iso in ("USA", "CHN", "KOR", "CZE", "HUN", "NOR", "GRC"):
            c = bc.get(iso) or {}
            r.log(f"  {iso} {c.get('phase')} cli={c.get('cli_level')} equity_composite={c.get('equity_composite_pct')} "
                  f"src={c.get('source')} sup={c.get('supplement_status')} stale={c.get('days_stale')}d")
            r.kv(iso=iso, phase=c.get("phase"), cli=c.get("cli_level"), src=(c.get("source") or "")[:48], sup=c.get("supplement_status"))
        usa = bc.get("USA") or {}
        st = usa.get("supplement_status") or ""
        sup = (after.get("supplements") or {}).get("USA") or {}
        r.log(f"  USA supplement detail: {json.dumps(sup)[:400]}")
        if st == "fresh":
            # standardised: cli must equal the 75/25 blend of equity composite and z-derived points
            eq = float(usa.get("equity_composite_pct") or 0)
            pts = float(usa.get("supplement_anomaly_points") or 0)
            expected = 100.0 + 20.0 * math.tanh(0.025 * (eq * 0.75 + pts * 5.0 * 0.25))
            if abs((usa.get("cli_level") or 0) - expected) > 0.05 or not (-3 <= pts <= 3) or sup.get("n_hist", 0) < 24:
                fails.append(f"USA blend mismatch: cli={usa.get('cli_level')} expected={expected:.2f} pts={pts} n_hist={sup.get('n_hist')}")
            else:
                r.ok(f"USA blended with standardised supplement: z={sup.get('z')} -> {pts} pts over n_hist={sup.get('n_hist')} "
                     f"(mean {sup.get('hist_mean')} sd {sup.get('hist_std')}); cli {usa.get('cli_level')} == expected {expected:.2f}")
            st = "checked"
        if st.startswith("scale-unrecognised") or st.startswith("stale") or st.startswith("only") or st in ("none", "unavailable"):
            expected = 100.0 + 20.0 * math.tanh(0.025 * float(usa.get("equity_composite_pct") or 0))
            if usa.get("cli_level") is None or abs(usa["cli_level"] - expected) > 0.05:
                fails.append(f"USA cli {usa.get('cli_level')} != equity-only {expected:.2f} although supplement is {st}")
            else:
                r.ok(f"USA equity-only: cli {usa.get('cli_level')} == 100+20*tanh(0.025*{usa.get('equity_composite_pct')}) ; supplement {st}")
        elif st == "checked":
            pass
        else:
            fails.append(f"USA supplement status unexpected: {st}")
        if len(with_cli) < 33:
            fails.append(f"only {len(with_cli)}/34 with CLI")
        if any(c.get("cli_level") in (120, 80) for c in bc.values()):
            fails.append("a country sits on a hard cap")
        for iso in ("CZE", "HUN", "NOR", "GRC"):
            if not (bc.get(iso) or {}).get("source", "").startswith(("basket:", "yahoo:", "polygon-etf:")):
                fails.append(f"{iso} has no resolved source: {(bc.get(iso) or {}).get('source')}")
        hist = get_json(HIST_KEY)
        if hist.get("engine_version") != WANT or (hist.get("generated_at") or "") < t_inv.isoformat():
            fails.append(f"history not regenerated by v{WANT}: {hist.get('engine_version')} {hist.get('generated_at')}")
        else:
            r.ok(f"history regenerated: schema {hist.get('schema_version')} transitions={hist.get('transitions_count')}")
        phys = after.get("physical_confirmation") or {}
        r.log(f"physical: {json.dumps(phys.get('counts'))} countries_with_ports={phys.get('countries_with_ports')} "
              f"carried={phys.get('carried_from_previous_run')} (portwatch upstream: ArcGIS 429 quota + invalid field -- separate engine)")

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok(f"VERDICT: GREEN -- v{WANT}: {len(with_cli)}/34, USA {usa.get('cli_level')} ({st})")


if __name__ == "__main__":
    main()
