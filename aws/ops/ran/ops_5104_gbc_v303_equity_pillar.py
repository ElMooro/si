"""ops_5104 -- GBC v3.0.3: equity pillar carried one month (was in the nowcast
for only 10/34 countries because the current calendar month had no bar yet
for markets whose last session was Aug 31). Wait for deploy, run, verify
pillar_counts.equity >= 30 and everything else unchanged.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
FN = "justhodl-global-business-cycle"
WANT = "3.0.3"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception as e:  # noqa: BLE001
        return {"_err": str(e)[:100]}


def main():
    with report("5104-gbc-v303-equity-pillar") as r:
        r.heading("ops 5104 -- GBC v3.0.3 equity pillar carry-forward")
        fails = []
        t0 = time.time()
        while time.time() - t0 < 900:
            c = lam.get_function_configuration(FunctionName=FN)
            if "v3.0.3" in (c.get("Description") or "") and c.get("LastUpdateStatus") == "Successful":
                r.ok(f"deployed after {time.time() - t0:.0f}s")
                break
            time.sleep(25)
        else:
            fails.append("v3.0.3 not deployed within 15 min")
        t_inv = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        after = None
        t0 = time.time()
        while time.time() - t0 < 900:
            time.sleep(20)
            d = get_json("data/global-business-cycle.json")
            if d.get("engine_version") == WANT and (d.get("generated_at") or "") > t_inv.isoformat():
                after = d
                break
        if not after:
            fails.append("no v3.0.3 feed within 15 min")
            after = get_json("data/global-business-cycle.json")
        comp = after.get("composite") or {}
        pc = comp.get("pillar_counts") or {}
        agg = after.get("aggregate") or {}
        cal = after.get("downturn_probability_6m") or {}
        r.log(f"composite: multi={comp.get('countries_multi_pillar')} pillar_counts={json.dumps(pc)} features_age={comp.get('features_age_h')}h")
        r.log(f"global: {agg.get('global_phase')} {agg.get('global_avg_cli')} mix={json.dumps(agg.get('global_phase_mix_pct'))} p6m={cal.get('probability_now')} latest={json.dumps(after.get('global_composite_latest'))}")
        bc = after.get("by_country") or {}
        for iso in ("USA", "CHN", "JPN", "DEU", "GBR", "IND", "KOR", "BRA"):
            c = bc.get(iso) or {}
            nc = c.get("composite") or {}
            r.log(f"  {iso} {c.get('phase')} cli={c.get('cli_level')} conf={nc.get('confidence')} pillars=" + " ".join(f"{p}:{v.get('z')}" for p, v in (nc.get('pillars') or {}).items()))
            r.kv(iso=iso, phase=c.get("phase"), cli=c.get("cli_level"), equity_z=((nc.get("pillars") or {}).get("equity") or {}).get("z"))
        if (pc.get("equity") or 0) < 30:
            fails.append(f"equity pillar in nowcast for only {pc.get('equity')} countries")
        if (comp.get("countries_multi_pillar") or 0) < 30:
            fails.append("multi-pillar coverage regressed")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok(f"VERDICT: GREEN -- v{WANT}: equity pillar {pc.get('equity')}/34, global {agg.get('global_phase')} {agg.get('global_avg_cli')}, p6m {cal.get('probability_now')}")


if __name__ == "__main__":
    main()
