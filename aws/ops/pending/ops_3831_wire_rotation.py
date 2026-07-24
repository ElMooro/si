"""
ops_3831 — WIRE rotation-dashboard -> best-setups + master-ranker

Probe 3829 forecast the naive join at 48% / 0% on PUBLISHED rows. The correct
read: both engines resolve sector INTERNALLY (best-setups via
_sector_map.get(tk) or sector; master-ranker via r.get("sector") at ingest), so
the tilt belongs beside the existing _roro / _nowcast overlays, not as a
post-hoc match on output. Mirrors _nowcast_scalar / _nowcast_overlay exactly.

GATES A NON-ZERO JOIN ON LIVE OUTPUT after invoke — the whole point. An overlay
that silently multiplies by 1.0 forever looks identical to a working one.
"""
import json
import sys
import time
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
CFG = boto3.session.Config(read_timeout=890, retries={"max_attempts": 0})


def settle(fn, rep, marker):
    """ZIP-SETTLE, not state-settle. ops 3830 failed exactly here: State==Active
    returns instantly when deploy-lambdas has not STARTED yet, so the ops invoked
    the OLD artifact and the new field was absent on every row. The only reliable
    proof is downloading the deployed zip and grepping for a string that exists
    only in the new code."""
    import io as _io, urllib.request as _u, zipfile as _z
    for i in range(60):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
            try:
                url = lam.get_function(FunctionName=fn)["Code"]["Location"]
                blob = _u.urlopen(url, timeout=60).read()
                with _z.ZipFile(_io.BytesIO(blob)) as zf:
                    src = zf.read("lambda_function.py").decode("utf-8", "ignore")
                if marker in src:
                    rep.ok(f"  {fn} ZIP-SETTLED with '{marker}' after {i*10}s")
                    return
                rep.log(f"    {fn}: deployed artifact lacks '{marker}' — waiting")
            except Exception as e:
                rep.log(f"    {fn}: zip read failed ({str(e)[:60]}) — retrying")
        time.sleep(10)
    rep.fail(f"{fn}: '{marker}' never reached the deployed artifact"); sys.exit(1)


def main():
    with report("3831_wire_rotation") as rep:
        rep.heading("ops 3831 — wire rotation tilt into setups + ranker")

        rep.section("G0. rotation feed is fresh and carries what we consume")
        rd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/rotation-dashboard.json")["Body"].read())
        sec = [a for a in rd.get("assets", [])
               if a["ticker"] in ("XLK", "XLF", "XLV", "XLE", "XLI", "XLY",
                                  "XLP", "XLB", "XLU", "XLRE", "XLC")]
        if len(sec) < 9:
            rep.fail(f"  only {len(sec)}/11 sector ETFs present"); sys.exit(1)
        rep.ok(f"  {len(sec)}/11 sector ETFs, regime="
               f"{(rd['layer1_regime'].get('quadrant') or {}).get('regime')}")
        for a in sec:
            if not (a.get("rrg") or {}).get("quadrant"):
                rep.fail(f"  {a['ticker']} missing rrg.quadrant"); sys.exit(1)
        rep.ok("  every sector ETF carries rrg.quadrant + trend_gate.eligible")

        results = {}
        for fn, key, listk, marker in (
                ("justhodl-best-setups", "data/best-setups.json", "top_setups",
                 "_rotation_scalar"),
                ("justhodl-master-ranker", "data/master-ranker.json", "top_tickers",
                 "_rotation_overlay")):
            rep.section(f"── {fn}")
            settle(fn, rep, marker)
            r = boto3.client("lambda", region_name="us-east-1", config=CFG).invoke(
                FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
            if r.get("FunctionError"):
                rep.fail(f"  invoke error: {r['Payload'].read()[:400]}"); sys.exit(1)
            rep.ok("  invoked clean")

            d = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            rows = d.get(listk) or []
            tilted = [x for x in rows
                      if x.get("rotation_mult") not in (None, 1.0)]
            present = [x for x in rows if "rotation_mult" in x]
            results[fn] = (len(tilted), len(present), len(rows))
            rep.log(f"  rows={len(rows)} · field present={len(present)} · "
                    f"actually tilted={len(tilted)}")
            for x in tilted[:6]:
                rep.log(f"    {x.get('ticker'):<6} x{x.get('rotation_mult')} "
                        f"{x.get('rotation_note') or x.get('rotation_quadrant')}")
            if not present:
                rep.fail(f"  rotation_mult ABSENT on every row — not wired")
                sys.exit(1)
            if not tilted:
                rep.fail(f"  rotation_mult present but 1.0 on EVERY row — "
                         f"silent no-op, the exact failure this gate exists for")
                sys.exit(1)
            rep.ok(f"  NON-ZERO JOIN: {len(tilted)}/{len(rows)} rows tilted")

        rep.kv(**{k.replace("justhodl-", "") + "_tilted": f"{v[0]}/{v[2]}"
                  for k, v in results.items()})
        rep.ok("PASS_ALL — rotation tilt live in both rankers")


if __name__ == "__main__":
    main()
