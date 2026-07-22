"""
ops 3713 — consensus observability RE-GATE (3712 was a gate typo, not an engine bug)

WHAT 3712 GOT WRONG
═══════════════════
ops 3712 failed on G4_consensus_observable with 8/9 gates green. The engine was
never at fault. The gate read:

    cov = d.get("consensus_honesty") or {}

but justhodl-readthrough v1.2.1 writes that block as:

    "consensus_coverage": {...}

So `cov` was always {}, and cov.get("rows_consensus_observed", 0) returned the
default 0 regardless of the data. The per-row debug print in 3712 read
consensus_observed straight off the row objects and correctly showed
observed=True on every line — the contradiction in that report (rows observed,
counter zero) is the signature of the typo.

Verified in-repo before writing this file:
    "consensus_coverage":  written by engine = True
    "consensus_honesty":   written by engine = False

THIRD TIME THIS CLASS OF BUG HAS COST AN OPS
════════════════════════════════════════════
  ops 3611  gate read "portfolios",         engine wrote "benchmark_portfolios"
  ops 3710  gate demanded same-day ledger growth on a daily-rotating slice
  ops 3712  gate read "consensus_honesty",  engine wrote "consensus_coverage"

Durable fix applied here: G0_key_contract asserts the producer key EXISTS before
any gate consumes it, so a future rename fails loudly at the contract line
instead of silently defaulting to 0 and blaming the engine.

NO ENGINE CHANGE. No redeploy. readthrough v1.2.1 is already live and proven by
3712's own G1. This ops only re-reads the existing artifact and re-gates it.
"""
import io as _io
import json
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

import boto3  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
FN = "justhodl-readthrough"
BUCKET = "justhodl-dashboard-live"
KEY = "data/readthrough.json"
SNAP = "readthrough/consensus-snapshots.json"

LAM = boto3.client("lambda", region_name="us-east-1")
S3C = boto3.client("s3", region_name="us-east-1")


def main():
    with report("3713_consensus_regate") as rep:
        rep.heading("ops 3713 — consensus observability re-gate (3712 key typo)")
        fails = []
        out = {}

        def gate(n, ok, d):
            rep.log(f"{n} {bool(ok)}")
            print(f"  {n:34} {bool(ok)}  {d}")
            out[n] = {"ok": bool(ok), "detail": d}
            if not ok:
                fails.append(n)

        # ── G0: KEY CONTRACT ────────────────────────────────────────────────
        # Read the producer source and prove the key this ops consumes is the
        # key the engine actually writes. This is the gate 3712 needed.
        import zipfile

        src = ""
        t0 = time.time()
        while time.time() - t0 < 300:
            try:
                loc = LAM.get_function(FunctionName=FN)["Code"]["Location"]
                z = zipfile.ZipFile(
                    _io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
                src = z.read("lambda_function.py").decode("utf-8", "ignore")
                if 'VERSION = "1.2.1"' in src:
                    break
            except Exception as e:  # noqa: BLE001
                print("  retry:", str(e)[:70])
            time.sleep(20)

        writes_coverage = '"consensus_coverage":' in src
        writes_honesty = '"consensus_honesty":' in src
        gate("G0_key_contract", writes_coverage and not writes_honesty,
             f"engine writes consensus_coverage={writes_coverage} "
             f"consensus_honesty={writes_honesty} (3712 read the latter)")

        gate("G1_shipped", 'VERSION = "1.2.1"' in src
             and "CONSENSUS_DISSENTING" in src and "consensus_dissenting" in src,
             f"v1.2.1={'VERSION = ' + chr(34) + '1.2.1' in src} "
             f"observed_field={'consensus_observed' in src}")

        # ── read the LIVE artifact (3712 already invoked twice; no re-invoke) ─
        d = json.loads(S3C.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        rows = d.get("beneficiaries") or []
        cov = d.get("consensus_coverage") or {}      # ← the correct key
        qc = d.get("quadrant_counts") or {}

        gate("G2_artifact_fresh", bool(rows) and d.get("version") == "1.2.1",
             f"version={d.get('version')} rows={len(rows)} "
             f"generated={str(d.get('generated_at'))[:19]} degraded={d.get('degraded')}")

        try:
            snap = json.loads(S3C.get_object(Bucket=BUCKET, Key=SNAP)["Body"].read())
        except Exception:  # noqa: BLE001
            snap = {}
        gate("G3_snapshot_ledger", len(snap) >= 20,
             f"consensus snapshot ledger holds {len(snap)} names")

        # ── G4: the gate 3712 meant to run ──────────────────────────────────
        observed_cov = cov.get("rows_consensus_observed", 0)
        observed_rows = sum(
            1 for x in rows
            if (x.get("fundamentals") or {}).get("consensus_observed"))
        gate("G4_consensus_observable", observed_cov > 0,
             f"rows_consensus_observed={observed_cov} rows={len(rows)} "
             f"sellside_covered={cov.get('names_with_sellside_coverage')} "
             f"deltas={cov.get('names_with_consensus_delta')}")

        # ── G5: counter must agree with the rows it claims to summarise ─────
        gate("G5_counter_matches_rows", observed_cov == observed_rows,
             f"coverage_block={observed_cov} recount_from_rows={observed_rows} "
             "(mismatch = rollup drifted from row truth)")

        # ── G6: quadrants must discriminate, not collapse to one bucket ─────
        nonzero = {k: v for k, v in qc.items() if v}
        top_share = (max(qc.values()) / sum(qc.values())) if sum(qc.values()) else 1.0
        gate("G6_quadrants_discriminate", len(nonzero) >= 2 and top_share < 0.95,
             f"non-empty={nonzero} top_share={round(top_share, 3)} "
             "(3710 collapsed to PRICE_ONLY 78/79)")

        # ── evidence ────────────────────────────────────────────────────────
        print("\n  quadrant distribution:")
        for q, n in sorted(qc.items(), key=lambda z: -z[1]):
            if n:
                print(f"    {q:26} {n}")

        print("\n  sample rows (observability truth):")
        for x in rows[:12]:
            f = x.get("fundamentals") or {}
            print(f"  {x['ticker']:6} {str(x.get('pricing_quadrant')):24} "
                  f"observed={f.get('consensus_observed')} "
                  f"moved={f.get('consensus_moved')} "
                  f"dissent={f.get('consensus_dissenting')} "
                  f"sc={x.get('catch_up_score')}")

        out["verdict"] = ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails))
        print("\nVERDICT:", out["verdict"])
        rep.log("VERDICT: " + out["verdict"])
        for _k, _v in out.items():
            if isinstance(_v, dict):
                rep.kv(gate=_k, ok=_v.get("ok"), detail=str(_v.get("detail"))[:160])

        if fails:
            sys.exit(1)


if __name__ == "__main__":
    main()
