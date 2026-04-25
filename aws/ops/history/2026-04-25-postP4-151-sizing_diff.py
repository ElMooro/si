#!/usr/bin/env python3
"""
Step 151 — Phase 3 sizing differentiation.

Issue: today's risk-sizer output has all 17 ideas at exactly 4.41%,
no differentiation between INCY (4/4 dims, composite 93.3) and JKHY
(3/4 dims, composite 77.7). The gross exposure cap (75% / 17 ideas =
4.41%) dominates. Kelly differentiation gets washed out because the
raw_conviction range is narrow (0.73-0.83).

Fix: scale individual sizes by composite_score (or raw_conviction
when composite missing) BEFORE applying gross/cluster caps. This
produces a meaningful spread:
  INCY composite 93.3 → ~5.5%
  FSLR composite 89.1 → ~5.3%
  ...
  JKHY composite 77.7 → ~4.6%

The total still respects the 75% gross cap, but high-conviction
names get genuinely larger allocations.

THE FORMULA:
  base_size = kelly_size(conviction)  # already exists, ~4-7% range
  weight = composite_score / mean(all composite_scores)  # 0.85-1.15 range
  weighted_size = base_size * weight
  → apply drawdown multiplier
  → apply per-cluster cap (25%)
  → apply gross exposure cap (regime-based)
  → cap individual at 8%

This stays conservative (still fractional Kelly, still all the caps)
but lets quality differentiation flow through.

VERIFICATION:
  After patch, top 5 should still be INCY/FSLR/RMD/DECK/PTC but
  with sizes that DIFFER (not all 4.41%). Spread should be roughly
  1.5x between top and bottom. Total should still equal max gross
  exposure.

NO behavioral changes when ideas are tied (everyone gets weight 1.0).
"""
import io
import json
import os
import time
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

BUCKET = "justhodl-dashboard-live"
FNAME = "justhodl-risk-sizer"


with report("phase3_sizing_differentiation") as r:
    r.heading("Phase 3 — composite-score-weighted sizing")

    src_path = REPO_ROOT / "aws/lambdas/justhodl-risk-sizer/source/lambda_function.py"
    src = src_path.read_text()

    # ─── 1. Patch the sizing block in lambda_handler ────────────────────
    r.section("1. Patch sizing logic to weight by composite_score")

    OLD_SIZING_BLOCK = '''    # ─── 6. Size each idea ──────────────────────────────────────────────
    sized = []
    for sym, idea in ideas.items():
        kelly = kelly_size(idea["raw_conviction"])
        # Apply drawdown multiplier
        adjusted = kelly * dd_multiplier
        # Apply regime exposure cap (proportional)
        # If sum of all ideas\\' sizes would exceed max_gross, scale all down later
        idea["kelly_raw"] = kelly
        idea["dd_adjusted"] = round(adjusted, 4)
        idea["cluster"] = sym_to_cluster.get(sym, "isolated")
        sized.append(idea)'''

    NEW_SIZING_BLOCK = '''    # ─── 6. Size each idea ──────────────────────────────────────────────
    # Step 151: composite-score-weighted sizing. Kelly produces base
    # sizes from raw_conviction (narrow range). We multiply by a weight
    # derived from composite_score (or raw_conviction as fallback) so
    # high-conviction names get larger allocations BEFORE caps bite.
    quality_signals = []
    for sym, idea in ideas.items():
        # Prefer phase2b_composite (0-100 scale, range usually 70-95).
        # Fallback: raw_conviction (0.5-0.92 scale → multiply by 100).
        qs = idea.get("phase2b_composite")
        if qs is None or qs <= 0:
            qs = idea.get("raw_conviction", 0.65) * 100
        quality_signals.append((sym, qs))
    avg_qs = (sum(qs for _, qs in quality_signals) / len(quality_signals)) if quality_signals else 1.0
    weight_by_sym = {sym: (qs / avg_qs) if avg_qs > 0 else 1.0 for sym, qs in quality_signals}
    # Cap weight range at [0.6x, 1.6x] to prevent extreme tilts
    weight_by_sym = {sym: max(0.6, min(1.6, w)) for sym, w in weight_by_sym.items()}

    sized = []
    for sym, idea in ideas.items():
        kelly = kelly_size(idea["raw_conviction"])
        weight = weight_by_sym.get(sym, 1.0)
        weighted_kelly = kelly * weight
        # Apply drawdown multiplier
        adjusted = weighted_kelly * dd_multiplier
        idea["kelly_raw"] = round(kelly, 4)
        idea["quality_weight"] = round(weight, 3)
        idea["dd_adjusted"] = round(adjusted, 4)
        idea["cluster"] = sym_to_cluster.get(sym, "isolated")
        sized.append(idea)'''

    if OLD_SIZING_BLOCK in src:
        src = src.replace(OLD_SIZING_BLOCK, NEW_SIZING_BLOCK)
        r.ok("  Patched sizing logic with composite-score weighting")
    elif "quality_weight" in src:
        r.log("  Already patched")
    else:
        r.fail("  Couldn't find sizing block anchor")
        raise SystemExit(1)

    # ─── 2. Validate + write ────────────────────────────────────────────
    r.section("2. Validate + write")
    import ast
    try:
        ast.parse(src)
        r.ok(f"  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        raise SystemExit(1)
    src_path.write_text(src)

    # ─── 3. Deploy ──────────────────────────────────────────────────────
    r.section("3. Deploy")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, src)
    zbytes = buf.getvalue()
    lam.update_function_code(
        FunctionName=FNAME, ZipFile=zbytes, Architectures=["arm64"],
    )
    lam.get_waiter("function_updated").wait(
        FunctionName=FNAME, WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    r.ok(f"  Deployed ({len(zbytes):,}B)")

    # ─── 4. Test invoke ─────────────────────────────────────────────────
    r.section("4. Test invoke")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=FNAME, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  FunctionError ({elapsed:.1f}s): {payload[:1000]}")
        raise SystemExit(1)
    r.ok(f"  Invoked in {elapsed:.1f}s")
    outer = json.loads(payload)
    body = json.loads(outer.get("body", "{}"))
    r.log(f"  Total size: {body.get('total_size_pct')}%")

    # ─── 5. Read sizing output to verify differentiation ────────────────
    r.section("5. Verify sizing now differentiates by quality")
    obj = s3.get_object(Bucket=BUCKET, Key="risk/recommendations.json")
    snap = json.loads(obj["Body"].read().decode("utf-8"))
    recs = snap.get("sized_recommendations", [])

    sizes = [r.get("recommended_size_pct", 0) for r in recs]
    if sizes:
        spread = max(sizes) - min(sizes)
        r.log(f"  Size range: {min(sizes):.2f}% - {max(sizes):.2f}% (spread {spread:.2f}%)")
        if spread > 0.5:
            r.ok(f"  ✅ Sizing now differentiates ({spread:.2f}% spread)")
        else:
            r.warn(f"  ⚠ Differentiation still weak ({spread:.2f}% spread)")

    r.log(f"\n  Top 10 (sorted by size):")
    for rec in sorted(recs, key=lambda r: -r.get("recommended_size_pct", 0))[:10]:
        comp = rec.get("phase2b_composite", "—")
        weight = rec.get("quality_weight", "—")
        r.log(f"    {rec['symbol']:6} comp={comp:>5} weight={weight:>5} "
              f"size={rec.get('recommended_size_pct'):>5}% "
              f"cluster={rec.get('cluster','?')[:20]}")

    r.kv(
        size_min=f"{min(sizes):.2f}%" if sizes else "—",
        size_max=f"{max(sizes):.2f}%" if sizes else "—",
        spread=f"{spread:.2f}%" if sizes else "—",
        total=body.get('total_size_pct'),
    )
    r.log("Done")
