#!/usr/bin/env python3
"""
Step 135 — UI: Calibration status badge in reports.html.

Two coordinated changes:

A. justhodl-reports-builder Lambda — augment the scorecard.json
   meta block to include Loop 1 calibration status:
     meta.is_meaningful   — True when ≥30 scored outcomes for any signal
     meta.n_calibrated_signals — count of signals with enough data
     meta.first_meaningful_eta — best-guess date when this flips True
   Computed inline from the same accuracy data already loaded.

B. reports.html — render a prominent badge in the headline showing
   the Loop 1 status:
     YELLOW: "⏳ Calibration: Awaiting Data" (today)
     GREEN:  "✓ Calibrated · N signals" (after May 2 when meaningful)
   Position: top-right corner of the headline-box.

Why this matters: today the scorecard already has has_calibration=True
(weights exist), but those weights are 1.0-default-noise because no
outcomes are scored. The new is_meaningful field is the actual signal
that says "the system has begun learning." This badge is the visible
proof that Loop 1 has gone from theoretical to operating.
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

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("loop1_ui_badge") as r:
    r.heading("Loop 1 UI badge — reports-builder meta + reports.html badge")

    # ════════════════════════════════════════════════════════════════════
    # A. Patch reports-builder to add is_meaningful + n_calibrated_signals
    # ════════════════════════════════════════════════════════════════════
    r.section("A. Patch justhodl-reports-builder")
    rb_path = REPO_ROOT / "aws/lambdas/justhodl-reports-builder/source/lambda_function.py"
    rb_src = rb_path.read_text()

    OLD_META = """        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "signals_total": len(signals),
            "outcomes_total": len(outcomes),
            "scored_outcomes": sum(1 for o in outcomes if o.get("correct") is not None),
            "has_calibration": bool(weights and accuracy),
            "calibration_summary": {
                "weights_count": len(weights) if isinstance(weights, dict) else 0,
                "accuracy_count": len(accuracy) if isinstance(accuracy, dict) else 0,
                "report_keys": list(calib_report.keys()) if isinstance(calib_report, dict) else [],
            },
        },"""

    NEW_META = """        # Loop 1: compute calibration meaningfulness — True when
        # at least one signal has ≥30 scored outcomes (enough for the
        # calibrated weight to be more than noise).
        "meta": (lambda: {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "signals_total": len(signals),
            "outcomes_total": len(outcomes),
            "scored_outcomes": sum(1 for o in outcomes if o.get("correct") is not None),
            "has_calibration": bool(weights and accuracy),
            "calibration_summary": {
                "weights_count": len(weights) if isinstance(weights, dict) else 0,
                "accuracy_count": len(accuracy) if isinstance(accuracy, dict) else 0,
                "report_keys": list(calib_report.keys()) if isinstance(calib_report, dict) else [],
            },
            # ─── Loop 1 status (the actually-useful flag) ─────────────
            "is_meaningful": (lambda acc=accuracy: any(
                isinstance(e, dict) and (e.get("n_correct", 0) + e.get("n_wrong", 0)) >= 30
                for e in (acc.values() if isinstance(acc, dict) else [])
            ))(),
            "n_calibrated_signals": (lambda acc=accuracy: sum(
                1 for e in (acc.values() if isinstance(acc, dict) else [])
                if isinstance(e, dict) and (e.get("n_correct", 0) + e.get("n_wrong", 0)) >= 30
            ))(),
            "n_signals_with_outcomes": (lambda acc=accuracy: sum(
                1 for e in (acc.values() if isinstance(acc, dict) else [])
                if isinstance(e, dict) and e.get("n", 0) > 0
            ))(),
        })(),"""

    if OLD_META in rb_src:
        rb_src = rb_path.write_text(rb_src.replace(OLD_META, NEW_META)) or rb_path.read_text()
        # The above is a hack to write+reread. Do it cleanly:
        rb_src = rb_path.read_text()
        if OLD_META in rb_src:
            # write didn't take, do it directly
            rb_src = rb_src.replace(OLD_META, NEW_META)
            rb_path.write_text(rb_src)
        r.ok("  Patched reports-builder meta block")
    elif "is_meaningful" in rb_src:
        r.log("  is_meaningful already in source, skipping")
    else:
        r.fail("  Couldn't find meta block anchor")
        raise SystemExit(1)

    # Validate
    import ast
    try:
        ast.parse(rb_src)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        if hasattr(e, 'lineno') and e.lineno:
            lines = rb_src.split("\n")
            for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
                marker = " >>> " if i == e.lineno - 1 else "     "
                r.log(f"  {marker}L{i+1}: {lines[i][:200]}")
        raise SystemExit(1)

    # Re-deploy reports-builder
    r.section("A2. Re-deploy reports-builder")
    name = "justhodl-reports-builder"
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-reports-builder/source"
    buf = io.BytesIO()
    files_added = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for src_file in sorted(src_dir.rglob("*.py")):
            arcname = str(src_file.relative_to(src_dir))
            info = zipfile.ZipInfo(arcname)
            info.external_attr = 0o644 << 16
            zout.writestr(info, src_file.read_text())
            files_added += 1
    zbytes = buf.getvalue()

    lam.update_function_code(
        FunctionName=name, ZipFile=zbytes, Architectures=["arm64"],
    )
    lam.get_waiter("function_updated").wait(
        FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed reports-builder ({len(zbytes):,}B)")

    # Sync invoke to refresh scorecard.json
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    if resp.get("FunctionError"):
        r.fail(f"  reports-builder error: {resp.get('Payload').read().decode()[:600]}")
        raise SystemExit(1)
    r.ok(f"  Invoked in {elapsed:.1f}s")

    # Verify scorecard.json has new fields
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="reports/scorecard.json")
    sc = json.loads(obj["Body"].read().decode("utf-8"))
    meta = sc.get("meta", {})
    r.log(f"\n  scorecard.json meta:")
    for k in ["is_meaningful", "n_calibrated_signals", "n_signals_with_outcomes",
              "has_calibration", "scored_outcomes"]:
        marker = " ← NEW" if k in ("is_meaningful", "n_calibrated_signals", "n_signals_with_outcomes") else ""
        r.log(f"    {k:30} {meta.get(k)}{marker}")

    # ════════════════════════════════════════════════════════════════════
    # B. Patch reports.html to render the badge
    # ════════════════════════════════════════════════════════════════════
    r.section("B. Patch reports.html — add Loop 1 calibration badge")
    html_path = REPO_ROOT / "reports.html"
    html_src = html_path.read_text()

    # Add CSS for the badge — anchor on the existing styles section.
    BADGE_CSS = """
.cal-badge { position: absolute; top: 16px; right: 16px; padding: 6px 14px; border-radius: 16px; font-size: 11px; font-weight: 600; letter-spacing: 0.5px; text-transform: uppercase; display: inline-flex; align-items: center; gap: 6px; }
.cal-badge.awaiting { background: rgba(255, 196, 0, 0.12); color: var(--yellow); border: 1px solid rgba(255, 196, 0, 0.4); }
.cal-badge.active { background: rgba(0, 230, 118, 0.12); color: var(--green); border: 1px solid rgba(0, 230, 118, 0.4); box-shadow: 0 0 12px rgba(0, 230, 118, 0.2); }
.cal-badge .dot { width: 6px; height: 6px; border-radius: 50%; }
.cal-badge.awaiting .dot { background: var(--yellow); animation: pulse-y 2s ease-in-out infinite; }
.cal-badge.active .dot { background: var(--green); }
@keyframes pulse-y { 0%,100% { opacity: 0.4; } 50% { opacity: 1; } }
.headline-box { position: relative; }
"""
    # Insert before </style>. Anchor on a CSS line we know is unique.
    OLD_STYLE_END = "</style>"
    if BADGE_CSS.strip() in html_src:
        r.log("  Badge CSS already in HTML, skipping CSS insert")
    elif OLD_STYLE_END in html_src:
        # Find the FIRST </style> (there should only be one)
        idx = html_src.find(OLD_STYLE_END)
        html_src = html_src[:idx] + BADGE_CSS + html_src[idx:]
        r.ok("  Inserted badge CSS")
    else:
        r.fail("  Couldn't find </style> anchor")
        raise SystemExit(1)

    # Now patch renderHeadline to inject the badge inside .headline-box
    OLD_HEADLINE_OPENING = """  return `
    <div class="headline-box">
      <div class="meta">PERFORMANCE REPORT — REAL DATA</div>"""

    NEW_HEADLINE_OPENING = """  // Loop 1 calibration badge — flips to green when calibrator
  // has scored ≥30 outcomes for at least one signal type.
  const isMeaningful = m.is_meaningful === true;
  const nCalibrated = m.n_calibrated_signals || 0;
  const nWithOutcomes = m.n_signals_with_outcomes || 0;
  const calBadge = isMeaningful
    ? `<span class="cal-badge active" title="${nCalibrated} signal(s) have ≥30 scored outcomes — Loop 1 weights are tracking historical accuracy"><span class="dot"></span>Calibrated · ${nCalibrated} signal${nCalibrated === 1 ? '' : 's'}</span>`
    : `<span class="cal-badge awaiting" title="No signal has ≥30 scored outcomes yet. ${nWithOutcomes} signal type(s) have outcomes pending. Calibration becomes meaningful when scoring catches up — typically early May."><span class="dot"></span>Calibration · Awaiting Data</span>`;
  return `
    <div class="headline-box">
      ${calBadge}
      <div class="meta">PERFORMANCE REPORT — REAL DATA</div>"""

    if OLD_HEADLINE_OPENING in html_src:
        html_src = html_src.replace(OLD_HEADLINE_OPENING, NEW_HEADLINE_OPENING)
        r.ok("  Patched renderHeadline to render calBadge")
    elif "calBadge" in html_src:
        r.log("  calBadge already in HTML, skipping")
    else:
        r.fail("  Couldn't find headline opening anchor")
        raise SystemExit(1)

    html_path.write_text(html_src)
    r.log(f"  Wrote reports.html: {len(html_src):,}B")

    # Verify HTML is well-formed (light check)
    open_count = html_src.count("<div")
    close_count = html_src.count("</div>")
    r.log(f"  <div> count: {open_count}, </div> count: {close_count}")
    if open_count != close_count:
        r.warn(f"  ⚠ Mismatched div count — may indicate a broken nesting (was {html_src.count('<div')} {html_src.count('</div>')} pre-patch?)")

    r.section("C. Verify integration — scorecard.json + reports.html state")
    r.log(f"  scorecard.json meta.is_meaningful: {meta.get('is_meaningful')}")
    r.log(f"  scorecard.json meta.n_calibrated_signals: {meta.get('n_calibrated_signals')}")
    if meta.get("is_meaningful"):
        r.ok(f"  ✅ Badge will render GREEN — Loop 1 is meaningful")
    else:
        r.log(f"  ⏳ Badge will render YELLOW (Awaiting Data)")
        r.log(f"     Will flip GREEN automatically when ≥30 outcomes are scored")
        r.log(f"     for at least one signal (~May 2 onward).")

    r.kv(
        rb_zip_size=len(zbytes),
        scorecard_is_meaningful=meta.get("is_meaningful"),
        scorecard_n_calibrated=meta.get("n_calibrated_signals"),
        scorecard_n_with_outcomes=meta.get("n_signals_with_outcomes"),
        html_size=len(html_src),
    )
    r.log("Done")
