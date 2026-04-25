#!/usr/bin/env python3
"""
Step 116 — Fix `re` import in reports-builder + add Section 1 UI to reports.html.

Step 115's compute_morning_archive used re.match but the module didn't
import re. Result: morning_archive_days=0 with no error visible.

This step:
  1. Adds `import re` to the Lambda
  2. Re-deploys + invokes — should now return ~30 archive entries
  3. Extends reports.html with Section 1: a card-grid showing each
     day's morning brief (date, regime badge, headline, key metrics)
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


with report("fix_re_import_and_section1_ui") as r:
    r.heading("Fix `re` import + add Section 1 UI to reports.html")

    # ─── 1. Patch Lambda — add `import re` ──────────────────────────────
    r.section("1. Add `import re` to reports-builder")
    src_path = REPO_ROOT / "aws/lambdas/justhodl-reports-builder/source/lambda_function.py"
    src = src_path.read_text()

    if "\nimport re\n" not in src and "import re\n" not in src.split("\n", 1)[0]:
        # Add after `import os`
        src = src.replace(
            "import os\nfrom collections",
            "import os\nimport re\nfrom collections",
        )
        src_path.write_text(src)
        r.ok("  Added `import re`")
    else:
        r.log("  `re` already imported, skipping")

    # Validate
    import ast
    ast.parse(src)
    r.ok("  Syntax OK")

    # Re-deploy
    name = "justhodl-reports-builder"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, src)
    zbytes = buf.getvalue()
    lam.update_function_code(FunctionName=name, ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed ({len(zbytes)}B)")

    # Invoke
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    if resp.get("FunctionError"):
        payload = resp.get("Payload").read().decode()
        r.fail(f"  FunctionError: {payload[:600]}")
        raise SystemExit(1)
    body = json.loads(json.loads(resp.get("Payload").read().decode()).get("body", "{}"))
    r.ok(f"  Invoked in {elapsed:.1f}s: {body}")

    # Verify in scorecard.json
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="reports/scorecard.json")
    data = json.loads(obj["Body"].read().decode("utf-8"))
    archive = data.get("morning_archive", [])
    r.log(f"  morning_archive entries: {len(archive)}")
    if archive:
        r.log(f"  Sample (newest):")
        latest = archive[0]
        for k, v in list(latest.items())[:10]:
            r.log(f"    {k:20} {json.dumps(v, default=str)[:120]}")

    # ─── 2. Extend reports.html — add Section 1 UI ──────────────────────
    r.section("2. Add Section 1 UI to reports.html")
    html_path = REPO_ROOT / "reports.html"
    html = html_path.read_text()

    # Add CSS for the morning brief cards
    css_addition = """
.brief-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:14px;margin-bottom:24px}
.brief-card{padding:18px 20px;background:var(--bg2);border:1px solid var(--brd);border-radius:12px;transition:border-color .2s;display:flex;flex-direction:column;gap:10px}
.brief-card:hover{border-color:var(--brd2)}
.brief-card-hdr{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}
.brief-date{font-family:'IBM Plex Mono',monospace;font-size:11px;color:var(--t3);letter-spacing:1.2px;text-transform:uppercase}
.brief-regime{padding:3px 10px;border-radius:11px;font-size:10px;font-weight:600;font-family:'IBM Plex Mono',monospace;letter-spacing:.5px;white-space:nowrap}
.regime-CRISIS,.regime-BEAR{background:rgba(255,23,68,.1);color:var(--red);border:1px solid rgba(255,23,68,.3)}
.regime-CAUTIOUS{background:rgba(255,109,0,.1);color:var(--orange);border:1px solid rgba(255,109,0,.3)}
.regime-NEUTRAL{background:rgba(255,196,0,.1);color:var(--yellow);border:1px solid rgba(255,196,0,.3)}
.regime-OPTIMISTIC{background:rgba(0,229,255,.1);color:var(--cyan);border:1px solid rgba(0,229,255,.3)}
.regime-EUPHORIA,.regime-BULLISH{background:rgba(0,230,118,.1);color:var(--green);border:1px solid rgba(0,230,118,.3)}
.brief-headline{font-size:14px;font-weight:600;line-height:1.4;color:var(--t1)}
.brief-detail{font-size:12px;color:var(--t2);line-height:1.5}
.brief-metrics{display:flex;flex-wrap:wrap;gap:8px;font-family:'IBM Plex Mono',monospace;font-size:11px;border-top:1px solid var(--brd);padding-top:10px}
.brief-metric{color:var(--t3)}
.brief-metric strong{color:var(--t1);font-weight:600}
.brief-action{padding:8px 12px;background:var(--bg);border-radius:6px;font-size:11px;color:var(--cyan);font-style:italic;border-left:3px solid var(--cyan)}
"""

    # Inject CSS before the closing </style>
    if ".brief-card" not in html:
        html = html.replace("@media(max-width:700px)", css_addition + "\n@media(max-width:700px)")
        r.ok("  Added Section 1 CSS")

    # Add the rendering function — find where renderTimeline is, add after
    js_func = '''

function renderMorningArchive(archive) {
  if (!archive || archive.length === 0) {
    return `<div class="empty-state">
      <h3>No morning briefs archived yet</h3>
      <p>Once the morning-intelligence Lambda has run for a few days, daily snapshots will appear here as cards.</p>
    </div>`;
  }
  let html = `<div class="brief-grid">`;
  for (const b of archive) {
    const regime = (b.regime || "?").toUpperCase();
    const dateStr = new Date(b.date + "T12:00:00Z").toLocaleDateString("en-US", {
      weekday: "short", month: "short", day: "numeric"
    });
    const metricItems = [];
    if (b.khalid_score !== null && b.khalid_score !== undefined) metricItems.push(`KI <strong>${b.khalid_score}</strong>`);
    if (b.vix !== null && b.vix !== undefined) metricItems.push(`VIX <strong>${num(b.vix, 1)}</strong>`);
    if (b.carry_risk !== null && b.carry_risk !== undefined) metricItems.push(`Carry <strong>${b.carry_risk}</strong>`);
    if (b.ml_risk !== null && b.ml_risk !== undefined) metricItems.push(`ML <strong>${b.ml_risk}</strong>`);
    if (b.plumbing !== null && b.plumbing !== undefined) metricItems.push(`Plumb <strong>${b.plumbing}</strong>`);

    html += `<div class="brief-card">
      <div class="brief-card-hdr">
        <span class="brief-date">${dateStr}</span>
        <span class="brief-regime regime-${regime}">${regime}</span>
      </div>
      ${b.headline ? `<div class="brief-headline">${b.headline}</div>` : ""}
      ${b.headline_detail ? `<div class="brief-detail">${b.headline_detail}</div>` : ""}
      ${metricItems.length ? `<div class="brief-metrics">${metricItems.map(m => '<span class="brief-metric">' + m + '</span>').join("")}</div>` : ""}
      ${b.action_required ? `<div class="brief-action">⚡ ${b.action_required}</div>` : ""}
    </div>`;
  }
  html += `</div>`;
  return html;
}
'''

    # Inject the function before renderAll
    if "function renderMorningArchive" not in html:
        html = html.replace("function renderAll(D) {", js_func + "\nfunction renderAll(D) {")
        r.ok("  Added renderMorningArchive function")

    # Hook into renderAll: add Section 1 BEFORE Section 2
    old_section_2 = '''  let html = "";
  html += renderHeadline(D);
  html += `<div class="section-hdr">📋 Signal Scorecard <span class="badge">SECTION 2</span></div>`;'''

    new_section_1_2 = '''  let html = "";
  html += renderHeadline(D);
  html += `<div class="section-hdr">📰 Morning Brief Archive <span class="badge">SECTION 1 · LAST 30 DAYS</span></div>`;
  html += renderMorningArchive(D.morning_archive || []);
  html += `<div class="section-hdr">📋 Signal Scorecard <span class="badge">SECTION 2</span></div>`;'''

    if old_section_2 in html and "SECTION 1 · LAST 30 DAYS" not in html:
        html = html.replace(old_section_2, new_section_1_2)
        r.ok("  Hooked Section 1 into renderAll")
    elif "SECTION 1 · LAST 30 DAYS" in html:
        r.log("  Section 1 already hooked")

    html_path.write_text(html)
    r.ok(f"  Wrote reports.html ({len(html):,}B)")

    r.kv(
        morning_archive_entries=len(archive),
        reports_html_size=len(html),
    )
    r.log("Done")
