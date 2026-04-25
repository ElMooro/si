#!/usr/bin/env python3
"""
Step 85 — Fix edge-data threshold (1.2KB was actually healthy) +
build the HTML dashboard.

The investigation in step 84 revealed:
  - edge-data.json was 1.2KB AND had complete JSON with all fields
    (composite_score, engine_scores, options_flow, fund_flow, etc).
    So the small size wasn't a degraded writer — it was a normal run
    with fewer alerts/correlations content.
  - Lowering expected_size to 1000 (1KB) catches truly empty writes
    while accepting the natural variance of compact runs.

Then build health.html — a single self-contained HTML page that:
  - Fetches _health/dashboard.json from S3 every 30 seconds
  - Renders one tile per component, color-coded by status
  - Groups by category (S3 / Lambda / DynamoDB / SSM / EB)
  - Shows tooltip with note + reason + age + size on hover
  - Shows last-checked timestamp prominently at top
  - Uses CSS custom properties so dark/light works automatically
"""
import io
import json
import os
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


# Re-tune expected_size on edge-data based on observed reality
EDGE_FIX_OLD = '''        "expected_size": 5_000,  # Lowered from 10K — current writer produces ~1.2KB (degraded).
                                  # When edge-engine bug is fixed, raise back to 10_000.
                                  # Dashboard correctly flags this red as of 2026-04-25.'''
EDGE_FIX_NEW = '''        "expected_size": 1_000,  # Healthy output ranges ~1.2KB (quiet run, no alerts)
                                  # to ~11KB (active run). Threshold catches truly-empty writes.'''

EDGE_NOTE_OLD = '''        "note": "Composite ML risk score, regime. edge-engine every 6h. SEE: ~1.2KB output is degraded; investigate edge-engine.",'''
EDGE_NOTE_NEW = '''        "note": "Composite ML risk score, regime. edge-engine every 6h.",'''


HEALTH_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>JustHodl.AI — System Health</title>
<style>
  :root {
    --bg: #0f1419;
    --bg-card: #1a1f29;
    --bg-card-hover: #232936;
    --fg: #e8eaed;
    --fg-muted: #9aa0a6;
    --fg-dim: #5f6368;
    --border: #2c3038;
    --green: #34a853;
    --yellow: #fbbc04;
    --red: #ea4335;
    --blue: #4285f4;
    --gray: #5f6368;
  }
  @media (prefers-color-scheme: light) {
    :root {
      --bg: #ffffff;
      --bg-card: #f8f9fa;
      --bg-card-hover: #e8eaed;
      --fg: #202124;
      --fg-muted: #5f6368;
      --fg-dim: #80868b;
      --border: #dadce0;
    }
  }
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    margin: 0;
    padding: 20px;
    background: var(--bg);
    color: var(--fg);
    line-height: 1.5;
  }
  header {
    margin-bottom: 24px;
    padding-bottom: 16px;
    border-bottom: 1px solid var(--border);
  }
  h1 {
    margin: 0 0 8px 0;
    font-size: 28px;
    font-weight: 600;
  }
  .system-banner {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 6px 14px;
    border-radius: 16px;
    font-weight: 600;
    font-size: 14px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  .system-banner.green { background: rgba(52, 168, 83, 0.15); color: var(--green); }
  .system-banner.yellow { background: rgba(251, 188, 4, 0.15); color: var(--yellow); }
  .system-banner.red { background: rgba(234, 67, 53, 0.15); color: var(--red); }
  .system-banner.unknown { background: rgba(95, 99, 104, 0.15); color: var(--fg-muted); }
  .meta {
    margin-top: 8px;
    color: var(--fg-muted);
    font-size: 14px;
  }
  .summary-counts {
    display: flex;
    gap: 16px;
    margin: 20px 0;
    flex-wrap: wrap;
  }
  .count-pill {
    padding: 8px 16px;
    border-radius: 8px;
    background: var(--bg-card);
    font-size: 14px;
    border: 1px solid var(--border);
  }
  .count-pill .num {
    font-weight: 700;
    font-size: 18px;
    margin-right: 6px;
  }
  .count-pill.green .num { color: var(--green); }
  .count-pill.yellow .num { color: var(--yellow); }
  .count-pill.red .num { color: var(--red); }
  .count-pill.info .num { color: var(--blue); }
  .count-pill.unknown .num { color: var(--fg-muted); }
  section { margin-bottom: 32px; }
  section h2 {
    font-size: 16px;
    margin: 0 0 12px 0;
    color: var(--fg-muted);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    font-weight: 600;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
    gap: 12px;
  }
  .tile {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-left: 4px solid var(--gray);
    border-radius: 8px;
    padding: 12px 16px;
    transition: background 0.15s, transform 0.15s;
    cursor: default;
    position: relative;
  }
  .tile:hover {
    background: var(--bg-card-hover);
    transform: translateY(-1px);
  }
  .tile.green { border-left-color: var(--green); }
  .tile.yellow { border-left-color: var(--yellow); }
  .tile.red { border-left-color: var(--red); animation: pulse 2s infinite; }
  .tile.info { border-left-color: var(--blue); opacity: 0.85; }
  .tile.unknown { border-left-color: var(--gray); }
  @keyframes pulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(234, 67, 53, 0.4); }
    50% { box-shadow: 0 0 0 6px rgba(234, 67, 53, 0); }
  }
  .tile-id {
    font-family: ui-monospace, "SF Mono", Menlo, monospace;
    font-size: 13px;
    font-weight: 600;
    word-break: break-all;
  }
  .tile-stats {
    margin-top: 6px;
    font-size: 12px;
    color: var(--fg-muted);
  }
  .tile-stats span { margin-right: 12px; }
  .tile-reason {
    margin-top: 6px;
    font-size: 12px;
    color: var(--red);
    font-style: italic;
  }
  .tile-note {
    margin-top: 8px;
    font-size: 11px;
    color: var(--fg-dim);
    border-top: 1px dashed var(--border);
    padding-top: 6px;
    line-height: 1.4;
  }
  .severity-badge {
    display: inline-block;
    padding: 1px 6px;
    border-radius: 4px;
    font-size: 10px;
    margin-left: 4px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    font-weight: 600;
  }
  .severity-critical { background: rgba(234, 67, 53, 0.2); color: var(--red); }
  .severity-important { background: rgba(251, 188, 4, 0.2); color: var(--yellow); }
  .severity-nice_to_have { background: rgba(95, 99, 104, 0.2); color: var(--fg-muted); }
  footer {
    margin-top: 40px;
    padding-top: 16px;
    border-top: 1px solid var(--border);
    color: var(--fg-dim);
    font-size: 12px;
    text-align: center;
  }
  a { color: var(--blue); text-decoration: none; }
  a:hover { text-decoration: underline; }
  #refresh-status {
    display: inline-block;
    margin-left: 12px;
    color: var(--fg-dim);
    font-size: 12px;
  }
  #refresh-status.refreshing { color: var(--blue); }
  #refresh-status.error { color: var(--red); }
</style>
</head>
<body>
<header>
  <h1>System Health <span id="banner" class="system-banner unknown">Loading…</span></h1>
  <div class="meta">
    <span id="last-check">Loading…</span>
    <span id="refresh-status">●</span>
    <span style="margin-left: 8px;">— auto-refreshes every 30s</span>
  </div>
  <div class="summary-counts" id="counts"></div>
</header>

<main id="content">
  <p style="color: var(--fg-muted);">Loading dashboard data…</p>
</main>

<footer>
  Generated by <code>justhodl-health-monitor</code> Lambda · checks every 15 min ·
  Source: <code>s3://justhodl-dashboard-live/_health/dashboard.json</code>
</footer>

<script>
const DASHBOARD_URL = "https://justhodl-dashboard-live.s3.amazonaws.com/_health/dashboard.json";

const TYPE_GROUPS = [
  { name: "Critical S3 data files", types: ["s3_file"], filter: c => c.severity === "critical" },
  { name: "Important S3 data files", types: ["s3_file"], filter: c => c.severity === "important" },
  { name: "Other S3 files", types: ["s3_file"], filter: c => !["critical", "important"].includes(c.severity) },
  { name: "Lambdas", types: ["lambda"], filter: () => true },
  { name: "DynamoDB tables", types: ["dynamodb"], filter: () => true },
  { name: "SSM parameters", types: ["ssm"], filter: () => true },
  { name: "EventBridge rules", types: ["eb_rule"], filter: () => true },
];

function fmtAge(sec) {
  if (sec == null) return "—";
  if (sec < 60) return Math.round(sec) + "s";
  if (sec < 3600) return Math.round(sec / 60) + "m";
  if (sec < 86400) return (sec / 3600).toFixed(1) + "h";
  return (sec / 86400).toFixed(1) + "d";
}

function fmtBytes(b) {
  if (b == null) return "—";
  if (b < 1024) return b + "B";
  if (b < 1048576) return (b / 1024).toFixed(1) + "KB";
  return (b / 1048576).toFixed(1) + "MB";
}

function fmtRelative(iso) {
  const ts = new Date(iso);
  const sec = (Date.now() - ts) / 1000;
  return fmtAge(sec) + " ago";
}

function tileHTML(c) {
  const status = c.status || "unknown";
  const severity = c.severity || "important";
  const reason = c.reason || c.error || "";
  const note = c.note || "";

  let stats = [];
  if (c.age_sec !== undefined) stats.push(`age: ${fmtAge(c.age_sec)}`);
  if (c.size_bytes !== undefined) stats.push(`size: ${fmtBytes(c.size_bytes)}`);
  if (c.invocations_24h !== undefined) stats.push(`inv/24h: ${c.invocations_24h}`);
  if (c.errors_24h !== undefined) stats.push(`err/24h: ${c.errors_24h}`);
  if (c.error_rate_24h !== undefined) stats.push(`err: ${(c.error_rate_24h*100).toFixed(1)}%`);
  if (c.item_count !== undefined) stats.push(`items: ${c.item_count.toLocaleString()}`);
  if (c.state !== undefined) stats.push(`state: ${c.state}`);
  if (c.schedule !== undefined) stats.push(`schedule: ${c.schedule}`);

  return `
    <div class="tile ${status}">
      <div class="tile-id">${c.id}<span class="severity-badge severity-${severity}">${severity}</span></div>
      ${stats.length ? `<div class="tile-stats">${stats.map(s => `<span>${s}</span>`).join("")}</div>` : ""}
      ${reason ? `<div class="tile-reason">${reason}</div>` : ""}
      ${note ? `<div class="tile-note">${note}</div>` : ""}
    </div>
  `;
}

async function refresh() {
  const status = document.getElementById("refresh-status");
  status.className = "refreshing";
  status.textContent = "↻";
  try {
    const r = await fetch(DASHBOARD_URL + "?_=" + Date.now());
    if (!r.ok) throw new Error("HTTP " + r.status);
    const dash = await r.json();
    render(dash);
    status.className = "";
    status.textContent = "●";
  } catch (e) {
    console.error(e);
    status.className = "error";
    status.textContent = "✕ " + e.message;
  }
}

function render(dash) {
  // Banner
  const banner = document.getElementById("banner");
  banner.className = "system-banner " + (dash.system_status || "unknown");
  banner.textContent = (dash.system_status || "unknown").toUpperCase();

  // Last check
  document.getElementById("last-check").textContent = "Last check: " + fmtRelative(dash.generated_at);

  // Counts
  const counts = dash.counts || {};
  const countsHTML = ["green", "yellow", "red", "info", "unknown"]
    .map(k => `<div class="count-pill ${k}"><span class="num">${counts[k] || 0}</span> ${k}</div>`)
    .join("");
  document.getElementById("counts").innerHTML = countsHTML;

  // Sections
  const components = dash.components || [];
  const sections = TYPE_GROUPS
    .map(group => {
      const filtered = components.filter(c =>
        group.types.includes(c.type) && group.filter(c)
      );
      if (filtered.length === 0) return "";
      return `
        <section>
          <h2>${group.name} (${filtered.length})</h2>
          <div class="grid">${filtered.map(tileHTML).join("")}</div>
        </section>
      `;
    })
    .join("");

  document.getElementById("content").innerHTML = sections || "<p>No components reported.</p>";
}

refresh();
setInterval(refresh, 30_000);
</script>
</body>
</html>
'''


with report("build_health_dashboard") as r:
    r.heading("Step 85 — Fix edge threshold + build health.html")

    # ─── 1. Re-tune edge-data threshold ───
    r.section("1. Re-tune edge-data threshold based on observed reality")
    exp_path = REPO_ROOT / "aws/ops/health/expectations.py"
    src = exp_path.read_text()

    if EDGE_FIX_OLD in src:
        src = src.replace(EDGE_FIX_OLD, EDGE_FIX_NEW, 1)
        r.ok("  Lowered edge-data expected_size 5K→1K (compact runs are normal)")
    else:
        r.warn("  Pattern not found; manual review")

    if EDGE_NOTE_OLD in src:
        src = src.replace(EDGE_NOTE_OLD, EDGE_NOTE_NEW, 1)
        r.ok("  Cleaned up edge-data note")

    exp_path.write_text(src)

    # Re-deploy monitor
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-health-monitor/source"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
        zout.write(exp_path, "expectations.py")
    zbytes = buf.getvalue()

    lam.update_function_code(FunctionName="justhodl-health-monitor", ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-health-monitor", WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed monitor: {len(zbytes)} bytes")

    # Sync invoke
    resp = lam.invoke(FunctionName="justhodl-health-monitor", InvocationType="RequestResponse")
    r.log(f"  Re-invoke status: {resp.get('StatusCode')}")

    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="_health/dashboard.json")
    dash = json.loads(obj["Body"].read())
    r.log(f"  System: {dash.get('system_status')}")
    r.log(f"  Counts: {dash.get('counts')}")

    # ─── 2. Upload health.html to S3 ───
    r.section("2. Upload health.html dashboard")
    s3.put_object(
        Bucket="justhodl-dashboard-live",
        Key="health.html",
        Body=HEALTH_HTML.encode(),
        ContentType="text/html; charset=utf-8",
        CacheControl="max-age=60",
    )
    r.ok(f"  Uploaded health.html ({len(HEALTH_HTML)} bytes)")

    # Verify it's publicly readable (root-level files need to be allowed)
    r.section("3. Verify health.html is in public-read bucket policy")
    try:
        pol_obj = s3.get_bucket_policy(Bucket="justhodl-dashboard-live")
        policy = json.loads(pol_obj["Policy"])
        statements = policy.get("Statement", [])
        # Find PublicReadRootDashboardFiles statement
        root_stmt = None
        for st in statements:
            if st.get("Sid") == "PublicReadRootDashboardFiles":
                root_stmt = st
                break

        if root_stmt:
            resources = root_stmt.get("Resource", [])
            if isinstance(resources, str):
                resources = [resources]
            health_arn = "arn:aws:s3:::justhodl-dashboard-live/health.html"
            if health_arn in resources:
                r.ok(f"  health.html already public-readable via PublicReadRootDashboardFiles")
            else:
                r.log(f"  Adding health.html to PublicReadRootDashboardFiles statement")
                resources.append(health_arn)
                # Add health/dashboard.json key for the JS fetch
                dash_arn = "arn:aws:s3:::justhodl-dashboard-live/_health/*"
                if dash_arn not in resources:
                    resources.append(dash_arn)
                root_stmt["Resource"] = resources
                # Also add a separate _health/* prefix statement
                # But actually the simpler route: just add /_health/ to our existing policy
                s3.put_bucket_policy(Bucket="justhodl-dashboard-live", Policy=json.dumps(policy))
                r.ok(f"  Updated bucket policy")
    except Exception as e:
        r.warn(f"  Policy update: {e}")

    r.kv(
        dashboard_url=f"https://justhodl-dashboard-live.s3-website-us-east-1.amazonaws.com/health.html",
        api_url=f"https://justhodl-dashboard-live.s3.amazonaws.com/health.html",
        next_step="step 86 wires Telegram alerting + EB schedule",
    )
    r.log("Done")
