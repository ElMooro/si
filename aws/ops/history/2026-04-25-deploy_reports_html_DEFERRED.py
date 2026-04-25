#!/usr/bin/env python3
"""
Step 110 — Fix HTML deployment.

Problem found from steps 107 + 109:
  The CI workflow's auto-commit step only stages files under
  aws/ops/reports/, aws/ops/audit/, and aws/lambdas/. Root-level
  HTML files written by ops scripts are silently dropped.

This means reports.html (written by step 107 to repo root) was
never committed back to git, so justhodl.ai still doesn't have it
and step 109's HTML patch failed because the file wasn't there
to read.

Fix: this script does THREE things:
  1. Rewrites reports.html (with the step 109 chart x-axis fix
     pre-applied — labels use ts not date).
  2. Replaces Reports.html stub with a clean redirect.
  3. Uses subprocess to explicitly `git add` + `commit` + `push`
     the two HTML files. This bypasses the workflow's narrow
     auto-commit scope.
  4. Patches .github/workflows/run-ops.yml to include root *.html
     in the auto-commit going forward (so future ops scripts can
     write root HTML without needing this workaround).

After this runs, justhodl.ai/reports.html should serve the live
Section 2 + 3 dashboard within a couple minutes (GitHub Pages
build delay).
"""
import os
import subprocess
from pathlib import Path

from ops_report import report

REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))


REPORTS_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>JustHodl.AI | Reports & Performance</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600;700&family=IBM+Plex+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
:root{--bg:#050810;--bg1:#0a0f1a;--bg2:#0f1524;--bg3:#141c2e;--brd:#1a2540;--brd2:#243050;--t1:#e8ecf4;--t2:#8b95ad;--t3:#5a6580;--t4:#3a4560;--green:#00e676;--green-bg:rgba(0,230,118,.06);--red:#ff1744;--red-bg:rgba(255,23,68,.06);--yellow:#ffc400;--yellow-bg:rgba(255,196,0,.06);--blue:#2979ff;--blue-bg:rgba(41,121,255,.06);--cyan:#00e5ff;--orange:#ff6d00;--orange-bg:rgba(255,109,0,.06);--purple:#7c4dff}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--t1);font-family:'IBM Plex Sans',system-ui,sans-serif;min-height:100vh}
.mono{font-family:'IBM Plex Mono',monospace}

.header{background:linear-gradient(180deg,rgba(10,15,30,.98),var(--bg));border-bottom:1px solid var(--brd);padding:12px 24px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:200;backdrop-filter:blur(24px)}
.logo{font-size:20px;font-weight:700;background:linear-gradient(135deg,var(--yellow),var(--orange));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.logo-sub{font-size:10px;color:var(--t3);text-transform:uppercase;letter-spacing:3px}
.hdr-right{display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.nav-link{color:var(--cyan);text-decoration:none;font-size:11px;padding:5px 12px;border:1px solid var(--brd2);border-radius:6px;transition:all .2s;font-weight:500}
.nav-link:hover{background:rgba(0,229,255,.06);border-color:var(--cyan)}
.live{display:flex;align-items:center;gap:5px;font-size:12px;color:var(--t2)}
.dot{width:7px;height:7px;border-radius:50%;background:var(--green);animation:pulse 2s infinite;box-shadow:0 0 8px var(--green)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}

.container{max-width:1280px;margin:0 auto;padding:20px 24px}
.section-hdr{font-size:12px;font-weight:600;color:var(--t2);text-transform:uppercase;letter-spacing:1.5px;margin:28px 0 14px;padding-bottom:10px;border-bottom:1px solid var(--brd);display:flex;align-items:center;gap:8px}
.section-hdr:first-child{margin-top:0}
.section-hdr .badge{font-size:9px;background:var(--bg2);color:var(--t3);padding:3px 8px;border-radius:4px;border:1px solid var(--brd);font-weight:500;letter-spacing:.5px}

.headline-box{padding:24px 30px;border-radius:14px;margin-bottom:24px;border:1px solid var(--brd2);background:linear-gradient(135deg,var(--bg1),var(--bg2))}
.headline-box .meta{font-size:11px;color:var(--t4);font-family:'IBM Plex Mono',monospace;letter-spacing:2px;margin-bottom:6px}
.headline-box h1{font-size:22px;font-weight:700;margin-bottom:8px;letter-spacing:.5px}
.headline-box .detail{font-size:13px;line-height:1.6;color:var(--t2)}

.stats-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin:16px 0 24px}
.stat-card{padding:14px 16px;border-radius:10px;background:var(--bg2);border:1px solid var(--brd)}
.stat-card .num{font-size:24px;font-weight:700;font-family:'IBM Plex Mono',monospace;line-height:1.1}
.stat-card .lbl{font-size:9px;color:var(--t3);text-transform:uppercase;letter-spacing:1.2px;margin-top:6px}
.stat-card .sub{font-size:10px;color:var(--t4);margin-top:3px;font-family:'IBM Plex Mono',monospace}

.mtable{width:100%;border-collapse:collapse;font-size:13px;border:1px solid var(--brd2);border-radius:10px;overflow:hidden;background:var(--bg2)}
.mtable thead{background:var(--bg1)}
.mtable th{text-align:left;padding:11px 14px;color:var(--t3);font-weight:600;text-transform:uppercase;letter-spacing:.8px;font-size:10px;border-bottom:2px solid var(--brd2);cursor:pointer;user-select:none}
.mtable th:hover{color:var(--cyan)}
.mtable th .arr{font-size:8px;margin-left:4px;color:var(--t4)}
.mtable th.sorted .arr{color:var(--cyan)}
.mtable td{padding:10px 14px;border-bottom:1px solid var(--brd);font-family:'IBM Plex Mono',monospace}
.mtable tr:hover td{background:rgba(255,255,255,.015)}
.mtable tr:last-child td{border-bottom:none}
.mtable .signal-name{color:var(--t1);font-weight:500;font-family:'IBM Plex Sans',sans-serif}
.mtable .num-cell{text-align:right;font-variant-numeric:tabular-nums}
.mtable .num-cell.center{text-align:center}
.hr-pill{display:inline-block;padding:3px 9px;border-radius:11px;font-size:11px;font-weight:600;font-family:'IBM Plex Mono',monospace;letter-spacing:.3px}
.hr-good{background:var(--green-bg);color:var(--green);border:1px solid rgba(0,230,118,.2)}
.hr-mid{background:var(--yellow-bg);color:var(--yellow);border:1px solid rgba(255,196,0,.2)}
.hr-bad{background:var(--red-bg);color:var(--red);border:1px solid rgba(255,23,68,.2)}
.hr-na{background:var(--bg);color:var(--t4);border:1px solid var(--brd)}
.trend-up{color:var(--green);font-size:10px}
.trend-down{color:var(--red);font-size:10px}
.trend-flat{color:var(--t3);font-size:10px}
.tiny{color:var(--t4);font-size:10px;font-weight:400}

.chart-wrap{padding:20px 22px;border-radius:10px;background:var(--bg2);border:1px solid var(--brd);margin-bottom:24px;position:relative;height:380px}
.chart-wrap canvas{max-height:340px}
.chart-empty{display:flex;align-items:center;justify-content:center;height:100%;flex-direction:column;color:var(--t3)}

.footer{margin-top:32px;padding:14px 18px;background:var(--bg1);border-radius:8px;border:1px solid var(--brd);font-size:11px;color:var(--t4);font-family:'IBM Plex Mono',monospace;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px}

.loading{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:80px 0;color:var(--t3)}
.spinner{width:36px;height:36px;border:3px solid var(--brd2);border-top-color:var(--yellow);border-radius:50%;animation:spin .8s linear infinite;margin-bottom:14px}
@keyframes spin{to{transform:rotate(360deg)}}
.empty-state{padding:48px 32px;text-align:center;background:var(--bg2);border:1px dashed var(--brd2);border-radius:10px;color:var(--t3)}
.empty-state h3{color:var(--t2);font-size:14px;margin-bottom:8px}
.empty-state p{font-size:12px;line-height:1.6;max-width:520px;margin:0 auto}

@media(max-width:700px){.container{padding:14px 12px}.mtable{font-size:11px}.mtable th{padding:8px 6px;font-size:9px}.mtable td{padding:8px 6px}.headline-box{padding:18px 16px}.headline-box h1{font-size:18px}}
</style>
</head>
<body>
<div class="header">
  <div>
    <div class="logo">📊 REPORTS &amp; PERFORMANCE</div>
    <div class="logo-sub">JustHodl.AI — Signal Scorecard &amp; Khalid Timeline</div>
  </div>
  <div class="hdr-right">
    <a href="/" class="nav-link">🏠 Terminal</a>
    <a href="/intelligence.html" class="nav-link">🧠 Intelligence</a>
    <a href="/trading-signals.html" class="nav-link">📡 Signals</a>
    <a href="/health.html" class="nav-link">❤️ Health</a>
    <div class="live"><div class="dot"></div><span>LIVE</span></div>
    <div class="mono" style="font-size:11px;color:var(--t3)" id="ts">…</div>
  </div>
</div>
<div class="container" id="main">
  <div class="loading">
    <div class="spinner"></div>
    <div style="font-size:14px">Loading reports data…</div>
    <div style="font-size:12px;color:var(--t4);margin-top:6px">Reading signals + outcomes + calibration</div>
  </div>
</div>

<script>
const URL = "https://justhodl-dashboard-live.s3.amazonaws.com/reports/scorecard.json";
const REFRESH_MS = 5 * 60 * 1000;
let chartInstance = null;
let lastData = null;
let sortBy = "total";
let sortDir = "desc";

function pct(v, digits = 1) {
  if (v === null || v === undefined) return "—";
  return (v * 100).toFixed(digits) + "%";
}
function num(v, digits = 0) {
  if (v === null || v === undefined) return "—";
  return Number(v).toLocaleString(undefined, {minimumFractionDigits: digits, maximumFractionDigits: digits});
}
function hrClass(v) {
  if (v === null || v === undefined) return "hr-na";
  if (v >= 0.60) return "hr-good";
  if (v >= 0.45) return "hr-mid";
  return "hr-bad";
}
function trendArrow(now, was) {
  if (now === null || was === null || now === undefined || was === undefined) return '<span class="trend-flat">—</span>';
  const d = now - was;
  if (Math.abs(d) < 0.02) return '<span class="trend-flat">→</span>';
  if (d > 0) return '<span class="trend-up">↑ ' + pct(d, 1) + '</span>';
  return '<span class="trend-down">↓ ' + pct(-d, 1) + '</span>';
}
function regimeColor(r) {
  if (!r) return "#5a6580";
  const R = String(r).toUpperCase();
  if (R.includes("CRISIS")) return "#ff1744";
  if (R.includes("CAUTIOUS")) return "#ff6d00";
  if (R.includes("NEUTRAL")) return "#ffc400";
  if (R.includes("OPTIMISTIC")) return "#00e5ff";
  if (R.includes("EUPHORIA") || R.includes("BULLISH")) return "#00e676";
  if (R.includes("BEAR")) return "#ff1744";
  return "#5a6580";
}

function renderHeadline(D) {
  const m = D.meta || {};
  const totalSignals = m.signals_total || 0;
  const totalOutcomes = m.outcomes_total || 0;
  const scored = m.scored_outcomes || 0;
  const hasCalib = m.has_calibration;
  return `
    <div class="headline-box">
      <div class="meta">PERFORMANCE REPORT — REAL DATA</div>
      <h1>System Performance &amp; Self-Calibration</h1>
      <div class="detail">
        Tracking ${num(totalSignals)} predictions across all signal types. ${num(scored)} of ${num(totalOutcomes)} outcomes have been scored against actual market moves.
        ${hasCalib ? "Calibration weights are <strong style='color:var(--green)'>active</strong>." :
          "<span style='color:var(--yellow)'>Calibrator hasn't run yet — first scheduled run is Sunday 09:00 UTC. Until then, scorecard values reflect raw outcomes only.</span>"}
      </div>
    </div>
    <div class="stats-row">
      <div class="stat-card"><div class="num">${num(totalSignals)}</div><div class="lbl">Total Signals Logged</div><div class="sub">all time</div></div>
      <div class="stat-card"><div class="num">${num(totalOutcomes)}</div><div class="lbl">Outcomes Tracked</div><div class="sub">awaiting score: ${num(totalOutcomes - scored)}</div></div>
      <div class="stat-card"><div class="num" style="color:${scored > 0 ? 'var(--green)' : 'var(--yellow)'}">${num(scored)}</div><div class="lbl">Scored</div><div class="sub">${pct(totalOutcomes ? scored / totalOutcomes : 0, 0)} of total</div></div>
      <div class="stat-card"><div class="num">${(D.signal_scorecard || []).length}</div><div class="lbl">Signal Types</div><div class="sub">distinct categories</div></div>
    </div>
  `;
}

function renderScorecard(rows) {
  if (!rows || rows.length === 0) {
    return `<div class="empty-state"><h3>No signals to score yet</h3><p>The signal logger is collecting data. Once outcomes are scored (next outcome-checker run is Sun 22:30 UTC), per-signal hit rates will appear here.</p></div>`;
  }
  const sorted = [...rows].sort((a, b) => {
    const av = a[sortBy] ?? -Infinity;
    const bv = b[sortBy] ?? -Infinity;
    if (av === bv) return 0;
    return (sortDir === "desc" ? bv - av : av - bv);
  });
  const cols = [
    {key: "signal_type", label: "Signal", numeric: false},
    {key: "total", label: "n", numeric: true},
    {key: "hit_rate", label: "Hit rate", numeric: true},
    {key: "avg_magnitude_error_pct", label: "Mag err %", numeric: true},
    {key: "trend_30d", label: "30d", numeric: true},
    {key: "trend_60d", label: "60d", numeric: true},
    {key: "trend_90d", label: "90d", numeric: true},
    {key: "calibrator_weight", label: "Weight", numeric: true},
  ];
  let html = `<table class="mtable" id="scorecard"><thead><tr>`;
  for (const c of cols) {
    const sortedClass = c.key === sortBy ? "sorted" : "";
    const arrow = c.key === sortBy ? (sortDir === "desc" ? "▼" : "▲") : "";
    html += `<th class="${sortedClass}" data-sort="${c.key}">${c.label} <span class="arr">${arrow}</span></th>`;
  }
  html += `</tr></thead><tbody>`;
  for (const r of sorted) {
    html += `<tr>
      <td class="signal-name">${r.signal_type}</td>
      <td class="num-cell center">${num(r.total)}<span class="tiny"> (${num(r.correct)}✓)</span></td>
      <td class="num-cell"><span class="hr-pill ${hrClass(r.hit_rate)}">${pct(r.hit_rate, 1)}</span></td>
      <td class="num-cell">${r.avg_magnitude_error_pct === null ? "—" : num(r.avg_magnitude_error_pct, 2) + "%"}</td>
      <td class="num-cell">${pct(r.trend_30d, 0)} ${trendArrow(r.trend_30d, r.hit_rate)}</td>
      <td class="num-cell">${pct(r.trend_60d, 0)} ${trendArrow(r.trend_60d, r.hit_rate)}</td>
      <td class="num-cell">${pct(r.trend_90d, 0)} ${trendArrow(r.trend_90d, r.hit_rate)}</td>
      <td class="num-cell">${r.calibrator_weight === undefined ? "—" : num(r.calibrator_weight, 3)}</td>
    </tr>`;
  }
  html += `</tbody></table>`;
  return html;
}

function attachSortHandlers() {
  const ths = document.querySelectorAll("#scorecard th");
  ths.forEach(th => {
    th.addEventListener("click", () => {
      const key = th.dataset.sort;
      if (key === sortBy) sortDir = (sortDir === "desc" ? "asc" : "desc");
      else { sortBy = key; sortDir = "desc"; }
      renderAll(lastData);
    });
  });
}

function renderTimeline(timeline) {
  const wrap = document.getElementById("ki-chart-wrap");
  if (!wrap) return;
  if (!timeline || timeline.length < 2) {
    wrap.innerHTML = `<div class="chart-empty">
      <div style="font-size:36px;margin-bottom:10px">📈</div>
      <div style="font-size:13px;color:var(--t2);margin-bottom:6px">Not enough Khalid Index history yet</div>
      <div style="font-size:11px;color:var(--t4)">Need at least 2 logged data points (current: ${(timeline || []).length}). The signal-logger fires every 6h.</div>
    </div>`;
    return;
  }
  if (chartInstance) { chartInstance.destroy(); chartInstance = null; }
  wrap.innerHTML = `<canvas id="ki-chart"></canvas>`;

  // Use formatted intra-day timestamps for x-axis labels (e.g. "04-25 14:30")
  const labels = timeline.map(p => {
    const d = new Date(p.ts);
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    const hh = String(d.getUTCHours()).padStart(2, '0');
    const mn = String(d.getUTCMinutes()).padStart(2, '0');
    return `${mm}-${dd} ${hh}:${mn}`;
  });
  const data = timeline.map(p => p.score);
  const colors = timeline.map(p => regimeColor(p.regime));

  const ctx = document.getElementById("ki-chart").getContext("2d");
  chartInstance = new Chart(ctx, {
    type: "line",
    data: {
      labels: labels,
      datasets: [{
        label: "Khalid Index",
        data: data,
        borderColor: "#00e5ff",
        backgroundColor: "rgba(0,229,255,.08)",
        borderWidth: 2,
        fill: true,
        tension: 0.25,
        pointRadius: 3,
        pointHoverRadius: 7,
        pointBackgroundColor: colors,
        pointBorderColor: colors,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {mode: "index", intersect: false},
      plugins: {
        legend: {display: false},
        tooltip: {
          backgroundColor: "#0a0f1a",
          borderColor: "#1a2540",
          borderWidth: 1,
          titleColor: "#e8ecf4",
          bodyColor: "#8b95ad",
          padding: 12,
          callbacks: {
            label: function(ctx) {
              const p = timeline[ctx.dataIndex];
              return `${p.score.toFixed(1)} — ${p.regime || "?"}`;
            }
          }
        }
      },
      scales: {
        x: {grid: {color: "rgba(26,37,64,.4)"}, ticks: {color: "#5a6580", font: {family: "IBM Plex Mono", size: 10}, maxTicksLimit: 12}},
        y: {grid: {color: "rgba(26,37,64,.4)"}, ticks: {color: "#5a6580", font: {family: "IBM Plex Mono", size: 11}}, min: 0, max: 100}
      }
    }
  });
}

function renderAll(D) {
  lastData = D;
  const main = document.getElementById("main");
  const ts = D.meta && D.meta.generated_at ? new Date(D.meta.generated_at).toUTCString() : "";
  document.getElementById("ts").textContent = ts.replace("GMT", "UTC");

  let html = "";
  html += renderHeadline(D);
  html += `<div class="section-hdr">📋 Signal Scorecard <span class="badge">SECTION 2</span></div>`;
  html += renderScorecard(D.signal_scorecard);
  html += `<div class="section-hdr">📈 Khalid Index Timeline <span class="badge">SECTION 3 · LAST 90 DAYS</span></div>`;
  html += `<div class="chart-wrap" id="ki-chart-wrap"></div>`;
  html += `<div class="footer">
    <span>Source: <a href="${URL}" style="color:var(--cyan);text-decoration:none" target="_blank">reports/scorecard.json</a></span>
    <span>Auto-refresh every 5 min · Generated ${ts || "?"}</span>
  </div>`;

  main.innerHTML = html;
  renderTimeline(D.khalid_timeline || []);
  attachSortHandlers();
}

async function loadData() {
  try {
    const r = await fetch(URL + "?t=" + Date.now());
    if (!r.ok) throw new Error("HTTP " + r.status);
    const D = await r.json();
    renderAll(D);
  } catch (e) {
    document.getElementById("main").innerHTML = `<div class="empty-state">
      <h3>⚠️ Couldn't load reports data</h3>
      <p>${e.message || e}</p>
      <p style="margin-top:12px"><a href="${URL}" style="color:var(--cyan)">Try the source JSON directly</a></p>
    </div>`;
  }
}

loadData();
setInterval(loadData, REFRESH_MS);
</script>
</body>
</html>
"""

REPORTS_STUB = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="0; url=/reports.html">
<title>Reports — Redirecting…</title>
</head><body>
<p>Redirecting to <a href="/reports.html">/reports.html</a></p>
</body></html>
"""


def run(cmd, **kwargs):
    """Run a shell command; return CompletedProcess."""
    return subprocess.run(cmd, capture_output=True, text=True, **kwargs)


with report("deploy_reports_html") as r:
    r.heading("Deploy reports.html via explicit git push (workflow auto-commit doesn't cover root *.html)")

    # ─── 1. Write the two HTML files ───────────────────────────────────
    r.section("1. Write reports.html + Reports.html")
    rh = REPO_ROOT / "reports.html"
    rh.write_text(REPORTS_HTML)
    r.ok(f"  reports.html: {len(REPORTS_HTML):,}B")

    rs = REPO_ROOT / "Reports.html"
    rs.write_text(REPORTS_STUB)
    r.ok(f"  Reports.html stub: {len(REPORTS_STUB):,}B (redirect to /reports.html)")

    # ─── 2. Patch run-ops.yml to include root *.html in auto-commit ────
    r.section("2. Patch workflow to include root *.html in future auto-commits")
    workflow_path = REPO_ROOT / ".github/workflows/run-ops.yml"
    workflow = workflow_path.read_text()
    old_check = """if [ -n \"$(git status --porcelain aws/ops/reports/ aws/ops/audit/ aws/lambdas/ 2>/dev/null)\" ]; then
            git add aws/ops/reports/ aws/ops/audit/ aws/lambdas/"""
    new_check = """if [ -n \"$(git status --porcelain aws/ops/reports/ aws/ops/audit/ aws/lambdas/ ./*.html cloudflare/ 2>/dev/null)\" ]; then
            git add aws/ops/reports/ aws/ops/audit/ aws/lambdas/ ./*.html cloudflare/ 2>/dev/null || git add aws/ops/reports/ aws/ops/audit/ aws/lambdas/"""
    if old_check in workflow:
        workflow = workflow.replace(old_check, new_check)
        workflow_path.write_text(workflow)
        r.ok("  Workflow patched — future runs will include root *.html + cloudflare/")
    else:
        r.warn("  Couldn't find expected workflow block — already patched? Skipping.")

    # ─── 3. Use git directly to commit + push these files ──────────────
    r.section("3. git add + commit + push HTML files")
    # Configure git identity (workflow already does this but be defensive)
    run(["git", "config", "user.name", "github-actions[bot]"], cwd=REPO_ROOT)
    run(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], cwd=REPO_ROOT)

    # Pull latest first to avoid races
    pr = run(["git", "pull", "--rebase", "origin", "main"], cwd=REPO_ROOT)
    r.log(f"  git pull: {pr.returncode}")
    if pr.stderr.strip():
        r.log(f"  pull stderr: {pr.stderr.strip()[:200]}")

    # Stage the HTML files + workflow patch
    add = run(["git", "add", "reports.html", "Reports.html",
               ".github/workflows/run-ops.yml"], cwd=REPO_ROOT)
    r.log(f"  git add: {add.returncode}")
    if add.stderr.strip():
        r.log(f"  add stderr: {add.stderr.strip()[:200]}")

    # Check if anything's actually staged
    diff = run(["git", "diff", "--cached", "--name-only"], cwd=REPO_ROOT)
    staged = diff.stdout.strip().split("\n") if diff.stdout.strip() else []
    r.log(f"  Staged files: {staged}")

    if staged:
        commit = run(["git", "commit", "-m",
                      "deploy: reports.html + Reports.html stub + workflow patch [skip-deploy]\n\n"
                      "- reports.html: Section 2 (Signal Scorecard) + Section 3 (Khalid Timeline)\n"
                      "  Reads s3://justhodl-dashboard-live/reports/scorecard.json\n"
                      "  Auto-refresh every 5 min\n"
                      "- Reports.html: redirect stub to /reports.html (was broken loop)\n"
                      "- run-ops.yml: include root *.html + cloudflare/ in auto-commit"],
                     cwd=REPO_ROOT)
        if commit.returncode == 0:
            r.ok("  Commit succeeded")
        else:
            r.fail(f"  Commit failed: rc={commit.returncode}")
            r.log(f"  stdout: {commit.stdout[:300]}")
            r.log(f"  stderr: {commit.stderr[:300]}")
            raise SystemExit(1)

        # Push
        push = run(["git", "push", "origin", "main"], cwd=REPO_ROOT)
        if push.returncode == 0:
            r.ok("  Push succeeded — files now live in GitHub repo")
            r.log("  GitHub Pages will rebuild within ~1-2 min")
            r.log("  Then justhodl.ai/reports.html will be live")
        else:
            r.fail(f"  Push failed: rc={push.returncode}")
            r.log(f"  stdout: {push.stdout[:300]}")
            r.log(f"  stderr: {push.stderr[:300]}")
            raise SystemExit(1)
    else:
        r.log("  No files actually changed (already deployed). Nothing to do.")

    # ─── 4. Verify scorecard.json is still fresh ───────────────────────
    r.section("4. Verify scorecard.json freshness")
    import boto3
    from datetime import datetime, timezone
    s3 = boto3.client("s3", region_name="us-east-1")
    head = s3.head_object(Bucket="justhodl-dashboard-live", Key="reports/scorecard.json")
    age_min = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds() / 60
    r.log(f"  scorecard.json: {head['ContentLength']:,}B, modified {age_min:.1f} min ago")
    if age_min > 70:
        r.warn("  scorecard.json is older than 70 min — EB rule may have stalled")

    r.kv(
        reports_html_size=len(REPORTS_HTML),
        committed_files=len(staged),
        live_url="https://justhodl.ai/reports.html",
    )
    r.log("Done")
