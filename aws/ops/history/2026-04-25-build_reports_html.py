#!/usr/bin/env python3
"""
Step 107 — Fix Khalid timeline + build reports.html (Sections 2 + 3).

Two changes:

A. Patch justhodl-reports-builder Lambda:
   The original khalid timeline relied on khalid_score_at_log on every
   signal — that field is only populated on signals after the Week 2A
   schema migration (2026-04-25), so we got only 2 timeline points.
   The right approach: scan signals where signal_type='khalid_index',
   use signal_value (the actual Khalid score) and metadata.regime
   (the regime label).

B. Build reports.html (frontend):
   - Matches intelligence.html's IBM Plex aesthetic (dark, monospace
     for numbers, IBM Plex Sans/Mono fonts)
   - Section 2: Signal Performance Scorecard table
     - Per-signal: hit_rate (color-coded), n, magnitude_error,
       trend_30/60/90d, calibrator weight & accuracy
     - Sortable columns
     - Empty state if no scored outcomes yet
   - Section 3: Khalid Index Timeline chart
     - Line chart with regime-band shading
     - Last 90 days
     - Uses Chart.js (already used elsewhere in the system)
   - Auto-refresh every 5 minutes
   - Reads s3://justhodl-dashboard-live/reports/scorecard.json

Also overwrites the broken Reports.html stub at the root (which was
a redirect loop pointing to itself) with a redirect to reports.html
(lowercase) for backward compat.
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("build_reports_html") as r:
    r.heading("Patch reports-builder + build reports.html (Sections 2 + 3)")

    # ─── A. Patch the Lambda's compute_khalid_timeline ──────────────────
    r.section("A. Patch Lambda — use signal_type='khalid_index' for timeline")

    src_path = REPO_ROOT / "aws/lambdas/justhodl-reports-builder/source/lambda_function.py"
    src = src_path.read_text()

    new_func = '''def compute_khalid_timeline(signals):
    """Extract Khalid Index timeline from logged signals.

    Two strategies (in priority order):
      1. Use signals where signal_type == 'khalid_index' — these are
         the dedicated Khalid Index logs (signal_value is the score,
         metadata.regime is the regime label).
      2. Fall back to khalid_score_at_log on signals with that field
         populated (Week 2A schema v2).
    """
    points = []

    # Strategy 1: dedicated khalid_index signals
    for s in signals:
        if s.get("signal_type") != "khalid_index":
            continue
        ts = s.get("logged_at")
        sv = s.get("signal_value")
        if sv is None or not ts:
            continue
        try:
            score = float(sv) if not isinstance(sv, str) else float(str(sv).replace("%", "").strip())
        except Exception:
            continue
        regime = (s.get("metadata") or {}).get("regime") or s.get("regime")
        dt = parse_iso(ts)
        if not dt:
            continue
        points.append({
            "date": dt.date().isoformat(),
            "ts": dt.isoformat(),
            "score": score,
            "regime": regime,
        })

    # Strategy 2: fallback to khalid_score_at_log on any signal
    if len(points) < 5:
        for s in signals:
            score = s.get("khalid_score_at_log")
            ts = s.get("logged_at")
            if score is None or not ts:
                continue
            try:
                score_f = float(score)
            except Exception:
                continue
            regime = s.get("regime_at_log")
            dt = parse_iso(ts)
            if not dt:
                continue
            points.append({
                "date": dt.date().isoformat(),
                "ts": dt.isoformat(),
                "score": score_f,
                "regime": regime,
            })

    # Group by date, take first reading of each day
    by_date = OrderedDict()
    for p in sorted(points, key=lambda x: x["ts"]):
        if p["date"] not in by_date:
            by_date[p["date"]] = p

    timeline = list(by_date.values())
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat()
    return [p for p in timeline if p["date"] >= cutoff]'''

    # Replace the existing compute_khalid_timeline function. Match from
    # `def compute_khalid_timeline(signals):` through to the next `def `
    # at column 0.
    import re
    pattern = re.compile(
        r"def compute_khalid_timeline\(signals\):.*?(?=\ndef |\Z)",
        re.DOTALL,
    )
    if not pattern.search(src):
        r.fail("  Couldn't locate compute_khalid_timeline in source")
        raise SystemExit(1)
    src_new = pattern.sub(new_func + "\n\n", src)
    if src_new == src:
        r.warn("  No change made (regex matched but produced same output)")
    src_path.write_text(src_new)
    r.ok(f"  Patched {src_path.name}")

    # Validate syntax
    import ast
    try:
        ast.parse(src_new)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax error after patch: {e}")
        raise SystemExit(1)

    # Re-deploy
    name = "justhodl-reports-builder"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, src_new)
    zbytes = buf.getvalue()
    lam.update_function_code(FunctionName=name, ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed Lambda ({len(zbytes)}B)")

    # Re-invoke + verify timeline now has more points
    time.sleep(3)
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    if resp.get("FunctionError"):
        payload = resp.get("Payload").read().decode()
        r.fail(f"  invoke FunctionError: {payload[:500]}")
    else:
        body_str = resp.get("Payload").read().decode()
        body = json.loads(json.loads(body_str).get("body", "{}"))
        r.ok(f"  Invoked: timeline_points={body.get('timeline_points')} "
             f"scorecard_rows={body.get('scorecard_rows')}")

    # ─── B. Build reports.html ──────────────────────────────────────────
    r.section("B. Build reports.html (Sections 2 + 3)")

    html_content = r"""<!DOCTYPE html>
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

/* HEADER */
.header{background:linear-gradient(180deg,rgba(10,15,30,.98),var(--bg));border-bottom:1px solid var(--brd);padding:12px 24px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:200;backdrop-filter:blur(24px)}
.logo{font-size:20px;font-weight:700;background:linear-gradient(135deg,var(--yellow),var(--orange));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.logo-sub{font-size:10px;color:var(--t3);text-transform:uppercase;letter-spacing:3px}
.hdr-right{display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.nav-link{color:var(--cyan);text-decoration:none;font-size:11px;padding:5px 12px;border:1px solid var(--brd2);border-radius:6px;transition:all .2s;font-weight:500}
.nav-link:hover{background:rgba(0,229,255,.06);border-color:var(--cyan)}
.live{display:flex;align-items:center;gap:5px;font-size:12px;color:var(--t2)}
.dot{width:7px;height:7px;border-radius:50%;background:var(--green);animation:pulse 2s infinite;box-shadow:0 0 8px var(--green)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}

/* LAYOUT */
.container{max-width:1280px;margin:0 auto;padding:20px 24px}
.section-hdr{font-size:12px;font-weight:600;color:var(--t2);text-transform:uppercase;letter-spacing:1.5px;margin:28px 0 14px;padding-bottom:10px;border-bottom:1px solid var(--brd);display:flex;align-items:center;gap:8px}
.section-hdr:first-child{margin-top:0}
.section-hdr .badge{font-size:9px;background:var(--bg2);color:var(--t3);padding:3px 8px;border-radius:4px;border:1px solid var(--brd);font-weight:500;letter-spacing:.5px}

/* HEADLINE */
.headline-box{padding:24px 30px;border-radius:14px;margin-bottom:24px;border:1px solid var(--brd2);background:linear-gradient(135deg,var(--bg1),var(--bg2))}
.headline-box .meta{font-size:11px;color:var(--t4);font-family:'IBM Plex Mono',monospace;letter-spacing:2px;margin-bottom:6px}
.headline-box h1{font-size:22px;font-weight:700;margin-bottom:8px;letter-spacing:.5px}
.headline-box .detail{font-size:13px;line-height:1.6;color:var(--t2)}

/* STATS ROW */
.stats-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin:16px 0 24px}
.stat-card{padding:14px 16px;border-radius:10px;background:var(--bg2);border:1px solid var(--brd)}
.stat-card .num{font-size:24px;font-weight:700;font-family:'IBM Plex Mono',monospace;line-height:1.1}
.stat-card .lbl{font-size:9px;color:var(--t3);text-transform:uppercase;letter-spacing:1.2px;margin-top:6px}
.stat-card .sub{font-size:10px;color:var(--t4);margin-top:3px;font-family:'IBM Plex Mono',monospace}

/* SCORECARD TABLE */
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

/* CHART */
.chart-wrap{padding:20px 22px;border-radius:10px;background:var(--bg2);border:1px solid var(--brd);margin-bottom:24px;position:relative;height:380px}
.chart-wrap canvas{max-height:340px}
.chart-empty{display:flex;align-items:center;justify-content:center;height:100%;flex-direction:column;color:var(--t3)}

/* FOOTER */
.footer{margin-top:32px;padding:14px 18px;background:var(--bg1);border-radius:8px;border:1px solid var(--brd);font-size:11px;color:var(--t4);font-family:'IBM Plex Mono',monospace;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px}

/* LOADING + EMPTY */
.loading{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:80px 0;color:var(--t3)}
.spinner{width:36px;height:36px;border:3px solid var(--brd2);border-top-color:var(--yellow);border-radius:50%;animation:spin .8s linear infinite;margin-bottom:14px}
@keyframes spin{to{transform:rotate(360deg)}}
.empty-state{padding:48px 32px;text-align:center;background:var(--bg2);border:1px dashed var(--brd2);border-radius:10px;color:var(--t3)}
.empty-state h3{color:var(--t2);font-size:14px;margin-bottom:8px}
.empty-state p{font-size:12px;line-height:1.6;max-width:520px;margin:0 auto}

/* RESPONSIVE */
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
  // Sort
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
      <div style="font-size:11px;color:var(--t4)">Need at least 2 days of logged signals (current: ${(timeline || []).length})</div>
    </div>`;
    return;
  }
  if (chartInstance) { chartInstance.destroy(); chartInstance = null; }
  wrap.innerHTML = `<canvas id="ki-chart"></canvas>`;

  const labels = timeline.map(p => p.date);
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
        pointRadius: 4,
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
        y: {grid: {color: "rgba(26,37,64,.4)"}, ticks: {color: "#5a6580", font: {family: "IBM Plex Mono", size: 11}, callback: v => v + " "},
            min: 0, max: 100,
            // Add regime band annotations as gridlines
            afterBuildTicks: (axis) => {
              axis.ticks = [{value: 0}, {value: 20, label: "CRISIS"},
                {value: 40, label: "CAUTIOUS"}, {value: 60, label: "NEUTRAL"},
                {value: 80, label: "OPTIMISTIC"}, {value: 100, label: "EUPHORIA"}];
              return axis.ticks;
            },
        }
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

  // SECTION 2 — Signal Scorecard
  html += `<div class="section-hdr">📋 Signal Scorecard <span class="badge">SECTION 2</span></div>`;
  html += renderScorecard(D.signal_scorecard);

  // SECTION 3 — Khalid Timeline
  html += `<div class="section-hdr">📈 Khalid Index Timeline <span class="badge">SECTION 3 · LAST 90 DAYS</span></div>`;
  html += `<div class="chart-wrap" id="ki-chart-wrap"></div>`;

  // Footer
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

    out_path = REPO_ROOT / "reports.html"
    out_path.write_text(html_content)
    r.ok(f"  Wrote: reports.html ({len(html_content):,}B, {html_content.count(chr(10))} lines)")

    # Also fix the broken Reports.html stub (was a redirect-to-itself loop)
    stub_path = REPO_ROOT / "Reports.html"
    stub_content = '''<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="0; url=/reports.html">
<title>Reports — Redirecting…</title>
</head><body>
<p>Redirecting to <a href="/reports.html">/reports.html</a></p>
</body></html>
'''
    stub_path.write_text(stub_content)
    r.ok(f"  Fixed: Reports.html stub now redirects correctly to /reports.html")

    r.kv(
        lambda_redeployed="justhodl-reports-builder",
        new_html_size=len(html_content),
        sections_built="2 (Scorecard) + 3 (Khalid Timeline)",
    )
    r.log("Done")
