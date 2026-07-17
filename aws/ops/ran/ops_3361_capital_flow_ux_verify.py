#!/usr/bin/env python3
"""ops 3361 — verify capital-flow.html UX reorganization live (commit 25614d9).

Gates (bare-URL doctrine, from runner):
  G1  new markers serving: fc-card leaderboard, pulse strip, sect headers, animBars
  G2  regressions absent: jh-enhance bars instance gone, nav \\x01 corruption gone,
      undefined var(--line) gone
  G3  all six tab hooks still present (nothing removed)
"""
import sys, json, time, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

URL = "https://justhodl.ai/capital-flow.html"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) JustHodl-Ops/3361"}

MUST = [
    'class="fc-card"', "Flow-score leaderboard", "fc-fill", 'class="pulse"',
    "animBars()", 'class="sect"', 'href="/pricing.html"',
    "Deep detail — every lens per name", "Sector dollar rotation",
    'data-tab="stocks"', 'data-tab="etfs"', 'data-tab="sectors"',
    'data-tab="ledger"', 'data-tab="intel"', 'data-tab="dollars"',
]
MUST_NOT = ['data-bars="accumulating', "\x01>Pricing", "var(--line)"]

with report("3361_capital_flow_ux_verify") as r:
    r.section("G1-G3 — live page markers")
    html, err = "", None
    for i in range(10):
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(URL + f"?t={int(time.time())}", headers=UA),
                    timeout=30) as resp:
                html = resp.read().decode("utf-8", "replace")
            if 'class="fc-card"' in html:
                break
            r.log(f"  poll {i+1}: page up, markers not yet ({len(html)}b) — CDN lag, retry")
        except Exception as e:
            err = e
            r.log(f"  poll {i+1}: {e!r}")
        time.sleep(25)
    missing = [m for m in MUST if m not in html]
    present_bad = [m for m in MUST_NOT if m in html]
    out = {"bytes": len(html), "missing": missing, "regressions": present_bad}
    Path(Path(__file__).resolve().parents[1] / "reports" / "3361.json").write_text(
        json.dumps({**out, "verdict": "PASS" if not missing and not present_bad else "FAIL"}, indent=1))
    if missing:
        r.fail(f"missing markers: {missing}" + (f" (last err {err!r})" if err else ""))
        sys.exit(1)
    if present_bad:
        r.fail(f"regressions present: {present_bad}")
        sys.exit(1)
    r.ok(f"G1-G3 ✓ all {len(MUST)} markers live, 0 regressions, {len(html):,}b")
    r.ok("VERDICT: PASS")
