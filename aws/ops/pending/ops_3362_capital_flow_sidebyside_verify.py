#!/usr/bin/env python3
"""ops 3362 — verify capital-flow.html side-by-side columns fix live (commit 9466d5b)."""
import sys, json, time, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

URL = "https://justhodl.ai/capital-flow.html"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) JustHodl-Ops/3362"}
MUST = [".row .lenses{font-size:7.5px}", ".fc-row{grid-template-columns:15px 40px 1fr 36px",
        ".grid{display:grid;grid-template-columns:1fr 1fr", 'class="fc-card"']
MUST_NOT = ["{.grid{grid-template-columns:1fr}}", "{.fc-cols{grid-template-columns:1fr}}"]

with report("3362_capital_flow_sidebyside_verify") as r:
    r.section("live markers")
    html = ""
    for i in range(10):
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(URL + f"?t={int(time.time())}", headers=UA),
                    timeout=30) as resp:
                html = resp.read().decode("utf-8", "replace")
            if MUST[0] in html:
                break
            r.log(f"  poll {i+1}: not yet ({len(html)}b)")
        except Exception as e:
            r.log(f"  poll {i+1}: {e!r}")
        time.sleep(25)
    missing = [m for m in MUST if m not in html]
    bad = [m for m in MUST_NOT if m in html]
    Path(Path(__file__).resolve().parents[1] / "reports" / "3362.json").write_text(
        json.dumps({"missing": missing, "regressions": bad,
                    "verdict": "PASS" if not missing and not bad else "FAIL"}, indent=1))
    if missing or bad:
        r.fail(f"missing {missing} / regressions {bad}")
        sys.exit(1)
    r.ok("side-by-side grid + compact mode live; stack collapse gone")
    r.ok("VERDICT: PASS")
