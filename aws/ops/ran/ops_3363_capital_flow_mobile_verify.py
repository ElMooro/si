#!/usr/bin/env python3
"""ops 3363 — verify capital-flow mobile organization pass live."""
import sys, json, time, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

URL = "https://justhodl.ai/capital-flow.html"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) JustHodl-Ops/3363"}
MUST = ['class="sbar"', ".sb-track{width:180px", ".lgc-f13{width:150px",
        ".lgc-f13{width:78px}", 'class="lg-head"', 'class="et-a"',
        ".sbar{gap:6px;font-size:9px;flex-wrap:wrap}"]
MUST_NOT = ['<span style="width:170px', 'width:150px;text-align:right">${f.new']

with report("3363_capital_flow_mobile_verify") as r:
    r.section("live markers")
    html = ""
    for i in range(10):
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(URL + f"?t={int(time.time())}", headers=UA),
                    timeout=30) as resp:
                html = resp.read().decode("utf-8", "replace")
            if MUST[0] in html and MUST[3] in html:
                break
            r.log(f"  poll {i+1}: not yet ({len(html)}b)")
        except Exception as e:
            r.log(f"  poll {i+1}: {e!r}")
        time.sleep(25)
    missing = [m for m in MUST if m not in html]
    bad = [m for m in MUST_NOT if m in html]
    Path(Path(__file__).resolve().parents[1] / "reports" / "3363.json").write_text(
        json.dumps({"missing": missing, "regressions": bad,
                    "verdict": "PASS" if not missing and not bad else "FAIL"}, indent=1))
    if missing or bad:
        r.fail(f"missing {missing} / regressions {bad}")
        sys.exit(1)
    r.ok(f"mobile pass live — {len(MUST)} markers, 0 inline leftovers")
    r.ok("VERDICT: PASS")
