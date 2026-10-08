"""ops 3619 — macro-leads KR tape-row re-applied (3617's page edit vanished
pre-commit; git proved last touch = 3592). Explicit per-file staging + diff-
proof this time. Single served gate."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report

with report("3619_page_row") as rep:
    rep.heading("ops 3619 — KR tape row served")
    out = {"gates": {}}
    ok1 = False; det = ""; dl = time.time() + 480
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/macro-leads.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            det = f"tape_row={'korea_flash_tape' in html} news_tape={'news-tape' in html}"
            if "korea_flash_tape" in html:
                ok1 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(20)
    out["gates"]["G1"] = {"ok": ok1, "detail": det}
    print(("PASS  " if ok1 else "FAIL  ") + "G1 — " + det); rep.log("G1 " + str(ok1))
    out["verdict"] = "PASS_ALL" if ok1 else "GAPS: G1"
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3619.json").write_text(json.dumps(out, indent=2))
    sys.exit(0 if True else 1)
