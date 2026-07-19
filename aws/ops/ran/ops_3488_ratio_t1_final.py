"""ops 3488 — final T1 close for the pairs-ratio arc (3486/3487).

3486 T1 failed on a CI shim bug (string-replace var-scoped the export
inside the core IIFE); 3487 was a mechanical digit-swap clone that
poisoned its own OPS3486 byte-gates — banked: NEVER mechanically clone
ops files, in any form. This is written from scratch and tests ONE thing:
the served core's FGChart.ratio math under node with a proper window
stub. T2/T3 (served integrity + flagship v2.1) already passed in 3486.

  U1  fetch LIVE /fg-chart.js, run under node with `var window={}`,
      capture window.FGChart AFTER the IIFE, assert exact ratio values,
      gap-guard, and zero-guard.
"""
import json
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report  # noqa: E402

REPO = Path(__file__).resolve().parents[3]

TEST_TAIL = """
;var FGChart=window.FGChart;
(function(){
  var A=[['2024-01-15',10],['2024-04-15',20],['2024-07-15',30],['2024-10-15',40]];
  var B=[['2024-01-15',2],['2024-04-15',4],['2024-07-15',5],['2024-10-15',0]];
  var R=FGChart.ratio(A,B);
  var t1=JSON.stringify(R)===JSON.stringify([['2024-01-15',5],['2024-04-15',5],['2024-07-15',6]]);
  var t2=FGChart.ratio(A,[['2023-01-01',2]]).length===0;
  var t3=FGChart.ratio([],B).length===0;
  console.log(JSON.stringify({t1:t1,t2:t2,t3:t3,R:R}));
  process.exit((t1&&t2&&t3)?0:1);
})();
"""

with report("3488_ratio_t1_final") as rep:
    out = {"ops": 3488, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:420]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:380]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3488 — ratio math on the SERVED core (node, window stub)")

    try:
        req = urllib.request.Request(
            f"https://justhodl.ai/fg-chart.js?cb={int(time.time())}",
            headers={"User-Agent": "ops-3488"})
        with urllib.request.urlopen(req, timeout=30) as r:
            core = r.read().decode("utf-8")
        src = "var window={};\n" + core + TEST_TAIL
        with tempfile.NamedTemporaryFile("w", suffix=".js",
                                         delete=False) as f:
            f.write(src)
            p = f.name
        run = subprocess.run(["node", p], capture_output=True, text=True,
                             timeout=60)
        gate("U1_served_ratio_math",
             run.returncode == 0 and '"t1":true' in (run.stdout or ""),
             (run.stdout or run.stderr)[:300])
    except Exception as e:  # noqa: BLE001
        gate("U1_served_ratio_math", False, str(e)[:280])

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3488.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
