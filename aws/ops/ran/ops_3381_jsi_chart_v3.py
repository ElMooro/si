"""ops 3381 — JSI page: Chart V3 + full field coverage (page-only push).

Khalid: verify all engine data reaches the page + improve UX, especially the
graph (12-month change view). Shipped: page now loads the DAILY 1990→ series
(jsi-history.json — was never fetched before) with weekly fallback; Chart V3
= regime bands (CALM→CRISIS), ≥90th-pct episode shading, crosshair with
regime-aware tip, pulsing now-marker + right-edge value, range toggles
(ALL/10y/5y/1y) and VIEW toggles (Level default · Δ12m pts · YoY %) —
transforms computed on the full series so 12m lookback survives any range;
Δ-views get a zero line + red/green split fill and drop bands/markers for a
clean read. Field-coverage additions: history-span/weights/runtime meta line,
historical MIN alongside max, signal-loop chip (armed/fired/regime-change).
jsdom harness PASS_ALL (11 behaviors) pre-push.

Gates:
  G1  live jsi.html carries all V3 markers (poll ≤240s)
  G2  site origin serves data/jsi-history.json: 200, series > 9000 daily obs
"""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report

UA={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) ops-3381"}
def get(u, timeout=30):
    try:
        with urllib.request.urlopen(urllib.request.Request(u+("&" if "?" in u else "?")+f"t={int(time.time())}",headers=UA),timeout=timeout) as r:
            return r.status, r.read().decode("utf-8","replace")
    except Exception as e: return -1, str(e)[:200]

with report("3381_jsi_chart_v3") as rep:
    rep.heading("ops 3381 — JSI Chart V3 + coverage gates")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:300]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:260]; print(line); rep.log(line)
        if not ok: fails.append(n)
    need=["JSI_CHART_V3","jsi-band","chart-ctl","YoY %","jsi-history.json",
          "grading loop armed","min ","jsi-ep","data-m=\"d12\""]
    ok1, missing = False, need
    dl=time.time()+240
    while time.time()<dl:
        st,b=get("https://justhodl.ai/jsi.html")
        if st==200:
            missing=[m for m in need if m not in b]
            if not missing: ok1=True; break
        time.sleep(12)
    gate("G1_page_v3_live", ok1, f"missing={missing}")
    st,b=get("https://justhodl.ai/data/jsi-history.json")
    n=0
    try: n=len(json.loads(b).get("series") or [])
    except Exception: pass
    gate("G2_daily_history_served", st==200 and n>9000, f"http {st} series_n={n}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3381.json").write_text(json.dumps(out,indent=2))
    sys.exit(0)
