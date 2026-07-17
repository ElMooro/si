"""ops 3382 — JSI Chart V3 marker re-gate. 3381-G1 grepped for the RENDERED
attribute data-m="d12" — page source holds the template literal data-m="${k}"
with 'd12' as an array element. Doctrine: gate source bytes as AUTHORED
(same class as 3373's escaped-emoji lesson). All else was live."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report
UA={"User-Agent":"Mozilla/5.0 (ops-3382)"}
with report("3382_jsi_v3_regate") as rep:
    rep.heading("ops 3382 — JSI V3 marker re-gate")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:280]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:240]; print(line); rep.log(line)
        if not ok: fails.append(n)
    need=["JSI_CHART_V3","jsi-band","chart-ctl","YoY %","jsi-history.json",
          "grading loop armed","min ","jsi-ep","'d12'","data-m=","Δ12m pts"]
    st,b=-1,""
    dl=time.time()+180
    while time.time()<dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                f"https://justhodl.ai/jsi.html?t={int(time.time())}",headers=UA),timeout=25) as r:
                st,b=r.status,r.read().decode("utf-8","replace")
        except Exception as e: st,b=-1,str(e)
        if st==200 and all(m in b for m in need): break
        time.sleep(12)
    missing=[m for m in need if m not in b]
    gate("G1_all_v3_markers", st==200 and not missing, f"http {st} missing={missing}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3382.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
