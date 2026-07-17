"""ops 3401 — tech-health section was render-wiped (inner append vs wholesale
content.innerHTML replace race, caught via Khalid's page dump). Fix: sibling
mount after #content + id guard. Gate: live source carries the fix."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report
UA={"User-Agent":"Mozilla/5.0 (ops-3401)"}
with report("3401_tech_health_mount") as rep:
    rep.heading("ops 3401 — tech-health sibling mount")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:280]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:240]; print(line); rep.log(line)
        if not ok: fails.append(n)
    need=["tech-health-sec","c.after(sec)","Sector Technical Health"]
    ok1, missing=False, need
    dl=time.time()+240
    while time.time()<dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                f"https://justhodl.ai/sector-flow.html?t={int(time.time())}",headers=UA),timeout=25) as r:
                b=r.read().decode("utf-8","replace")
            missing=[m for m in need if m not in b]
            if not missing: ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_sibling_mount_live", ok1, f"missing={missing}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3401.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
