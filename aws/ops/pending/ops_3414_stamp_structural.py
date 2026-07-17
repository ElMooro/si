"""ops 3414 — regime-stamp structural proof (dedupe blocks same-day runtime
re-proof; first fresh emission tomorrow carries it). Verify inside the
DEPLOYED best-setups zip's bundled signals_emit.py: _regime_snapshot exists
AND is wired into log_signal's metadata path."""
import io, json, sys, urllib.request, zipfile
from pathlib import Path
import boto3
from ops_report import report
LAM=boto3.client("lambda","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3414)"}
with report("3414_stamp_structural") as rep:
    rep.heading("ops 3414 — stamp structural proof")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:280]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:240]; print(line); rep.log(line)
        if not ok: fails.append(n)
    info=LAM.get_function(FunctionName="justhodl-best-setups")
    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
        z=zipfile.ZipFile(io.BytesIO(r.read()))
        se=z.read("signals_emit.py").decode("utf-8","replace")
    wired = "_regime_snapshot" in se and 'md.setdefault("regime", _regime_snapshot())' in se
    gate("G1_stamp_wired_in_deployed_zip", wired,
         "bundled signals_emit carries _regime_snapshot + metadata wiring; "
         "runtime rows stamp at first fresh emission (dedupe blocked today's re-run)")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3414.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
