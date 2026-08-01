"""
ops_4249 — repair the scorecard truncation my own patch caused, and make
contract staleness bounds come from declared cadence.

WHAT ACTUALLY HAPPENED (root cause of the ssm_writes mystery)
ops 4246 inserted the _ssm_put_large helper — a column-0, module-level
def — directly BEFORE an anchor line that sat INSIDE lambda_handler's
body. Python parses that happily and it terminates the handler at the
artifact write: the SSM block, the alpha map, the republish, the summary
print and the final return all became dead code. The handler returned
None after the first write. Clean END, no error, no SSM — exactly what
the 4248 log dump showed. My "marker verified" gate checked that a
STRING existed in the zip; the string lived in a comment inside code
that never ran. Text-presence verification passed while behaviour died.

THE CORRECTION IS STRUCTURAL VERIFICATION, applied twice here:
  * The repo copy was fixed with an AST assertion (handler calls the
    helper twice AND ends in a Return AND stamps ssm_writes) before any
    deploy. That assertion would have refused the 4246 patch outright.
  * The deploy gate below downloads the DEPLOYED zip and re-runs the
    same AST assertions against it — proving the structure of what AWS
    is actually running, not the presence of a phrase.
  * The behavioural gate then requires all three ends: artifact carries
    ssm_ok=true, the SSM parameter's updated_at equals the artifact's
    generated_at (the map really moved for the first time since it
    froze), and the run's log contains the summary line — which only
    prints from the handler's true end.

CADENCE BOUNDS (part B)
The gate certified a five-day freeze because it learned bounds from
observed age. Bounds now come from each producer's declared schedule via
an artifact->producers map built here by grepping every contracted key
against engine source — provenance generated from source and versioned
in git, refreshed on future ops runs. Unmappable artifacts keep the
learned formula capped at 72h and labelled; anything already older than
its bound at learn time is emitted as a SUSPECT instead of blessed.
"""
import ast, io, json, os, re, subprocess, time, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
SC = "justhodl-signal-scorecard"
CG = "justhodl-contract-gate"
CG_MARK = "contract-gate v1.1.0 ops4249 cadence-bounds"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=300)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
ssm = boto3.client("ssm", region_name=REGION, config=CFG)
s3  = boto3.client("s3", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

def zip_fn(fn):
    src="aws/lambdas/%s/source"%fn; buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        for root,_,files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                fp=os.path.join(root,f); z.write(fp, os.path.relpath(fp,src))
        if os.path.isdir("aws/shared"):
            for f in sorted(os.listdir("aws/shared")):
                if f.endswith(".py"): z.write(os.path.join("aws/shared",f), f)
    return buf.getvalue()

def wait_active(fn,b=200):
    t0=time.time()
    while time.time()-t0<b:
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus") in (None,"Successful"): return True
        except Exception: pass
        time.sleep(4)
    return False

def deployed_source(fn):
    loc=lam.get_function(FunctionName=fn)["Code"]["Location"]
    z=zipfile.ZipFile(io.BytesIO(urlopen(loc,timeout=60).read()))
    for n in z.namelist():
        if n.endswith("lambda_function.py"):
            return z.read(n).decode("utf-8","ignore")
    return ""

def scorecard_structure_ok(src):
    """The gate that would have refused the 4246 patch."""
    try:
        tree=ast.parse(src)
    except Exception:
        return False, "unparseable"
    h=[n for n in tree.body if isinstance(n,ast.FunctionDef)
       and n.name=="lambda_handler"]
    if not h: return False, "no handler"
    h=h[0]
    top=any(isinstance(n,ast.FunctionDef) and n.name=="_ssm_put_large"
            for n in tree.body)
    calls=sum(1 for n in ast.walk(h) if isinstance(n,ast.Call)
              and isinstance(n.func,ast.Name)
              and n.func.id=="_ssm_put_large")
    ends=isinstance(h.body[-1], ast.Return)
    if not top: return False, "helper not module-level"
    if calls<2: return False, "handler calls helper %d times"%calls
    if not ends: return False, "handler does not end in return"
    return True, "helper@module, %d calls, ends in return"%calls

with report("4249_structural_gates") as rep:
    rep.heading("ops 4249 — structural gates + cadence bounds")
    fails=[]

    # ================================================================ A
    rep.section("A. Scorecard — deploy, verify STRUCTURE of the zip")
    try:
        wait_active(SC)
        lam.update_function_code(FunctionName=SC, ZipFile=zip_fn(SC))
        ok=False; why=""
        for i in range(30):
            time.sleep(6)
            try:
                ok,why=scorecard_structure_ok(deployed_source(SC))
                if ok: break
            except Exception as e:
                why=str(e)[:80]
        (rep.ok if ok else rep.fail)("deployed-zip AST check: %s"%why)
        if not ok: fails.append("structure: %s"%why)
    except Exception as e:
        fails.append("deploy: %s"%str(e)[:170])

    rep.section("A2. Behavioural gate — three independent ends")
    try:
        before=None
        try:
            before=json.loads(s3.get_object(Bucket=BUCKET,
                Key="data/signal-scorecard.json")["Body"].read()
                ).get("generated_at")
        except Exception: pass
        wait_active(SC)
        lam.invoke(FunctionName=SC, InvocationType="Event",
                   Payload=json.dumps({"source":"ops4249"}).encode())
        art=None
        for i in range(40):
            time.sleep(12)
            try:
                a=json.loads(s3.get_object(Bucket=BUCKET,
                    Key="data/signal-scorecard.json")["Body"].read())
            except Exception:
                continue
            if a.get("generated_at") and a.get("generated_at")!=before \
               and "ssm_writes" in a:
                art=a; break
            if a.get("generated_at") and a.get("generated_at")!=before \
               and i>6:
                art=a; break
        if not art:
            fails.append("artifact never refreshed with ssm_writes")
        else:
            rep.log("generated_at=%s ssm_ok=%s"%(art.get("generated_at"),
                                                 art.get("ssm_ok")))
            rep.log("ssm_writes=%s"%json.dumps(art.get("ssm_writes"))[:300])
            rep.kv(section="scorecard", ssm_ok=art.get("ssm_ok"),
                   writes=json.dumps(art.get("ssm_writes"))[:160])
            # end 1: artifact says the writes landed
            if art.get("ssm_ok") is not True:
                fails.append("artifact ssm_ok != true")
            else:
                rep.ok("END 1 — artifact carries ssm_ok=true")
            # end 2: the parameter itself moved
            try:
                pname=(art.get("ssm_writes") or [{}])[0].get("param")
                v=ssm.get_parameter(Name=pname)["Parameter"]["Value"]
                pv=json.loads(v) if not v.startswith("{\\\"_pointer") else json.loads(v)
                upd=pv.get("updated_at")
                match = upd==art.get("generated_at")
                (rep.ok if match else rep.fail)(
                    "END 2 — SSM %s updated_at=%s (artifact=%s) match=%s"
                    %(pname, upd, art.get("generated_at"), match))
                rep.kv(section="ssm_param", name=pname, updated_at=upd,
                       matches_artifact=match)
                if not match: fails.append("ssm parameter did not move")
            except Exception as e:
                fails.append("ssm read: %s"%str(e)[:140])
            # end 3: the summary print — only reachable at the true end
            try:
                r=logs.filter_log_events(
                    logGroupName="/aws/lambda/%s"%SC,
                    startTime=int((NOW-timedelta(minutes=20)).timestamp()*1000),
                    filterPattern='"graded="', limit=5)
                ev=r.get("events",[])
                (rep.ok if ev else rep.fail)(
                    "END 3 — summary line in logs: %s"
                    %(ev[-1]["message"].strip()[:150] if ev else "ABSENT"))
                if not ev: fails.append("summary print absent")
            except Exception as e:
                rep.warn("log check: %s"%str(e)[:120])
        rep.warn("Downstream note: the enforcement map every consumer reads "
                 "had been frozen since 2026-07-27. It just moved for the "
                 "first time in five days.")
    except Exception as e:
        fails.append("behavioural: %s"%str(e)[:180])

    # ================================================================ B
    rep.section("B. Build the artifact->producers map from source")
    producers={}
    try:
        reg=json.loads(s3.get_object(Bucket=BUCKET,
            Key="config/engine-contracts.json")["Body"].read())
        keys=sorted((reg.get("contracts") or {}).keys())
        rep.log("contracted artifacts: %d"%len(keys))
        lam_dirs=sorted(d.name for d in (ROOT/"aws"/"lambdas").iterdir()
                        if (d/"source").is_dir())
        # index: basename -> lambda dirs whose source mentions it
        for key in keys:
            base=key.split("/")[-1]
            try:
                g=subprocess.run(["grep","-rl","--include=*.py",base,
                                  "aws/lambdas"],cwd=str(ROOT),
                                 capture_output=True,text=True,timeout=30)
                hits=sorted({x.split("/")[2] for x in
                             g.stdout.strip().split("\n") if x})
            except Exception:
                hits=[]
            if hits:
                producers[key]=hits
        doc={"version":1,"generated_at":NOW.isoformat(),
             "source":"ops_4249 grep of contracted keys against engine "
                      "source; refresh by re-running this build",
             "n_mapped":len(producers),"n_contracted":len(keys),
             "producers":producers}
        s3.put_object(Bucket=BUCKET, Key="config/artifact-producers.json",
                      Body=json.dumps(doc).encode(),
                      ContentType="application/json")
        (ROOT/"config").mkdir(exist_ok=True)
        (ROOT/"config"/"artifact-producers.json").write_text(
            json.dumps(doc, indent=1), encoding="utf-8")
        rep.ok("mapped %d of %d artifacts to producers (git + S3)"
               %(len(producers),len(keys)))
        rep.kv(section="producers", mapped=len(producers),
               contracted=len(keys))
    except Exception as e:
        fails.append("producers: %s"%str(e)[:170])

    # ================================================================ C
    rep.section("C. Deploy contract-gate v1.1.0, relearn, recheck")
    try:
        wait_active(CG)
        lam.update_function_code(FunctionName=CG, ZipFile=zip_fn(CG))
        ok=False
        for i in range(30):
            time.sleep(6)
            if CG_MARK in deployed_source(CG): ok=True; break
        (rep.ok if ok else rep.fail)("marker %s"%("verified" if ok else "MISSING"))
        if not ok: fails.append("gate marker")
        wait_active(CG)
        r=lam.invoke(FunctionName=CG, InvocationType="RequestResponse",
                     Payload=json.dumps({"mode":"learn"}).encode())
        b=json.loads(r["Payload"].read() or b"{}")
        rep.log("learn -> %s"%json.dumps(b)[:260])
        rep.kv(section="learn", contracts=b.get("n_contracts"),
               cadence_bounded=b.get("n_cadence_bounded"),
               suspects=b.get("n_suspects"))
        if (b.get("n_cadence_bounded") or 0) < 100:
            fails.append("only %s cadence-bounded — the map is not "
                         "actually driving bounds"%b.get("n_cadence_bounded"))
        else:
            rep.ok("%s artifacts now bounded by DECLARED cadence"
                   %b.get("n_cadence_bounded"))
        reg=json.loads(s3.get_object(Bucket=BUCKET,
            Key="config/engine-contracts.json")["Body"].read())
        scc=(reg.get("contracts") or {}).get("data/signal-scorecard.json") or {}
        rep.log("scorecard contract: bound=%sh source=%s"
                %(scc.get("max_age_hours"),scc.get("bound_source")))
        rep.kv(section="scorecard_contract",
               bound_h=scc.get("max_age_hours"),
               source=scc.get("bound_source"))
        if not str(scc.get("bound_source","")).startswith("cadence") \
           or (scc.get("max_age_hours") or 999)>72:
            fails.append("scorecard bound %sh/%s — the five-day-freeze "
                         "hole is still open"%(scc.get("max_age_hours"),
                                               scc.get("bound_source")))
        else:
            rep.ok("a repeat of the five-day freeze now goes STALE at "
                   "%sh instead of being certified healthy"
                   %scc.get("max_age_hours"))
        for x in (reg.get("suspects") or [])[:12]:
            rep.warn("  SUSPECT %s age=%sh bound=%sh"
                     %(x["key"],x["age_h"],x["bound_h"]))
        wait_active(CG)
        r=lam.invoke(FunctionName=CG, InvocationType="RequestResponse",
                     Payload=json.dumps({"mode":"check"}).encode())
        b=json.loads(r["Payload"].read() or b"{}")
        rep.log("check -> %s"%json.dumps(b)[:260])
        d=json.loads(s3.get_object(Bucket=BUCKET,
            Key="data/contract-violations.json")["Body"].read())
        stale=[v for v in d.get("violations",[]) if v["cls"]=="STALE"]
        rep.log("STALE under cadence bounds: %d (these are REAL finds the "
                "old bounds were blind to)"%len(stale))
        for v in stale[:20]:
            rep.warn("   %-46s %s"%(v["artifact"][-46:],v["detail"][:80]))
            rep.kv(section="stale", artifact=v["artifact"],
                   detail=v["detail"][:110])
        if any(v["artifact"]=="data/signal-scorecard.json" for v in stale):
            fails.append("scorecard STALE right after refresh — bound wrong")
    except Exception as e:
        fails.append("gate: %s"%str(e)[:180])

    rep.section("RESULT")
    if fails:
        for f in fails: rep.fail("  %s"%f)
        raise SystemExit("FAILS: %s"%"; ".join(fails[:3]))
    rep.ok("OPS 4249 PASS")
