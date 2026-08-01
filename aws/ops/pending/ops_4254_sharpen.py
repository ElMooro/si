"""
ops_4254 — four billing-independent sharpenings from the capacity audit.

  S1  WRITER-AWARE PRODUCERS MAP v2. The v1 map grepped key basenames
      against source, so an engine that merely READS an artifact was
      credited as its producer — which is why "asset-compass frozen,
      producer equity-research healthy (750 runs)" was misleading:
      equity-research reads it. v2 classifies every (artifact, engine)
      edge by evidence: WRITER when the key sits near a put_object or a
      Key-constant assignment, READER near get_object, MENTION otherwise.
      Then the RUNS-BUT-SILENT class is re-triaged on WRITERS ONLY, and
      joined against the 36 collapsed LLM engines — most "silence" should
      resolve to the one billing outage.

  S2  WEEKEND-AWARE STALENESS. v1.1's cadence parser ignored the
      day-of-week field, so cron(.. MON-FRI ..) engines got 12-14h
      bounds and their artifacts would flag STALE every single weekend —
      the Saturday check's 129 -> 136 jump was this parser manufacturing
      findings. v1.3.0 detects weekday-only schedules and floors their
      bound at 78h so Fri-close -> Mon-open lives inside contract.

  S3  A REAL SELFTEST for the gate. Error #6 today was my ops gating on
      a selftest mode the gate never had. It has one now — eight pure-
      function cases pinning every bug class this file has ever had:
      dotted keys (v1.0.0), legacy paths, weekday crons, the weekend
      floor, learned-while-stale. The gate below requires 8/8.

  S4  EXEMPTION LEDGER + LLM PAGE-WIRE. Twelve one-shot audit reports
      and nine event-driven state files are exempted by an explicit,
      reasoned ledger (config/contract-exemptions.json) — reviewed
      silence, not deleted contracts. And llm-health v1.1 now emits a
      ProvidersUp EMF gauge with an alarm at < 1: the engine that KNEW
      about the outage for two days can finally page. The alarm firing
      immediately is CORRECT — both providers are billing-dead right now.
"""
import io, json, os, re, subprocess, time, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
CG, LH = "justhodl-contract-gate", "justhodl-llm-health"
CG_MARK = "contract-gate v1.3.0 ops4254 weekday-aware"
LH_MARK = "llm-health v1.1 ops4254 emf-gauge"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=300)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
cw  = boto3.client("cloudwatch", region_name=REGION, config=CFG)
s3  = boto3.client("s3", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
OUT = {"ops": 4254, "ts": NOW.isoformat()}

EXEMPT = {
    # one-shot ops-era reports — produced once by retired tooling
    "data/_freshness-status.json": "one-shot ops report (retired tooling)",
    "data/engine-wiring.json": "one-shot ops report",
    "data/eventbridge-audit.json": "one-shot ops report",
    "data/llm-cost-audit.json": "one-shot ops report",
    "data/page-ai-live.json": "one-shot ops report",
    "data/source-utilization.json": "one-shot ops report",
    "data/stale-triage.json": "one-shot ops report",
    "data/subscribe-endpoint.json": "one-shot config",
    "data/system-audit.json": "one-shot ops report",
    "data/tv-bookmarklet.json": "one-shot config",
    "data/tv-fleet-map.json": "one-shot ops report",
    "data/tv-pipeline-status.json": "one-shot ops report",
    # event-driven state — silence is the normal condition
    "data/_probe_shares.json": "event-driven probe state",
    "data/askdesk-config.json": "manual config; changes only by hand",
    "data/auction-crisis-alert-state.json": "fires only during a crisis",
    "data/config-backtest-url.json": "manual config",
    "data/divergence-interpreted-state.json": "event-driven state",
    "data/finviz-signals-state.json": "event-driven state",
    "data/history-api-url.json": "manual config",
    "data/ka-config.json": "manual config",
    "data/khalid-config.json": "manual config",
}

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

def marker_settled(fn, mark):
    for i in range(30):
        time.sleep(6)
        try:
            loc=lam.get_function(FunctionName=fn)["Code"]["Location"]
            src=zipfile.ZipFile(io.BytesIO(urlopen(loc,timeout=60).read())
                                ).read("lambda_function.py").decode("utf-8","ignore")
            if mark in src: return True
        except Exception: pass
    return False

with report("4254_sharpen") as rep:
    rep.heading("ops 4254 — writer map v2, weekend bounds, real selftest, "
                "LLM page-wire")
    fails=[]

    # ================================================================ S1
    rep.section("S1. Writer-aware producers map v2")
    try:
        reg=json.loads(s3.get_object(Bucket=BUCKET,
            Key="config/engine-contracts.json")["Body"].read())
        keys=sorted((reg.get("contracts") or {}).keys())
        producers={}
        n_w=n_r=n_m=0
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
            if not hits: continue
            entry={"writers":[],"readers":[],"mentions":[]}
            for fn in hits:
                fp=ROOT/"aws"/"lambdas"/fn/"source"/"lambda_function.py"
                try: src=fp.read_text(errors="ignore")
                except Exception: continue
                role="mentions"
                for m in re.finditer(re.escape(base), src):
                    w=src[max(0,m.start()-500):m.start()+500]
                    if "put_object" in w or \
                       re.search(r"(OUT_KEY|S3_KEY|KEY)\s*=\s*[\"'][^\"']*"
                                 +re.escape(base), w):
                        role="writers"; break
                    if "get_object" in w and role!="writers":
                        role="readers"
                entry[role].append(fn)
            producers[key]=entry
            n_w+=1 if entry["writers"] else 0
            n_r+=1 if (not entry["writers"] and entry["readers"]) else 0
            n_m+=1 if (not entry["writers"] and not entry["readers"]) else 0
        doc={"version":2,"generated_at":NOW.isoformat(),
             "schema":"producers[key] = {writers, readers, mentions}; "
                      "role decided by put_object / Key-constant / "
                      "get_object proximity (±500 chars)",
             "n_mapped":len(producers),
             "n_with_writer":n_w,"n_reader_only":n_r,"n_mention_only":n_m,
             "producers":producers}
        s3.put_object(Bucket=BUCKET,Key="config/artifact-producers.json",
                      Body=json.dumps(doc).encode(),
                      ContentType="application/json")
        (ROOT/"config"/"artifact-producers.json").write_text(
            json.dumps(doc,indent=1),encoding="utf-8")
        rep.ok("v2: %d mapped | WRITER identified for %d | reader-only %d "
               "| mention-only %d"%(len(producers),n_w,n_r,n_m))
        rep.kv(section="producers_v2", mapped=len(producers),
               with_writer=n_w, reader_only=n_r, mention_only=n_m)
        if n_w < 300:
            rep.warn("writer coverage lower than expected — bounds fall "
                     "back to reader cadence where no writer resolves")
    except Exception as e:
        fails.append("producers v2: %s"%str(e)[:170])

    # ================================================================ S2/S3
    rep.section("S2/S3. Deploy gate v1.3.0 — selftest 8/8 required")
    try:
        (ROOT/"config"/"contract-exemptions.json").write_text(
            json.dumps({"version":1,"reviewed_at":NOW.isoformat(),
                        "note":"reviewed silence — exempted from STALE, "
                               "still row-counted daily",
                        "exempt":EXEMPT},indent=1),encoding="utf-8")
        s3.put_object(Bucket=BUCKET,Key="config/contract-exemptions.json",
                      Body=json.dumps({"version":1,
                                       "reviewed_at":NOW.isoformat(),
                                       "exempt":EXEMPT}).encode(),
                      ContentType="application/json")
        rep.ok("exemption ledger written — %d keys, each with a reason"
               %len(EXEMPT))
        wait_active(CG)
        lam.update_function_code(FunctionName=CG, ZipFile=zip_fn(CG))
        ok=marker_settled(CG, CG_MARK)
        (rep.ok if ok else rep.fail)("marker %s"%("verified" if ok else "MISSING"))
        if not ok: fails.append("gate marker")
        wait_active(CG)
        r=lam.invoke(FunctionName=CG,InvocationType="RequestResponse",
                     Payload=json.dumps({"mode":"selftest"}).encode())
        b=json.loads(r["Payload"].read() or b"{}")
        for c in b.get("cases",[]):
            (rep.ok if c["pass"] else rep.fail)(
                "   %-20s %s %s"%(c["case"],
                                  "PASS" if c["pass"] else "FAIL",
                                  c.get("error","")))
            rep.kv(section="selftest", case=c["case"], passed=c["pass"])
        if not b.get("passed"):
            fails.append("gate selftest %s"%json.dumps(b)[:150])
        else:
            rep.ok("selftest 8/8 — the mode ops 4252 gated on now "
                   "actually exists, and it pins every historical bug "
                   "class of this file")
        wait_active(CG)
        r=lam.invoke(FunctionName=CG,InvocationType="RequestResponse",
                     Payload=json.dumps({"mode":"learn"}).encode())
        b=json.loads(r["Payload"].read() or b"{}")
        rep.log("learn -> %s"%json.dumps(b)[:220])
        reg=json.loads(s3.get_object(Bucket=BUCKET,
            Key="config/engine-contracts.json")["Body"].read())
        wd=[k for k,c in (reg.get("contracts") or {}).items()
            if ",wd" in str(c.get("bound_source",""))]
        rep.log("weekday-floored contracts: %d (bound >= 78h so weekends "
                "sit inside contract)"%len(wd))
        for k in wd[:8]:
            rep.log("   %s -> %sh"%(k,reg["contracts"][k]["max_age_hours"]))
        rep.kv(section="weekday_bounds", count=len(wd))
        wait_active(CG)
        r=lam.invoke(FunctionName=CG,InvocationType="RequestResponse",
                     Payload=json.dumps({"mode":"check"}).encode())
        b=json.loads(r["Payload"].read() or b"{}")
        d=json.loads(s3.get_object(Bucket=BUCKET,
            Key="data/contract-violations.json")["Body"].read())
        rep.ok("check -> violations=%s (was 136) exempted=%s"
               %(d.get("n_violations"),d.get("n_exempted")))
        rep.kv(section="check", violations=d.get("n_violations"),
               exempted=d.get("n_exempted"), sev1=d.get("sev1"))
        if d.get("n_exempted") != len(EXEMPT):
            rep.warn("exempted %s != ledger %d (keys absent from live "
                     "set are simply not hit)"%(d.get("n_exempted"),
                                                len(EXEMPT)))
        leak=[v for v in d.get("violations",[])
              if v["artifact"] in EXEMPT]
        if leak:
            fails.append("exempted keys leaked into violations: %d"%len(leak))
        else:
            rep.ok("no exempted key appears in violations")
    except Exception as e:
        fails.append("gate v1.3.0: %s"%str(e)[:180])

    # ================================================================ S1b
    rep.section("S1b. RUNS-BUT-SILENT re-triaged on WRITERS only")
    try:
        cap=json.loads((ROOT/"aws"/"ops"/"reports"/
                        "4252_capacity_audit.json").read_text())
        collapsed={c["fn"] for c in cap.get("work_collapse",[])}
        d=json.loads(s3.get_object(Bucket=BUCKET,
            Key="data/contract-violations.json")["Body"].read())
        stale=[v["artifact"] for v in d.get("violations",[])
               if v["cls"]=="STALE"]
        pmap=json.loads(s3.get_object(Bucket=BUCKET,
            Key="config/artifact-producers.json")["Body"].read()
            ).get("producers",{})
        buckets={"WRITER-IS-COLLAPSED-LLM":[], "WRITER-SILENT-OTHER":[],
                 "NO-WRITER-RESOLVED":[]}
        for key in stale:
            wr=(pmap.get(key) or {}).get("writers") or []
            if not wr:
                buckets["NO-WRITER-RESOLVED"].append((key,"-"))
            elif any(w in collapsed for w in wr):
                buckets["WRITER-IS-COLLAPSED-LLM"].append(
                    (key, next(w for w in wr if w in collapsed)))
            else:
                buckets["WRITER-SILENT-OTHER"].append((key, wr[0]))
        for name,rows in buckets.items():
            rep.log("")
            rep.log("%s: %d"%(name,len(rows)))
            for key,fn in rows[:15]:
                (rep.warn if name!="WRITER-SILENT-OTHER" else rep.fail)(
                    "   %-46s %s"%(key.split("/")[-1][:46],fn))
                rep.kv(section=name.lower().replace("-","_"),
                       artifact=key, writer=fn)
        rep.log("")
        rep.ok("read: %d frozen artifacts trace to the ONE billing "
               "outage; %d need engine-level work; %d lack a resolved "
               "writer (map refinement continues)"
               %(len(buckets["WRITER-IS-COLLAPSED-LLM"]),
                 len(buckets["WRITER-SILENT-OTHER"]),
                 len(buckets["NO-WRITER-RESOLVED"])))
        OUT["silent_triage_v2"]={k:len(v) for k,v in buckets.items()}
    except Exception as e:
        rep.warn("re-triage: %s"%str(e)[:150])

    # ================================================================ S4
    rep.section("S4. llm-health v1.1 — the knower becomes a pager")
    try:
        wait_active(LH)
        lam.update_function_code(FunctionName=LH, ZipFile=zip_fn(LH))
        ok=marker_settled(LH, LH_MARK)
        (rep.ok if ok else rep.fail)("marker %s"%("verified" if ok else "MISSING"))
        if not ok: fails.append("llm-health marker")
        wait_active(LH)
        import base64
        r=lam.invoke(FunctionName=LH,InvocationType="RequestResponse",
                     LogType="Tail")
        tail=base64.b64decode(r.get("LogResult","")).decode("utf-8","ignore")
        emf=[l for l in tail.splitlines() if '"ProvidersUp"' in l]
        if emf:
            rep.ok("EMF line emitted: %s"%emf[0][:180])
            rep.kv(section="llm_emf", line=emf[0][:150])
        else:
            fails.append("no EMF line in llm-health tail")
        cw.put_metric_alarm(
            AlarmName="justhodl-llm-providers-down",
            AlarmDescription="llm-health reports fewer than 1 model "
                             "provider up — the intelligence layer is "
                             "blind. Check Anthropic/z.ai billing.",
            Namespace="JustHodl/LLM", MetricName="ProvidersUp",
            Statistic="Minimum", Period=3600, EvaluationPeriods=2,
            Threshold=1, ComparisonOperator="LessThanThreshold",
            TreatMissingData="breaching",
            AlarmActions=["arn:aws:sns:us-east-1:857687956942:jh-ops-alerts"])
        rep.ok("alarm justhodl-llm-providers-down armed (missing data = "
               "breaching: a health canary that stops running IS the "
               "emergency). It WILL fire now — both providers are "
               "billing-dead, and paging on that is the entire point.")
    except Exception as e:
        fails.append("llm-health: %s"%str(e)[:170])

    (ROOT/"aws"/"ops"/"reports"/"4254_sharpen.json").write_text(
        json.dumps(OUT, indent=1, default=str), encoding="utf-8")
    rep.section("RESULT")
    if fails:
        for f in fails: rep.fail("  %s"%f)
        raise SystemExit("FAILS: %s"%"; ".join(fails[:3]))
    rep.ok("OPS 4254 PASS")
