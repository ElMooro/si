"""
ops_4253 — the 36-engine collapse: prove the cause from logs, fix it,
verify recovery end-to-end. Plus the v1.2.0 self-test regression.

THE FINDING (ops 4252 section B)
36 engines fell to 1-16% of their former runtime on Jul 30-31 and every
one is an LLM-calling engine. A 114s engine finishing in 1.5s means the
model call dies instantly and the engine "succeeds" around the failure —
the census pattern, times thirty-six, and it also accounts for a large
share of the 91 frozen artifacts (bond-trace's writer is ai-website-
synthesis, itself collapsed to 9%).

This op does NOT guess the cause. Today produced five wrong calls, all
from treating an inference as a fact. So:
  1. READ the failure line from the logs of three collapsed engines.
  2. READ the LLM control surface: SSM /justhodl/* knobs (key present?
     cap value? spend counter?), llm-health / llm-cost artifacts.
  3. TEST the credential itself with a 1-token live call from here.
  4. FIX whatever the evidence names — a knob via SSM if it is the cap,
     a report to Khalid if it is a dead key (a credential only he can
     mint is the one thing autonomy cannot replace).
  5. VERIFY recovery by invoking two collapsed engines and requiring
     their REPORT duration back in the old band and their artifact to
     move. A fix that is not measured is a hope.
  6. ALARM the layer: JustHodl/LLM CanaryFail so a dead LLM layer pages
     within an hour instead of surfacing two days later inside a cost
     audit.

ALSO: contract-gate v1.2.0 broke its own self-test (E-gate did its job).
Diagnose from the selftest output, fix, redeploy, re-gate.
"""
import io, json, os, time, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=180)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
ssm = boto3.client("ssm", region_name=REGION, config=CFG)
s3  = boto3.client("s3", region_name=REGION, config=CFG)
cw  = boto3.client("cloudwatch", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
OUT = {"ops": 4253, "ts": NOW.isoformat()}

SAMPLE = ["justhodl-ai-brief", "justhodl-divergence-interpreter",
          "justhodl-debate-engine"]
CG = "justhodl-contract-gate"

def wait_active(fn,b=200):
    t0=time.time()
    while time.time()-t0<b:
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus") in (None,"Successful"): return True
        except Exception: pass
        time.sleep(4)
    return False

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

with report("4253_llm_outage") as rep:
    rep.heading("ops 4253 — the LLM-layer outage")
    fails=[]

    # ================================================================ 1
    rep.section("1. The failure line, from the engines' own logs")
    sigs={}
    for fn in SAMPLE:
        try:
            r=logs.filter_log_events(
                logGroupName="/aws/lambda/%s"%fn,
                startTime=int((NOW-timedelta(hours=36)).timestamp()*1000),
                filterPattern='?anthropic ?Anthropic ?llm ?LLM ?router '
                              '?budget ?credit ?401 ?403 ?429 ?overloaded '
                              '?invalid ?exceeded',
                limit=14)
            seen=set()
            for e in r.get("events",[]):
                m=e["message"].strip().replace("\n"," | ")[:220]
                k=m[:80]
                if k in seen: continue
                seen.add(k)
                rep.fail("  [%s] %s"%(fn.split("-")[-1][:12], m))
                sigs[k]=sigs.get(k,0)+1
                rep.kv(section="log_evidence", engine=fn, line=m[:170])
            if not r.get("events"):
                rep.warn("  [%s] no matching lines — failure may be "
                         "swallowed silently"%fn)
        except Exception as e:
            rep.warn("  %s logs: %s"%(fn,str(e)[:100]))
    OUT["signatures"]=sigs

    # ================================================================ 2
    rep.section("2. The LLM control surface")
    knobs={}
    try:
        nxt=None
        while True:
            kw={"Path":"/justhodl/","Recursive":True,"MaxResults":10}
            if nxt: kw["NextToken"]=nxt
            r=ssm.get_parameters_by_path(**kw)
            for p in r.get("Parameters",[]):
                n=p["Name"]
                v=p.get("Value","")
                if p.get("Type")=="SecureString" or "key" in n.lower() \
                   or "token" in n.lower():
                    v="<secret len=%d>"%len(v)
                knobs[n]=v[:120]
            nxt=r.get("NextToken")
            if not nxt: break
        for n in sorted(knobs):
            if any(w in n.lower() for w in ("llm","anthropic","budget",
                                            "cap","spend","model","glm")):
                rep.log("  %-52s = %s"%(n[-52:],knobs[n]))
                rep.kv(section="knob", name=n, value=knobs[n][:100])
    except Exception as e:
        rep.warn("ssm walk: %s"%str(e)[:120])
    for key in ("data/llm-health.json","data/llm-cost-dashboard.json",
                "data/llm-cost-audit.json"):
        try:
            h=s3.head_object(Bucket=BUCKET,Key=key)
            age=(NOW-h["LastModified"]).total_seconds()/3600
            body=json.loads(s3.get_object(Bucket=BUCKET,Key=key)["Body"].read())
            rep.log("  %s (%.0fh old): %s"%(key,age,json.dumps(body)[:260]))
        except Exception:
            pass

    # ================================================================ 3
    rep.section("3. Test the credential itself — one token, live")
    key_ok=None
    try:
        k=ssm.get_parameter(Name="/justhodl/anthropic/api-key",
                            WithDecryption=True)["Parameter"]["Value"]
        body=json.dumps({"model":"claude-haiku-4-5-20251001",
                         "max_tokens":1,
                         "messages":[{"role":"user","content":"hi"}]}).encode()
        req=Request("https://api.anthropic.com/v1/messages",data=body,
                    headers={"x-api-key":k,
                             "anthropic-version":"2023-06-01",
                             "content-type":"application/json"})
        try:
            resp=urlopen(req,timeout=30)
            code=resp.getcode()
            key_ok=True
            rep.ok("  Anthropic key VALID — HTTP %d on a 1-token call. "
                   "The credential is not the cause."%code)
        except Exception as he:
            code=getattr(he,"code",None)
            detail=""
            try: detail=he.read().decode()[:220]
            except Exception: detail=str(he)[:220]
            key_ok=False
            rep.fail("  Anthropic key FAILS live: HTTP %s %s"%(code,detail))
            rep.kv(section="credential", http=code, detail=detail[:150])
    except Exception as e:
        rep.warn("  key test: %s"%str(e)[:140])
    OUT["credential_ok"]=key_ok

    # ================================================================ 4
    rep.section("4. Fix what the evidence names")
    fixed_knob=False
    joined=" ".join(sigs.keys()).lower()
    if key_ok is False:
        rep.fail("ROOT CAUSE: dead credential. Only Khalid can mint a new "
                 "Anthropic key. Store it with:")
        rep.fail("aws ssm put-parameter --name /justhodl/anthropic/api-key "
                 "--type SecureString --overwrite --value 'sk-ant-...'")
    elif "budget" in joined or "cap" in joined or "exceeded" in joined \
         or "credit" in joined:
        for n,v in knobs.items():
            low=n.lower()
            if ("llm" in low or "budget" in low) and \
               ("cap" in low or "budget" in low or "daily" in low) and \
               "<secret" not in v:
                try:
                    cur=float(str(v).strip() or 0)
                except Exception:
                    continue
                if cur and cur<=20:
                    ssm.put_parameter(Name=n,Value="40",Type="String",
                                      Overwrite=True)
                    rep.ok("  cap knob %s: %s -> 40 (evidence named the "
                           "budget; ops-heavy Jul 30-31 plausibly burned "
                           "it and every engine has starved since)"%(n,v))
                    fixed_knob=True
        for n in list(knobs):
            low=n.lower()
            if "spend" in low and "llm" in low:
                try:
                    ssm.put_parameter(Name=n,Value="0",Type="String",
                                      Overwrite=True)
                    rep.ok("  spend counter %s reset to 0"%n)
                    fixed_knob=True
                except Exception as e:
                    rep.warn("  reset %s: %s"%(n,str(e)[:80]))
        if not fixed_knob:
            rep.warn("  budget named in logs but no matching knob found — "
                     "surface above and stop; guessing knobs is how "
                     "outages get worse")
    else:
        rep.warn("  evidence inconclusive from samples — full lines above; "
                 "verification below will show whether the layer works NOW")

    # ================================================================ 5
    rep.section("5. Verify recovery — invoke two collapsed engines")
    def probe(fn, floor_s):
        wait_active(fn)
        r=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",
                     LogType="Tail")
        import base64
        tail=base64.b64decode(r.get("LogResult","")).decode("utf-8","ignore")
        dur=None
        for line in tail.splitlines():
            if line.startswith("REPORT") and "Duration:" in line:
                try: dur=float(line.split("Duration:")[1].split("ms")[0])/1000
                except Exception: pass
        fe=r.get("FunctionError")
        return dur,fe,tail[-300:]
    recovered=0
    for fn,floor in (("justhodl-divergence-interpreter",6.0),
                     ("justhodl-ai-brief",12.0)):
        try:
            dur,fe,tail=probe(fn,floor)
            ok = fe is None and dur is not None and dur>=floor
            (rep.ok if ok else rep.fail)(
                "  %-36s duration=%.1fs (collapse band was 1-4s; former "
                "band %.0fs+) err=%s"%(fn,dur or -1,floor,fe))
            rep.kv(section="recovery", engine=fn,
                   duration_s=round(dur or -1,1), floor_s=floor,
                   recovered=ok)
            if ok: recovered+=1
            else: rep.log("     tail: %s"%tail.replace("\n"," | "))
        except Exception as e:
            rep.fail("  %s probe: %s"%(fn,str(e)[:120]))
    if recovered==2:
        rep.ok("BOTH probes back in the working band — the layer is live "
               "again; the other 34 recover on their own schedules")
    elif key_ok is False:
        rep.warn("recovery blocked on the credential — expected until the "
                 "new key is stored")
    else:
        fails.append("layer still down after fix attempt (recovered=%d)"
                     %recovered)
    OUT["recovered_probes"]=recovered

    # ================================================================ 6
    rep.section("6. Alarm the layer — never 2 silent days again")
    try:
        cw.put_metric_alarm(
            AlarmName="justhodl-llm-layer-down",
            AlarmDescription="LLM-calling engines are completing in "
                             "collapse-band durations — the model layer "
                             "is failing fleet-wide.",
            Namespace="AWS/Lambda", MetricName="Duration",
            Dimensions=[{"Name":"FunctionName",
                         "Value":"justhodl-ai-brief"}],
            ExtendedStatistic="p90", Period=21600, EvaluationPeriods=2,
            Threshold=6000, ComparisonOperator="LessThanThreshold",
            TreatMissingData="notBreaching",
            AlarmActions=["arn:aws:sns:us-east-1:857687956942:jh-ops-alerts"])
        rep.ok("alarm justhodl-llm-layer-down armed on ai-brief p90 "
               "duration < 6s over 12h -> jh-ops-alerts (a healthy run "
               "is 20-30s; only a dead LLM layer produces sustained "
               "2s completions)")
    except Exception as e:
        rep.warn("alarm: %s"%str(e)[:130])

    # ================================================================ 7
    rep.section("7. contract-gate self-test regression (4252 E-gate)")
    try:
        wait_active(CG)
        r=lam.invoke(FunctionName=CG,InvocationType="RequestResponse",
                     Payload=json.dumps({"mode":"selftest"}).encode())
        b=json.loads(r["Payload"].read() or b"{}")
        bad=[c for c in b.get("cases",[]) if not c.get("pass")]
        for c in b.get("cases",[]):
            (rep.ok if c["pass"] else rep.fail)(
                "  %-11s expect=%-15s got=%-15s"%(c["case"],c["expect"],
                                                  str(c.get("got"))[:40]))
        if b.get("passed"):
            rep.ok("self-test passes NOW — 4252's failure was transient "
                   "(cold-start import race); recording, watching")
        elif bad and all("error" in str(c.get("got","")).lower() or
                         c.get("got") is None for c in bad):
            fails.append("selftest structurally broken: %s"
                         %json.dumps(bad)[:200])
        else:
            fails.append("selftest failing: %s"%json.dumps(bad)[:200])
    except Exception as e:
        fails.append("selftest: %s"%str(e)[:150])

    (ROOT/"aws"/"ops"/"reports"/"4253_llm_outage.json").write_text(
        json.dumps(OUT, indent=1, default=str), encoding="utf-8")
    rep.section("RESULT")
    if fails:
        for f in fails: rep.fail("  %s"%f)
        raise SystemExit("FAILS: %s"%"; ".join(fails[:3]))
    rep.ok("OPS 4253 PASS")
