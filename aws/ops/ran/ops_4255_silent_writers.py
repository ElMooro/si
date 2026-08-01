"""
ops_4255 v2 — silent-writer forensics, rebuilt after v1 was CANCELLED.

WHY v1 DIED
run-ops.yml carried timeout-minutes: 30 — never hit before because ops
finish in minutes. v1 probed 33 engines SEQUENTIALLY with a per-engine
wait of up to 120s for slow ones; a handful of those alone blew the
ceiling and GitHub killed the job at ~30min with nothing committed.

v2 STRUCTURE (fits inside any ceiling, and survives being killed)
  Phase 1  static evidence for all pairs — deployed-zip vs repo drift,
           30h log excerpts around the key, successor-key scan. Deploy
           drift is repaired inline (redeploy from repo).
  Phase 2  probes: fast engines (Timeout<=90s) get a RequestResponse
           with tail; everything slower is FIRED ASYNC IMMEDIATELY —
           no per-engine sleeping. One shared 150s settle after the
           volley, then every artifact is head-checked at once.
  Cursor   per-engine results persist to S3 as they land, so a killed
           run RESUMES instead of restarting — the same lesson the
           fundamental-census fix taught in ops 4229.
"""
import base64, io, json, os, re, time, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FAST = Config(retries={"max_attempts": 4, "mode": "adaptive"}, read_timeout=100)
lam = boto3.client("lambda", region_name=REGION, config=FAST)
logs = boto3.client("logs", region_name=REGION, config=FAST)
s3 = boto3.client("s3", region_name=REGION, config=FAST)
NOW = datetime.now(timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
PROG_KEY = "data/_state/ops4255-progress.json"

def load_pairs():
    d = json.loads(s3.get_object(Bucket=BUCKET,
        Key="data/contract-violations.json")["Body"].read())
    p = json.loads(s3.get_object(Bucket=BUCKET,
        Key="config/artifact-producers.json")["Body"].read()
        ).get("producers", {})
    out = []
    for v in d.get("violations", []):
        if v["cls"] != "STALE":
            continue
        w = (p.get(v["artifact"]) or {}).get("writers") or []
        if w:
            out.append((v["artifact"], w[0]))
    return sorted(set(out))

def deployed_src(fn):
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    z = zipfile.ZipFile(io.BytesIO(urlopen(loc, timeout=60).read()))
    best = ""
    for n in z.namelist():
        if n.endswith(".py"):
            t = z.read(n).decode("utf-8", "ignore")
            if "lambda_handler" in t and len(t) > len(best):
                best = t
    return best

def writes_key(src, base):
    for m in re.finditer(re.escape(base), src):
        w = src[max(0, m.start()-500):m.start()+500]
        if "put_object" in w or re.search(
                r"(OUT_KEY|S3_KEY|KEY)\s*=\s*[\"'][^\"']*"+re.escape(base), w):
            return True
    return False

def zip_repo(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, src))
        if os.path.isdir("aws/shared"):
            for f in sorted(os.listdir("aws/shared")):
                if f.endswith(".py"):
                    z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()

def wait_active(fn, b=150):
    t0 = time.time()
    while time.time()-t0 < b:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus") in (None,"Successful"):
                return True
        except Exception: pass
        time.sleep(4)
    return False

with report("4255_silent_writers") as rep:
    PAIRS = load_pairs()
    rep.heading("ops 4255 v2 — silent-writer forensics (%d pairs)" % len(PAIRS))
    try:
        prog = json.loads(s3.get_object(Bucket=BUCKET, Key=PROG_KEY)["Body"].read())
        if prog.get("date") != NOW.strftime("%Y%m%d"):
            prog = {}
    except Exception:
        prog = {}
    done = prog.get("engines", {})
    rep.log("resume cursor: %d pair(s) already evidenced today" % len(done))

    live = {}
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix="data/", Delimiter="/"):
        for o in page.get("Contents", []):
            live[o["Key"]] = o["LastModified"]

    rows = {}
    fired_async, fast_fns = [], []
    seen_fn = set()
    redeployed = []
    rep.section("Phase 1 — static evidence + drift repair")
    for key, fn in PAIRS:
        if key in done:
            rows[key] = done[key]; continue
        base = key.split("/")[-1]; stem = base.replace(".json", "")
        row = {"artifact": key, "writer": fn}
        try:
            h = live.get(key)
            row["age_h"] = round((NOW-h).total_seconds()/3600,1) if h else None
            dsrc = deployed_src(fn)
            rp = ROOT/"aws"/"lambdas"/fn/"source"/"lambda_function.py"
            rsrc = rp.read_text(errors="ignore") if rp.exists() else ""
            row["deployed_writes"] = writes_key(dsrc, base)
            row["repo_writes"] = writes_key(rsrc, base) if rsrc else None
            sib = [k for k,t in live.items() if k!=key and
                   stem[:max(6,len(stem)-4)] in k and
                   (NOW-t).total_seconds() < 48*3600]
            row["fresh_siblings"] = sib[:3]
            try:
                r = logs.filter_log_events(
                    logGroupName="/aws/lambda/%s"%fn,
                    startTime=int((NOW-timedelta(hours=30)).timestamp()*1000),
                    filterPattern='"%s"'%stem, limit=4)
                row["log_lines"] = [e["message"].strip().replace("\n"," | ")[:140]
                                    for e in r.get("events", [])][:2]
            except Exception:
                row["log_lines"] = []
            if row["repo_writes"] and not row["deployed_writes"]:
                rep.warn("DEPLOY-DRIFT %s <- %s — redeploying from repo" % (base, fn))
                wait_active(fn)
                lam.update_function_code(FunctionName=fn, ZipFile=zip_repo(fn))
                redeployed.append(fn)
        except Exception as e:
            row["static_error"] = str(e)[:110]
        rows[key] = row
    for fn in redeployed:
        wait_active(fn)

    rep.section("Phase 2 — probe volley")
    tails = {}
    for key, fn in PAIRS:
        if key in done or fn in seen_fn:
            continue
        seen_fn.add(fn)
        try:
            to = lam.get_function_configuration(FunctionName=fn).get("Timeout", 60)
            if to <= 90:
                fast_fns.append((fn, to))
            else:
                lam.invoke(FunctionName=fn, InvocationType="Event")
                fired_async.append(fn)
        except Exception as e:
            tails[fn] = ("CFG-ERR %s" % str(e)[:80], "cfg")
    rep.log("async volley fired: %d | fast RequestResponse queue: %d"
            % (len(fired_async), len(fast_fns)))
    for fn, to in fast_fns:
        try:
            r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                           LogType="Tail")
            tails[fn] = (base64.b64decode(r.get("LogResult","")
                         ).decode("utf-8","ignore"), r.get("FunctionError"))
        except Exception as e:
            tails[fn] = ("INVOKE-EXC %s" % str(e)[:90], "exc")
    if fired_async:
        rep.log("single settle wait 150s for the async volley…")
        time.sleep(150)

    rep.section("Classification")
    man = json.loads(s3.get_object(Bucket=BUCKET,
        Key="config/schedule-manifest.json")["Body"].read())
    def sched_inputs(fn):
        out=[]
        for r_ in man.get("rules",[])+man.get("schedules",[]):
            for t in r_.get("targets") or []:
                if (t.get("arn") or "").endswith(":"+fn) and t.get("input"):
                    out.append(t["input"][:80])
        return out[:2]
    counts={}; fixedn=0
    for key, fn in PAIRS:
        row = rows[key]
        if "class" in row:
            counts[row["class"]] = counts.get(row["class"],0)+1
            continue
        base = key.split("/")[-1]; stem=base.replace(".json","").lower()
        try:
            h = s3.head_object(Bucket=BUCKET, Key=key)
            moved = (NOW := datetime.now(timezone.utc)) and \
                (datetime.now(timezone.utc)-h["LastModified"]).total_seconds() < 900
        except Exception:
            moved = False
        tail, fe = tails.get(fn, ("", None))
        tl = tail.lower()
        if moved and row.get("repo_writes") and not row.get("deployed_writes"):
            cls="FIXED-DEPLOY-DRIFT"; fixedn+=1
        elif moved:
            row["sched_inputs"]=sched_inputs(fn)
            cls="SCHEDULE-INPUT" if row["sched_inputs"] else "WRITES-ON-PROBE"
        elif fe:
            cls="PROBE-ERRORED"
        elif stem in tl:
            for line in tail.splitlines():
                if stem in line.lower():
                    row["probe_line"]=line.strip()[:140]; break
            if any(w in tl for w in ("skip","unchanged","no data","empty",
                                     "not due","cached")):
                cls="CONDITIONAL-SKIP"
            elif any(w in tl for w in ("error","exception","traceback","denied")):
                cls="WRITE-ERROR"
            else:
                cls="MENTIONS-NO-WRITE"
        elif row.get("repo_writes") is False and not row.get("deployed_writes"):
            cls="NOT-THE-WRITER"
        elif row.get("fresh_siblings"):
            cls="SUPERSEDED?"
        elif fn in fired_async:
            cls="ASYNC-NO-WRITE"
        else:
            cls="CODE-PATH-SILENT"
        row["moved_on_probe"]=moved; row["probe_error"]=fe; row["class"]=cls
        counts[cls]=counts.get(cls,0)+1
        mark = rep.ok if cls.startswith(("FIXED","WRITES")) else (
            rep.warn if cls in ("SUPERSEDED?","NOT-THE-WRITER",
                                "CONDITIONAL-SKIP") else rep.fail)
        mark("%-38s %-28s %-18s %s"
             % (base[:38], fn[:28], cls,
                (row.get("probe_line") or ";".join(row.get("log_lines",[]))[:70]
                 or ";".join(row.get("fresh_siblings",[]))[:60])[:72]))
        rep.kv(section="engine", **{k:(json.dumps(v)[:140]
               if isinstance(v,(list,dict)) else v) for k,v in row.items()})
        done[key]=row
        s3.put_object(Bucket=BUCKET, Key=PROG_KEY,
                      Body=json.dumps({"date":datetime.now(timezone.utc
                      ).strftime("%Y%m%d"),"engines":done}).encode(),
                      ContentType="application/json")

    rep.section("MATRIX")
    for c,n in sorted(counts.items(), key=lambda x:-x[1]):
        rep.log("  %-22s %d"%(c,n)); rep.kv(section="matrix",cls=c,count=n)
    rep.log("auto-repaired deploy-drift, artifact verified moving: %d"%fixedn)
    (ROOT/"aws"/"ops"/"reports"/"4255_silent_writers.json").write_text(
        json.dumps({"ops":4255,"matrix":counts,
                    "engines":list(done.values())},indent=1,default=str),
        encoding="utf-8")
    rep.section("RESULT")
    rep.ok("OPS 4255 v2 PASS — matrix complete, cursor-resumable")
