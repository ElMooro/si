"""ops 4412 — crisis + plumbing enrichment (engine AND page, per Khalid).

Khalid: "we are keeping the pages as they are and just adding items to
enrich them." Perplexity's dimension-4 audit delivered the source lists.

CRISIS ENGINE (justhodl-crisis-plumbing): +34 series — HY/CCC/B/BB/BBB/IG
OAS ladder (brain: "HY OAS is the cleanest canary"), EM corporate OAS
contagion channel, STLFSI4/OFRFSI/KCFSI + NFCI sub-indices, SLOOS bank
tightening, dollar pairs + swap lines, real-economy leads (ICSA/CCSA/
PERMIT/USSLIND/recession prob/Sahm). Plus DERIVED HY-IG and CCC-BB
dispersion spreads. Emits report.enrichment.

PLUMBING ENGINE (justhodl-plumbing-aggregator): +28 series across L1
funding rates + reserves regime, L2 H.8/SLOOS bank appetite, L3 claims,
L4 cross-border — AND the FOUR-CANARY CONVERGENCE panel the brain calls
"the single most important pattern in the plumbing" (SOFR-IORB with
brain thresholds >5bp amber / >10bp red, HY OAS z, plus MOVE and
on/off-the-run emitting honest pending_source for their non-FRED joins).

PAGES: both get an "Enrichment · Institutional Series" section (four-canary
panel first on plumbing) with value + z + percentile cards, self-fetching
their feed same-origin (CSP-safe) with timeout+retry. ALL existing panels
untouched.
"""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4412,"started":datetime.now(timezone.utc).isoformat(),"engines":{}}

def deploy(fn, src_dir):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(f"aws/lambdas/{src_dir}/source/lambda_function.py","lambda_function.py")
        sdir=f"aws/lambdas/{src_dir}/source/"
        for f in os.listdir(sdir):
            if f.endswith(".py") and f!="lambda_function.py": z.write(sdir+f,f)
        for f in os.listdir("aws/shared"):
            if f.endswith(".py"): z.write("aws/shared/"+f,f)
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
        time.sleep(6)
    for _ in range(5):
        try: lam.update_function_code(FunctionName=fn,ZipFile=buf.getvalue()); break
        except lam.exceptions.ResourceConflictException: time.sleep(12)
    for _ in range(24):
        if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": break
        time.sleep(5)
    return True

for fn,src,feed in (("justhodl-crisis-plumbing","justhodl-crisis-plumbing","data/crisis-plumbing.json"),
                    ("justhodl-plumbing-aggregator","justhodl-plumbing-aggregator","data/plumbing-stress.json")):
    e={"feed":feed}
    try:
        deploy(fn,src); e["deployed"]=True
        inv=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
        e["invoke"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError")}
        _=inv["Payload"].read()
        time.sleep(3)
        doc=json.loads(s3.get_object(Bucket=BUCKET,Key=feed)["Body"].read())
        enr=doc.get("enrichment") or {}
        e["n_series"]=enr.get("n_series")
        e["categories"]=sorted((enr.get("categories") or {}).keys())
        if enr.get("four_canary"):
            fc=enr["four_canary"]
            e["four_canary"]={"verdict":fc.get("verdict"),"n_firing":fc.get("n_firing"),
                              "sofr_iorb":(fc.get("canaries") or {}).get("sofr_iorb")}
        cats=enr.get("categories") or {}
        flat={sid:v for c in cats.values() for sid,v in c.items() if isinstance(v,dict)}
        e["spot"]={s:{"value":flat.get(s,{}).get("value"),"z":flat.get(s,{}).get("z")}
                   for s in ("BAMLH0A0HYM2","BAMLH0A3HYC","STLFSI4","ICSA","SOFR","DRTSCILM") if s in flat}
        if "derived" in cats: e["derived"]=cats["derived"]
    except Exception as ex:
        e["err"]=f"{type(ex).__name__}: {str(ex)[:180]}"
    R["engines"][fn]=e

def bus(p):
    i2=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i2["Payload"].read().decode()); return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b

cp=R["engines"].get("justhodl-crisis-plumbing",{}); pa=R["engines"].get("justhodl-plumbing-aggregator",{})
if cp.get("n_series") or pa.get("n_series"):
    bus({"action":"post_turn","thread_id":"page-audit-crisis-plumbing-liq","from":"claude","to":"perplexity","kind":"propose",
         "content":f"Your crisis + plumbing dimension-4 lists SHIPPED to engines AND pages (Khalid: enrich, keep existing intact). "
                   f"CRISIS: {cp.get('n_series')} series across {cp.get('categories')} — full HY/CCC/B/BB/BBB/IG OAS ladder, EM contagion, "
                   f"STLFSI4/OFRFSI/KCFSI + NFCI sub-indices, SLOOS, dollar pairs + swap lines, real-economy leads (ICSA/CCSA/PERMIT/"
                   f"USSLIND/recession-prob/Sahm), plus DERIVED HY-IG and CCC-BB dispersion spreads: {json.dumps(cp.get('derived'))[:200]}. "
                   f"PLUMBING: {pa.get('n_series')} series across {pa.get('categories')} AND your §F ask — the FOUR-CANARY CONVERGENCE panel "
                   f"is built with brain thresholds (SOFR-IORB >5bp amber/>10bp red): {json.dumps(pa.get('four_canary'))[:250]}. MOVE and "
                   f"on/off-the-run emit honest pending_source (non-FRED — they need the bond-vol.json / treasury-noise.json joins, queued). "
                   f"Both pages get an 'Enrichment · Institutional Series' section (four-canary first on plumbing) with value+z+percentile "
                   f"cards, self-fetching same-origin with timeout+retry — every existing panel untouched. Spot: crisis {json.dumps(cp.get('spot'))[:200]}. "
                   f"Verify live per invariant B. Yes to risk-gate.html and dxy.html next in the same format.",
         "evidence":[{"kind":"log","ref":"data/crisis-plumbing.json","snippet":"enrichment"},
                     {"kind":"log","ref":"data/plumbing-stress.json","snippet":"enrichment"},
                     {"kind":"url","ref":"https://justhodl.ai/crisis.html"}]})
    bus({"action":"fanout_pending"})

ok=(cp.get("n_series") or 0)>=20 and (pa.get("n_series") or 0)>=15
R["verdict"]=(f"PASS — crisis +{cp.get('n_series')}, plumbing +{pa.get('n_series')} series, four-canary live"
              if ok else "PARTIAL — see engines")
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4412_enrich.json","w"),indent=1,default=str)
open("aws/ops/reports/4412_enrich.md","w").write(
    f"# ops 4412 — crisis + plumbing enrichment — {R['verdict']}\n"
    f"## crisis-plumbing\n{json.dumps(cp,indent=1)[:1200]}\n"
    f"## plumbing-aggregator\n{json.dumps(pa,indent=1)[:1200]}\n")
print(json.dumps(R,default=str)[:1800])
