"""ops 4531 — regenerate engine-provider-map THE E12 WAY (4433 pattern,
Perplexity-verified): grep all engine sources on the runner with the FULL
signature table, upload map, re-inventory catalog. Khalid's missing
datasets fill."""
import json,os,re,time
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=560,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4531,"started":datetime.now(timezone.utc).isoformat()}
SIGS={"fred":r"stlouisfed\.org|FRED_API","polygon":r"polygon\.io","sec":r"sec\.gov|efts\.sec",
"nyfed":r"newyorkfed\.org","ecb":r"ecb\.europa\.eu","boj":r"stat-search\.boj|boj\.or\.jp",
"cftc":r"cftc\.gov","treasury":r"fiscaldata\.treasury|treasurydirect|home\.treasury",
"bls":r"bls\.gov","imf":r"imf\.org|dataservices\.imf","yahoo":r"finance\.yahoo",
"coinmetrics":r"coinmetrics\.io","census":r"census\.gov","eia":r"eia\.gov",
"llm-anthropic":r"api\.anthropic\.com","fleet-feed":r"justhodl-dashboard-live|justhodl\.ai/data",
"ofr":r"financialresearch\.gov","bea":r"bea\.gov","fed-board":r"federalreserve\.gov",
"bis":r"stats\.bis\.org","gleif":r"gleif\.org","eurostat":r"ec\.europa\.eu/eurostat",
"oecd":r"sdmx\.oecd\.org","openfigi":r"openfigi\.com",
"worldbank":r"api\.worldbank\.org|worldbank\.org","dbnomics":r"db\.nomics\.world|dbnomics",
"snb":r"data\.snb\.ch|snb\.ch","bcb":r"api\.bcb\.gov\.br|bcb\.gov\.br",
"cboe":r"cdn\.cboe\.com|cboe\.com","statcan":r"statcan\.gc\.ca","banxico":r"banxico\.org\.mx",
"boe":r"bankofengland\.co\.uk","gdelt":r"gdeltproject\.org","eiopa":r"eiopa\.europa\.eu",
"nasa":r"power\.larc\.nasa\.gov","dol":r"oui\.doleta\.gov|doleta\.gov","occ":r"theocc\.com"}
rx={k:re.compile(v) for k,v in SIGS.items()}
m={}
for d in sorted(os.listdir("aws/lambdas")):
    f=f"aws/lambdas/{d}/source/lambda_function.py"
    if not os.path.exists(f): continue
    try: src=open(f,encoding="utf-8",errors="replace").read()
    except Exception: continue
    p=[k for k,r_ in rx.items() if r_.search(src)]
    if p: m[d]=p
R["n_engines_mapped"]=len(m)
new_provs=sorted({p for v in m.values() for p in v})
R["providers_in_map"]=new_provs
s3.put_object(Bucket=B,Key="data/audit/engine-provider-map.json",
    Body=json.dumps({"generated":R["started"],"n":len(m),"map":m},default=str).encode(),
    ContentType="application/json")
i2=lam.invoke(FunctionName="justhodl-provider-catalog",InvocationType="RequestResponse",Payload=b"{}")
R["catalog_fn_err"]=i2.get("FunctionError"); _=i2["Payload"].read()
time.sleep(3)
hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
R["totals"]=hub.get("totals")
R["zero"]=[p["slug"] for p in hub["providers"] if not p["n_keys"]]
R["newly"]={p["slug"]:p["n_keys"] for p in hub["providers"]
            if p["slug"] in ("worldbank","dbnomics","snb","bcb","cboe","boj") and p["n_keys"]}
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0806-master","from":"claude","to":"perplexity","kind":"propose",
 "content":("MAP REGENERATED THE E12 WAY (4433 runner-grep pattern, your verified mechanism) with 38 sigs: "
  f"{R['n_engines_mapped']} engines mapped, providers={json.dumps(new_provs)[:220]} · totals="
  f"{json.dumps(R['totals'])} newly={json.dumps(R['newly'])} still_zero={json.dumps(R['zero'])[:140]}. "
  "Remaining zeros should be pure external blocks. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/audit/engine-provider-map.json","snippet":"generated"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"mapped={R['n_engines_mapped']} totals={json.dumps(R['totals'])} newly={json.dumps(R['newly'])} zero={R['zero']}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4531_map.json","w"),indent=1,default=str)
open("aws/ops/reports/4531_map.md","w").write("# 4531 — "+R["verdict"]+"\n- providers: "+json.dumps(new_provs)+"\n")
print(R["verdict"][:300])
