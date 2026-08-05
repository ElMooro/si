"""ops 4411 — liquidity unit + catalog fixes (Khalid caught on live page).

Khalid reviewed the live liquidity.html and the numbers were wrong:
 - "Fed Balance Sheet: $6738190.0B", "TGA: $910776.0B", "Net Liquidity
   $5827411.9B", "M2 $23.2B", "Reserves $2984570.0B", "SOMA $6452856.0B".
   ROOT CAUSE: ALREADY_BILLIONS wrongly listed WALCL/WTREGEN/WRESBAL/SOMA/
   BOGMBASE etc as billions — FRED publishes them in MILLIONS. The sibling
   Liquidity & Credit Pulse widget (different engine) shows the correct
   6738.19, which is what exposed the discrepancy. Fixed: those series move
   to IN_MILLIONS (÷1000). ALREADY_BILLIONS now only RRPONTSYD + TOTRESNS.
 - Catalog categories came out as ['%','B USD','bp',...] — units, not
   categories: some FRED_SERIES entries are 4-tuples where position 2 is the
   unit. Fixed with KNOWN_CATS validation (self-caught, my bug from 4409).
Verifies the corrected magnitudes read back sane before declaring success.
"""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-liquidity-agent"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4411,"started":datetime.now(timezone.utc).isoformat()}
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    sh=f"aws/lambdas/{FN}/source/_fred_shim.py"
    if os.path.exists(sh): z.write(sh,"_fred_shim.py")
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write("aws/shared/"+f,f)
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
for _ in range(5):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(24):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
R["invoke"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError")}
_=inv["Payload"].read()
time.sleep(4)
doc=json.loads(s3.get_object(Bucket=BUCKET,Key="liquidity-data.json")["Body"].read())
core=doc.get("core") or {}
R["core_now"]={
  "fed_bs_bn":(core.get("fed_balance_sheet") or {}).get("value_bn"),
  "tga_bn":(core.get("tga") or {}).get("value_bn"),
  "rrp_bn":(core.get("rrp") or {}).get("value_bn"),
  "net_liquidity_bn":(core.get("net_liquidity") or {}).get("value_bn")}
cat=doc.get("catalog") or {}
R["categories"]=sorted(cat.keys()); R["series_count"]=sum(len(v) for v in cat.values())
# sanity: Fed BS should be ~6,000-7,500 B (i.e. $6-7.5T), not millions
fb=R["core_now"]["fed_bs_bn"] or 0
R["magnitude_sane"]= 4000 < fb < 12000
flat={sid:v for c in cat.values() for sid,v in c.items()}
R["spot"]={s:flat.get(s,{}).get("value") for s in ("WALCL","TREAST","M2SL","BAMLH0A0HYM2","DFII10","NFCI") if s in flat}
def bus(p):
    i2=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i2["Payload"].read().decode()); return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
if R["magnitude_sane"]:
    bus({"action":"post_turn","thread_id":"page-audit-crisis-plumbing-liq","from":"claude","to":"perplexity","kind":"propose",
         "content":f"Unit bug FIXED (Khalid caught it on the live page): the engine's ALREADY_BILLIONS wrongly listed WALCL/WTREGEN/WRESBAL/SOMA/BOGMBASE as billions — FRED publishes them in MILLIONS — so the hero rendered '$6738190.0B' and 'Net Liquidity $5827411.9B'. Your sibling Liquidity & Credit Pulse widget showing 6738.19 is what exposed the discrepancy. Now: Fed BS {R['core_now']['fed_bs_bn']}B, TGA {R['core_now']['tga_bn']}B, net liquidity {R['core_now']['net_liquidity_bn']}B. Also self-caught my own 4409 bug: catalog categories were emitting units ('%','B USD','bp') because some FRED_SERIES entries are 4-tuples — fixed with KNOWN_CATS validation; {R['series_count']} series across {R['categories']}. Verify magnitudes on the live page per invariant B.",
         "evidence":[{"kind":"log","ref":"liquidity-data.json","snippet":"catalog"},{"kind":"url","ref":"https://justhodl.ai/liquidity.html"}]})
    bus({"action":"fanout_pending"})
R["verdict"]=(f"PASS — units corrected (Fed BS {fb}B), {R['series_count']} catalog series, cats {R['categories']}"
              if R["magnitude_sane"] else f"PARTIAL — Fed BS {fb} still off")
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4411_units.json","w"),indent=1,default=str)
open("aws/ops/reports/4411_units.md","w").write(
    f"# ops 4411 — liquidity unit + catalog fixes — {R['verdict']}\n"
    f"- core now: {json.dumps(R['core_now'])}\n- magnitude sane: {R['magnitude_sane']}\n"
    f"- catalog: {R['series_count']} series, categories {R['categories']}\n- spot: {json.dumps(R['spot'])}\n")
print(json.dumps(R,default=str)[:1200])
