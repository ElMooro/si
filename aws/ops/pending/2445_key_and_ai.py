import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
SRC=["justhodl-ai-chat","justhodl-buyback-scanner","justhodl-morning-intelligence"]
want=["ANTHROPIC_API_KEY","FMP_KEY"]
found={}
for fn in SRC:
    try:
        e=lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
        for k in want:
            if k not in found and e.get(k): found[k]=e[k]
    except Exception as ex: print("src",fn,"err",str(ex)[:40])
print("keys located:",{k:(v[:8]+"..."+v[-4:]) for k,v in found.items()})
if "ANTHROPIC_API_KEY" not in found:
    print("ANTHROPIC_API_KEY not found on any source fn"); print("DONE 2445"); raise SystemExit
# merge into signal-backtest env
cur=lam.get_function_configuration(FunctionName="justhodl-signal-backtest").get("Environment",{}).get("Variables",{})
cur.update(found)
lam.update_function_configuration(FunctionName="justhodl-signal-backtest",Environment={"Variables":cur})
import botocore
waiter=lam.get_waiter("function_updated"); waiter.wait(FunctionName="justhodl-signal-backtest")
print("env updated; now has ANTHROPIC:",bool(cur.get("ANTHROPIC_API_KEY")))
# re-invoke + verify AI
lam.invoke(FunctionName="justhodl-signal-backtest",InvocationType="Event",Payload=b"{}")
print("re-invoked; waiting 185s..."); time.sleep(185)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/signal-backtest.json")["Body"].read())
ai=d.get("ai_analysis") or {}
print("\n=== AI ANALYSIS (v%s) ==="%d.get("version"))
if ai.get("_skip") or ai.get("_error"): print("AI still failed:",ai)
else:
    print("HEADLINE:",ai.get("headline"))
    print("DIAGNOSIS:",ai.get("diagnosis"))
    print("PATTERNS:"); [print("  -",p) for p in (ai.get("patterns") or [])]
    print("RECOMMENDATIONS:"); [print("  -",r) for r in (ai.get("recommendations") or [])]
    vn=ai.get("verdict_notes") or {}
    print("VERDICT NOTES:"); [print("  %s: %s"%(k,v)) for k,v in list(vn.items())[:6]]
    print("model:",ai.get("model"),"parse_error:",ai.get("_parse_error"))
print("DONE 2445")
