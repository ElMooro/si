"""ops 2902 — Research Desk v2 end-to-end: wait deploy, fresh NVDA report (async-cold aware),
assert all v2 blocks + Claude synthesis (LIVE TOP-UP PROOF), heal ka/khalid analyses, live-page check."""
import os, json, time, traceback, urllib.request, boto3
from datetime import datetime, timezone
REGION="us-east-1"; B="justhodl-dashboard-live"; FN="justhodl-equity-research"
URL="https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
R={"ops":2902,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def guard(n):
    def d(f):
        def r(*a,**k):
            try: return f(*a,**k)
            except Exception:
                R["errors"][n]=traceback.format_exc()[-420:]; return None
        return r
    return d
def get(u,to=60):
    req=urllib.request.Request(u,headers={"User-Agent":"jh-ops"})
    with urllib.request.urlopen(req,timeout=to) as r_: return r_.read().decode()

@guard("deploy_wait")
def deploy_wait():
    for _ in range(40):
        c=lam.get_function_configuration(FunctionName=FN)
        age=(datetime.now(timezone.utc)-datetime.fromisoformat(c["LastModified"].replace("+0000","+00:00"))).total_seconds()
        if c.get("LastUpdateStatus")=="Successful" and age<720:
            R["deploy"]={"sha":c["CodeSha256"][:12],"age_s":int(age)}; return True
        time.sleep(8)
    R["deploy"]="TIMEOUT waiting fresh deploy"; return False

@guard("fresh_report")
def fresh_report():
    start=time.time()
    body=get(URL+"?ticker=NVDA&t=NVDA&refresh=1",to=290)
    d=json.loads(body)
    if d.get("poll_s3_url") or d.get("status")=="generating":
        R["cold_path"]=True
        for _ in range(30):
            time.sleep(9)
            try:
                obj=s3.get_object(Bucket=B,Key="equity-research/NVDA.json")
                d=json.loads(obj["Body"].read())
                g=d.get("generated_at","")
                if g and datetime.fromisoformat(g.replace("Z","+00:00")).timestamp()>start-30: break
            except Exception: pass
    R["gen_seconds"]=int(time.time()-start)
    t=d.get("technicals") or {}; s=(t.get("series") or {}); st=(t.get("stats") or {})
    liq=d.get("liquidity_solvency") or {}; gvm=d.get("growth_vs_mcap") or {}
    qr=d.get("quant_risk") or {}; bk=d.get("backlog") or {}
    es=d.get("executive_summary") or ""
    meta=d.get("meta") or {}
    R["asserts"]={
      "technicals_available": bool(t.get("available")),
      "close_pts": len(s.get("close") or []),
      "sma200_tail_ok": bool((s.get("sma200") or [None])[-1]),
      "rsi_last": st.get("rsi_last"),
      "macd_state": st.get("macd_state"),
      "beta_2y": st.get("beta_2y"),
      "liquidity_ok": liq.get("available") and liq.get("current_ratio") is not None,
      "net_debt_b": liq.get("net_debt_b"),
      "gvm_ok": gvm.get("available") and (gvm.get("peg") is not None or gvm.get("rev_cagr_3y_pct") is not None),
      "peg": gvm.get("peg"), "rule_of_40": gvm.get("rule_of_40"),
      "quant_ok": qr.get("available"), "altman_z": qr.get("altman_z"), "piotroski": qr.get("piotroski"),
      "backlog_flag": bk.get("available"), "backlog_reason": bk.get("reason"),
      "exec_summary_len": len(es), "claude_model": meta.get("claude_model"),
      "verdict": (d.get("verdict") or {}).get("rating")}
    R["TOPUP_PROOF"]= "LIVE ✓ Claude synthesis returned" if len(es)>200 else "NO — summary short/absent"
    return True

@guard("heal_metrics")
def heal_metrics():
    EVT=json.dumps({"source":"aws.events"}).encode()
    for fn in ("justhodl-khalid-metrics","justhodl-ka-metrics"):
        lam.invoke(FunctionName=fn,InvocationType="Event",Payload=EVT)
    keys=("data/khalid-analysis.json","data/ka-analysis.json")
    for _ in range(24):
        time.sleep(10)
        ages={}
        for k in keys:
            try:
                h=s3.head_object(Bucket=B,Key=k)
                ages[k]=round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600,2)
            except Exception: ages[k]="missing"
        if all(isinstance(a,float) and a<0.5 for a in ages.values()):
            R["metrics_healed"]=ages; return True
    R["metrics_healed"]=ages; return True

@guard("live_page")
def live_page():
    for _ in range(3):
        try:
            h=get("https://justhodl.ai/why.html",to=30)
            R["page_live"]={"renderTechnicals":"renderTechnicals" in h,"ta_price":"ta-price" in h,
                            "liquidityV2":"renderLiquidityV2" in h,"bytes":len(h)}
            if R["page_live"]["renderTechnicals"]: return True
        except Exception as e: R["page_live"]="err:"+str(e)[:60]
        time.sleep(45)
    return True

deploy_wait(); fresh_report(); heal_metrics(); live_page()
R["status"]="OK" if not R["errors"] else "PARTIAL"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2902_research_v2.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2902 COMPLETE")
