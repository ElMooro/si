"""ops 2906 — v2 fix re-verify: top-level v2 keys on fresh NVDA doc; ka/khalid Sonnet-4-6 healing."""
import os, json, time, urllib.request, boto3, traceback
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"
URL="https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
R={"ops":2906,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); logs=boto3.client("logs",region_name=REGION)
PUSH_TS=datetime.now(timezone.utc)
def wait_deploy(fn):
    for _ in range(50):
        c=lam.get_function_configuration(FunctionName=fn)
        lm=datetime.fromisoformat(c["LastModified"].replace("+0000","+00:00"))
        if lm>PUSH_TS and c.get("LastUpdateStatus")=="Successful": return True
        time.sleep(6)
    return False
def get(url,to=290):
    req=urllib.request.Request(url,headers={"User-Agent":"ops2906"})
    with urllib.request.urlopen(req,timeout=to) as r: return json.loads(r.read())
try:
    R["deploys"]="already-flipped (2905)"
    # fire metrics async first (they run while research generates)
    EVT=json.dumps({"source":"aws.events"}).encode()
    for fn in ("justhodl-khalid-metrics","justhodl-ka-metrics"):
        lam.invoke(FunctionName=fn,InvocationType="Event",Payload=EVT)
    t0=time.time()
    d=get(URL+"?ticker=NVDA&t=NVDA&refresh=1")
    if d.get("__http_error__"):
        R["http_error"]=d; raise RuntimeError("research URL error")
    if d.get("status")=="generating" or d.get("poll_s3_url"):
        start=datetime.now(timezone.utc)
        for _ in range(30):
            time.sleep(9)
            try:
                doc=json.loads(s3.get_object(Bucket=B,Key="equity-research/NVDA.json")["Body"].read())
                if doc.get("generated_at","")>PUSH_TS.isoformat(): d=doc; break
            except Exception: pass
    R["gen_seconds"]=int(time.time()-t0)
    t=d.get("technicals") or {}; s_=(t.get("series") or {}); st=(t.get("stats") or {})
    liq=d.get("liquidity_solvency") or {}; gvm=d.get("growth_vs_mcap") or {}
    qr=d.get("quant_risk") or {}; bk=d.get("backlog") or {}
    R["asserts"]={
      "schema": d.get("schema_version"),
      "technicals_available": bool(t.get("available")), "close_pts": len(s_.get("close") or []),
      "sma200_tail": (s_.get("sma200") or [None])[-1], "rsi_last": st.get("rsi_last"),
      "macd_state": st.get("macd_state"), "beta_2y": st.get("beta_2y"), "adv_musd": st.get("adv_dollar_3m_musd"),
      "liq_available": liq.get("available"), "current_ratio": liq.get("current_ratio"),
      "net_debt_b": liq.get("net_debt_b"), "liq_read": (liq.get("read") or "")[:60],
      "gvm_available": gvm.get("available"), "peg": gvm.get("peg"),
      "rev_cagr_3y": gvm.get("rev_cagr_3y_pct"), "rule_of_40": gvm.get("rule_of_40"),
      "quant_available": qr.get("available"), "altman_z": qr.get("altman_z"), "piotroski": qr.get("piotroski"),
      "backlog_flag": bk.get("available"), "backlog_note": (bk.get("reason") or bk.get("note") or "")[:70],
      "exec_len": len(d.get("executive_summary") or ""),
      "claude_model": (d.get("metadata") or {}).get("claude_model")}
    core=R["asserts"]
    R["V2_STATUS"]="PASS" if (core["technicals_available"] and core["close_pts"]>400 and core["liq_available"]
                              and core["gvm_available"] and core["quant_available"]) else "FAIL"
    # metrics healing poll
    def age(k):
        try:
            h=s3.head_object(Bucket=B,Key=k)
            return round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600,2)
        except Exception: return "missing"
    for _ in range(26):
        a1,a2=age("data/khalid-analysis.json"),age("data/ka-analysis.json")
        if isinstance(a1,float) and a1<0.4 and isinstance(a2,float) and a2<0.4: break
        time.sleep(12)
    R["metrics_ages"]={"khalid":age("data/khalid-analysis.json"),"ka":age("data/ka-analysis.json")}
    try:
        stm=logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-khalid-metrics",orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
        evs=logs.get_log_events(logGroupName="/aws/lambda/justhodl-khalid-metrics",logStreamName=stm[0]["logStreamName"],limit=14,startFromHead=False)["events"]
        R["khalid_tail"]=[e["message"].strip()[:150] for e in evs][-7:]
    except Exception as e: R["khalid_tail"]=[str(e)[:80]]
    R["status"]="OK"
except Exception:
    R["errors"]["main"]=traceback.format_exc()[-500:]; R["status"]="FAILED"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3400])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2906_v2_reverify.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2906 COMPLETE")
