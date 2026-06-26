import boto3, json, time, urllib.request
lam=boto3.client("lambda","us-east-1")
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-equity-research")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
URL="https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
def get(u,timeout=170):
    t=time.time()
    req=urllib.request.Request(u,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"})
    with urllib.request.urlopen(req,timeout=timeout) as r:
        return r.status, time.time()-t, r.read().decode("utf-8","replace")
# force fresh LDOS with the new code
s,dt,b=get(f"{URL}?ticker=LDOS&refresh=1")
print(f"LDOS refresh -> status={s} {dt:.1f}s")
d=json.loads(b)
es=(d.get("executive_summary") or "")
verdict=d.get("verdict") or {}
da=d.get("devils_advocate") or {}
meta=d.get("metadata") or {}
print("exec_summary FAILED?:", es.startswith("AI synthesis failed"), "| len:",len(es))
print("exec_summary[:180]:", es[:180])
print("verdict:", {k:verdict.get(k) for k in ("rating","conviction_grade","price_target_12m","upside_pct","verdict_rationale")})
print("DEVILS_ADVOCATE present:", bool(da), "| title:", da.get("title"))
print("  short_thesis[:200]:", str(da.get("short_thesis"))[:200])
print("  kill_points:", [ (kp.get('point'),kp.get('evidence')) for kp in (da.get('kill_points') or [])[:3]])
print("  what_bulls_underestimate:", str(da.get("what_bulls_underestimate"))[:140])
rf=d.get("risk_factors") or {}
print("risk_factors title:", rf.get("title"), "| n_risks:", len(rf.get("key_risks") or []))
print("model used:", meta.get("claude_model") or meta.get("model") or meta.get("ai_model"), "| from_cache:", d.get("from_cache"))
print("DONE 2248")
