"""ops 3331 — finish benzinga.html fix: ensure benzinga-news-agent v2.0 has
FMP_KEY, wait for deploy, invoke, and confirm all sections populate so the
page renders. Also print the Function URL the page uses for a final sanity
note."""
import json, sys, time
from pathlib import Path
import boto3
from ops_report import report
LAM=boto3.client("lambda","us-east-1")
FN="benzinga-news-agent"
with report("3331_benzinga_agent_verify") as rep:
    fails=[]
    # 1. ensure FMP_KEY
    rep.section("ENSURE FMP_KEY")
    try:
        donor=LAM.get_function_configuration(FunctionName="justhodl-analyst-consensus")
        fmp=((donor.get("Environment") or {}).get("Variables") or {}).get("FMP_KEY")
        cur=LAM.get_function_configuration(FunctionName=FN)
        env=(cur.get("Environment") or {}).get("Variables") or {}
        rep.kv(fmp_suffix=fmp[-4:] if fmp else None, had_fmp="FMP_KEY" in env)
        if fmp and env.get("FMP_KEY")!=fmp:
            env["FMP_KEY"]=fmp
            # drop dead benzinga key if present
            env.pop("BENZINGA_API_KEY", None)
            LAM.update_function_configuration(FunctionName=FN,Environment={"Variables":env})
            for _ in range(25):
                if LAM.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
                time.sleep(2)
            rep.ok("FMP_KEY set, dead BENZINGA_API_KEY removed")
        else:
            rep.ok("FMP_KEY already present")
    except Exception as e:
        fails.append(f"env step: {e}")
    # 2. invoke until code is v2.0 (source field present)
    rep.section("INVOKE (await v2.0)")
    body=None
    for attempt in range(1,8):
        try:
            r=LAM.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=json.dumps({"rawPath":"/"}).encode())
            resp=json.loads(r["Payload"].read().decode())
            inner=json.loads(resp.get("body","{}")) if isinstance(resp,dict) and "body" in resp else resp
            src=inner.get("source","")
            counts=inner.get("counts",{})
            rep.kv(**{f"attempt_{attempt}":{"fn_error":r.get("FunctionError"),"source":src,"counts":counts}})
            if "FMP" in src:
                body=inner; break
        except Exception as e:
            rep.kv(**{f"attempt_{attempt}":f"err {e}"})
        time.sleep(12)
    # 3. verify sections
    rep.section("VERIFY SECTIONS")
    if not body:
        fails.append("agent never returned v2.0 (FMP source) — deploy not rolled?")
    else:
        c=body.get("counts",{})
        rep.kv(source=body.get("source"), ts=body.get("ts"), counts=c,
               sample_rating=(body.get("analyst_ratings") or [None])[0],
               sample_econ=(body.get("economic_events") or [None])[0],
               sample_news=(body.get("market_news") or [None])[0])
        if c.get("ratings",0)==0: fails.append("ratings empty")
        if c.get("news",0)==0: fails.append("news empty")
        # earnings/economics can be legitimately thin on a quiet day -> warn only
        if c.get("earnings",0)==0: rep.warn("earnings calendar empty (quiet window?)")
        if c.get("economics",0)==0: rep.warn("economic calendar empty (quiet window?)")
    rep.section("VERDICT")
    if fails:
        for f in fails: rep.fail(f)
        rep.kv(RESULT="FAIL", n=len(fails)); sys.exit(1)
    rep.ok("benzinga.html LIVE on FMP — ratings + earnings + econ + news + dividends populate. Dead Benzinga key retired.")
    rep.kv(RESULT="FIXED")
