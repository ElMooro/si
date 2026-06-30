"""ops 2570 — deploy thesis-engine v2, async invoke, verify enriched blocks."""
import boto3, json, io, zipfile, time, glob, os, datetime
lam = boto3.client("lambda", "us-east-1"); s3 = boto3.client("s3", "us-east-1")
FN = "justhodl-upside-thesis"
def ready():
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): return
        time.sleep(5)
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open("aws/lambdas/justhodl-upside-thesis/source/lambda_function.py").read())
    for p in glob.glob("aws/shared/*.py"): z.writestr(os.path.basename(p), open(p).read())
lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); ready()
# bump TOP_AI to 16
cfg = lam.get_function_configuration(FunctionName=FN); env = cfg.get("Environment",{}).get("Variables",{})
env["TOP_AI"] = "16"; lam.update_function_configuration(FunctionName=FN, Environment={"Variables": env}); ready()
print("deployed v2 (TOP_AI=16)")
lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
print("async invoked — polling…")
for i in range(13):
    time.sleep(20)
    try:
        out = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/upside-theses.json")["Body"].read())
        ga = out.get("generated_at","")
        try: age=int((datetime.datetime.now(datetime.timezone.utc)-datetime.datetime.fromisoformat(ga)).total_seconds())
        except: age=9999
        print(f"  poll {i}: v={out.get('version')} age~{age}s n_ai={out.get('n_ai')} n_cand={out.get('n_candidates')}")
        if out.get("version")=="2.0.0" and age<280 and (out.get("n_ai") or 0)>0:
            print("\n✓ ENGINE V2 LIVE — enriched theses")
            for t in out.get("top_ranked",[])[:3]:
                d=out["theses"][t]; sm=d["smart_money"]; val=d["valuation"]; dna=d["dna"]; ai=d.get("ai") or {}
                print(f"\n══ {t} ({d.get('name') or ''}) · disc {d['discovery_score']} ══")
                print(f"  scores: CANSLIM {d['canslim']['score']} SQGLP {d['sqglp']['score']} Lynch {d['lynch']['score']}")
                print(f"  🧬 DNA: {dna['archetype']} ({dna['fit_pct']}%) — {dna['examples']}")
                print(f"  🐋 smart money (score {sm['score']}): funds={sm['n_funds']} famous={sm['famous_funds']} insider={'yes' if sm['insider'] else 'no'} ark={sm['ark_funds']} congress={sm['congress']}")
                print(f"  💲 valuation: PE={val['pe']} PS={val['ps']} FCFyield={val['fcf_yield_pct']}% → {val['verdict']}")
                if d.get("catalyst"): print(f"  📅 catalyst: earnings {d['catalyst'].get('next_earnings')} ({d['catalyst'].get('days_to_earnings')}d)")
                if d.get("pattern"): print(f"  📐 pattern: {d['pattern'].get('pattern')} {d['pattern'].get('status')} q{d['pattern'].get('quality')}")
                if ai:
                    print(f"  🤖 {ai.get('headline')}")
                    print(f"     bull_target: {ai.get('bull_target')}")
                    print(f"     smart_money_read: {str(ai.get('smart_money_read'))[:140]}")
            break
    except Exception as e: print(f"  poll {i}: {str(e)[:50]}")
print("DONE 2570")
