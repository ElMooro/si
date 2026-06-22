import json, urllib.request, time, boto3
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def gj(u):
    for _ in range(3):
        try:
            with urllib.request.urlopen(u,timeout=30) as r: return json.loads(r.read())
        except Exception as e: time.sleep(2); err=str(e)[:50]
    return {"_err":err}

print("=== INPUT ENGINE FRESHNESS (fusion sources) ===")
for k in ["data/revenue-acceleration.json","data/supply-inflection.json","data/bagger-engine.json","data/ai-rerating-radar.json"]:
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        ts=d.get("generated_at") or d.get("generated") or "?"
        # find a list to size
        n=next((len(v) for v in d.values() if isinstance(v,list)),"?")
        print(f"  {k.split('/')[-1]:<28} asof={str(ts)[:16]}  (a list has {n})")
    except Exception as e: print(f"  {k.split('/')[-1]:<28} MISSING/{str(e)[:30]}")

def discriminators(sym):
    inc=gj(f"https://financialmodelingprep.com/stable/income-statement?symbol={sym}&period=quarter&limit=12&apikey={FMP}")
    if not isinstance(inc,list) or not inc: return None
    rows=list(reversed(inc))
    gm=[]; om=[]; eps=[]; rev=[]
    for r in rows:
        rv=r.get("revenue") or 0
        if rv<=0: continue
        gm.append((r.get("grossProfit") or 0)/rv*100)
        om.append((r.get("operatingIncome") or 0)/rv*100)
        eps.append(r.get("epsdiluted") or r.get("eps") or 0)
        rev.append(rv)
    if len(gm)<6: return None
    ttm_eps=[sum(eps[max(0,i-3):i+1]) for i in range(len(eps))]
    return {
      "gm_trough": round(min(gm),1), "gm_now": round(gm[-1],1),
      "om_trough": round(min(om),1), "om_now": round(om[-1],1),
      "om_swing_pp": round(gm[-1]-min(om),1) if False else round(om[-1]-min(om),1),
      "ttmEPS_trough": round(min(ttm_eps),2), "ttmEPS_now": round(ttm_eps[-1],2),
      "eps_neg_to_pos": (min(ttm_eps)<0 and ttm_eps[-1]>0),
      "rev_scaling_x": round(rev[-1]/min(rev),2),
    }

print("\n=== DISCRIMINATOR CALIBRATION ===")
print(f"{'sym':<6}{'gm_trough':>10}{'gm_now':>8}{'om_trough':>10}{'om_now':>8}{'om_swing':>9}{'epsTr':>8}{'epsNow':>8}{'neg2pos':>8}{'revX':>6}  verdict")
POS=["MU","SNDK"]; NEG=["AVGO","AMD","TXN","KLAC"]
for s in POS+NEG:
    d=discriminators(s)
    if not d: print(f"{s:<6} no data"); continue
    # crude coil+violence verdict for calibration eyeballing
    coil = d["gm_trough"]<5 or d["om_trough"]<-10
    violence = d["om_swing_pp"]>=40
    spring = coil and violence and d["eps_neg_to_pos"]
    tag = "CYCLICAL-20x SHAPE" if spring else ("partial" if (coil or violence) else "no-coil (secular/stable)")
    print(f"{s:<6}{d['gm_trough']:>10}{d['gm_now']:>8}{d['om_trough']:>10}{d['om_now']:>8}{d['om_swing_pp']:>9}{d['ttmEPS_trough']:>8}{d['ttmEPS_now']:>8}{str(d['eps_neg_to_pos']):>8}{d['rev_scaling_x']:>6}  [{'POS' if s in POS else 'NEG'}] {tag}")
print("\nRULE BEING TESTED: coil(gm_trough<5 OR om_trough<-10) AND violence(om_swing>=40pp) AND eps_neg_to_pos")
print("PASS if MU/SNDK=CYCLICAL-20x SHAPE and AVGO/AMD/TXN/KLAC do NOT")
print("DONE 2112")
