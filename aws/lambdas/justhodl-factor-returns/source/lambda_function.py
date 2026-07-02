"""justhodl-factor-returns — DAILY CROSS-SECTIONAL FACTOR DESK (stock-analysis pillar).

The equity stack scores names; nothing measured WHICH STYLE is being paid daily.
This engine computes real long-short factor returns every close from the full
FinViz Elite cross-section (~5k names, 4 stacked views), self-accumulates the
return history, and publishes crowding + a factor-rotation regime:

  MOMENTUM 12-1  Perf Year minus Perf Month
  VALUE          -ln(P/E) & -ln(P/B) composite
  QUALITY        ROE
  SIZE (SMB)     -ln(MktCap)  (long small / short big)
  LOW-VOL        -Volatility(M)

ls_ret_1d = mean(today %% change, top decile) - mean(bottom decile), deciles
formed on characteristics EXCLUDING today's move. Crowding = momentum-decile
share of universe dollar volume + its valuation stretch. Regime = 5d leader
ranking + MOMENTUM_CRASH / JUNK_RALLY flags. Distinct from factor-risk
(per-name loadings) and smart-beta (per-name composite): those score stocks;
this measures the FACTORS themselves. Output data/factor-returns.json +
history data/history/factor-returns.json. Consumers: signal-board, page.
"""
import json, math, time, statistics as st
from datetime import datetime, timezone
import boto3
import finviz as FV

BUCKET, OUT = "justhodl-dashboard-live", "data/factor-returns.json"
HIST = "data/history/factor-returns.json"
s3 = boto3.client("s3", region_name="us-east-1")
def _s3json(k, d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception: return d

def _z(vals):
    xs=[v for v in vals if v is not None]
    if len(xs)<50: return {}
    mu, sd = st.fmean(xs), (st.pstdev(xs) or 1)
    return {"mu":mu,"sd":sd}

def lambda_handler(event=None, context=None):
    t0=time.time()
    export_err=None
    try:
        uni=FV.build_universe()  # ONE custom export: whole universe, 72 parsed fields
    except Exception as e:
        uni={}; export_err=str(e)[:160]
    if not uni:
        # surface the real failure instead of a silent 0 (SSM token / auth / tier / block)
        try:
            FV.fetch_custom()
        except Exception as e:
            export_err=(export_err or "")+" | fetch_custom: "+str(e)[:160]
        print("[fv] EXPORT EMPTY:",export_err)
        return {"ok":False,"universe":0,"export_err":export_err}
    print("[fv] universe rows",len(uni))
    recs=[]
    for t,r in uni.items():
        px=r.get("price"); vol=r.get("volume") or r.get("avg_volume"); mc=r.get("market_cap")
        chg=r.get("change_pct") if r.get("change_pct") is not None else r.get("change")
        if not (px and vol and mc) or chg is None: continue
        if px<3 or mc<3e8 or vol<2e5: continue
        if (r.get("asset_type") or "").upper()=="ETF": continue
        pe=r.get("pe"); pb=r.get("pb")
        roe=r.get("roe")
        py=r.get("perf_y") if r.get("perf_y") is not None else r.get("perf_year")
        pm=r.get("perf_m") if r.get("perf_m") is not None else r.get("perf_month")
        volm=r.get("volatility_m_pct") if r.get("volatility_m_pct") is not None else (r.get("volatility_m") or r.get("volatility_month"))
        recs.append({"t":t,"chg":chg,"dv":px*vol,"mc":mc,
                     "mom":(py-pm) if (py is not None and pm is not None) else None,
                     "val_pe":-math.log(pe) if pe and 1<pe<300 else None,
                     "val_pb":-math.log(pb) if pb and 0.05<pb<80 else None,
                     "qual":roe if roe is not None and -100<roe<200 else None,
                     "size":-math.log(mc),
                     "lowvol":-volm if volm else None})
    n=len(recs)
    print("[fv] investable universe",n)
    zpe=_z([r["val_pe"] for r in recs]); zpb=_z([r["val_pb"] for r in recs])
    for r in recs:
        a=(r["val_pe"]-zpe["mu"])/zpe["sd"] if r["val_pe"] is not None and zpe else None
        b=(r["val_pb"]-zpb["mu"])/zpb["sd"] if r["val_pb"] is not None and zpb else None
        r["val"]=st.fmean([x for x in (a,b) if x is not None]) if (a is not None or b is not None) else None

    FACTORS={"MOMENTUM":"mom","VALUE":"val","QUALITY":"qual","SIZE":"size","LOWVOL":"lowvol"}
    out={}; total_dv=sum(r["dv"] for r in recs) or 1
    for name,key in FACTORS.items():
        pool=sorted([r for r in recs if r[key] is not None],key=lambda r:r[key])
        if len(pool)<300: out[name]={"status":"THIN","n":len(pool)}; continue
        d=max(30,len(pool)//10)
        bot,top=pool[:d],pool[-d:]
        ls=round(st.fmean(x["chg"] for x in top)-st.fmean(x["chg"] for x in bot),3)
        out[name]={"status":"OK","ls_ret_1d_pct":ls,"n":len(pool),"decile":d,
                   "top_ret":round(st.fmean(x["chg"] for x in top),3),
                   "bot_ret":round(st.fmean(x["chg"] for x in bot),3),
                   "top_names":[x["t"] for x in sorted(top,key=lambda r:-r["dv"])[:8]]}
        if name=="MOMENTUM":
            out[name]["crowding_dollar_share_pct"]=round(100*sum(x["dv"] for x in top)/total_dv,1)
            pes=[math.exp(-x["val_pe"]) for x in top if x["val_pe"] is not None]
            upe=[math.exp(-x["val_pe"]) for x in recs if x["val_pe"] is not None]
            if pes and upe:
                out[name]["top_decile_median_pe"]=round(sorted(pes)[len(pes)//2],1)
                out[name]["universe_median_pe"]=round(sorted(upe)[len(upe)//2],1)

    hist=_s3json(HIST,{}) or {}
    today=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist[today]={k:v.get("ls_ret_1d_pct") for k,v in out.items() if v.get("status")=="OK"}
    hist=dict(sorted(hist.items())[-750:])
    s3.put_object(Bucket=BUCKET,Key=HIST,Body=json.dumps(hist,separators=(",",":")).encode(),
                  ContentType="application/json")
    days=sorted(hist.items())
    for name,v in out.items():
        if v.get("status")!="OK": continue
        ser=[d[1].get(name) for d in days if d[1].get(name) is not None]
        v["cum_5d_pct"]=round(sum(ser[-5:]),2) if len(ser)>=2 else None
        v["cum_21d_pct"]=round(sum(ser[-21:]),2) if len(ser)>=5 else None
        v["z_1d"]=round((v["ls_ret_1d_pct"]-st.fmean(ser))/st.pstdev(ser),2) if len(ser)>=40 and st.pstdev(ser) else None

    ok={k:v for k,v in out.items() if v.get("status")=="OK"}
    lead=sorted(ok.items(),key=lambda kv:-(kv[1].get("cum_5d_pct") if kv[1].get("cum_5d_pct") is not None else kv[1]["ls_ret_1d_pct"]))
    flags=[]
    if ok.get("MOMENTUM",{}).get("ls_ret_1d_pct",0)<-1.5: flags.append("MOMENTUM_CRASH")
    if ok.get("QUALITY",{}).get("ls_ret_1d_pct",0)<-1.0 and ok.get("VALUE",{}).get("ls_ret_1d_pct",0)<-0.8: flags.append("JUNK_RALLY")
    if ok.get("LOWVOL",{}).get("ls_ret_1d_pct",0)>0.8: flags.append("DEFENSIVE_BID")
    regime={"leader":lead[0][0] if lead else None,"laggard":lead[-1][0] if lead else None,
            "ranking":[k for k,_ in lead],"flags":flags,
            "read":"%s leading / %s lagging%s"%(lead[0][0],lead[-1][0]," · "+" ".join(flags) if flags else "") if lead else "n/a"}

    doc={"engine":"justhodl-factor-returns","version":"1.0.0",
         "generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),
         "universe_n":n,"factors":out,"regime":regime,
         "history_days":len(hist),
         "chart":{k:[{"date":d,"value":round(sum(x[1].get(k) or 0 for x in days[:i+1]),2)} for i,(d,_) in enumerate(days)] for k in ("MOMENTUM","VALUE")} if len(days)>=2 else {},
         "method":("Daily long-short decile returns on the full FinViz Elite cross-section "
                   "(views 111/121/141/161 joined, %d investable after px>=3/mc>=300M/vol>=200k). "
                   "Characteristics exclude today's move; today's %%change measures the factor. "
                   "History self-accumulates (z@40d). MOM crowding = top-decile $vol share + "
                   "valuation stretch. Distinct from factor-risk loadings & smart-beta scores."%n)}
    s3.put_object(Bucket=BUCKET,Key=OUT,Body=json.dumps(doc,separators=(",",":")).encode(),
                  ContentType="application/json",CacheControl="public, max-age=1800")
    print("[factors]",{k:v.get("ls_ret_1d_pct") for k,v in ok.items()},"| regime:",regime["read"],"| %.0fs"%(time.time()-t0))
    return {"ok":True,"universe":n,"factors":{k:v.get("ls_ret_1d_pct") for k,v in ok.items()},"regime":regime["read"]}
