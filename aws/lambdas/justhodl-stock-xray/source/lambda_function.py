"""justhodl-stock-xray — THE per-name institutional umbrella (Khalid's directive).

The fleet computes 20+ per-name signals across scattered engines; nothing joins
them into ONE card. This engine batch-builds a compact X-Ray card for every
liquid name nightly:

  BACKBONE   one FinViz Elite export (151 cols) -> price/MA stack (20/50/200),
             52w position, Weinstein stage, momentum 12-1, sector-relative
             valuation percentile, growth (sales QoQ), profitability +
             TURNING-PROFITABLE (loss-making now, forward-PE positive),
             ownership (inst/insider levels + transactions), short float.
  JOINS      master-ranker score/rank · equity-confluence composite ·
             resilience state · dark-pool xray_map (dp%%, accel, state,
             daily short-z, conviction, DIS flag) · factor-ranks memberships
             (MOM/VAL/QUAL/SIZE/LOWVOL decile tags) · estimate-revisions
             direction · best-setups tag · backlog/RPO · finra daily short%%.
  DERIVED BOARDS  multibagger_candidates · turning_profitable ·
             accumulation_leaders · dis_warnings · full_stack_highs ·
             laggards_watch (weak names whose supply-chain peers are strong).

Output data/stock-xray.json {boards, cards{T:{...}}}. Consumers: dossier.html
(rebuilt as Stock X-Ray), signal-board.
"""
import json, math, statistics as st
from datetime import datetime, timezone
import boto3
from finviz import build_universe

BUCKET, OUT = "justhodl-dashboard-live", "data/stock-xray.json"
s3 = boto3.client("s3", region_name="us-east-1")

def _j(k,d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET,Key=k)["Body"].read())
    except Exception: return d

def _tmap(doc, tick_keys=("ticker","symbol","sym","t"), depth=0, out=None):
    """Tolerant: harvest {TICKER: rowdict} from any feed shape."""
    if out is None: out={}
    if depth>7: return out
    if isinstance(doc,dict):
        for tk in tick_keys:
            t=doc.get(tk)
            if isinstance(t,str) and 1<=len(t)<=6 and t.isupper():
                out.setdefault(t,doc); return out
        for k,v in doc.items():
            if isinstance(k,str) and 1<=len(k)<=6 and k.isupper() and isinstance(v,dict):
                out.setdefault(k,v)
            else: _tmap(v,tick_keys,depth+1,out)
    elif isinstance(doc,list):
        for v in doc[:20000]: _tmap(v,tick_keys,depth+1,out)
    return out

def _stage(a20,a50,a200,pos52,perf_h):
    if a200 and a50 and (pos52 or 0)>55: return "STAGE_2_ADVANCE"
    if a200 and not a50: return "STAGE_3_TOP"
    if not a200 and (perf_h or 0)<-5: return "STAGE_4_DECLINE"
    if not a200 and a50: return "STAGE_1_BASE"
    return "STAGE_1_BASE" if not a200 else "STAGE_2_ADVANCE"

def lambda_handler(event=None, context=None):
    uni=build_universe()
    if not uni: return {"ok":False,"err":"finviz export empty"}
    # sector valuation percentiles (per-name "CAPE-style" relative read, cross-sectional)
    bysec={}
    for t,r in uni.items():
        pe=r.get("pe"); sec=r.get("sector")
        if pe and 0<pe<400 and sec: bysec.setdefault(sec,[]).append(pe)
    for v in bysec.values(): v.sort()

    mr=_tmap(_j("data/master-ranker.json",{}) or _j("data/master-rank.json",{}) or {})
    ec=_tmap(_j("data/equity-confluence.json",{}) or {})
    rs=_tmap(_j("data/resilience.json",{}) or {})
    dp=(_j("data/dark-pool.json",{}) or {}).get("xray_map") or {}
    er=_tmap(_j("data/estimate-revisions.json",{}) or {})
    bs=_tmap(_j("data/best-setups.json",{}) or {})
    bl=_tmap(_j("data/backlog.json",{}) or {})
    # factor decile memberships: prefer stored ranks file (any of 3 shapes),
    # else derive from the LIVE factor-returns doc's top/bottom samples per factor.
    fmem={}
    fr=(_j("data/history/factor-ranks.json") or _j("data/factor-ranks.json") or {})
    rk=fr.get("ranks") or fr.get("factors") or fr
    if isinstance(rk,dict):
        for f,d in rk.items():
            if not isinstance(d,dict): continue
            for key,tag in (("long","+"),("top","+"),("long_decile","+"),("short","-"),("bottom","-"),("short_decile","-")):
                for t in (d.get(key) or []):
                    if isinstance(t,str): fmem.setdefault(t.upper(),[]).append(f[:6]+tag)
    if not fmem:
        fd=_j("data/factor-returns.json",{}) or {}
        for f,d in (fd.get("factors") or {}).items():
            if not isinstance(d,dict): continue
            for key,tag in (("top_names","+"),("long_sample","+"),("longs","+"),
                            ("bottom_names","-"),("short_sample","-"),("shorts","-")):
                for t in (d.get(key) or []):
                    if isinstance(t,str): fmem.setdefault(t.upper(),[]).append(f[:6]+tag)
    graph=_j("data/polygon-related-graph.json",{}) or {}
    adj={}
    def _edge(e):
        if isinstance(e,(list,tuple)) and len(e)>=2: return e[0],e[1]
        if isinstance(e,dict):
            a=e.get("a") or e.get("source") or e.get("from"); b=e.get("b") or e.get("target") or e.get("to")
            return a,b
        return None,None
    for e in (graph.get("pairs") or graph.get("edges") or []):
        a,b=_edge(e)
        if a and b: adj.setdefault(a,[]).append(b); adj.setdefault(b,[]).append(a)
    src=graph.get("adjacency") or graph.get("map") or graph.get("related") or graph
    if isinstance(src,dict):
        for t,ns in src.items():
            if isinstance(t,str) and t.isupper() and len(t)<=6 and isinstance(ns,(list,tuple)):
                peers=[p for p in ns if isinstance(p,str)]
                if peers: adj.setdefault(t,[]).extend(peers[:8])

    cards={}; joins={"mr":0,"ec":0,"rs":0,"dp":0,"er":0,"fm":0}
    for t,r in uni.items():
        px=r.get("price"); mc=r.get("market_cap")
        if not px or not mc: continue
        mc_usd=mc*1e6 if mc<5e6 else mc
        if px<3 or mc_usd<2.5e8: continue
        a20=(r.get("sma20_pct") or 0)>0; a50=(r.get("sma50_pct") or 0)>0; a200=(r.get("sma200_pct") or 0)>0
        lo=r.get("off_52w_low_pct"); hi=r.get("off_52w_high_pct")
        pos52=round(100*lo/(lo-hi),0) if (lo is not None and hi is not None and (lo-hi)) else None
        pe=r.get("pe"); fpe=r.get("fwd_pe"); sec=r.get("sector") or ""
        vp=None
        if pe and 0<pe<400 and bysec.get(sec):
            arr=bysec[sec]; vp=round(100*sum(1 for x in arr if x<=pe)/len(arr),0)
        py=r.get("perf_y"); pm=r.get("perf_m")
        momo=round(py-pm,1) if (py is not None and pm is not None) else None
        sq=r.get("sales_growth_qoq") if r.get("sales_growth_qoq") is not None else r.get("sales_qoq")
        profitable = bool(pe and pe>0)
        turning = (not profitable) and bool(fpe and fpe>0)
        stack="FULL_BULL" if (a20 and a50 and a200) else "FULL_BEAR" if not (a20 or a50 or a200) else "MIXED"
        c={"px":px,"mc_b":round(mc_usd/1e9,2),"sec":sec,
           "ma":{"a20":a20,"a50":a50,"a200":a200,"stack":stack},
           "pos52":pos52,"stage":_stage(a20,a50,a200,pos52,r.get("perf_h")),
           "momo_12_1":momo,"perf":{"w":r.get("perf_w"),"m":pm,"q":r.get("perf_q"),"y":py},
           "val":{"pe":pe,"fwd_pe":fpe,"sec_pctile":vp},
           "grow":{"sales_qoq":sq,"eps_ny":r.get("eps_growth_ny")},
           "profit":{"now":profitable,"turning":turning},
           "own":{"inst":r.get("inst_own_pct"),"inst_tr":r.get("inst_trans_pct"),
                  "insider_tr":r.get("insider_trans_pct"),"short_flt":r.get("short_float_pct")},
           "rsi":r.get("rsi"),"beta":r.get("beta")}
        m=mr.get(t)
        if m: c["rank"]={"score":m.get("score"),"rank":m.get("rank")}; joins["mr"]+=1
        e=ec.get(t)
        if e: c["confl"]={"comp":e.get("composite"),"n_eff":e.get("n_eff")}; joins["ec"]+=1
        x=rs.get(t)
        if x: c["resil"]=x.get("state"); joins["rs"]+=1
        d=dp.get(t)
        if d: c["dark"]=d; joins["dp"]+=1
        v=er.get(t)
        if v: c["est_rev"]=v.get("direction"); joins["er"]+=1
        if fmem.get(t): c["factors"]=fmem[t]; joins["fm"]+=1
        b=bs.get(t)
        if b: c["setup"]=b.get("verdict") or b.get("tag") or True
        k=bl.get(t)
        if k: c["backlog"]=k.get("backlog") or k.get("rpo") or k.get("value")
        if adj.get(t): c["peers"]=adj[t][:5]
        cards[t]=c
    n=len(cards)
    print("[xray] cards=%d joins=%s"%(n,joins))

    def top(pred,key,rev=True,k=15):
        rows=[(t,c) for t,c in cards.items() if pred(c)]
        rows.sort(key=key,reverse=rev)
        return [t for t,_ in rows[:k]]
    boards={
     "multibagger_candidates": top(lambda c: c["mc_b"]<10 and (c["grow"]["sales_qoq"] or 0)>25 and (c["momo_12_1"] or 0)>15 and (c["own"]["inst_tr"] or 0)>0,
                                   lambda tc:(tc[1]["grow"]["sales_qoq"] or 0)+(tc[1]["momo_12_1"] or 0)),
     "turning_profitable": top(lambda c: c["profit"]["turning"], lambda tc:(tc[1]["grow"]["sales_qoq"] or 0)),
     "accumulation_leaders": top(lambda c: (c.get("dark") or {}).get("st")=="ACCUMULATION" and ((c.get("dark") or {}).get("sz") or 9)<0.3,
                                 lambda tc:((tc[1].get("dark") or {}).get("dp") or 0)),
     "dis_warnings": [t for t,c in cards.items() if (c.get("dark") or {}).get("fl")=="DISTRIBUTION_INTO_STRENGTH"][:15],
     "full_stack_highs": top(lambda c: c["ma"]["stack"]=="FULL_BULL" and (c["pos52"] or 0)>=88,
                             lambda tc:(tc[1]["pos52"] or 0)),
     "laggards_watch": top(lambda c: (c["perf"]["q"] or 0)<-8 and any((cards.get(p) or {}).get("momo_12_1",0) and cards[p]["momo_12_1"]>25 for p in (c.get("peers") or [])),
                           lambda tc:-(tc[1]["perf"]["q"] or 0)),
    }
    doc={"engine":"justhodl-stock-xray","version":"1.0.0",
         "generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),
         "n_cards":n,"joins":joins,"boards":boards,"cards":cards,
         "method":("Nightly per-name X-Ray: FinViz 151-col backbone (MA stack/stage/52w/momentum/"
                   "sector-relative valuation/growth/turning-profitable/ownership) joined with "
                   "master-rank, confluence, resilience, dark-pool xray_map, factor memberships, "
                   "estimate revisions, best-setups, backlog, supply-chain peers. Boards derived "
                   "cross-sectionally each run.")}
    s3.put_object(Bucket=BUCKET,Key=OUT,Body=json.dumps(doc,separators=(",",":"),default=str).encode(),
                  ContentType="application/json",CacheControl="public, max-age=1800")
    return {"ok":True,"n":n,"joins":joins,"boards":{k:len(v) for k,v in boards.items()}}
