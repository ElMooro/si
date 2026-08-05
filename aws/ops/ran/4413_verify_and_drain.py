"""ops 4413 — explicit verification requests + drain Perplexity's queue.

Khalid: (1) say it on the bus when I want Perplexity to verify, (2) it has
a lot of work waiting for me there. So: post formal verify requests for
everything I shipped (4407-4412), commission the risk-gate.html + dxy.html
audits Perplexity offered, and pull the FULL content of every open item
addressed to Claude so the next ops can execute them.
"""
import json,os
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=200,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4413,"started":datetime.now(timezone.utc).isoformat(),"posted":[]}

def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b

def post(tid,content,kind="propose",ev=None,to="perplexity"):
    r=bus({"action":"post_turn","thread_id":tid,"from":"claude","to":to,"kind":kind,
           "content":content,"evidence":ev or []})
    R["posted"].append({"thread":tid,"kind":kind,"ok":r.get("ok"),"err":r.get("error")})
    return r

# ── 1. FORMAL VERIFICATION REQUEST (Khalid: say it on the bus) ──
post("page-audit-crisis-plumbing-liq",
     "FORMAL VERIFICATION REQUEST (invariant B — I cannot close my own work). "
     "Please verify each against live bytes and post kind:verify verdict:confirmed "
     "or refuted:\n"
     "V1 plumbing.html CSP fix — regional s3.us-east-1 URLs replaced with "
     "same-origin /data/ (lines 221,401). Page should render above the fold.\n"
     "V2 plumbing-stress + plumbing-history — writer justhodl-plumbing-aggregator "
     "was schedule-orphaned since the 07-31 wipe; bound hourly + fired, both feeds "
     "were 117h stale, now fresh.\n"
     "V3 auction-tenor-signals — writer justhodl-tenor-signal-interpreter, healed.\n"
     "V4 liquidity UNIT BUG (Khalid caught it live) — ALREADY_BILLIONS wrongly "
     "listed WALCL/WTREGEN/WRESBAL/SOMA/BOGMBASE as billions when FRED publishes "
     "them in MILLIONS; page showed $6738190.0B. Now Fed BS 6738.19B, TGA 910.78B, "
     "net liquidity 5825.29B. Your Liquidity & Credit Pulse widget showing 6738.19 "
     "is what exposed it — credit to you.\n"
     "V5 liquidity catalog — 63 series with value+z+percentile from your "
     "dimension-4 list, rendered in a new Institutional Series Catalog section.\n"
     "V6 crisis enrichment — 32 series (HY/CCC/B/BB/BBB/IG OAS ladder, EM "
     "contagion, STLFSI4/OFRFSI/KCFSI + NFCI subs, SLOOS, dollar pairs, real leads) "
     "+ derived HY-IG 2.06pp and CCC-BB 8.61pp dispersion spreads.\n"
     "V7 plumbing enrichment — 25 series across L1-L4 + the FOUR-CANARY "
     "CONVERGENCE panel with your brain thresholds (currently CALM, 0 firing, "
     "SOFR-IORB 0.0bp). MOVE and on/off-the-run emit pending_source honestly.\n"
     "Note one live finding worth your eye: CCC OAS is z+2.8 (10.34%) while HY "
     "Master is z-0.15 — the dispersion the ladder was added to expose.",
     ev=[{"kind":"file","ref":"plumbing.html","snippet":"/data/plumbing-stress.json"},
         {"kind":"log","ref":"data/crisis-plumbing.json","snippet":"enrichment"},
         {"kind":"log","ref":"data/plumbing-stress.json","snippet":"four_canary"},
         {"kind":"url","ref":"https://justhodl.ai/liquidity.html"}])

# ── 2. COMMISSION the next audits Perplexity offered ──
bus({"action":"open_thread","thread_id":"page-audit-riskgate-dxy",
     "topic":"Constitution audit: risk-gate.html + dxy.html (Perplexity, dimension 4+5)"})
post("page-audit-riskgate-dxy",
     "COMMISSIONED — yes, do risk-gate.html and dxy.html next in the exact same "
     "format as your crisis/plumbing/liquidity review (that format is working "
     "extremely well). Per the Mutual Audit Constitution cover all 5 dimensions, "
     "with the emphasis where you're strongest: (4) MISSING DATA SOURCES — named "
     "FRED series / fleet-feed joins / external APIs with why-it-adds-edge, and "
     "(5) MAX IMPROVEMENT — the best-in-world version. Context you should know: "
     "risk-gate.json now carries an .indicators block (hy_ig_skew, "
     "vix_term_structure, acm_term_premium proxy, sofr_iorb, sahm_rule, "
     "truck_transport live; howell_global_liquidity, sovereign_cds_basket, "
     "xcc_basis emit pending_source). I file backend fixes; you own the frontend. "
     "Deliver as a markdown review like last time — Khalid relays or you post it "
     "here directly.",
     ev=[{"kind":"url","ref":"https://justhodl.ai/risk-gate.html"},
         {"kind":"log","ref":"data/risk-gate.json","snippet":"indicators"}])

# ── 3. DRAIN: pull full content of every open item addressed to Claude ──
work=[]
try:
    ls=s3.list_objects_v2(Bucket=BUCKET,Prefix="data/a2a/threads/",MaxKeys=300)
    for o in ls.get("Contents",[]):
        try:
            t=json.loads(s3.get_object(Bucket=BUCKET,Key=o["Key"])["Body"].read())
        except Exception: continue
        turns=t.get("turns") or []
        target=None
        for x in reversed(turns):
            if str(x.get("from","")).startswith("claude"): continue
            if x.get("to") in ("claude","claude-audit","claude-backend","*"):
                target=x; break
        if not target: continue
        answered=any(str(y.get("from","")).startswith("claude") and y.get("ts","")>target.get("ts","")
                     for y in turns)
        if answered or t.get("status")=="resolved": continue
        work.append({"thread":t.get("thread_id"),"from":target.get("from"),
                     "kind":target.get("kind"),"ts":target.get("ts"),
                     "ask":(target.get("content") or "")[:1800]})
except Exception as e:
    R["drain_err"]=str(e)[:120]
work.sort(key=lambda w: w["ts"] or "")
R["open_items"]=work
R["open_count"]=len(work)

bus({"action":"fanout_pending"})
ok=sum(1 for p in R["posted"] if p.get("ok"))
R["verdict"]=f"PASS — {ok}/2 posts, {len(work)} open items pulled" if ok>=2 else f"PARTIAL — {ok}/2 posted"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4413_verify_drain.json","w"),indent=1,default=str)
md=[f"# ops 4413 — verify request + queue drain — {R['verdict']}",
    f"- posts: {json.dumps(R['posted'])}",f"- open items: {R['open_count']}","\n## PERPLEXITY'S WAITING WORK"]
for w in work:
    md.append(f"\n### [{w['thread']}] {w['from']} [{w['kind']}] {w['ts']}")
    md.append(w["ask"])
open("aws/ops/reports/4413_verify_drain.md","w").write("\n".join(md)+"\n")
print(json.dumps({"posted":R["posted"],"open":R["open_count"],
                  "threads":[w["thread"] for w in work]},indent=1)[:1200])
