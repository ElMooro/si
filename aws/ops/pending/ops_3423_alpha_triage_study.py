"""ops 3423 — NEGATIVE-ALPHA TRIAGE, phase 1: the evidence study.

For the worst graded families, pull every scored outcome from the signals
table and compute, per family x window: n, hit, avg/median excess — plus the
FLIP economics (hit', avg' minus a 10bps cost haircut) and a time-half
consistency split (an inversion only counts if it works in BOTH halves).

Disposition rules (codified, no vibes):
  INVERT  n>=60 AND flip_hit>=0.65 in both halves AND flip_avg(day_21)
          >= +30bps after cost AND >=2 windows agree in sign
  FIX     hit <= 0.05 with large uniform |excess| (smells like a direction
          bug at the emitter) -> patch source, don't trade it
  RETIRE  everything else negative: small/inconsistent edge = noise; stop
          burning compute and polluting inputs
Writes data/alpha-triage.json. Actions ship in 3424 from these verdicts.
"""
import json, sys, time, statistics
from datetime import datetime, timezone
from pathlib import Path
import boto3
from boto3.dynamodb.conditions import Attr
from ops_report import report

DDB=boto3.resource("dynamodb","us-east-1")
S3C=boto3.client("s3","us-east-1")
FAMILIES=["ignition","eng:accumulation-radar","eng:radar-backtest","eng:smart-money-13f",
          "eng:ai-infra-stack","eng:boom-radar","nobrainer_XOP","nobrainer_AIQ",
          "eng:attention-signals","eng:finnhub-signals"]
COST_BPS=10.0

with report("3423_alpha_triage_study") as rep:
    rep.heading("ops 3423 — negative-alpha evidence study")
    tbl=DDB.Table("justhodl-signals")
    rows=[]; lek=None
    fe=Attr("signal_type").is_in(FAMILIES)
    while True:
        kw={"FilterExpression":fe}
        if lek: kw["ExclusiveStartKey"]=lek
        r=tbl.scan(**kw); rows+=r.get("Items") or []
        lek=r.get("LastEvaluatedKey")
        if not lek: break
    print(f"[study] pulled {len(rows)} signal rows for {len(FAMILIES)} families")
    rep.log(f"rows={len(rows)}")

    out={}
    for fam in FAMILIES:
        frows=[x for x in rows if x.get("signal_type")==fam]
        frows.sort(key=lambda x: str(x.get("logged_at") or ""))
        if not frows: continue
        mid=len(frows)//2
        stats={}
        for wk in ("day_5","day_21","day_63"):
            obs=[]
            for i,x in enumerate(frows):
                o=(x.get("outcomes") or {}).get(wk)
                if not isinstance(o,dict): continue
                ex=o.get("excess_return_pct")
                if ex is None:
                    ex=o.get("return_pct")
                if ex is None: continue
                try: ex=float(ex)
                except Exception: continue
                pred_up = str(x.get("predicted_direction") or "UP")=="UP"
                # normalize: excess in direction of the prediction
                dir_ex = ex if pred_up else -ex
                obs.append({"h":0 if i<mid else 1,"ex":dir_ex})
            if len(obs)<10: continue
            n=len(obs)
            hit=sum(1 for o in obs if o["ex"]>0)/n
            avg=statistics.fmean(o["ex"] for o in obs)
            med=statistics.median(o["ex"] for o in obs)
            fh=[o for o in obs if o["h"]==0]; sh=[o for o in obs if o["h"]==1]
            flip_hit=1-hit
            flip_hit_h1=(sum(1 for o in fh if o["ex"]<=0)/len(fh)) if fh else None
            flip_hit_h2=(sum(1 for o in sh if o["ex"]<=0)/len(sh)) if sh else None
            flip_avg_after_cost=(-avg*100-COST_BPS)/100.0
            stats[wk]={"n":n,"hit":round(hit,3),"avg_pct":round(avg,3),"med_pct":round(med,3),
                       "flip_hit":round(flip_hit,3),
                       "flip_hit_h1":round(flip_hit_h1,3) if flip_hit_h1 is not None else None,
                       "flip_hit_h2":round(flip_hit_h2,3) if flip_hit_h2 is not None else None,
                       "flip_avg_after_cost_pct":round(flip_avg_after_cost,3)}
        if not stats: continue
        d21=stats.get("day_21") or {}
        both_halves = (d21.get("flip_hit_h1") or 0)>=0.65 and (d21.get("flip_hit_h2") or 0)>=0.65
        windows_agree = sum(1 for wk,s in stats.items() if s["avg_pct"]<0)>=2
        n_tot=max((s["n"] for s in stats.values()), default=0)
        verdict="RETIRE"
        if n_tot>=60 and both_halves and (d21.get("flip_avg_after_cost_pct") or -9)>=0.30 and windows_agree:
            verdict="INVERT"
        elif any(s["hit"]<=0.05 and abs(s["avg_pct"])>1.0 for s in stats.values()):
            verdict="FIX"
        out[fam]={"verdict":verdict,"windows":stats}
        line=f"{fam:28s} -> {verdict:6s} d21 n={d21.get('n')} hit={d21.get('hit')} flip_hit h1/h2={d21.get('flip_hit_h1')}/{d21.get('flip_hit_h2')} flip_avg_ac={d21.get('flip_avg_after_cost_pct')}%"
        print(line); rep.log(line)

    doc={"ok":True,"generated_at":datetime.now(timezone.utc).isoformat(),
         "cost_bps":COST_BPS,"families":out,
         "method":"direction-normalized excess per window; flip=1-hit with 10bps cost haircut; "
                  "INVERT needs n>=60 + flip_hit>=0.65 in BOTH time halves + flip day_21 >= +30bps after cost; "
                  "hit<=5% with big |excess| = FIX (emitter bug); else RETIRE"}
    S3C.put_object(Bucket="justhodl-dashboard-live",Key="data/alpha-triage.json",
        Body=json.dumps(doc,separators=(",",":")).encode(),ContentType="application/json")
    Path("aws/ops/reports/3423.json").write_text(json.dumps(doc,indent=2))
    sys.exit(0)
