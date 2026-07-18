"""justhodl-credit-composite v1.0 — the dealer-plumbing credit front-run,
composed (creative #4, ops 3452).

Five first-party lenses nobody joins but this desk:
  L1 dealer positioning (25) — FR2004 corporate net-short / squeeze setup
  L2 corporate settlement fails (25) — spike class from the fails engine
  L3 funding (20) — GCF-TRI spread (OFR STFM, eurodollar fallback)
  L4 OFR FSI (15) — financial-stress index level
  L5 credit-stress aggregate (15) — the CDS-doctrine composite
Composite >=55 → DOWN LQD vs SPY [5,21]; >=70 adds DOWN HYG. Individual
component signals keep emitting; this is the JOINED trade — orthogonality
clusters in the composer handle the correlation.

Feed: data/credit-composite.json · Signals: type "credit-composite".
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

from signals_emit import log_signal, yprice

VERSION = "1.0.1"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/credit-composite.json"
s3 = boto3.client("s3", "us-east-1")
ddb = boto3.resource("dynamodb", "us-east-1")


def rj(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def hunt(doc, hints, want=("num", "bool", "str"), depth=4):
    """BFS: first value whose key contains any hint. Returns (value, path)."""
    q = [(doc, "")]
    while q:
        o, pth = q.pop(0)
        if len(pth.split(".")) > depth or not isinstance(o, dict):
            continue
        for k, v in o.items():
            kl = str(k).lower()
            if any(h in kl for h in hints):
                if isinstance(v, bool) and "bool" in want:
                    return v, f"{pth}.{k}"
                if isinstance(v, (int, float)) and "num" in want:
                    return float(v), f"{pth}.{k}"
                if isinstance(v, str) and "str" in want and v:
                    return v, f"{pth}.{k}"
                if isinstance(v, dict):
                    for kk in ("value", "level", "bp", "spread_bp", "latest"):
                        if isinstance(v.get(kk), (int, float)):
                            return float(v[kk]), f"{pth}.{k}.{kk}"
            if isinstance(v, dict):
                q.append((v, f"{pth}.{k}"))
    return None, None


def score_lenses(docs):
    L = {}
    ny = docs.get("nyfed") or {}
    sq, sp = hunt(ny, ("squeeze",), want=("bool", "num"))
    rg, _ = hunt(ny, ("regime",), want=("str",))
    cn, cp = hunt(ny.get("corporate") or {}, ("net",), want=("num",))
    pts = 25 if sq else (18 if (rg and "SHORT" in str(rg).upper())
                         else (10 if (cn is not None and cn < 0) else 0))
    L["dealer_positioning"] = {"pts": pts,
                               "detail": f"squeeze={sq} regime={rg} corp_net={cn} ({cp})"}
    sf = docs.get("fails") or {}
    cls = sf.get("classes")
    if isinstance(cls, dict):
        corp = cls.get("corporate") or {}
    elif isinstance(cls, list):
        corp = next((c for c in cls if isinstance(c, dict)
                     and "corp" in str(c.get("name") or c.get("class")
                                       or c.get("asset_class")
                                       or c.get("id") or "").lower()), {})
    else:
        corp = sf.get("corporate") if isinstance(sf.get("corporate"), dict) else {}
    spike, _ = hunt({"x": corp}, ("spike",), want=("bool", "num"))
    st, _ = hunt({"x": corp}, ("status", "level"), want=("str",))
    stU = str(st or "").upper()
    pts = 25 if (spike or "SPIKE" in stU) else (12 if "ELEV" in stU else 0)
    L["corporate_fails"] = {"pts": pts, "detail": f"spike={spike} status={st}"}
    stfm = docs.get("stfm") or {}
    bp, bpath = hunt(stfm, ("gcf_tri", "gcf"), want=("num",))
    if bp is None:
        us = (docs.get("eurodollar") or {}).get("us_core") or {}
        for m in (us.get("metrics") or []):
            if isinstance(m, dict) and m.get("id") == "gcf_tri":
                bp, bpath = m.get("value"), "eurodollar.us_core.gcf_tri"
    pts = 20 if (isinstance(bp, (int, float)) and bp >= 8) else \
          (10 if (isinstance(bp, (int, float)) and bp >= 4) else 0)
    L["funding_gcf_tri"] = {"pts": pts, "detail": f"bp={bp} ({bpath})"}
    fsi, fpath = hunt({"fsi": stfm.get("fsi")} if stfm.get("fsi") is not None
                      else stfm, ("fsi",), want=("num",))
    pts = 15 if (isinstance(fsi, (int, float)) and fsi >= 0) else \
          (7 if (isinstance(fsi, (int, float)) and fsi >= -1.0) else 0)
    L["ofr_fsi"] = {"pts": pts, "detail": f"fsi={fsi} ({fpath})"}
    cs = docs.get("credit") or {}
    cv, cvp = hunt(cs, ("composite", "score", "danger", "level"), want=("num",))
    pts = 15 if (isinstance(cv, (int, float)) and cv >= 60) else \
          (8 if (isinstance(cv, (int, float)) and cv >= 45) else 0)
    L["credit_stress"] = {"pts": pts, "detail": f"value={cv} ({cvp})"}
    return L, sum(v["pts"] for v in L.values())


def lambda_handler(event, context):
    t0 = time.time()
    if (event or {}).get("_probe"):
        docs = event["_probe"]
        L, comp = score_lenses(docs)
        plans = ([{"etf": "LQD", "direction": "DOWN"}] if comp >= 55 else []) \
            + ([{"etf": "HYG", "direction": "DOWN"}] if comp >= 70 else [])
        return {"statusCode": 200, "body": json.dumps(
            {"composite": comp, "lenses": L, "plans": plans})}
    docs = {"nyfed": rj("data/nyfed-primary-dealer.json"),
            "fails": rj("data/settlement-fails.json"),
            "stfm": rj("data/ofr-stfm.json"),
            "eurodollar": rj("data/eurodollar-plumbing.json"),
            "credit": rj("data/credit-stress.json")}
    L, comp = score_lenses(docs)
    plans = []
    if comp >= 55:
        plans.append({"etf": "LQD", "direction": "DOWN", "conf": 0.62})
    if comp >= 70:
        plans.append({"etf": "HYG", "direction": "DOWN", "conf": 0.66})
    tbl = ddb.Table("justhodl-signals")
    logged = 0
    for p in plans:
        mark = yprice(p["etf"])
        time.sleep(0.15)
        p["mark"] = mark
        if mark and log_signal(
                tbl, "credit-composite", p["etf"], "DOWN", [5, 21], mark,
                confidence=p["conf"],
                rationale=(f"credit plumbing confluence {comp}/100: " +
                           ", ".join(f"{k}={v['pts']}" for k, v in L.items()
                                     if v["pts"] > 0)),
                benchmark="SPY",
                metadata={"engine": "credit-composite", "composite": comp}):
            logged += 1
    out = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 2),
           "composite": comp, "lenses": L, "plans": plans, "logged": logged,
           "thresholds": {"emit_lqd": 55, "add_hyg": 70},
           "methodology": ("Five first-party plumbing lenses composed; "
                           ">=55 → DOWN LQD vs SPY [5,21]; >=70 adds HYG. "
                           "PROVEN gate controls promotion; components keep "
                           "emitting individually.")}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=600")
    print(f"[credit-composite] {comp}/100 plans={len(plans)} logged={logged} "
          + " ".join(f"{k}:{v['pts']}" for k, v in L.items()))
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "composite": comp, "logged": logged})}
