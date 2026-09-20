"""Credit research context with explicit abstention.

The active handler preserves dated public inputs and the typed dealer research
reference. Unvalidated heuristic scores and LQD/HYG trade thresholds are retired:
the output is WAIT, with no signal emission, portfolio size or execution authority.
The legacy implementation is retained for audit but is not selectable by events.
HTTP reads do not collect. Original-source credit research remains pending.

Feed: data/credit-composite.json; contract credit-composite-abstention.v1.
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


def _legacy_unvalidated_score_lenses(docs):
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


def _legacy_unvalidated_handler(event, context):
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


def score_lenses(docs):
    """No unvalidated donor may become a point score, neutral vote or trade."""
    from dealer_research_context import project
    docs = {k: v if isinstance(v, dict) else {} for k, v in docs.items()}
    dealer = project(docs.get('nyfed') or {})
    names = {'dealer_positioning': 'nyfed', 'corporate_fails': 'fails',
             'funding_gcf_tri': 'stfm', 'ofr_fsi': 'stfm', 'credit_stress': 'credit'}
    lenses = {name: {'pts': None, 'status': 'unqualified',
                    'source_generated_at': (docs.get(key) or {}).get('generated_at'),
                    'reason': 'No registered out-of-sample strategy permission for this lens',
                    'calls_eligible': False, 'sizing_eligible': False}
              for name, key in names.items()}
    from credit_research import context as credit_context
    lenses['credit_stress']['measurement_context'] = credit_context(docs.get('credit') or {})
    lenses['dealer_positioning']['measurement_context'] = dealer
    lenses['corporate_fails']['source_replay'] = (docs.get('fails') or {}).get('replay')
    return lenses, None


def lambda_handler(event=None, context=None):
    """Abstain until donor strategies are qualified; no quotes, logs or notifications."""
    if isinstance(event, dict) and isinstance(event.get('requestContext'), dict) and event['requestContext'].get('http'):
        packet = rj(OUT_KEY)
        if not isinstance(packet, dict) or packet.get('contract') != 'credit-composite-abstention.v1':
            return {'statusCode': 503, 'body': json.dumps({'status': 'unavailable', 'portfolio_action': 'WAIT'})}
        return {'statusCode': 200, 'headers': {'Content-Type': 'application/json', 'Cache-Control': 'no-store'},
                'body': json.dumps(packet, allow_nan=False)}
    docs = {name: rj(key) for name, key in {'nyfed': 'data/nyfed-primary-dealer.json',
            'fails': 'data/settlement-fails.json', 'stfm': 'data/ofr-stfm.json',
            'eurodollar': 'data/eurodollar-plumbing.json', 'credit': 'data/credit-stress.json'}.items()}
    lenses, composite = score_lenses(docs)
    packet = {'contract': 'credit-composite-abstention.v1', 'version': '1.1.0',
              'generated_at': datetime.now(timezone.utc).isoformat(), 'composite': composite,
              'lenses': lenses, 'plans': [], 'logged': 0, 'call': None, 'portfolio_action': 'WAIT',
              'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False,
              'quality': {'status': 'unqualified', 'eligible_votes': 0},
              'methodology': 'Dated measurements remain context. No donor strategy has earned a composite vote or position size. Full composite research replay remains pending.'}
    # Retain the former public claim before replacing it; never overwrite a newer run.
    import hashlib
    current = s3.get_object(Bucket=S3_BUCKET, Key=OUT_KEY)
    previous_raw = current['Body'].read()
    previous = json.loads(previous_raw)
    if previous.get('generated_at') and datetime.fromisoformat(previous['generated_at'].replace('Z', '+00:00')) > datetime.fromisoformat(packet['generated_at']):
        return {'statusCode': 200, 'body': json.dumps({'ok': True, 'published': False, 'reason': 'newer output exists'})}
    if previous.get('contract') != packet['contract']:
        sha = hashlib.sha256(previous_raw).hexdigest()
        key = 'audit-private/20260909-originals/credit-composite/' + sha + '.bin'
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=previous_raw, ContentType='application/octet-stream', IfNoneMatch='*')
        except Exception as exc:
            if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('PreconditionFailed', '412'): raise
        if s3.get_object(Bucket=S3_BUCKET, Key=key)['Body'].read() != previous_raw: raise ValueError('Legacy credit composite preservation differs')
        packet['legacy_retention'] = {'sha256': sha, 'bytes': len(previous_raw), 'protected_backup': True}
    else:
        packet['legacy_retention'] = previous.get('legacy_retention')
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(packet, allow_nan=False, separators=(',', ':')).encode(),
                  ContentType='application/json', CacheControl='no-store', IfMatch=current['ETag'])
    return {'statusCode': 200, 'body': json.dumps({'ok': True, 'composite': None, 'logged': 0, 'portfolio_action': 'WAIT'})}
