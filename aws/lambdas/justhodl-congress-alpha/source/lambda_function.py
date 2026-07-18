"""justhodl-congress-alpha v1.0 — grade the filers (ops 3457).

The 13F clone-alpha pattern applied to the free congress rail: every fresh
Senate PURCHASE (buys only — sales are tax/diversification noise) becomes a
graded UP signal at DISCLOSURE price (the copy-tradeable moment), tagged
with its filer. As outcomes mature, the engine self-builds a per-filer
skill ledger; filers reaching n>=10 with hit>=60% get PROVEN_FILER status
and their future buys carry boosted confidence. Nobody follows a legislator
here on reputation — only on graded record.

Feeds: data/congress-alpha.json + data/congress-filer-skill.json
Signals: type "congress-buy" [21,63] vs SPY.
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr

from signals_emit import log_signal, yprice

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SRC_KEY = "data/congress-direct.json"
OUT_KEY = "data/congress-alpha.json"
SKILL_KEY = "data/congress-filer-skill.json"
s3 = boto3.client("s3", "us-east-1")
ddb = boto3.resource("dynamodb", "us-east-1")


def plan(txns):
    plans, seen = [], set()
    for t in txns:
        if not isinstance(t, dict):
            continue
        tk = t.get("ticker")
        typ = str(t.get("type") or "")
        if not tk or "purchase" not in typ.lower():
            continue
        key = (t.get("filer"), tk)
        if key in seen:
            continue
        seen.add(key)
        plans.append({"filer": t.get("filer"), "ticker": tk,
                      "tx_date": t.get("tx_date"),
                      "amount": t.get("amount"), "type": typ})
    return plans[:15]


def build_skill(tbl):
    rows, lek = [], None
    fe = Attr("signal_type").eq("congress-buy")
    while True:
        kw = {"FilterExpression": fe}
        if lek:
            kw["ExclusiveStartKey"] = lek
        r = tbl.scan(**kw)
        rows += r.get("Items") or []
        lek = r.get("LastEvaluatedKey")
        if not lek:
            break
    sk = {}
    for x in rows:
        f = ((x.get("metadata") or {}).get("filer")) or "UNKNOWN"
        e = sk.setdefault(f, {"n_signals": 0, "n_scored": 0, "hits": 0,
                              "sum_excess": 0.0})
        e["n_signals"] += 1
        for wk in ("day_21", "day_63"):
            o = (x.get("outcomes") or {}).get(wk)
            if not isinstance(o, dict):
                continue
            ex = o.get("excess_return_pct") or o.get("return_pct")
            try:
                ex = float(ex)
            except Exception:
                continue
            e["n_scored"] += 1
            e["hits"] += 1 if ex > 0 else 0
            e["sum_excess"] += ex
            break
    out = []
    for f, e in sk.items():
        hit = round(e["hits"] / e["n_scored"], 3) if e["n_scored"] else None
        avg = round(e["sum_excess"] / e["n_scored"], 3) if e["n_scored"] else None
        out.append({"filer": f, **e, "hit": hit, "avg_excess_pct": avg,
                    "proven": bool(e["n_scored"] >= 10 and (hit or 0) >= 0.60)})
    out.sort(key=lambda r: (-(r["hit"] or 0), -r["n_scored"]))
    return out


def lambda_handler(event, context):
    t0 = time.time()
    if (event or {}).get("_probe"):
        return {"statusCode": 200, "body": json.dumps(
            {"plans": plan(event["_probe"].get("transactions") or [])})}
    try:
        doc = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                       Key=SRC_KEY)["Body"].read())
    except Exception as e:
        print(f"[congress-alpha] source: {str(e)[:70]}")
        doc = {}
    txns = ((doc.get("senate") or {}).get("transactions")) or []
    plans = plan(txns)
    tbl = ddb.Table("justhodl-signals")
    skills = build_skill(tbl)
    proven = {r["filer"] for r in skills if r.get("proven")}
    logged = 0
    for p in plans:
        mark = yprice(p["ticker"])
        time.sleep(0.15)
        p["mark"] = mark
        is_pf = p["filer"] in proven
        p["proven_filer"] = is_pf
        if mark and log_signal(
                tbl, "congress-buy", p["ticker"], "UP", [21, 63], mark,
                confidence=0.70 if is_pf else 0.58,
                rationale=(f"Senate purchase by {p['filer']} "
                           f"({p['amount']}, traded {p['tx_date']}), entered "
                           "at disclosure"
                           + (" — PROVEN filer" if is_pf else "")),
                benchmark="SPY",
                metadata={"engine": "congress-alpha", "filer": p["filer"],
                          "amount": p["amount"], "tx_date": p["tx_date"]}):
            logged += 1
    now = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=S3_BUCKET, Key=SKILL_KEY,
                  Body=json.dumps({"ok": True, "generated_at": now,
                                   "n_filers": len(skills),
                                   "filers": skills},
                                  separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=1800")
    out = {"ok": True, "version": VERSION, "generated_at": now,
           "elapsed_s": round(time.time() - t0, 2),
           "n_src_transactions": len(txns), "n_purchases": len(plans),
           "plans": plans, "logged": logged,
           "n_filers_tracked": len(skills),
           "proven_filers": sorted(proven),
           "methodology": ("Senate PURCHASES only, at disclosure price, UP "
                           "[21,63] vs SPY, filer-tagged. Skill ledger "
                           "self-builds from graded outcomes; n>=10 & "
                           "hit>=60% => PROVEN_FILER (conf 0.58→0.70). "
                           "Follow record, not reputation.")}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=1800")
    print(f"[congress-alpha] src={len(txns)} buys={len(plans)} "
          f"logged={logged} filers={len(skills)}")
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "buys": len(plans), "logged": logged})}
