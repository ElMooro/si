"""
justhodl-finnhub-signals — INSIDER ACCUMULATION + ANALYST UPGRADE MOMENTUM + BEATS
==================================================================================
Three of the cleanest pre-move tells, from Finnhub's free tier:
  • INSIDER SENTIMENT  — monthly share-purchase ratio (mspr, -100..+100) and net share
    change. Insiders buying their own stock ahead of a move is a classic early tell.
  • ANALYST REVISION   — recommendation-trend momentum (buy-side share rising month/month).
  • EARNINGS SURPRISE   — last reported beat/miss vs estimate.
Combined into a per-name accumulation_score, written for the re-rating radar / deal
scanner to consume as a timing kicker (the "is smart + insider money moving now?" layer
that complements the slow 13F).

Universe = AI-infra-stack + re-rating candidates + smart-money longs (deduped, capped).
Free tier is ~60 req/min, so calls are throttled and the universe is bounded.

OUTPUT data/finnhub-signals.json   SCHEDULE daily 12:00 UTC. Real data, research only.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/finnhub-signals.json"
FINNHUB = "d8qlt5pr01qrf6e278d0d8qlt5pr01qrf6e278dg"
MAX_NAMES = 58
THROTTLE = 1.05            # ~57/min, under the 60/min free cap
s3 = boto3.client("s3", region_name="us-east-1")


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _fh(path):
    url = "https://finnhub.io/api/v1/" + path + ("&" if "?" in path else "?") + "token=" + FINNHUB
    try:
        return json.loads(urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "jh-fh"}), timeout=12).read())
    except Exception:
        return None
    finally:
        time.sleep(THROTTLE)


def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


def build_universe():
    syms = []
    seen = set()
    def add(s):
        s = (s or "").upper()
        if s and s not in seen:
            seen.add(s); syms.append(s)
    rr = _read("data/ai-rerating-radar.json") or {}
    for r in ((rr.get("summary", {}) or {}).get("top_setups", []) or []):
        add(r.get("symbol"))
    sm = _read("data/smart-money-13f.json") or {}
    for f in sm.get("funds", []) or []:
        for h in (f.get("top_longs", []) or [])[:20]:
            add(h.get("ticker"))
    stack = _read("data/ai-infra-stack.json") or {}
    for layer in stack.get("stack", []):
        for n in (layer.get("names", []) or [])[:6]:
            add(n.get("symbol"))
    return syms[:MAX_NAMES]


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    frm = (now - timedelta(days=130)).strftime("%Y-%m-%d")
    to = now.strftime("%Y-%m-%d")
    universe = build_universe()

    rows = []
    for sym in universe:
        q = urllib.parse.quote(sym)
        ins = _fh(f"stock/insider-sentiment?symbol={q}&from={frm}&to={to}")
        rec = _fh(f"stock/recommendation?symbol={q}")
        ern = _fh(f"stock/earnings?symbol={q}")

        # insider sentiment
        idata = (ins or {}).get("data", []) if isinstance(ins, dict) else []
        idata = sorted(idata, key=lambda d: (d.get("year", 0), d.get("month", 0)), reverse=True)[:3]
        mspr = round(sum(d.get("mspr", 0) for d in idata) / len(idata), 1) if idata else None
        net = sum(d.get("change", 0) for d in idata) if idata else None

        # recommendation momentum
        buy_pct = momentum = None
        if isinstance(rec, list) and rec:
            rec = sorted(rec, key=lambda d: d.get("period", ""), reverse=True)
            def bp(r):
                t = (r.get("strongBuy", 0) + r.get("buy", 0) + r.get("hold", 0)
                     + r.get("sell", 0) + r.get("strongSell", 0))
                return (r["strongBuy"] + r["buy"]) / t if t else None
            buy_pct = bp(rec[0])
            prev = bp(rec[2]) if len(rec) > 2 else (bp(rec[1]) if len(rec) > 1 else None)
            if buy_pct is not None and prev is not None:
                momentum = round(buy_pct - prev, 3)

        # earnings surprise
        surprise = None
        if isinstance(ern, list) and ern:
            sp = ern[0].get("surprisePercent")
            surprise = round(sp, 1) if isinstance(sp, (int, float)) else None

        # accumulation score
        ins_pts = 0.0
        if mspr is not None:
            ins_pts = _clamp(mspr, -100, 100) * 0.25 + (10 if (net or 0) > 0 else 0)
        rec_pts = 0.0
        if momentum is not None:
            rec_pts = _clamp(momentum, -0.3, 0.3) * 100
        if buy_pct is not None:
            rec_pts += buy_pct * 20
        earn_pts = _clamp(surprise or 0, -20, 20) * 0.5
        score = round(ins_pts + rec_pts + earn_pts, 1)

        why = []
        if mspr is not None and mspr > 10:
            why.append(f"insiders net buying (mspr {mspr})")
        elif mspr is not None and mspr < -40:
            why.append(f"insiders selling (mspr {mspr})")
        if momentum is not None and momentum > 0.03:
            why.append("analyst buy-side rising")
        elif momentum is not None and momentum < -0.03:
            why.append("analyst buy-side falling")
        if surprise is not None and surprise > 0:
            why.append(f"last EPS beat +{surprise}%")
        rows.append({"symbol": sym, "mspr": mspr, "insider_net_shares": net,
                     "rec_buy_pct": round(buy_pct, 3) if buy_pct is not None else None,
                     "rec_momentum": momentum, "last_surprise_pct": surprise,
                     "accumulation_score": score, "why": "; ".join(why)})
        if time.time() - t0 > 250:
            break

    rows.sort(key=lambda r: r["accumulation_score"], reverse=True)
    insider_buying = sorted([r for r in rows if (r["mspr"] or -999) > 10],
                            key=lambda r: r["mspr"], reverse=True)[:15]
    upgrade_mom = sorted([r for r in rows if (r["rec_momentum"] or -9) > 0.02],
                         key=lambda r: r["rec_momentum"], reverse=True)[:15]
    out = {
        "engine": "finnhub-signals", "version": VERSION,
        "generated_at": now.isoformat(), "n_names": len(rows),
        "thesis": "Insider accumulation + analyst upgrade momentum + earnings beats — the fast pre-move "
                  "confirmation layer that complements the slow 13F.",
        "summary": {
            "top_accumulation": rows[:20],
            "top_insider_buying": insider_buying,
            "top_upgrade_momentum": upgrade_mom,
        },
        "all": rows,
        "source": "Finnhub free tier (insider-sentiment, recommendation, earnings)",
        "caveats": "Insider mspr/recommendation update monthly; not intraday. Insider selling is often "
                   "noise (taxes/diversification) — buying is the cleaner tell. Confirmation, not a trigger. "
                   "Research only, not investment advice.",
        "elapsed_s": round(time.time() - t0, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[finnhub] names={len(rows)} insider_buying={len(insider_buying)} "
          f"upgrade_mom={len(upgrade_mom)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n": len(rows),
            "insider_buying": len(insider_buying), "upgrade_momentum": len(upgrade_mom)})}
