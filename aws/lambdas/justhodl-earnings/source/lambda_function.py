"""justhodl-earnings -- beat league + transcript growth desk.
Marker: earnings v1.1.0
Doctrine: every number traces to an FMP field proven in ops
4879/4880; surprises computed from actual-vs-estimate in the
calendar itself; transcript scoring is a transparent lexicon
with verbatim evidence -- an LLM one-liner layer activates only
when ANTHROPIC_KEY works (billing outage self-heals).
"""
import gzip
import json
import os
import re
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3

VERSION = "1.1.0"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/earnings.json"
HIST_KEY = "data/earnings/history.json"
S = "https://financialmodelingprep.com/stable"
WINDOW_D = 35
MAX_TRANSCRIPTS = 42
PICK_MIN = 62

s3 = boto3.client("s3", region_name=REGION)

POS = {"record backlog": 6, "raising guidance": 6,
       "raise our guidance": 6, "raised our guidance": 6,
       "demand exceeds": 6, "unprecedented demand": 6,
       "capacity constrained": 5, "sold out": 5,
       "demand outpac": 5, "record revenue": 4,
       "inflection": 4, "strong demand": 4,
       "pricing power": 4, "accelerat": 3, "multi-year": 3,
       "expanding margin": 3, "share gains": 3, "backlog": 2,
       "ramp": 2, "tailwind": 2, "record": 1.5,
       "pipeline": 1.5}
NEG = {"lowering guidance": 6, "lower our guidance": 6,
       "inventory correction": 5, "soft demand": 4,
       "deceler": 4, "pushout": 4, "softness": 3,
       "delay": 2.5, "headwind": 2, "cautious": 2,
       "macro uncertainty": 2, "churn": 2, "pause": 2}
DEMAND_FLAGS = ("demand exceeds", "capacity constrained",
                "sold out", "unprecedented demand",
                "industry-wide")


def _g(key):
    try:
        raw = s3.get_object(Bucket=BUCKET,
                            Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")


def http_json(url):
    """Seam."""
    req = urllib.request.Request(
        url, headers={"User-Agent": "justhodl-earnings"})
    with urllib.request.urlopen(req, timeout=50) as r:
        return json.loads(r.read())


def clip(v, lo, hi):
    return max(lo, min(hi, v))


def surprise_pct(a, e):
    if a is None or e is None or abs(e) < 0.01:
        return None
    return round((a - e) / abs(e) * 100.0, 1)


def beat_score(eps_pct, rev_pct):
    if eps_pct is None:
        return None
    s = 0.6 * clip(eps_pct, -100, 100)
    if rev_pct is not None:
        s += 0.4 * clip(rev_pct * 5, -100, 100)
    # eps-only reporters keep the 0.6-weighted score: a missing
    # revenue estimate never inflates a rank
    return round(s, 1)


def load_universe():
    """Proven bind (spx-beaters house pattern): data/universe.json
    rows under stocks|rows|universe; symbol|ticker, name, sector,
    industry, market_cap, cap_bucket (mega folded into large,
    nano into micro)."""
    uni = _g("data/universe.json") or {}
    rows = uni.get("stocks") or uni.get("rows") \
        or uni.get("universe") or []
    out = {}
    for r in rows:
        sym = str(r.get("symbol") or r.get("ticker")
                  or "").upper()
        if not sym:
            continue
        cb = str(r.get("cap_bucket") or "").lower()
        if cb == "nano":
            cb = "micro"
        if cb == "mega":
            cb = "large"
        mc = r.get("market_cap")
        out[sym] = {"name": r.get("name"),
                    "sector": r.get("sector"),
                    "industry": r.get("industry"),
                    "mcap_b": round(mc / 1e9, 2)
                    if isinstance(mc, (int, float)) and mc
                    else None,
                    "bucket": cb if cb in
                    ("large", "mid", "small", "micro")
                    else None}
    return out


def fetch_reporters(key, today):
    d0 = (today - timedelta(days=WINDOW_D)).isoformat()
    d1 = (today + timedelta(days=1)).isoformat()
    cal = http_json("%s/earnings-calendar?from=%s&to=%s"
                    "&apikey=%s" % (S, d0, d1, key))
    best = {}
    for r in cal if isinstance(cal, list) else []:
        sym = str(r.get("symbol") or "")
        if not sym or "." in sym or len(sym) > 6:
            continue
        a = r.get("epsActual")
        e = r.get("epsEstimated")
        if a is None or e is None:
            continue
        d = str(r.get("date") or "")
        if sym not in best or d > best[sym]["date"]:
            ep = surprise_pct(a, e)
            rp = surprise_pct(r.get("revenueActual"),
                              r.get("revenueEstimated")) \
                if r.get("revenueEstimated") else None
            best[sym] = {"t": sym, "date": d, "eps_a": a,
                         "eps_e": e, "eps_surprise_pct": ep,
                         "rev_surprise_pct": rp,
                         "beat_score": beat_score(ep, rp)}
    rows = [x for x in best.values()
            if x["beat_score"] is not None]
    rows.sort(key=lambda x: x["beat_score"], reverse=True)
    return rows


def score_transcript(text):
    words = max(1, len(text.split()))
    low = text.lower()
    hits_p, hits_n = {}, {}
    pw = nw = 0.0
    for ph, w in POS.items():
        c = low.count(ph)
        if c:
            hits_p[ph] = c
            pw += w * c
    for ph, w in NEG.items():
        c = low.count(ph)
        if c:
            hits_n[ph] = c
            nw += w * c
    per10k = 10000.0 / words
    score = clip(round(50 + 4 * (pw - nw) * per10k, 1),
                 0, 100)
    ev = []
    ranked = sorted(hits_p, key=lambda p: -POS[p] * hits_p[p])
    for ph in ranked[:2]:
        m = re.search(r"[^.!?]{0,180}%s[^.!?]{0,180}[.!?]"
                      % re.escape(ph), low)
        if m:
            i = m.start()
            ev.append(text[i:i + len(m.group(0))]
                      .strip()[:240])
    demand = any(f in low for f in DEMAND_FLAGS)
    return {"growth_score": score, "words": words,
            "pos_hits": hits_p, "neg_hits": hits_n,
            "demand_flag": demand, "evidence": ev}


def llm_line(key_a, t, ev):
    if not key_a:
        return None, ("rules_only (Anthropic billing down -- "
                      "self-heals on credits)")
    try:
        body = json.dumps({
            "model": "claude-haiku-4-5",
            "max_tokens": 90,
            "messages": [{"role": "user", "content":
                          "One factual sentence: what does "
                          "this earnings-call evidence from "
                          "%s imply for forward growth? "
                          "Evidence: %s" % (t, " | ".join(
                              ev)[:800])}]})
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=body.encode(),
            headers={"x-api-key": key_a,
                     "anthropic-version": "2023-06-01",
                     "content-type": "application/json"})
        with urllib.request.urlopen(req, timeout=40) as r:
            j = json.loads(r.read())
        return j["content"][0]["text"].strip()[:280], "llm"
    except Exception as e:  # noqa: BLE001
        return None, ("rules_only (llm error %s -- "
                      "self-heals)" % str(e)[:40])


def build(event=None):
    now = datetime.now(timezone.utc)
    key = os.environ.get("FMP_KEY", "")
    key_a = os.environ.get("ANTHROPIC_KEY", "")
    doc = {"v": VERSION, "engine": "justhodl-earnings",
           "as_of": now.date().isoformat(),
           "generated_at": now.isoformat(),
           "window_days": WINDOW_D, "status": "LIVE",
           "method": {"beat": "0.6*clip(eps_surprise,±100) + "
                              "0.4*clip(5*rev_surprise,±100); "
                              "|est|<0.01 -> excluded",
                      "calls": "lexicon per-10k-words, "
                               "verbatim evidence; picks need "
                               "score>=%d, >=3 distinct "
                               "positive phrases, >=3000 "
                               "words" % PICK_MIN}}
    if not key:
        doc.update({"status": "MISSING",
                    "why": "FMP_KEY absent"})
        _put(OUT_KEY, doc)
        return doc
    rows = fetch_reporters(key, now.date())
    n = len(rows)
    uni = load_universe()
    matched = 0
    for r in rows:
        m = uni.get(r["t"]) or {}
        r["name"] = m.get("name")
        r["sector"] = m.get("sector")
        r["industry"] = m.get("industry")
        r["mcap_b"] = m.get("mcap_b")
        r["bucket"] = m.get("bucket")
        if m:
            matched += 1
    doc["universe_join"] = {
        "status": "LIVE" if uni else "MISSING",
        "matched": matched, "of": n,
        "why": None if uni else "data/universe.json absent "
                                "-- fields null, never guessed"}
    beats = sum(1 for r in rows
                if r["eps_a"] > r["eps_e"])
    doc["stats"] = {
        "n_reporters": n,
        "beat_rate_pct": round(100.0 * beats / n, 1)
        if n else None,
        "avg_eps_surprise_pct": round(
            sum(r["eps_surprise_pct"] for r in rows
                if r["eps_surprise_pct"] is not None)
            / max(1, sum(1 for r in rows
                         if r["eps_surprise_pct"]
                         is not None)), 1) if n else None}
    bb = {}
    for b in ("large", "mid", "small", "micro"):
        sub = [r for r in rows if r.get("bucket") == b]
        if sub:
            bb[b] = {"n": len(sub),
                     "beat_rate_pct": round(
                         100.0 * sum(1 for r in sub
                                     if r["eps_a"]
                                     > r["eps_e"])
                         / len(sub), 1)}
    doc["stats"]["by_bucket"] = bb
    for i, r in enumerate(rows):
        r["rank"] = i + 1
    doc["beat_league"] = rows[:250]
    doc["biggest_misses"] = rows[-20:][::-1] if n >= 20 \
        else []

    trl = http_json("%s/earning-call-transcript-latest"
                    "?apikey=%s" % (S, key))
    fresh = {str(x.get("symbol")): x for x in
             (trl if isinstance(trl, list) else [])
             if isinstance(x, dict)}
    by_t = {r["t"]: r for r in rows}
    cands = [t for t in fresh if t in by_t]
    for r in rows[:60]:
        if r["t"] not in cands:
            cands.append(r["t"])
    picks, scanned = [], 0
    for t in cands:
        if scanned >= MAX_TRANSCRIPTS:
            break
        meta = fresh.get(t)
        if meta:
            q = int(str(meta.get("period", "Q0"))[1:] or 0)
            y = meta.get("fiscalYear")
        else:
            dts = http_json("%s/earning-call-transcript-dates"
                            "?symbol=%s&apikey=%s"
                            % (S, t, key))
            if not (isinstance(dts, list) and dts):
                continue
            q = dts[0].get("quarter")
            y = dts[0].get("fiscalYear")
        if not q or not y:
            continue
        tr = http_json("%s/earning-call-transcript?symbol=%s"
                       "&year=%s&quarter=%s&apikey=%s"
                       % (S, t, y, q, key))
        scanned += 1
        if not (isinstance(tr, list) and tr
                and tr[0].get("content")):
            continue
        sc = score_transcript(str(tr[0]["content"]))
        if sc["growth_score"] < PICK_MIN \
                or len(sc["pos_hits"]) < 3 \
                or sc["words"] < 3000:
            continue
        br = by_t.get(t, {})
        pct = (1 - (br.get("rank", n) - 1)
               / max(1, n - 1)) * 100 if n > 1 else 50
        um = uni.get(t) or {}
        pick = {"t": t, "name": um.get("name"),
                "sector": um.get("sector"),
                "mcap_b": um.get("mcap_b"),
                "bucket": um.get("bucket"),
                "call_date": tr[0].get("date"),
                "period": "%sQ%s" % (y, q),
                "growth_score": sc["growth_score"],
                "beat_rank": br.get("rank"),
                "beat_score": br.get("beat_score"),
                "pick_score": round(0.55 * sc["growth_score"]
                                    + 0.45 * pct, 1),
                "demand_flag": sc["demand_flag"],
                "pos_hits": sc["pos_hits"],
                "neg_hits": sc["neg_hits"],
                "evidence": sc["evidence"],
                "words": sc["words"]}
        line, mode = llm_line(key_a, t, sc["evidence"])
        pick["ai_line"] = line
        pick["ai_mode"] = mode
        picks.append(pick)
    picks.sort(key=lambda p: p["pick_score"], reverse=True)
    doc["growth_calls"] = {"scanned": scanned,
                           "picks": picks[:25]}

    hist = _g(HIST_KEY) or {"runs": []}
    hist["runs"].append(
        {"run": now.isoformat(),
         "picks": [{"t": p["t"],
                    "pick_score": p["pick_score"]}
                   for p in picks[:10]],
         "top_beats": [r["t"] for r in rows[:5]]})
    hist["runs"] = hist["runs"][-260:]
    _put(HIST_KEY, hist)
    _put(OUT_KEY, doc)
    return doc


def lambda_handler(event, context):
    doc = build(event)
    return {"statusCode": 200,
            "body": json.dumps({"v": doc.get("v"),
                                "status": doc.get("status")})}
