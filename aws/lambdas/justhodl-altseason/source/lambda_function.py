"""
justhodl-altseason v1.0 — The Confirm/Reject Tribunal
=====================================================
Not one signal — a voting panel of the metrics that rightfully detect
altcoin-season start, each with a measured threshold, each casting
CONFIRM / REJECT / NEUTRAL:

  1. ETH/BTC thrust (season-starter: 180d low → ≥25% rip ≤60 sessions)
  2. ETH/BTC trend (above 200DMA; 50>200 golden state)
  3. ALTSEASON INDEX — % of alt basket beating BTC over 90d (canonical
     ≥70% = season; computed as a full HISTORY so the threshold itself
     is event-studied with real sequels)
  4. Alt breadth above own 90DMA
  5. Alt 90d-high breadth
  6. Alt volume share (alt $vol / total, 14d MA, 1y percentile) — the
     retail-rotation footprint, from candle volumes
  7. BTC consolidation (30d realized-vol percentile low + near highs:
     calm leader = rotation environment; vol shock = reject)
  8. MACRO GATES — regime quadrant + crisis canaries from the desk's own
     feeds; risk-off vetoes weigh on the reject side

A weighted score → phase ladder DORMANT / SETUP / IGNITION / CONFIRMED
(with REJECTED overlay), first cross into CONFIRMED logs to the closed
loop, and a server-side Claude tribunal narrates ONLY these votes.
Data: Coinbase Exchange daily candles (proven source), 16-alt basket.
"""
import json, os, time, re, urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/altseason.json"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
VERSION = "1.0.1"
UA = {"User-Agent": "JustHodl Research admin@justhodl.ai"}
DIAG = []

ALTS = [("LTC", "2016-09-01"), ("BCH", "2017-12-20"), ("ETC", "2018-08-07"),
        ("XLM", "2019-03-13"), ("LINK", "2019-07-01"), ("ALGO", "2019-08-14"),
        ("XTZ", "2019-08-05"), ("UNI", "2020-09-17"), ("AAVE", "2020-12-15"),
        ("FIL", "2020-12-09"), ("ADA", "2021-03-20"), ("SOL", "2021-06-01"),
        ("DOGE", "2021-06-03"), ("DOT", "2021-06-16"), ("SHIB", "2021-09-09"),
        ("AVAX", "2021-09-29")]


def cb_daily(product, start):
    """Coinbase Exchange daily candles (close, volume), paginated 300/req."""
    out = {}
    try:
        cur = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_all = datetime.now(timezone.utc)
        while cur < end_all:
            seg = min(cur + timedelta(days=295), end_all)
            u = (f"https://api.exchange.coinbase.com/products/{product}/candles"
                 f"?granularity=86400&start={cur.strftime('%Y-%m-%dT00:00:00Z')}"
                 f"&end={seg.strftime('%Y-%m-%dT00:00:00Z')}")
            req = urllib.request.Request(u, headers=UA)
            j = json.loads(urllib.request.urlopen(req, timeout=40).read())
            if isinstance(j, list):
                for row in j:
                    try:
                        d = datetime.fromtimestamp(row[0], tz=timezone.utc).date().isoformat()
                        out[d] = (float(row[4]), float(row[5]))
                    except Exception:
                        pass
            else:
                DIAG.append(f"{product}: {str(j)[:50]}")
            cur = seg
            time.sleep(0.12)
    except Exception as e:
        DIAG.append(f"{product}: {str(e)[:60]}")
    return out


def thrust_state(dates, vals):
    if len(vals) < 200:
        return None
    lo180 = min(vals[-180:])
    lo_i = len(vals) - 180 + vals[-180:].index(lo180)
    off = (vals[-1] / lo180 - 1) * 100
    return {"value": round(vals[-1], 6), "off_180d_low_pct": round(off, 1),
            "days_since_180d_low": len(vals) - 1 - lo_i,
            "thrust_live": bool(off >= 25 and len(vals) - 1 - lo_i <= 60),
            "as_of": dates[-1]}


def sma(vals, n, i=None):
    i = len(vals) - 1 if i is None else i
    if i + 1 < n:
        return None
    return sum(vals[i - n + 1:i + 1]) / n


def s3json(keys):
    for k in keys:
        try:
            return k, json.loads(S3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
        except Exception:
            continue
    return None, None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    DIAG.clear()
    prev_key, prev = s3json([OUT_KEY])
    prev_score = ((prev or {}).get("composite") or {}).get("score")

    btc = cb_daily("BTC-USD", "2015-01-15")
    eth = cb_daily("ETH-USD", "2016-06-01")
    alts = {}
    for sym, since in ALTS:
        h = cb_daily(f"{sym}-USD", since)
        if len(h) > 200:
            alts[sym] = h
    DIAG.append(f"coins: BTC {len(btc)} · ETH {len(eth)} · alts {len(alts)}/16")
    firsts = {sym: h[min(h)][0] for sym, h in alts.items()}

    dates = sorted(btc)
    bidx = {d: i for i, d in enumerate(dates)}
    bclose = [btc[d][0] for d in dates]

    # ── ETH/BTC derived series + thrust + trend ──
    eb_dates = [d for d in dates if d in eth]
    eb = [eth[d][0] / btc[d][0] for d in eb_dates]
    ts = thrust_state(eb_dates, eb)
    ma50, ma200 = sma(eb, 50), sma(eb, 200)
    ethbtc_trend = {"above_200dma": bool(ma200 and eb[-1] > ma200),
                     "golden": bool(ma50 and ma200 and ma50 > ma200),
                     "ma50": round(ma50, 6) if ma50 else None,
                     "ma200": round(ma200, 6) if ma200 else None}

    # ── per-date alt aggregates → histories ──
    hist = {"alt_index": [], "breadth_90dma": [], "high90": [], "vol_share": []}
    basket_usd = {}
    raw_share = []
    for d in dates:
        i = bidx[d]
        if i < 90:
            continue
        d90 = dates[i - 90]
        btc_r90 = bclose[i] / bclose[i - 90] - 1
        beat = above = hi = avail = 0
        altdv = 0.0
        bk = []
        for sym, h in alts.items():
            if d not in h:
                continue
            c, v = h[d]
            altdv += c * v
            if d90 in h:
                avail += 1
                if c / h[d90][0] - 1 > btc_r90:
                    beat += 1
                win = [h[dates[j]][0] for j in range(i - 89, i + 1) if dates[j] in h]
                if len(win) >= 80:
                    if c > sum(win) / len(win):
                        above += 1
                    if c >= max(win) * 0.999:
                        hi += 1
                bk.append(c / firsts[sym])
        if avail >= 8:
            hist["alt_index"].append((d, round(100 * beat / avail, 1)))
            hist["breadth_90dma"].append((d, round(100 * above / avail, 1)))
            hist["high90"].append((d, round(100 * hi / avail, 1)))
            basket_usd[d] = sum(bk) / len(bk)
        tot = altdv + bclose[i] * btc[d][1] + (eth[d][0] * eth[d][1] if d in eth else 0)
        if tot and altdv:
            raw_share.append((d, 100 * altdv / tot))
    # 14d MA of volume share + 1y percentile
    sh_d = [d for d, _ in raw_share]
    sh_v = [v for _, v in raw_share]
    for i in range(13, len(sh_v)):
        m = sum(sh_v[i - 13:i + 1]) / 14
        hist["vol_share"].append((sh_d[i], round(m, 2)))
    vs_now = vs_pct = None
    if len(hist["vol_share"]) > 250:
        tail = [v for _, v in hist["vol_share"][-365:]]
        vs_now = tail[-1]
        vs_pct = round(100 * sum(1 for x in tail if x < vs_now) / len(tail), 1)

    # ── BTC consolidation ──
    rv = []
    for i in range(30, len(bclose)):
        ch = [bclose[j] / bclose[j - 1] - 1 for j in range(i - 29, i + 1)]
        mu = sum(ch) / 30
        rv.append((dates[i], (sum((x - mu) ** 2 for x in ch) / 30) ** 0.5 * (365 ** 0.5) * 100))
    rv_now = rv[-1][1]
    rv_tail = [v for _, v in rv[-365:]]
    rv_pct = round(100 * sum(1 for x in rv_tail if x < rv_now) / len(rv_tail), 1)
    btc_vs_hi = round((bclose[-1] / max(bclose[-365:]) - 1) * 100, 1)
    btc_cons = {"rv30_ann_pct": round(rv_now, 1), "rv30_1y_pctile": rv_pct,
                 "vs_365d_high_pct": btc_vs_hi,
                 "consolidating": bool(rv_pct <= 40 and btc_vs_hi >= -15)}

    # ── event study: alt_index crossing UP through 70 (and 50) ──
    ai_d = [d for d, _ in hist["alt_index"]]
    ai_v = [v for _, v in hist["alt_index"]]
    def crossings(level):
        evs, i = [], 1
        while i < len(ai_v):
            if ai_v[i - 1] < level <= ai_v[i]:
                evs.append(i)
                i += 120
            else:
                i += 1
        return evs
    def bk_fwd(i, w):
        d0 = ai_d[i]
        tgt = (datetime.strptime(d0, "%Y-%m-%d") + timedelta(days=w)).date().isoformat()
        ks = sorted(basket_usd)
        import bisect
        j = bisect.bisect_left(ks, tgt)
        if j >= len(ks) or abs((datetime.strptime(ks[j], "%Y-%m-%d")
                                 - datetime.strptime(tgt, "%Y-%m-%d")).days) > 6:
            return None
        a = basket_usd.get(d0)
        return round((basket_usd[ks[j]] / a - 1) * 100, 1) if a else None
    study = {}
    for lv in (50, 70):
        rows = [{"date": ai_d[i], "alt_index": ai_v[i],
                  "basket_fwd_30": bk_fwd(i, 30), "basket_fwd_90": bk_fwd(i, 90)}
                 for i in crossings(lv)]
        def st(key):
            xs = sorted(x[key] for x in rows if x[key] is not None)
            return ({"n": len(xs), "median_pct": xs[len(xs) // 2],
                      "pos_pct": round(100 * sum(1 for x in xs if x > 0) / len(xs), 1)}
                     if xs else None)
        study[f"cross_up_{lv}"] = {"events": rows, "fwd_30": st("basket_fwd_30"),
                                     "fwd_90": st("basket_fwd_90")}

    # ── gates from the desk ──
    _, regime = s3json(["data/regime.json"])
    quad = ((regime or {}).get("current") or {}).get("quadrant")
    _, can = s3json(["data/crisis-canaries.json", "data/canaries.json",
                      "data/funding-canaries.json"])
    can_level = None
    if isinstance(can, dict):
        can_level = (can.get("composite") or {}).get("level") or can.get("level") \
                     or (can.get("score") or {}).get("level")

    # ── votes ──
    ai_now = ai_v[-1] if ai_v else None
    br_now = hist["breadth_90dma"][-1][1] if hist["breadth_90dma"] else None
    hi_now = hist["high90"][-1][1] if hist["high90"] else None
    V = []
    def vote(name, value, conf, rej, w, note):
        v = "CONFIRM" if conf else "REJECT" if rej else "NEUTRAL"
        V.append({"metric": name, "value": value, "vote": v, "weight": w, "note": note})
    vote("ETH/BTC thrust", f"{ts['off_180d_low_pct']}% off low, {ts['days_since_180d_low']}d"
          if ts else None, ts and ts["thrust_live"],
          ts and ts["off_180d_low_pct"] < 5 and ts["days_since_180d_low"] > 90, 18,
          "season-starter: 180d low → ≥25% in ≤60 sessions")
    vote("ETH/BTC > 200DMA", ethbtc_trend["above_200dma"], ethbtc_trend["above_200dma"],
          not ethbtc_trend["above_200dma"], 8, "trend confirmation")
    vote("ETH/BTC golden (50>200)", ethbtc_trend["golden"], ethbtc_trend["golden"],
          False, 6, "regime persistence")
    vote("Altseason Index (% alts beat BTC 90d)", ai_now, ai_now is not None and ai_now >= 70,
          ai_now is not None and ai_now <= 30, 20, "canonical ≥70% = season")
    vote("Alt breadth > 90DMA", br_now, br_now is not None and br_now >= 60,
          br_now is not None and br_now <= 25, 10, "participation")
    vote("Alt 90d-high breadth", hi_now, hi_now is not None and hi_now >= 25,
          False, 8, "leadership expansion")
    vote("Alt volume share (1y pctile)", vs_pct, vs_pct is not None and vs_pct >= 70,
          vs_pct is not None and vs_pct <= 20, 10, f"14d share {vs_now}% of spot $vol")
    vote("BTC consolidation", f"RV pctile {rv_pct}, {btc_vs_hi}% vs hi",
          btc_cons["consolidating"], rv_pct >= 85, 8,
          "calm leader = rotation env; vol shock = reject")
    vote("Macro regime gate", quad, quad in ("GOLDILOCKS", "REFLATION"),
          quad in ("STAGFLATION", "DEFLATION-BUST"), 7, "from data/regime.json")
    vote("Crisis canaries gate", can_level, can_level == "CALM",
          can_level in ("ELEVATED", "ACUTE", "ACUTE_STRESS", "CRITICAL"), 5,
          "funding-stress veto")

    wtot = sum(x["weight"] for x in V)
    score = round(100 * sum(x["weight"] for x in V if x["vote"] == "CONFIRM") / wtot, 1)
    rej_w = sum(x["weight"] for x in V if x["vote"] == "REJECT")
    rejected = bool(rej_w >= 35 or (quad in ("STAGFLATION", "DEFLATION-BUST")
                                      and can_level not in (None, "CALM")))
    phase = ("CONFIRMED" if score >= 70 else "IGNITION" if score >= 50 else
              "SETUP" if score >= 25 else "DORMANT")
    composite = {"score": score, "phase": phase, "rejected_overlay": rejected,
                  "reject_weight": rej_w,
                  "confirms": [x["metric"] for x in V if x["vote"] == "CONFIRM"],
                  "rejects": [x["metric"] for x in V if x["vote"] == "REJECT"]}

    # closed loop: first cross into CONFIRMED
    n_logged = 0
    if score >= 70 and (prev_score is None or prev_score < 70) and not rejected:
        try:
            nowt = datetime.now(timezone.utc)
            st70 = (study.get("cross_up_70") or {}).get("fwd_90") or {}
            px = eth[max(eth)][0]
            DDB.Table("justhodl-signals").put_item(Item={
                "signal_id": f"altseason-confirmed#ETH#{nowt.strftime('%Y-%m-%d')}",
                "signal_type": "altseason_confirmed", "signal_value": f"score{score}",
                "predicted_direction": "UP", "confidence": Decimal(str(
                    round(min(0.68, 0.45 + (st70.get("pos_pct", 50) / 100) * 0.25), 2))),
                "measure_against": "ticker", "baseline_price": str(px),
                "benchmark": "SPY", "check_windows": ["day_21", "day_63"],
                "check_timestamps": {f"day_{w}": (nowt + timedelta(days=w)).isoformat()
                                      for w in (21, 63)},
                "outcomes": {}, "accuracy_scores": {}, "logged_at": nowt.isoformat(),
                "logged_epoch": int(nowt.timestamp()), "status": "pending",
                "schema_version": "2", "horizon_days_primary": 63,
                "regime_at_log": quad or "NA",
                "ttl": int(nowt.timestamp()) + 150 * 86400,
                "metadata": {"engine": "altseason", "v": VERSION,
                             "confirms": str(len(composite["confirms"]))},
                "rationale": (f"Altseason tribunal CONFIRMED at {score} "
                               f"({len(composite['confirms'])} confirms). Historical "
                               f"index-cross-70 sequel: basket +90d median "
                               f"{st70.get('median_pct')}% ({st70.get('pos_pct')}%+, "
                               f"n={st70.get('n')})")})
            n_logged = 1
        except Exception as e:
            print(f"[log] {str(e)[:60]}")

    out = {"engine": "altseason", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "composite": composite, "votes": V,
           "ethbtc": {"thrust": ts, "trend": ethbtc_trend},
           "btc_consolidation": btc_cons,
           "histories": {k: v[-500:] for k, v in hist.items()},
           "event_study": study,
           "gates": {"regime_quadrant": quad, "canaries_level": can_level},
           "signals_logged": n_logged, "diagnostics": list(DIAG),
           "methodology": (
             "Voting tribunal of measured altseason detectors: ETH/BTC thrust + trend, "
             "the canonical Altseason Index (% of 16-alt Coinbase basket beating BTC "
             "over 90d, full history so the 50/70 thresholds carry event-studied "
             "sequels), breadth above 90DMA, 90d-high breadth, alt volume share "
             "percentile, BTC consolidation regime, and the desk's own macro-regime "
             "and crisis-canary gates as vetoes. Weighted score → DORMANT/SETUP/"
             "IGNITION/CONFIRMED with REJECTED overlay; first CONFIRMED cross logs "
             "to the closed loop at sequel-table confidence.")}
    out["duration_s"] = round(time.time() - t0, 1)
    # AI tribunal
    ai = {"error": None}
    try:
        if ANTHROPIC_KEY:
            compact = {"composite": composite, "votes": V,
                        "alt_index_now": ai_now, "event_study_stats":
                          {k: {kk: v.get(kk) for kk in ("fwd_30", "fwd_90")}
                           for k, v in study.items()},
                        "gates": out["gates"], "btc": btc_cons,
                        "ethbtc_thrust": ts}
            prompt = ("You are the ALTSEASON TRIBUNAL — a cross-asset judge. Using ONLY "
                       "the votes and measured tables below, return JSON keys: verdict "
                       "(<=160 chars, name the phase and score), confirmations (what is "
                       "voting CONFIRM and why it matters), rejections (what vetoes), "
                       "phase_read (where in DORMANT→SETUP→IGNITION→CONFIRMED we are and "
                       "the single next domino), what_flips_confirmed (exact numeric "
                       "triggers), what_invalidates, watch_next (array of 3 short "
                       "strings). Cite n from event-study stats when referencing "
                       "thresholds. <380 words. JSON only.\n\nDATA:\n"
                       + json.dumps(compact, default=str))
            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages",
                data=json.dumps({"model": "claude-haiku-4-5-20251001", "max_tokens": 2100,
                                  "messages": [{"role": "user", "content": prompt}]}).encode(),
                headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01",
                         "content-type": "application/json"})
            rj = json.loads(urllib.request.urlopen(req, timeout=90).read())
            txt = "".join(b.get("text", "") for b in rj.get("content", [])
                           if b.get("type") == "text")
            txt = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", txt)
            ai.update(json.loads(txt[txt.find("{"):txt.rfind("}") + 1]))
        else:
            ai["error"] = "no ANTHROPIC_API_KEY"
    except Exception as e:
        ai["error"] = str(e)[:120]
    out["ai_brief"] = ai
    # NaN/Infinity are valid to Python's json but fatal to browser JSON.parse —
    # scrub by round-tripping with parse_constant→None before publishing.
    clean = json.loads(json.dumps(out, default=str), parse_constant=lambda c: None)
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(clean).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[altseason] {phase} {score} confirms={len(composite['confirms'])} "
          f"rejects={len(composite['rejects'])} alt_index={ai_now} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"phase": phase, "score": score})}
