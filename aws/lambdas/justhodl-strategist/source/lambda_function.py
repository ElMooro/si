"""
justhodl-strategist — THE STRATEGIST: reasons over the WHOLE fleet, not one engine
═══════════════════════════════════════════════════════════════════════════════════
The interpretation gap: every LLM layer in the system tunnel-visions on a slice
(ai-brief reads 14 feeds, ask-desk retrieves 6, debate looks at one pick). A real
CIO surveys the ENTIRE board at once and asks "what is the totality of my system
saying, and where does it contradict itself?" This engine does that.

PIPELINE
  1. ASSEMBLE the full fleet: read engine-manifest.json (~430 engines → feeds) + core
     feeds, thread-fetch every data/*.json, and distill each to a headline VERDICT via a
     generic extractor that handles heterogeneous schemas (verdict-vocabulary strings +
     score/z numbers + top_picks). Freshness from S3 LastModified.
  2. TRUST-WEIGHT every read by the engine's earned effective_trust (engine-trust.json) —
     proven-alpha engines count; net-negative ones are discounted.
  3. COMPUTE fleet consensus (trust-weighted risk-on vs risk-off vs crisis tally),
     the loudest engines (extremity × trust × freshness), and the notable CONTRADICTIONS
     (high-trust engines pointing opposite) — programmatically, before the model reasons.
  4. REASON (llm_router tier=reason → GLM now, auto-upgrades to Sonnet when Claude credits
     return): a structured PM pass — dominant driver + mechanism → confirming → contradicting
     & resolution → second-order implication → decisive call → conviction → FALSIFIERS.
  5. LOG testable claims to data/strategist-log/{date}.json so the interpretation itself can
     be GRADED forward (measure-before-trust applied to JUDGMENT, not just signals).

Output: data/strategist.json + strategist.html.
"""
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/strategist.json"
S3 = boto3.client("s3", REGION)

# Always-include high-signal feeds (the spine of any read), beyond the manifest.
CORE = ["data/risk-regime.json", "data/regime-composite.json", "data/crisis-composite.json",
        "data/strategy-portfolio.json", "data/engine-alpha.json", "data/best-setups.json",
        "data/signal-board.json", "data/eurodollar-plumbing.json", "data/capital-flow-radar.json",
        "data/tail-risk.json", "data/master-ranker.json", "data/crisis-canaries.json"]

# verdict vocabulary → (direction, extremity)
POS = {"RISK_ON": 1.0, "BULLISH": .7, "BULL": .7, "BUY": .7, "STRONG_BUY": 1.0, "EXPANDING": .7,
       "FAVOR": .7, "FAVOR-NOW": 1.0, "ACCUMULATION": .7, "GREED": .6, "EXTREME_GREED": 1.0,
       "LONG": .6, "OVERWEIGHT": .7, "POSITIVE": .6, "UPTREND": .7, "RISK-ON": 1.0, "EXPANSION": .7}
NEG = {"RISK_OFF": 1.0, "BEARISH": .7, "BEAR": .7, "SELL": .7, "STRONG_SELL": 1.0, "SLOWING": .6,
       "CONTRACTING": .8, "CONTRACTION": .8, "AVOID": .7, "DISTRIBUTION": .7, "FEAR": .6,
       "EXTREME_FEAR": 1.0, "SHORT": .6, "UNDERWEIGHT": .7, "NEGATIVE": .6, "DOWNTREND": .7,
       "STRESSED": .9, "ELEVATED": .6, "CRISIS": 1.0, "SEIZING": 1.0, "STRAINED": .8,
       "RISK-OFF": 1.0, "FLIGHT_TO_QUALITY": 1.0, "DETERIORATING": .7}
NEU = {"NEUTRAL", "MIXED", "QUIET", "CALM", "NORMAL", "HOLD", "FUNCTIONING", "STABLE", "WATCH",
       "UNKNOWN", "MILD", "OK", "FLAT", "RANGE", "BALANCED"}

VERDICT_KEYS = ["regime", "posture", "composite_signal", "signal", "verdict", "stance", "call",
                "bias", "recommendation", "defcon_name", "risk_regime", "gamma_regime",
                "overall_signal", "reading", "label", "state", "level", "status", "alpha_status"]
SCORE_KEYS = ["risk_regime_score", "composite_score", "master_crisis_score", "treasury_stress",
              "plumbing_health", "score", "gauge", "z_score", "z", "net", "percentile"]
PICK_KEYS = ["top_picks", "picks", "top_setups", "board", "top_next_up"]


def classify(val):
    if not isinstance(val, str):
        return None
    v = val.strip().upper().replace(" ", "_")
    if v in POS:
        return (1, POS[v], val)
    if v in NEG:
        return (-1, NEG[v], val)
    if v in NEU:
        return (0, 0.0, val)
    return None


def extract(d):
    """Distill a feed dict to {verdict, direction, extremity, picks}."""
    if not isinstance(d, dict):
        return None
    verdict = None
    direction = 0
    extremity = 0.0
    for k in VERDICT_KEYS:
        if k in d:
            c = classify(d[k])
            if c:
                direction, extremity, verdict = c[0], c[1], f"{d[k]}"
                break
    # numeric extremity (boost / fallback)
    if verdict is None:
        for k in SCORE_KEYS:
            if k in d and isinstance(d[k], (int, float)):
                v = float(d[k])
                if k in ("z", "z_score"):
                    extremity = min(1.0, abs(v) / 3.0)
                    direction = 1 if v > 0 else (-1 if v < 0 else 0)
                elif 0 <= v <= 100:
                    extremity = abs(v - 50) / 50.0
                    direction = 1 if v > 55 else (-1 if v < 45 else 0)
                else:
                    extremity = min(1.0, abs(v) / 100.0)
                verdict = f"{k}={round(v, 1)}"
                break
    picks = []
    if verdict is None:
        # one level deep — many feeds nest the verdict (e.g. summary.regime, composite.signal)
        for v in d.values():
            if isinstance(v, dict):
                for k in VERDICT_KEYS:
                    if k in v:
                        c = classify(v[k])
                        if c:
                            direction, extremity, verdict = c[0], c[1], f"{v[k]}"
                            break
                if verdict:
                    break
    for k in PICK_KEYS:
        arr = d.get(k)
        if isinstance(arr, list):
            for it in arr[:5]:
                t = it.get("ticker") if isinstance(it, dict) else it
                if t:
                    picks.append(str(t).upper())
            if picks:
                break
        elif isinstance(arr, dict):
            picks = [str(x).upper() for x in list(arr.keys())[:5]]
            if picks:
                break
    if verdict is None and not picks:
        return None
    return {"verdict": verdict, "direction": direction, "extremity": round(extremity, 2), "picks": picks}


def _parse_json(raw):
    """Tolerant extraction of the JSON object GLM returns (it often malforms slightly)."""
    txt = (raw or "").strip()
    txt = re.sub(r"^```[a-z]*", "", txt).strip("`").strip()
    i = txt.find("{")
    if i < 0:
        return None, txt
    depth, end = 0, -1
    for j in range(i, len(txt)):
        if txt[j] == "{":
            depth += 1
        elif txt[j] == "}":
            depth -= 1
            if depth == 0:
                end = j + 1
                break
    cand = txt[i:end] if end > 0 else txt[i:]
    for attempt in (cand, re.sub(r",\s*([}\]])", r"\1", cand)):
        try:
            return json.loads(attempt), None
        except Exception:
            continue
    return None, cand


def load(key):
    try:
        o = S3.get_object(Bucket=BUCKET, Key=key)
        d = json.loads(o["Body"].read())
        lm = o["LastModified"]
        age_h = (datetime.now(timezone.utc) - lm).total_seconds() / 3600.0
        return key, d, age_h
    except Exception:
        return key, None, None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    # 1. fleet feed list
    try:
        man = json.loads(S3.get_object(Bucket=BUCKET, Key="data/engine-manifest.json")["Body"].read())
        feeds = {}
        for e in man.get("engines", []):
            ks = e.get("keys") or []
            if ks:
                feeds[ks[0]] = e["engine"].replace("justhodl-", "")
    except Exception as ex:
        feeds = {}
        print(f"[strat] manifest err {str(ex)[:60]}")
    for c in CORE:
        feeds.setdefault(c, c.replace("data/", "").replace(".json", ""))

    # trust map
    trust = {}
    try:
        et = json.loads(S3.get_object(Bucket=BUCKET, Key="data/engine-trust.json")["Body"].read())
        for e in et.get("engines", []):
            nm = (e.get("signal_type") or e.get("engine") or "").replace("eng:", "").replace("justhodl-", "")
            t = e.get("effective_trust")
            if nm and t is not None:
                trust[nm] = t
    except Exception:
        pass

    # 2. fetch fleet
    items = []
    with ThreadPoolExecutor(max_workers=24) as ex:
        for key, d, age_h in ex.map(load, list(feeds.keys())):
            if d is None:
                continue
            info = extract(d)
            if not info:
                continue
            short = feeds.get(key, key.replace("data/", "").replace(".json", ""))
            tr = trust.get(short, trust.get(short.replace("-", "_"), 0.5))
            fresh = age_h is not None and age_h < 240   # active within ~10 days
            items.append({"engine": short, "key": key, "age_h": round(age_h or 9999, 1), "fresh": fresh,
                          "trust": round(tr, 2), **info,
                          "salience": round(info["extremity"] * (0.4 + tr) * (1.0 if fresh else 0.3), 3)})

    fresh_items = [i for i in items if i["fresh"]]
    # 3. consensus (trust-weighted), loud engines, contradictions
    pos_w = sum((i["trust"]) * i["extremity"] for i in fresh_items if i["direction"] > 0)
    neg_w = sum((i["trust"]) * i["extremity"] for i in fresh_items if i["direction"] < 0)
    n_pos = sum(1 for i in fresh_items if i["direction"] > 0)
    n_neg = sum(1 for i in fresh_items if i["direction"] < 0)
    n_neu = sum(1 for i in fresh_items if i["direction"] == 0)
    net = pos_w - neg_w
    consensus = ("RISK-ON" if net > 1.5 else "RISK-OFF" if net < -1.5 else "MIXED")
    loud = sorted(fresh_items, key=lambda x: -x["salience"])[:55]
    # contradictions: high-trust loud engines disagreeing
    strong_pos = [i for i in loud if i["direction"] > 0 and i["trust"] >= 0.5 and i["extremity"] >= 0.6]
    strong_neg = [i for i in loud if i["direction"] < 0 and i["trust"] >= 0.5 and i["extremity"] >= 0.6]
    contradictions = []
    for a in strong_pos[:6]:
        for b in strong_neg[:6]:
            contradictions.append(f"{a['engine']} ({a['verdict']}) vs {b['engine']} ({b['verdict']})")
    contradictions = contradictions[:8]
    # aggregate high-conviction picks across pick engines
    pick_tally = {}
    for i in fresh_items:
        for p in i["picks"]:
            pick_tally[p] = pick_tally.get(p, 0) + i["trust"]
    top_picks = sorted(pick_tally.items(), key=lambda x: -x[1])[:12]

    # proven-alpha book + regime context
    def grab(key, *fields):
        for i in items:
            if i["key"] == key:
                return i
        return None
    regime_i = grab("data/risk-regime.json") or grab("data/regime-composite.json")
    crisis_i = grab("data/crisis-composite.json")

    # 4. build briefing for the model
    brief = []
    brief.append(f"FLEET CONSENSUS: {consensus} (trust-wt risk-on {pos_w:.1f} vs risk-off {neg_w:.1f}; "
                 f"{n_pos} engines positive / {n_neg} negative / {n_neu} neutral, of {len(fresh_items)} fresh).")
    if regime_i:
        brief.append(f"REGIME: {regime_i['engine']} → {regime_i['verdict']}.")
    if crisis_i:
        brief.append(f"CRISIS: {crisis_i['engine']} → {crisis_i['verdict']}.")
    # proven book
    try:
        sp = json.loads(S3.get_object(Bucket=BUCKET, Key="data/strategy-portfolio.json")["Body"].read())
        if sp.get("ok"):
            rw = sp.get("recommended", {}).get("weights", {})
            brief.append("PROVEN-ALPHA BOOK (HRP): " + ", ".join(f"{k} {round(v*100)}%" for k, v in rw.items()))
    except Exception:
        pass
    brief.append("\nLOUDEST ENGINES (verdict · trust · age_h):")
    for i in loud[:45]:
        brief.append(f"  {i['engine']}: {i['verdict']} · t{i['trust']} · {i['age_h']}h")
    if contradictions:
        brief.append("\nNOTABLE CONTRADICTIONS:\n  " + "\n  ".join(contradictions))
    if top_picks:
        brief.append("\nMOST-BACKED NAMES (trust-wt): " + ", ".join(f"{p}({round(w,1)})" for p, w in top_picks))
    briefing = "\n".join(brief)

    # 5. reason
    SYS = ("You are the Chief Investment Officer of a quant macro fund. Below is the DISTILLED "
           "state of your ENTIRE signal fleet (~430 engines), each engine's verdict weighted by the "
           "track-record trust it has earned, plus the fleet consensus, contradictions, and your "
           "proven-alpha book. Reason across the WHOLE board like a top PM — do not fixate on one "
           "engine. Be decisive and honest; name the real tension. "
           "Output ONLY a single valid minified JSON object — no markdown, no prose, no trailing "
           "commas, and escape any double-quotes inside string values. Schema: "
           '{"dominant_driver":"...","mechanism":"...","confirming":["..."],'
           '"contradicting":[{"tension":"...","resolution":"..."}],"second_order":["..."],'
           '"decisive_call":"posture + what to favor/avoid","conviction":0-100,'
           '"falsifiers":["specific observable that would prove this read wrong"],'
           '"key_claims":[{"claim":"testable directional claim","horizon_days":N}]}')
    reasoning = None
    model_used = None
    try:
        from llm_router import complete
        raw = complete(briefing, tier="reason", max_tokens=2400, system=SYS)
        model_used = "glm-reason"
        parsed, raw_fallback = _parse_json(raw)
        if parsed:
            reasoning = parsed
        else:
            reasoning = {"raw": (raw_fallback or raw or "")[:1800],
                         "parse_note": "model returned malformed JSON; raw read preserved"}
    except Exception as ex:
        print(f"[strat] reasoning err {str(ex)[:120]}")
        reasoning = {"error": f"reasoning model unavailable: {str(ex)[:80]}",
                     "note": "Fleet state assembled; restore an LLM tier to enable interpretation."}

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    payload = {
        "engine": "justhodl-strategist", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(), "model": model_used,
        "thesis": "Whole-fleet interpretation: reasons across all ~430 engines weighted by earned trust, not one engine.",
        "fleet": {"n_feeds_read": len(items), "n_fresh": len(fresh_items),
                  "consensus": consensus, "risk_on_weight": round(pos_w, 1), "risk_off_weight": round(neg_w, 1),
                  "n_positive": n_pos, "n_negative": n_neg, "n_neutral": n_neu},
        "loudest_engines": [{"engine": i["engine"], "verdict": i["verdict"], "direction": i["direction"],
                             "trust": i["trust"], "extremity": i["extremity"], "age_h": i["age_h"]} for i in loud],
        "contradictions": contradictions,
        "most_backed_names": [{"ticker": p, "trust_weight": round(w, 1)} for p, w in top_picks],
        "interpretation": reasoning,
        "briefing_preview": briefing[:1400],
        "caveats": [
            "Generic extractor distills ~430 heterogeneous feeds to a headline verdict — robust at fleet scale "
            "but a single engine's nuance may be flattened; the loudest/highest-trust reads dominate by design.",
            "Reasoning runs on GLM (tier=reason) because Anthropic credits are out; auto-upgrades to Sonnet "
            "(tier=critical) the moment credits are restored — interpretation quality roughly doubles then.",
            "Interpretation is MEASURE-BEFORE-TRUST: key_claims + falsifiers are logged daily for forward grading "
            "(the interpretation scorecard) — until that matures, treat the read as a rigorous hypothesis, not gospel.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    # 5b. log testable claims for forward grading
    if isinstance(reasoning, dict) and reasoning.get("key_claims"):
        log = {"date": today, "generated_at": payload["generated_at"], "model": model_used,
               "consensus": consensus, "dominant_driver": reasoning.get("dominant_driver"),
               "decisive_call": reasoning.get("decisive_call"), "conviction": reasoning.get("conviction"),
               "key_claims": reasoning.get("key_claims"), "falsifiers": reasoning.get("falsifiers")}
        try:
            S3.put_object(Bucket=BUCKET, Key=f"data/strategist-log/{today}.json",
                          Body=json.dumps(log, default=str).encode(), ContentType="application/json")
        except Exception as ex:
            print(f"[strat] log err {str(ex)[:60]}")
    print(f"[strat] feeds={len(items)} fresh={len(fresh_items)} consensus={consensus} "
          f"contradictions={len(contradictions)} model={model_used} in {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_feeds": len(items), "n_fresh": len(fresh_items), "consensus": consensus,
        "dominant_driver": (reasoning or {}).get("dominant_driver"),
        "conviction": (reasoning or {}).get("conviction")})}
