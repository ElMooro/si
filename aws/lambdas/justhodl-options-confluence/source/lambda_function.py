"""
justhodl-options-confluence  ·  v1.0  —  THE OPTIONS/DEALER SYNTHESIZER
================================================================================
The audit found the options/dealer cluster was fragmented: ~21 engines, none
cross-reading each other. Each saw one slice (dealer gamma, call/put flow, skew,
vol-squeeze) but nothing fused them, so a name lit up by FOUR independent options
engines looked the same as one lit by a single noisy feed. This synthesizer closes
that gap — it reads the per-ticker options engines and produces ONE net options
posture per name, with a confluence count, so cross-engine agreement becomes legible.

Fuses (per ticker, alpha-gated):
  • directional flow   — options-analytics, polygon-options-flow, options-flow-scanner
  • pre-catalyst skew   — catalyst-skew-premove (call/put premium ratio)
  • put/call extremes    — put-call-extreme
  • dealer gamma         — dealer-gex, options-gamma (net_gex < 0 = dealers short gamma
                            = squeeze fuel / upside convexity)
  • vol compression      — volatility-squeeze, precatalyst-vol-expansion (COILED setups)

Posture per name: SQUEEZE_FUEL (neg-gamma + bullish flow) / BULLISH_FLOW /
BEARISH_FLOW / COILED (compressed, awaiting catalyst) / MIXED. The ALPHA_PROVEN
squeeze_risk engine lifts the squeeze read; any ALPHA_NEGATIVE options engine is
dropped via the engine-trust gate. Output is a clean signal other engines can read.
"""
import json, time, re
from datetime import datetime, timezone
import boto3

VERSION = "1.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/options-confluence.json"
s3 = boto3.client("s3", "us-east-1")

# engine file -> the engine-trust signal_type to gate on (where one exists)
TRUST_KEY = {"dealer-gex": "dealer_gex", "options-analytics": "options_analytics",
             "volatility-squeeze": "squeeze_risk"}  # vol-squeeze inherits the PROVEN squeeze engine

_BULL = re.compile(r"bull|call|long|accumulat|buy|up\b", re.I)
_BEAR = re.compile(r"bear|put|short|distribut|sell|down\b", re.I)


def _read(key):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return {}

def _tk(x):
    if isinstance(x, dict): return (x.get("ticker") or x.get("symbol") or "").upper()
    if isinstance(x, str): return x.strip().upper()
    return ""

def _rows(doc):
    """All top-level lists of per-ticker dicts, concatenated. Robust to varied schemas."""
    out = []
    if isinstance(doc, dict):
        for v in doc.values():
            if isinstance(v, list) and v and isinstance(v[0], dict) and _tk(v[0]):
                out.extend(v)
        # also handle a {ticker: {...}} map
        for k, v in doc.items():
            if isinstance(v, dict) and isinstance(k, str) and 1 <= len(k) <= 6 and k.isupper():
                vv = dict(v); vv.setdefault("ticker", k); out.append(vv)
    return out

def _dir_from(item):
    """Infer (direction in -1..1, magnitude 0..1) from whatever fields an engine exposes."""
    # explicit direction/signal/side text
    for f in ("direction", "signal", "side", "bias", "sentiment", "posture"):
        val = item.get(f)
        if isinstance(val, str):
            if _BULL.search(val) and not _BEAR.search(val): return 1.0, 0.6
            if _BEAR.search(val) and not _BULL.search(val): return -1.0, 0.6
    # call/put premium or ratio
    for f in ("call_put_ratio", "cpr", "pc_call_ratio"):
        v = item.get(f)
        if isinstance(v, (int, float)) and v > 0:
            return (1.0, min(1.0, abs(v - 1))) if v >= 1.05 else ((-1.0, min(1.0, abs(1 - v))) if v <= 0.95 else (0.0, 0.0))
    cp, pp = item.get("call_premium"), item.get("put_premium")
    if isinstance(cp, (int, float)) and isinstance(pp, (int, float)) and (cp + pp) > 0:
        net = (cp - pp) / (cp + pp)
        return (1.0 if net > 0 else -1.0 if net < 0 else 0.0), min(1.0, abs(net))
    pc = item.get("put_call") or item.get("pc_ratio")
    if isinstance(pc, (int, float)) and pc > 0:
        return (-1.0, min(1.0, abs(pc - 1))) if pc >= 1.05 else ((1.0, min(1.0, abs(1 - pc))) if pc <= 0.95 else (0.0, 0.0))
    return 0.0, 0.0


def lambda_handler(event, context):
    t0 = time.time()
    et = _read("data/engine-trust.json")
    trust_by = {e.get("signal_type"): e for e in (et.get("engines") or []) if isinstance(e, dict)}
    def gated(file_key):
        """(include?, lift_mult) per the alpha gate."""
        st = TRUST_KEY.get(file_key)
        info = trust_by.get(st) if st else None
        if not info: return True, 1.0
        if info.get("alpha_status") == "ALPHA_NEGATIVE": return False, 1.0
        if info.get("alpha_status") == "ALPHA_PROVEN":
            return True, max(1.0, min(1.35, info.get("effective_trust") or 1.0))
        return True, 1.0

    acc = {}  # ticker -> aggregates
    def touch(tk):
        return acc.setdefault(tk, {"ticker": tk, "score": 0.0, "engines": set(),
                                   "neg_gamma": False, "coiled": False, "tags": []})
    def add(tk, eng, dscore=0.0, neg_gamma=False, coiled=False, tag=None):
        if not tk or len(tk) > 6: return
        a = touch(tk); a["engines"].add(eng); a["score"] += dscore
        if neg_gamma: a["neg_gamma"] = True
        if coiled: a["coiled"] = True
        if tag and tag not in a["tags"]: a["tags"].append(tag)

    # 1. options-analytics — the richest source: board(58)+top_picks+squeeze_setups+most_unusual
    inc, lift = gated("options-analytics")
    if inc:
        oa = _read("data/options-analytics.json")
        for bk in ("board", "top_picks", "squeeze_setups", "most_unusual"):
            for it in (oa.get(bk) or []):
                if not isinstance(it, dict): continue
                tk = _tk(it)
                gr = (it.get("gamma_regime") or "").lower()
                ng = it.get("net_gex_musd"); ng = it.get("net_gex_musd_per_1pct") if ng is None else ng
                neg = ("short" in gr) or (isinstance(ng, (int, float)) and ng < 0)
                d = 0.0
                dirf = (it.get("direction") or "").lower()
                if "bull" in dirf or "long" in dirf: d += 0.6
                elif "bear" in dirf or "short" in dirf: d -= 0.6
                pcr = it.get("pcr_vol")
                if isinstance(pcr, (int, float)) and pcr > 0:
                    d += 0.4 if pcr < 0.9 else (-0.4 if pcr > 1.1 else 0.0)
                npx = it.get("net_premium_usd")
                if isinstance(npx, (int, float)) and npx: d += 0.3 if npx > 0 else -0.3
                add(tk, "options-analytics", d * lift, neg_gamma=neg, coiled=(bk == "squeeze_setups"),
                    tag=("squeeze setup" if bk == "squeeze_setups" else ("dealers short gamma" if neg else None)))

    # 2. dealer-gex — underlyings MAP + squeeze_candidates
    inc, _ = gated("dealer-gex")
    if inc:
        dg = _read("data/dealer-gex.json")
        und = dg.get("underlyings") or {}
        if isinstance(und, dict):
            for sym, v in und.items():
                if not isinstance(v, dict): continue
                g = v.get("net_gex"); g = v.get("net_gex_billions") if g is None else g
                g = v.get("total_gex") if g is None else g
                neg = isinstance(g, (int, float)) and g < 0
                add(str(sym).upper(), "dealer-gex", 0.0, neg_gamma=neg,
                    tag="dealers short gamma" if neg else None)
        for it in (dg.get("squeeze_candidates") or []):
            if isinstance(it, dict): add(_tk(it), "dealer-gex", 0.2, neg_gamma=True, tag="GEX squeeze candidate")

    # 3. polygon-options-flow — bullish/extreme call-flow lists (+) + all_results cv_pv_ratio
    inc, _ = gated("polygon-options-flow")
    if inc:
        pf = _read("data/polygon-options-flow.json")
        for bk, w in [("extreme_call_flow", 0.7), ("bullish_call_flow", 0.6)]:
            for it in (pf.get(bk) or []):
                if isinstance(it, dict): add(_tk(it), "polygon-options-flow", w, tag="bullish call flow")
        for it in (pf.get("all_results") or []):
            if not isinstance(it, dict): continue
            r = it.get("cv_pv_ratio")
            if isinstance(r, (int, float)) and r > 0:
                add(_tk(it), "polygon-options-flow", 0.4 if r >= 1.3 else (-0.4 if r <= 0.7 else 0.0))

    # 4. catalyst-skew-premove — directional pre-catalyst skew books
    inc, _ = gated("catalyst-skew-premove")
    if inc:
        cs = _read("data/catalyst-skew-premove.json")
        for it in (cs.get("bull_skew_setups") or []):
            if isinstance(it, dict): add(_tk(it), "catalyst-skew", 0.6, tag="pre-catalyst bull skew")
        for it in (cs.get("bear_skew_setups") or []):
            if isinstance(it, dict): add(_tk(it), "catalyst-skew", -0.6, tag="pre-catalyst bear skew")

    # 5. options-flow scanner + volatility-squeeze — COILED (vol compression, tier S/A)
    for fk, key in [("options-flow-scanner", "data/options-flow.json"),
                    ("volatility-squeeze", "data/volatility-squeeze.json")]:
        inc, _ = gated(fk)
        if not inc: continue
        for it in (_read(key).get("all_qualifying") or []):
            if not isinstance(it, dict): continue
            if (it.get("tier") or "").upper() in ("S", "A"):
                add(_tk(it), fk, 0.0, coiled=True, tag="coiled (vol compression)")

    # earnings IV — options pricing into earnings is vol context for the options posture
    ivc = _read("data/earnings-iv-crush.json")
    for L, ds, tg in (("top_rich", -0.1, "rich IV into earnings (crush risk)"),
                      ("top_cheap", 0.15, "cheap IV into earnings (catalyst)")):
        for it in (ivc.get(L) or []):
            if isinstance(it, dict):
                _tk = (it.get("ticker") or it.get("symbol") or "").upper()
                if _tk:
                    add(_tk, "earnings-iv", ds, tag=tg)

    # ---- classify posture ----
    book = []
    for tk, a in acc.items():
        a["n_engines"] = len(a["engines"]); a["engines"] = sorted(a["engines"])
        a["score"] = round(a["score"], 3)
        s = a["score"]
        if a["neg_gamma"] and s > 0.3:
            a["posture"] = "SQUEEZE_FUEL"
        elif s >= 0.6 and a["n_engines"] >= 2:
            a["posture"] = "BULLISH_FLOW"
        elif s <= -0.6:
            a["posture"] = "BEARISH_FLOW"
        elif a["coiled"]:
            a["posture"] = "COILED"
        elif s >= 0.3:
            a["posture"] = "BULLISH_LEAN"
        elif s <= -0.3:
            a["posture"] = "BEARISH_LEAN"
        else:
            a["posture"] = "MIXED"
        book.append(a)

    book.sort(key=lambda x: (-x["n_engines"], -abs(x["score"])))
    by_posture = {}
    for p in ("SQUEEZE_FUEL", "BULLISH_FLOW", "BEARISH_FLOW", "COILED", "BULLISH_LEAN", "BEARISH_LEAN"):
        by_posture[p] = [b for b in book if b["posture"] == p][:25]
    multi = [b for b in book if b["n_engines"] >= 2]

    out = {"engine": "options-confluence", "version": VERSION, "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "thesis": ("Fuses the fragmented options/dealer cluster into one net posture per name. Cross-engine "
                      "agreement (n_engines >= 2) is the real signal; a single feed is noise. Dealers short gamma "
                      "(neg net-GEX) + bullish flow = SQUEEZE_FUEL (upside convexity)."),
           "alpha_gate": "ALPHA_NEGATIVE options engines dropped; squeeze read lifted by ALPHA_PROVEN squeeze_risk.",
           "counts": {"names": len(book), "multi_engine": len(multi),
                      **{p.lower(): len(v) for p, v in by_posture.items()}},
           "multi_engine_confluence": multi[:40],
           "by_posture": by_posture,
           "ticker_map": {b["ticker"]: {"posture": b["posture"], "score": b["score"],
                                        "n_engines": b["n_engines"], "neg_gamma": b["neg_gamma"],
                                        "coiled": b["coiled"], "tags": b["tags"]} for b in book},
           "note": "New synthesizer — consumable by best-setups/master-ranker so options confluence finally counts as one coherent factor."}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print("[options-confluence v%s] names=%d multi=%d squeeze_fuel=%d bullish=%d bearish=%d coiled=%d" % (
        VERSION, len(book), len(multi), len(by_posture["SQUEEZE_FUEL"]), len(by_posture["BULLISH_FLOW"]),
        len(by_posture["BEARISH_FLOW"]), len(by_posture["COILED"])))
    return {"statusCode": 200, "body": json.dumps(out["counts"])}
