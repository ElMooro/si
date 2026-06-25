"""
justhodl-flow-confluence  ·  v1.0  —  THE FLOW / POSITIONING SYNTHESIZER
================================================================================
Companion to options-confluence. The audit found the flow/positioning cluster
even more fragmented (~35 engines, ~1 cross-read): 13F institutional buying, dark-
pool accumulation, ETF look-through flows, short positioning, stealth accumulation —
each measuring real money movement, none fused. So a name bought by institutions
AND accumulating in the dark pool AND seeing ETF inflows looked the same as one
flagged by a single feed. This synthesizer fuses them into ONE net flow posture
per name, with a confluence count, so corroborated smart-money movement is legible.

Fuses (per ticker, alpha-gated):
  • institutional (13F)  — 13f-positions most_bought/most_sold, smart-money clusters
  • dark pool            — dark-pool top_accumulation / top_distribution
  • ETF look-through     — flow-lookthrough actual_accumulation/distribution, capital-flow
  • short positioning    — short-interest (squeeze risk / covering), finra-short squeeze
  • stealth              — stealth-accumulation convergence

Posture: SHORT_SQUEEZE_SETUP (heavy short + accumulation/covering) / ACCUMULATION /
DISTRIBUTION / STEALTH_ACCUMULATION / *_LEAN / MIXED. ALPHA_NEGATIVE flow engines
(e.g. etf_rotation) dropped via the engine-trust gate. Output is a clean signal
consumable by best-setups/master-ranker.
"""
import json, time
from datetime import datetime, timezone
import boto3

VERSION = "1.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/flow-confluence.json"
s3 = boto3.client("s3", "us-east-1")

TRUST_KEY = {"capital-flow": "capital_flow", "etf-flows": "etf_rotation",
             "flow-lookthrough": "etf_flow_extreme"}


def _read(key):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return {}

def _tk(x):
    if isinstance(x, dict): return (x.get("ticker") or x.get("symbol") or "").upper()
    if isinstance(x, str): return x.strip().upper()
    return ""


def lambda_handler(event, context):
    t0 = time.time()
    et = _read("data/engine-trust.json")
    trust_by = {e.get("signal_type"): e for e in (et.get("engines") or []) if isinstance(e, dict)}
    def gated(file_key):
        info = trust_by.get(TRUST_KEY.get(file_key))
        if not info: return True, 1.0
        if info.get("alpha_status") == "ALPHA_NEGATIVE": return False, 1.0
        if info.get("alpha_status") == "ALPHA_PROVEN":
            return True, max(1.0, min(1.35, info.get("effective_trust") or 1.0))
        return True, 1.0

    acc = {}
    def touch(tk): return acc.setdefault(tk, {"ticker": tk, "score": 0.0, "engines": set(),
                                              "heavy_short": False, "covering": False, "stealth": False, "tags": []})
    def add(tk, eng, dscore=0.0, heavy_short=False, covering=False, stealth=False, tag=None):
        if not tk or len(tk) > 6: return
        a = touch(tk); a["engines"].add(eng); a["score"] += dscore
        if heavy_short: a["heavy_short"] = True
        if covering: a["covering"] = True
        if stealth: a["stealth"] = True
        if tag and tag not in a["tags"]: a["tags"].append(tag)

    # 1. 13F institutional — most_bought (+) / most_sold (-)
    pos = _read("data/13f-positions.json")
    for it in (pos.get("most_bought") or []):
        if isinstance(it, dict): add(_tk(it), "13f", 0.7, tag="13F institutions adding")
    for it in (pos.get("most_sold") or []):
        if isinstance(it, dict): add(_tk(it), "13f", -0.7, tag="13F institutions trimming")

    # 2. smart-money clusters — n_buyers vs n_sellers
    for it in (_read("data/smart-money-clusters.json").get("clusters") or []):
        if not isinstance(it, dict): continue
        nb, ns = it.get("n_buyers") or 0, it.get("n_sellers") or 0
        if nb or ns:
            d = 0.6 if nb > ns else (-0.6 if ns > nb else 0.0)
            add(_tk(it), "smart-money", d, tag="smart-money cluster buying" if d > 0 else None)

    # 3. dark pool — accumulation (+) / distribution (-)
    dp = _read("data/dark-pool.json")
    for it in (dp.get("top_accumulation") or []):
        if isinstance(it, dict): add(_tk(it), "dark-pool", 0.7, tag="dark-pool accumulation")
    for it in (dp.get("top_distribution") or []):
        if isinstance(it, dict): add(_tk(it), "dark-pool", -0.7, tag="dark-pool distribution")

    # 4. ETF look-through flow
    inc, lift = gated("flow-lookthrough")
    if inc:
        fl = _read("data/flow-lookthrough.json")
        for it in (fl.get("actual_accumulation") or []):
            if isinstance(it, dict): add(_tk(it), "etf-lookthrough", 0.6 * lift, tag="ETF inflow accumulation")
        for it in (fl.get("actual_distribution") or []):
            if isinstance(it, dict): add(_tk(it), "etf-lookthrough", -0.6 * lift, tag="ETF outflow")

    # 5. capital-flow — accumulating (+) / distributing (-)
    inc, lift = gated("capital-flow")
    if inc:
        cf = _read("data/capital-flow.json")
        for it in (cf.get("accumulating") or []):
            if isinstance(it, dict): add(_tk(it), "capital-flow", 0.5 * lift)
        for it in (cf.get("distributing") or []):
            if isinstance(it, dict): add(_tk(it), "capital-flow", -0.5 * lift)

    # 6. short positioning — squeeze risk / crowded (heavy short) + covering (bullish)
    si = _read("data/short-interest.json")
    for bk in ("top_squeeze_risk", "top_crowded_shorts", "top_high_dtc"):
        for it in (si.get(bk) or []):
            if isinstance(it, dict): add(_tk(it), "short-interest", 0.0, heavy_short=True, tag="heavy short interest")
    for it in (si.get("top_covering") or []):
        if isinstance(it, dict): add(_tk(it), "short-interest", 0.4, covering=True, tag="shorts covering")
    for it in (_read("data/finra-short.json").get("squeeze_candidates") or []):
        if isinstance(it, dict): add(_tk(it), "finra-short", 0.0, heavy_short=True, tag="FINRA squeeze candidate")

    # 7. stealth accumulation — convergence + smart-money-only
    sa = _read("data/stealth-accumulation.json")
    for bk in ("convergence", "top_smart_money_only"):
        for it in (sa.get(bk) or []):
            if isinstance(it, dict): add(_tk(it), "stealth", 0.5, stealth=True, tag="stealth accumulation")

    # 8. insider buying clusters — corporate insiders buying is independent positioning flow
    for it in (_read("data/insider-clusters.json").get("clusters") or []):
        if isinstance(it, dict):
            ni = it.get("n_insiders") or 0
            add(_tk(it), "insider", min(0.9, 0.4 + 0.1 * ni), tag=f"insider cluster ({ni})")
    # 9. corporate buybacks — a company buying its own stock is a flow tailwind
    bb = _read("data/buyback-scanner.json")
    for bk in ("top_buybacks", "accelerating", "top_picks", "buybacks", "all_buybacks", "board"):
        for it in (bb.get(bk) or []):
            if isinstance(it, dict): add(_tk(it), "buyback", 0.6, tag="buyback")
    # 10. insider + buyback confluence — both at once is the strongest corporate-flow tell
    ibc = _read("data/insider-buyback-confluence.json")
    for bk in ("top_confluences", "all_confluences", "high_conviction"):
        for it in (ibc.get(bk) or []):
            if isinstance(it, dict): add(_tk(it), "insider-buyback", 0.8, tag="insider+buyback confluence")

    # ---- classify posture ----
    book = []
    for tk, a in acc.items():
        a["n_engines"] = len(a["engines"]); a["engines"] = sorted(a["engines"]); a["score"] = round(a["score"], 3)
        s = a["score"]
        if a["heavy_short"] and (s > 0.3 or a["covering"]):
            a["posture"] = "SHORT_SQUEEZE_SETUP"
        elif s >= 0.6 and a["n_engines"] >= 2:
            a["posture"] = "ACCUMULATION"
        elif s <= -0.6:
            a["posture"] = "DISTRIBUTION"
        elif a["stealth"] and s > 0:
            a["posture"] = "STEALTH_ACCUMULATION"
        elif s >= 0.3:
            a["posture"] = "ACCUMULATION_LEAN"
        elif s <= -0.3:
            a["posture"] = "DISTRIBUTION_LEAN"
        else:
            a["posture"] = "MIXED"
        book.append(a)

    book.sort(key=lambda x: (-x["n_engines"], -abs(x["score"])))
    by_posture = {}
    for p in ("SHORT_SQUEEZE_SETUP", "ACCUMULATION", "DISTRIBUTION", "STEALTH_ACCUMULATION",
              "ACCUMULATION_LEAN", "DISTRIBUTION_LEAN"):
        by_posture[p] = [b for b in book if b["posture"] == p][:25]
    multi = [b for b in book if b["n_engines"] >= 2]

    out = {"engine": "flow-confluence", "version": VERSION, "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "thesis": ("Fuses the fragmented flow/positioning cluster into one net posture per name. A name bought by "
                      "institutions AND accumulating in the dark pool AND seeing ETF inflows (n_engines >= 2) is "
                      "corroborated smart-money movement; a single feed is noise. Heavy short + accumulation/covering "
                      "= SHORT_SQUEEZE_SETUP."),
           "alpha_gate": "ALPHA_NEGATIVE flow engines dropped (e.g. etf_rotation) via engine-trust.",
           "counts": {"names": len(book), "multi_engine": len(multi),
                      **{p.lower(): len(v) for p, v in by_posture.items()}},
           "multi_engine_confluence": multi[:40],
           "by_posture": by_posture,
           "ticker_map": {b["ticker"]: {"posture": b["posture"], "score": b["score"], "n_engines": b["n_engines"],
                                        "heavy_short": b["heavy_short"], "stealth": b["stealth"], "tags": b["tags"]}
                          for b in book},
           "note": "New synthesizer — consumable by best-setups/master-ranker so flow confluence counts as one coherent factor."}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print("[flow-confluence v%s] names=%d multi=%d squeeze=%d accum=%d distrib=%d stealth=%d" % (
        VERSION, len(book), len(multi), len(by_posture["SHORT_SQUEEZE_SETUP"]), len(by_posture["ACCUMULATION"]),
        len(by_posture["DISTRIBUTION"]), len(by_posture["STEALTH_ACCUMULATION"])))
    return {"statusCode": 200, "body": json.dumps(out["counts"])}
