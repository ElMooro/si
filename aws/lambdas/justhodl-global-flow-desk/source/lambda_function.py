"""justhodl-global-flow-desk — WHERE THE MONEY IS GOING (Khalid's mandate).

One roof over the entire revived flow fleet:
  ASSET CLASSES  equity / duration ladder / credit / TIPS / real estate /
                 gold / silver / commodities / crypto / cash — net 5d $ per
                 class from etf-true-flows categories + etf-flows fmap.
  SECTORS        11 SPDR net-flow ranking -> rotation leaders/laggards.
  INST vs RETAIL institutional composite (dark-pool OWN-DIX, 13F money-flow,
                 radar complex tide) vs retail composite (AAII, crypto
                 exchange/stablecoin pulse) -> divergence verdict.
  HOT MONEY MAP  per-country score: country-ETF 5d flows (new COUNTRY
                 category) + TIC foreign-holder deltas (sovereign-fiscal +
                 tic-flows) + FX momentum vs USD (polygon-fx-regime).
  CAPEX IMPULSE  structural-pre-signals capex language by sector (if live).
  AI BRIEF       llm_router tier=reason.
Output data/global-flow-desk.json. Page global-flow-desk.html. Board row.
"""
import json, re, statistics as st
from datetime import datetime, timezone
import boto3

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
OUT = "data/global-flow-desk.json"
s3 = boto3.client("s3", region_name=REGION)

CTRY = {"MCHI":"China","FXI":"China","ASHR":"China","EWJ":"Japan","EWG":"Germany","EWY":"South Korea",
        "EWT":"Taiwan","EWU":"UK","EWC":"Canada","EWA":"Australia","EWW":"Mexico","EZA":"South Africa",
        "TUR":"Turkey","EPOL":"Poland","ARGT":"Argentina","EIDO":"Indonesia","VNM":"Vietnam","THD":"Thailand",
        "EWQ":"France","EWL":"Switzerland","EWI":"Italy","EWP":"Spain","EWS":"Singapore","INDA":"India",
        "EWZ":"Brazil","EEM":"EM (broad)","VWO":"EM (broad)","EFA":"DM ex-US","VEA":"DM ex-US"}
FX_CTRY = {"JPY":"Japan","EUR":"Eurozone","GBP":"UK","CNH":"China","CNY":"China","KRW":"South Korea",
           "TWD":"Taiwan","CAD":"Canada","AUD":"Australia","MXN":"Mexico","ZAR":"South Africa","TRY":"Turkey",
           "PLN":"Poland","BRL":"Brazil","INR":"India","CHF":"Switzerland","SGD":"Singapore","THB":"Thailand"}
CLASSES = {"EQUITY_US":["BROAD_EQUITY_US"],"EQUITY_INTL":["INTERNATIONAL"],"SECTORS":["SECTOR_EQUITY"],
           "TREASURIES":["RATES_TREASURIES"],"CREDIT":["CREDIT"],"TIPS":["TIPS_INFLATION"],
           "CRYPTO":["CRYPTO_ETF","CRYPTO"],"COMMODITIES":["COMMODITIES"],"THEMATIC":["THEMATIC"],
           "DIVIDEND_VALUE":["DIVIDEND_VALUE"],"GROWTH":["GROWTH"],"VOL":["VOLATILITY"]}


def _finite(x):
    """Recursive NaN/Inf -> None so browsers' strict JSON.parse never chokes."""
    if isinstance(x, float):
        return x if x == x and x not in (float("inf"), float("-inf")) else None
    if isinstance(x, dict):
        return {k: _finite(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_finite(v) for v in x]
    return x


def _j(k, d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception: return d

def _tmap(doc, depth=0, out=None):
    if out is None: out = {}
    if depth > 6: return out
    if isinstance(doc, dict):
        t = doc.get("ticker") or doc.get("symbol")
        if isinstance(t, str) and t.isupper() and len(t) <= 6:
            out.setdefault(t, doc); return out
        for k, v in doc.items():
            if isinstance(k, str) and k.isupper() and 1 <= len(k) <= 6 and isinstance(v, dict):
                out.setdefault(k, v)
            else:
                _tmap(v, depth + 1, out)
    elif isinstance(doc, list):
        for v in doc[:8000]: _tmap(v, depth + 1, out)
    return out

def _num(x):
    return x if isinstance(x, (int, float)) else None

def flow_of(rec):
    for k in ("net_flow_5d_usd", "flow_5d_usd", "net_5d_usd", "flow_5d", "net_flow_usd"):
        v = _num(rec.get(k))
        if v is not None: return v
    return None

def _clean_brief(txt):
    """Reason-tier models sometimes emit their planning scaffold. Contract:
    plain prose, sentence-complete, <=700 chars, or None (caller falls back)."""
    if not txt: return None
    t = str(txt).strip()
    if any(k in t[:250] for k in ("Analyze the Request", "**Analyze", "Let me ", "First,", "1. **")):
        paras = [p.strip() for p in t.replace("\r", "").split("\n\n") if len(p.strip()) >= 80]
        paras = [p for p in paras if "**" not in p[:6] and not p.lstrip().startswith(("1.", "2.", "-", "*"))]
        t = paras[-1] if paras else ""
    t = t.replace("**", "").replace("##", "").strip()
    # leading header/step lines ("8. Final Polish (...):", "Step 2:", short label lines)
    lines = [l for l in t.split("\n") if l.strip()]
    while lines and (re.match(r"^\s*\d+\.\s", lines[0]) or lines[0].rstrip().endswith(":")
                     or len(lines[0].strip()) < 40):
        lines.pop(0)
    if lines: t = " ".join(l.strip() for l in lines)

    _bad = ("Wait", "Hmm", "Actually", "Okay", "OK", "So,", "Let", "First", "Alright", "Now,", "Note", "Final", "Draft", "Review", "Refine", "Sentence", "Step")
    def _delib(word): w = word.rstrip(",.:;"+chr(39)+"s"); return any(w.startswith(b.rstrip(",:")) for b in _bad)
    if _delib(t.split(" ", 1)[0]):      # deliberation-opener leak (prefix match)
        parts = [p.strip() for p in t.split(". ") if p.strip()]
        while parts and _delib(parts[0].split(" ", 1)[0]):
            parts.pop(0)
        t = (". ".join(parts)).strip()
        if t and not t.endswith((".", "!", "?")): t += "."
    if len(t) > 700: t = t[:700]
    if t and not t.rstrip().endswith((".", "!", "?")):
        t = t.rsplit(".", 1)[0] + "." if "." in t else ""
    return t if 90 <= len(t) <= 720 else None


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    tf = _j("data/etf-true-flows.json", {}) or {}
    fmap_daily = {m.get("ticker"): m for m in (_j("etf-flows/daily.json", {}) or {}).get("metrics", []) if m.get("ticker")}
    tfm = _tmap(tf)

    # ── categories from true-flows (authoritative for class ladder) ──
    cat_of = {}
    cats = tf.get("categories") or tf.get("by_category") or {}
    if isinstance(cats, dict):
        for cname, arr in cats.items():
            for it in (arr if isinstance(arr, list) else []):
                t = it.get("ticker") if isinstance(it, dict) else it
                if isinstance(t, str): cat_of[t] = cname
    if not cat_of:
        for t, r in tfm.items():
            c = r.get("category") or r.get("cat")
            if c: cat_of[t] = c

    def class_sum(cat_names):
        tot = 0.0; n = 0; z = []
        for t, c in cat_of.items():
            if c not in cat_names: continue
            rec = tfm.get(t) or fmap_daily.get(t) or {}
            v = flow_of(rec)
            if v is None and t in fmap_daily: v = flow_of(fmap_daily[t])
            if v is not None: tot += v; n += 1
            zz = _num(rec.get("flow_z") or rec.get("z"))
            if zz is not None: z.append(zz)
        return {"net_5d_usd_m": round(tot / 1e6, 1), "n": n,
                "z": round(st.fmean(z), 2) if z else None,
                "verdict": "INFLOW" if tot > 0 else "OUTFLOW" if tot < 0 else "FLAT"}

    classes = {name: class_sum(cats_) for name, cats_ in CLASSES.items()}
    # precious metals + real estate as first-class citizens
    def single(ts):
        tot = 0.0; n = 0
        for t in ts:
            v = flow_of(tfm.get(t) or fmap_daily.get(t) or {})
            if v is not None: tot += v; n += 1
        return {"net_5d_usd_m": round(tot / 1e6, 1), "n": n,
                "verdict": "INFLOW" if tot > 0 else "OUTFLOW" if tot < 0 else "FLAT"}
    classes["GOLD"] = single(["GLD", "IAU", "GDX"])
    classes["SILVER"] = single(["SLV"])
    classes["REAL_ESTATE"] = single(["XLRE", "VNQ", "IYR"])
    classes["CASH"] = single(["BIL", "SGOV", "SHV"])

    # ── sector rotation (SPDRs) ──
    spdr = {t: flow_of(fmap_daily.get(t) or tfm.get(t) or {}) for t in
            ("XLK","XLF","XLE","XLV","XLI","XLY","XLP","XLU","XLB","XLRE","XLC")}
    spdr = {t: v for t, v in spdr.items() if v is not None}
    ranked = sorted(spdr.items(), key=lambda kv: kv[1], reverse=True)
    sectors = {"ranked": [{"etf": t, "net_5d_usd_m": round(v / 1e6, 1)} for t, v in ranked],
               "leaders": [t for t, _ in ranked[:3]], "laggards": [t for t, _ in ranked[-3:]]}

    # ── institutional vs retail ──
    dp = _j("data/dark-pool.json", {}) or {}
    own_dix = ((dp.get("dix") or {}).get("own_dix_pct"))
    mfs = _j("data/money-flow-state.json", {}) or {}
    radar = _j("data/capital-flow-radar.json", {}) or {}
    comp = radar.get("complexes") or []
    tide = sum(1 for c in comp if _num(c.get("net_flow_5d_usd")) and c["net_flow_5d_usd"] > 0)
    tide_pct = round(100 * tide / len(comp), 1) if comp else None
    inst_parts = []
    if own_dix is not None: inst_parts.append(max(-100, min(100, (own_dix - 54.5) * 12)))
    if tide_pct is not None: inst_parts.append(max(-100, min(100, (tide_pct - 50) * 2.2)))
    inst = round(st.fmean(inst_parts), 1) if inst_parts else None
    aaii = _j("data/aaii-sentiment.json", {}) or _j("data/aaii.json", {}) or {}
    def _aaii_spread(doc, d=0):
        if d > 3 or not isinstance(doc, (dict, list)): return None
        if isinstance(doc, dict):
            b, r_ = _num(doc.get("bullish") or doc.get("bull")), _num(doc.get("bearish") or doc.get("bear"))
            if b is not None and r_ is not None: return b - r_
            for v in doc.values():
                got = _aaii_spread(v, d + 1)
                if got is not None: return got
        else:
            for v in doc[-3:]:
                got = _aaii_spread(v, d + 1)
                if got is not None: return got
        return None
    bb = None
    bb = _aaii_spread(aaii)
    if bb is None:
        b, r_ = _num(aaii.get("bullish") or aaii.get("bull")), _num(aaii.get("bearish") or aaii.get("bear"))
        if b is not None and r_ is not None: bb = b - r_
    stbl = _j("data/stablecoin-flow.json", {}) or {}
    st7 = None
    def _st7(doc, d=0):
        if d > 3: return None
        if isinstance(doc, dict):
            for k, v in doc.items():
                if "delta_7d" in k or "net_7d" in k or "mint_7d" in k:
                    n_ = _num(v)
                    if n_ is not None: return n_
            for v in doc.values():
                got = _st7(v, d + 1)
                if got is not None: return got
        elif isinstance(doc, list):
            tot = [x for x in (_num((it or {}).get("delta_7d")) for it in doc if isinstance(it, dict)) if x is not None]
            if tot: return sum(tot)
        return None
    st7 = _st7(stbl)
    ret_parts = []
    if bb is not None: ret_parts.append(max(-100, min(100, bb * 3)))
    if st7 is not None: ret_parts.append(max(-100, min(100, st7 / 2e8)))
    retail = round(st.fmean(ret_parts), 1) if ret_parts else None
    div = None
    if inst is not None and retail is not None:
        gap = inst - retail
        div = ("INSTITUTIONS BUYING / RETAIL SELLING" if gap > 35 else
               "RETAIL BUYING / INSTITUTIONS SELLING" if gap < -35 else
               "ALIGNED " + ("RISK-ON" if inst + retail > 30 else "RISK-OFF" if inst + retail < -30 else "NEUTRAL"))
    inst_retail = {"institutional": inst, "retail": retail, "own_dix": own_dix,
                   "radar_breadth_pct": tide_pct, "aaii_spread": bb,
                   "aaii_spread_pct": round(bb * 100, 1) if bb is not None else None,
                   "stablecoin_7d_usd_m": round(st7 / 1e6, 1) if st7 is not None else None,
                   "divergence": div}

    # ── hot money world map ──
    sv = _j("data/sovereign-fiscal.json", {}) or {}
    tic = _j("data/tic-flows.json", {}) or {}
    tic_ctry = {}
    for doc in (sv, tic):
        for h in (doc.get("holders") or doc.get("countries") or doc.get("by_country") or []):
            if not isinstance(h, dict): continue
            nm = h.get("country") or h.get("name")
            d = _num(h.get("chg_12m_usd_b") or h.get("net_purchases_12m") or h.get("delta") or h.get("chg"))
            if nm and d is not None: tic_ctry[nm] = d
    fx = _j("data/polygon-fx-regime.json", {}) or {}
    fx_mom = {}
    for p in (fx.get("pairs") or []):
        if not isinstance(p, dict): continue
        sym = (p.get("pair") or p.get("symbol") or "")
        mom = _num(p.get("mom_20d_pct") or p.get("chg_20d") or p.get("momentum"))
        if mom is None: continue
        ccy = sym.replace("USD", "").replace("/", "").strip()[:3]
        c = FX_CTRY.get(ccy)
        if not c: continue
        usd_first = sym.upper().startswith("USD")
        fx_mom[c] = -mom if usd_first else mom      # ccy strength = money toward country
    countries = {}
    src_counts = {"true_flows": 0, "etf_global": 0}
    warming = []
    for t, c in CTRY.items():
        r1, r2 = tfm.get(t), fmap_daily.get(t)
        rec = None
        if r1 and flow_of(r1) is not None: rec = r1; src_counts["true_flows"] += 1
        elif r2 and flow_of(r2) is not None: rec = r2; src_counts["etf_global"] += 1
        if not rec:
            warming.append(t); continue
        v = flow_of(rec)
        if v is None or v == 0.0:            # exact $0 over 5d = degenerate fresh history
            warming.append(t); continue
        e = countries.setdefault(c, {"etf_5d_usd_m": 0.0, "etfs": []})
        e["etf_5d_usd_m"] += v / 1e6; e["etfs"].append(t)
    for c, e in countries.items():
        parts = [max(-100, min(100, e["etf_5d_usd_m"] / 3))]
        if c in fx_mom: parts.append(max(-100, min(100, fx_mom[c] * 18))); e["fx_mom_pct"] = round(fx_mom[c], 2)
        if c in tic_ctry: parts.append(max(-100, min(100, tic_ctry[c] * 1.2))); e["tic_12m_usd_b"] = tic_ctry[c]
        e["etf_5d_usd_m"] = round(e["etf_5d_usd_m"], 1)
        e["score"] = round(st.fmean(parts), 1)
        e["verdict"] = "HOT MONEY IN" if e["score"] > 18 else "HOT MONEY OUT" if e["score"] < -18 else "NEUTRAL"
    ctry_rank = sorted(countries.items(), key=lambda kv: kv[1]["score"], reverse=True)
    hot = {"countries": {c: e for c, e in ctry_rank},
           "top_inflows": [c for c, e in ctry_rank[:5]],
           "top_outflows": [c for c, e in ctry_rank[-5:]][::-1],
           "n_scored": len(countries),
           "src_counts": src_counts,
           "warming_etfs": warming,
           "warming_note": ("%d country ETFs added to true-flows on 2026-07-01 accrue 5d "
                            "shares-history through the week; map self-completes." % len(warming)) if warming else None}

    # ── capex impulse (best-effort) ──
    cpx = _j("data/capex-pulse.json", {}) or {}
    if cpx.get("hyperscalers"):
        hs = cpx["hyperscalers"]; mk = cpx.get("market") or {}
        top_sec = sorted(((n, v) for n, v in (cpx.get("sectors") or {}).items() if v.get("yoy_pct") is not None),
                         key=lambda kv: kv[1]["yoy_pct"], reverse=True)
        capex = {"status": "OK", "source": "capex-pulse",
                 "market_ttm_b": mk.get("capex_ttm_b"), "market_yoy_pct": mk.get("yoy_pct"),
                 "hyperscalers_ttm_b": hs.get("total_ttm_b"), "hyperscalers_yoy_pct": hs.get("yoy_pct"),
                 "top_sector": {"name": top_sec[0][0], "yoy_pct": top_sec[0][1]["yoy_pct"]} if top_sec else None,
                 "top_accelerators": [r["ticker"] for r in ((cpx.get("boards") or {}).get("top_accelerators") or [])[:5]]}
    else:
        sps = _j("data/structural-pre-signals.json", {}) or _j("data/structural-presignals.json", {}) or {}
        capex = {"status": "N/A", "source": "mentions-fallback"}
        for k in ("capex_by_sector", "capex", "capex_mentions"):
            if isinstance(sps.get(k), (dict, list)):
                capex = {"status": "OK", "source": "mentions-fallback", "data": sps[k]}
                break

    # ── AI brief ──
    brief, brief_src = None, "deterministic"
    try:
        from llm_router import complete
        prompt = ("Respond with ONLY three plain prose sentences — no preamble, no plan, no lists, "
                  "no markdown. Global capital flow snapshot. Classes(net5d $M): " +
                  ", ".join("%s %s" % (k, v.get("net_5d_usd_m")) for k, v in classes.items()) +
                  ". Sector leaders %s laggards %s. Inst %s vs Retail %s (%s). Hot money in: %s; out: %s. "
                  "Sentence 1 where money is going, sentence 2 the single most important rotation, "
                  "sentence 3 one risk."
                  % (sectors["leaders"], sectors["laggards"], inst, retail, div,
                     hot["top_inflows"][:3], hot["top_outflows"][:3]))
        brief = _clean_brief(complete(prompt, tier="reason", max_tokens=380))
        if brief: brief_src = "llm"
    except Exception:
        brief = None
    if not brief:
        _cm = {k: (v.get("net_5d_usd_m") or 0) for k, v in classes.items()}
        top_in = max(_cm, key=_cm.get); top_out = min(_cm, key=_cm.get)
        brief = ("Money is leaving %s ($%.0fM over 5d) and rotating toward %s (+$%.0fM), with %s leading "
                 "and %s lagging on the sector tape. The defining rotation is institutions (%+.1f) leaning "
                 "against retail (%+.1f) — %s — while hot money favors %s over %s. The risk: if the "
                 "institutional bid fades, the %s exit accelerates into thin summer liquidity."
                 % (top_out, abs(_cm[top_out]), top_in, _cm[top_in],
                    "/".join(sectors["leaders"][:3]), "/".join(sectors["laggards"][:3]),
                    inst or 0, retail or 0, str(div).lower(),
                    ", ".join(hot["top_inflows"][:3]), ", ".join(hot["top_outflows"][:3]), top_out))
    HK = "data/history/global-flow-desk.json"
    ph = _j(HK, {}) or {}
    ph[now.strftime("%Y-%m-%d")] = {"inst": inst, "retail": retail,
                                    "eq_us": (classes.get("EQUITY_US") or {}).get("net_5d_usd_m"),
                                    "tips": (classes.get("TIPS") or {}).get("net_5d_usd_m")}
    ph = dict(sorted(ph.items())[-400:])
    s3.put_object(Bucket=BUCKET, Key=HK, Body=json.dumps(_finite(ph), allow_nan=False).encode(),
                  ContentType="application/json")
    history = [{"d": k, **v} for k, v in list(ph.items())[-60:]]

    doc = {"engine": "justhodl-global-flow-desk", "version": "1.1.0",
           "generated_at": now.isoformat(timespec="seconds"),
           "asset_classes": classes, "sectors": sectors, "inst_vs_retail": inst_retail,
           "hot_money": hot, "capex": capex, "ai_brief": brief, "ai_brief_src": brief_src,
           "history": history,
           "method": ("Fusion of the revived flow fleet: etf-true-flows categories (+COUNTRY) & "
                      "etf-flows fmap for $ ladders; radar complex breadth + OWN-DIX for the "
                      "institutional tide; AAII + stablecoin pulse for retail; country ETFs + TIC "
                      "holder deltas + FX momentum for the hot-money map.")}
    s3.put_object(Bucket=BUCKET, Key=OUT, Body=json.dumps(_finite(doc), separators=(",", ":"), default=str, allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="public, max-age=60")
    return {"ok": True, "classes": {k: v.get("net_5d_usd_m") for k, v in classes.items()},
            "inst": inst, "retail": retail, "divergence": div,
            "hot_in": hot["top_inflows"][:3], "hot_out": hot["top_outflows"][:3],
            "n_countries": hot["n_scored"], "ai": bool(brief)}
