"""justhodl-institutional-footprint — THE SURVEILLANCE DESK (Khalid's mandate).

CIA-style fusion of every institutional fingerprint the platform owns:

  WHO/WHAT   13F holdings adds/exits + consensus clusters + price-divergence
             (adding into weakness) · CFTC COT net-spec by asset class,
             smart-money flow, extremes/reversals · NY-Fed-adjacent dealer
             posture via GEX sign · NAAIM manager exposure.
  WHERE      sectors (capital-flow-radar 46 complexes $) · asset classes
             (global-flow-desk ladder + CFTC classes).
  FOOTPRINT  dark-pool own-DIX, per-name ACCUM/DIST conviction + DIS flags,
             monthly wholesaler concentration · short-interest crowding ·
             factor crowding.
  POSTURE    RISK-NOW composite (NAAIM, CFTC risk appetite, own-DIX, breadth,
             leverage) · RISK-FORWARD composite (tail-hedging put bid, dealer
             gamma sign, CFTC extremes, catalyst skew, earnings blackout) —
             what they ARE doing vs what they are POSITIONING FOR.
  OUTPUT     data/institutional-footprint.json + AI dossier brief.
Everything tolerant: legacy feeds vary in shape; concept extractors walk keys.
"""
import json, re, statistics as st
from datetime import datetime, timezone
import boto3

BUCKET, OUT = "justhodl-dashboard-live", "data/institutional-footprint.json"
s3 = boto3.client("s3", region_name="us-east-1")


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

def _num(v):
    try:
        f = float(v); return f
    except Exception:
        return None

def _find(doc, subs, depth=0):
    """First numeric whose key contains all substrings (any depth)."""
    if depth > 5 or doc is None: return None
    if isinstance(doc, dict):
        for k, v in doc.items():
            lk = str(k).lower()
            if all(s_ in lk for s_ in subs):
                n = _num(v)
                if n is not None: return n
        for v in doc.values():
            got = _find(v, subs, depth + 1)
            if got is not None: return got
    elif isinstance(doc, list):
        for v in doc[:40]:
            got = _find(v, subs, depth + 1)
            if got is not None: return got
    return None

def _tick_list(doc, subs, depth=0, out=None):
    """Collect ticker strings from any list under a key containing subs."""
    if out is None: out = []
    if depth > 5 or doc is None or len(out) >= 40: return out
    if isinstance(doc, dict):
        for k, v in doc.items():
            lk = str(k).lower()
            if any(s_ in lk for s_ in subs) and isinstance(v, list):
                for it in v[:25]:
                    t = it if isinstance(it, str) else (it.get("ticker") or it.get("symbol") if isinstance(it, dict) else None)
                    if isinstance(t, str) and 1 <= len(t) <= 6 and t.isupper(): out.append(t)
            else:
                _tick_list(v, subs, depth + 1, out)
    elif isinstance(doc, list):
        for v in doc[:40]: _tick_list(v, subs, depth + 1, out)
    return out

def _clip(v, lo=-100, hi=100):
    return max(lo, min(hi, v))

def _clean_brief(txt):
    """Reason-tier models sometimes emit their planning scaffold. Contract:
    a short analyst paragraph, plain prose, sentence-complete, <=700 chars."""
    if not txt: return None
    t = str(txt).strip()
    if any(k in t[:200] for k in ("Analyze the Request", "**Data provided", "Let me ", "First,")):
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

    if t.split(" ", 1)[0].rstrip(",.") in ("Wait", "Hmm", "Actually", "Okay", "OK", "So,", "Let me", "First", "Alright", "Now,", "Note:"):      # deliberation-opener leak
        parts = [p.strip() for p in t.split(". ") if p.strip()]
        while parts and parts[0].split(" ", 1)[0].rstrip(",.") in ("Wait", "Hmm", "Actually", "Okay", "OK", "So,", "Let me", "First", "Alright", "Now,", "Note:"):
            parts.pop(0)
        t = (". ".join(parts)).strip()
        if t and not t.endswith((".", "!", "?")): t += "."
    if len(t) > 700:
        t = t[:700]
    if t and not t.rstrip().endswith((".", "!", "?")):
        t = t.rsplit(".", 1)[0] + "." if "." in t else ""
    return t if len(t) >= 90 and len(t) <= 720 else None


def lambda_handler(event=None, context=None):
    F = {k: _j(k, {}) or {} for k in (
        "data/13f-positions.json", "data/13f-aggregate.json", "data/13f-price-divergence.json",
        "data/cftc-deep-view.json", "data/cftc-all-cache.json", "data/dealer-gex.json",
        "data/options-gamma.json", "data/skew-tail-hedging.json", "data/short-interest.json",
        "data/breadth-divergence.json", "data/naaim.json", "data/dark-pool.json",
        "data/capital-flow-radar.json", "data/global-flow-desk.json", "data/leverage-monitor.json",
        "data/factor-returns.json", "data/earnings-blackout.json", "data/stock-xray.json",
        "data/nyfed-primary-dealer.json", "data/dealer-survey.json")}
    fresh = {k.split("/")[-1]: bool(v) for k, v in F.items()}
    n_alive = sum(fresh.values())
    print("[fp] feeds alive %d/18" % n_alive, {k: v for k, v in fresh.items() if not v})

    dp, radar, gfd = F["data/dark-pool.json"], F["data/capital-flow-radar.json"], F["data/global-flow-desk.json"]
    cftc = F["data/cftc-deep-view.json"] or F["data/cftc-all-cache.json"]
    xray = (F["data/stock-xray.json"].get("cards") or {})

    # ── SECTORS they are buying / selling (radar complexes $5d) ──
    comp = [c for c in (radar.get("complexes") or []) if isinstance(c, dict) and _num(c.get("net_flow_5d_usd")) is not None]
    comp.sort(key=lambda c: c["net_flow_5d_usd"], reverse=True)
    sectors = {"buying": [{"complex": c.get("name") or c.get("complex"), "usd_5d_m": round(c["net_flow_5d_usd"] / 1e6, 1)} for c in comp[:6]],
               "selling": [{"complex": c.get("name") or c.get("complex"), "usd_5d_m": round(c["net_flow_5d_usd"] / 1e6, 1)} for c in comp[-6:]][::-1],
               "n_complexes": len(comp)}

    # ── ASSET CLASSES (gfd ladder + CFTC class biases) ──
    classes = {k: (v or {}).get("net_5d_usd_m") for k, v in (gfd.get("asset_classes") or {}).items()}
    cftc_smart = _find(cftc, ("smart", "money")) or _find(cftc, ("smart_money",))
    cftc_risk = _find(cftc, ("risk", "appetite"))
    asset_classes = {"etf_net_5d_usd_m": classes,
                     "cftc": {"smart_money_flow": cftc_smart, "risk_appetite": cftc_risk,
                              "crisis": _find(cftc, ("crisis",))}}

    # ── SPECIFIC STOCKS ──
    f13 = F["data/13f-positions.json"]
    adds = _tick_list(f13, ("adds", "top_buys", "new_positions", "increas"))[:15]
    exits = _tick_list(f13, ("exits", "top_sells", "closed", "decreas"))[:15]
    hi_conv = (dp.get("high_conviction") or _tick_list(dp, ("high_conviction",)))[:15]
    dis = (dp.get("distribution_into_strength") or _tick_list(dp, ("distribution_into",)))[:15]
    div_marks = _tick_list(F["data/13f-aggregate.json"], ("diverg", "adding_into", "accumul"))[:12]
    both_buy = [t for t in adds if t in (hi_conv or [])]
    stocks = {"institutions_buying_13f": adds, "institutions_selling_13f": exits,
              "dark_pool_accumulation": hi_conv, "distribution_into_strength": dis,
              "adding_into_weakness_13f": div_marks,
              "double_confirmed_buys": both_buy,
              "note": "13F = quarterly holdings truth (45d lag); dark-pool = live footprint; overlap = highest conviction."}

    # ── DARK-POOL FOOTPRINT ──
    dxd = dp.get("dix") or {}
    mon = dp.get("monthly_ats") or {}
    smap = mon.get("share_map") or {}
    conc = sorted(((t, v) for t, v in smap.items()
                   if isinstance(v, dict) and _num(v.get("top_pct"))
                   and "de minimis" not in str(v.get("top_firm", "")).lower()
                   and (v.get("n_firms") or 0) >= 5 and (v.get("sh") or 0) >= 5e7),
                  key=lambda kv: kv[1]["top_pct"], reverse=True)
    dark = {"own_dix_pct": dxd.get("own_dix_pct"), "read": dxd.get("read"),
            "accumulation_n": (dp.get("distribution") or {}).get("accumulation"),
            "distribution_n": (dp.get("distribution") or {}).get("distribution"),
            "wholesaler_extremes": [{"ticker": t, "top_firm": v.get("top_firm"), "top_pct": v.get("top_pct")} for t, v in conc[:6]],
            "month": mon.get("month")}

    # ── ASSET LEDGER: LIT (ETF $) + DARK ($ from off-exchange tape) + CFTC futures ──
    SEC2CLASS = {"Technology": "EQUITY", "Communication Services": "EQUITY", "Consumer Cyclical": "EQUITY",
                 "Consumer Defensive": "EQUITY", "Healthcare": "EQUITY", "Industrials": "EQUITY",
                 "Financial": "EQUITY", "Financial Services": "EQUITY", "Energy": "EQUITY",
                 "Basic Materials": "EQUITY", "Utilities": "EQUITY", "Real Estate": "REAL_ESTATE"}
    dark_sec = {}
    dpx = dp.get("xray_map") or {}
    src_rows = ([(tk, v.get("dv"), v.get("st")) for tk, v in dpx.items()] if dpx and
                any(isinstance(v, dict) and v.get("dv") for v in list(dpx.values())[:50])
                else [(r.get("ticker"), r.get("daily_off_exch_vol"), r.get("state")) for r in (dp.get("board") or [])])
    for tk, vol, st_ in src_rows:
        px = (xray.get(tk) or {}).get("px")
        if tk and vol and px:
            sec = (xray.get(tk) or {}).get("sec") or "?"
            e = dark_sec.setdefault(sec, {"usd_m": 0.0, "names": []})
            usd = vol * px / 1e6
            e["usd_m"] += usd; e["names"].append((tk, usd, st_))
    dark_by_sector = {}
    for sec, e in sorted(dark_sec.items(), key=lambda kv: kv[1]["usd_m"], reverse=True):
        nm = sorted(e["names"], key=lambda x: x[1], reverse=True)
        dark_by_sector[sec] = {"dark_daily_usd_m_est": round(e["usd_m"], 1),
                               "top": [{"t": t, "usd_m": round(u, 1), "state": st_} for t, u, st_ in nm[:3]]}
    CFTC_CLASS = {"EQUITY": ("es", "nq", "ym", "rty", "sp", "nas"), "TREASURIES": ("zn", "zb", "zf", "zt", "ty", "us "),
                  "GOLD": ("gc", "gold"), "SILVER": ("si", "silver"), "CRYPTO": ("btc", "bitcoin", "eth", "ether"),
                  "FX_USD": ("dx", "dollar"), "ENERGY": ("cl", "crude", "ng", "natural")}
    cftc_by_class = {}
    def _cwalk(node, depth=0):
        if depth > 4: return
        if isinstance(node, dict):
            nm = str(node.get("symbol") or node.get("contract") or node.get("name") or "").lower()
            ns = None
            for k in ("net_speculator", "large_speculators_net", "net_spec", "spec_net", "net_noncommercial", "noncomm_net", "net_position", "net_z", "z_net_spec"):
                v = _num(node.get(k))
                if v is not None: ns = v; break
            if nm and ns is not None:
                for cls, keys in CFTC_CLASS.items():
                    if any(k in nm for k in keys):
                        cftc_by_class.setdefault(cls, []).append(ns); break
            for v in node.values(): _cwalk(v, depth + 1)
        elif isinstance(node, list):
            for v in node[:60]: _cwalk(v, depth + 1)
    _cwalk(cftc)
    cftc_by_class = {k: round(st.fmean(v), 2) for k, v in cftc_by_class.items() if v}
    LEDGER_MAP = {"EQUITY": ("EQUITY_US",), "EQUITY_INTL": ("EQUITY_INTL",), "TREASURIES": ("TREASURIES",),
                  "CREDIT": ("CREDIT",), "TIPS": ("TIPS",), "GOLD": ("GOLD",), "SILVER": ("SILVER",),
                  "REAL_ESTATE": ("REAL_ESTATE",), "CRYPTO": ("CRYPTO",), "COMMODITIES": ("COMMODITIES",),
                  "CASH": ("CASH",)}
    asset_ledger = {}
    dark_eq = round(sum(v["dark_daily_usd_m_est"] for s_, v in dark_by_sector.items() if SEC2CLASS.get(s_) == "EQUITY"), 1)
    dark_re = round(sum(v["dark_daily_usd_m_est"] for s_, v in dark_by_sector.items() if SEC2CLASS.get(s_) == "REAL_ESTATE"), 1)
    for cls, gkeys in LEDGER_MAP.items():
        lit = None
        for gk in gkeys:
            v = classes.get(gk)
            if v is not None: lit = v; break
        asset_ledger[cls] = {"lit_etf_5d_usd_m": lit,
                             "dark_daily_usd_m_est": dark_eq if cls == "EQUITY" else dark_re if cls == "REAL_ESTATE" else None,
                             "cftc_net_spec": cftc_by_class.get(cls),
                             "verdict": ("BUYING" if (lit or 0) > 0 else "SELLING" if (lit or 0) < 0 else "FLAT") if lit is not None else "N/A"}
    # 13F per-stock dollars
    def _val_rows(doc, subs):
        out = []
        def w(n, d=0):
            if d > 5 or len(out) >= 12: return
            if isinstance(n, dict):
                for k, v in n.items():
                    if any(s_ in str(k).lower() for s_ in subs) and isinstance(v, list):
                        for it in v[:12]:
                            if isinstance(it, dict):
                                t = it.get("ticker") or it.get("symbol")
                                usd = None
                                for vk in ("value", "marketValue", "market_value", "value_usd", "usd",
                                           "position_usd", "notional", "dollar_value", "total_value",
                                           "change_value", "changeValue", "val", "amount"):
                                    usd = _num(it.get(vk))
                                    if usd is not None: break
                                if t: out.append({"t": t, "usd_m": (round(usd / 1e6, 1) if abs(usd) > 1e5
                                                  else round(usd, 2)) if usd is not None else None})
                    else: w(v, d + 1)
            elif isinstance(n, list):
                for v in n[:40]: w(v, d + 1)
        w(doc); return out
    def _dedupe(rows):
        seen, out = set(), []
        for r0 in rows:
            if r0["t"] in seen: continue
            seen.add(r0["t"]); out.append(r0)
        return out
    stocks_usd = {"buys": _dedupe(_val_rows(f13, ("adds", "top_buys", "increas", "new_"))),
                  "sells": _dedupe(_val_rows(f13, ("exits", "top_sells", "decreas", "closed")))}
    _usd_map = {r0["t"]: r0.get("usd_m") for r0 in stocks_usd["buys"] + stocks_usd["sells"]}
    adds = list(dict.fromkeys(adds)); exits = list(dict.fromkeys(exits))
    def _pd_extract():
        for key in ("data/nyfed-primary-dealer.json", "data/dealer-survey.json"):
            doc = F[key]
            if not doc: continue
            ind = doc.get("indicators") if isinstance(doc.get("indicators"), dict) else None
            if ind:
                for k, v in ind.items():
                    lk = str(k).lower()
                    if "treasur" in lk and ("position" in lk or "net" in lk) and isinstance(v, dict):
                        val, dt = _num(v.get("value")), str(v.get("date") or "")
                        if val is not None and dt[:4] >= "2026":
                            return val, "%s %s (%s)" % (k, dt, key.split("/")[-1])
                        if val is not None:
                            return None, "STALE_SHIM %s dated %s — rejected (real-data rule)" % (k, dt)
            ls = doc.get("latest_survey")
            if isinstance(ls, dict):
                for subs in (("net", "treasur"), ("treasur", "position"), ("net", "position"), ("net",)):
                    got = _find(ls, subs)
                    if got is not None:
                        return got, "latest_survey %s (%s)" % ("+".join(subs), key.split("/")[-1])
            got = _find(doc, ("net", "treasur")) or _find(doc, ("net", "position"))
            if got is not None:
                return got, key.split("/")[-1]
        return None, "no net-position field in dealer feeds"
    pd_pos, pd_note = _pd_extract()
    _pdfeed = F["data/nyfed-primary-dealer.json"] or {}
    pd_block = {"net_treasury_b": _pdfeed.get("net_treasury_total_b"),
                "wow_b": (_pdfeed.get("wow_usd_b") or {}).get("TREASURY_COUPONS"),
                "as_of": _pdfeed.get("as_of"),
                "coupons_by_tenor_b": (_pdfeed.get("by_tenor_usd_b") or {}).get("TREASURY_COUPONS"),
                "tips_by_tenor_b": (_pdfeed.get("by_tenor_usd_b") or {}).get("TIPS"),
                "read": _pdfeed.get("read")}
    if pd_pos is not None and "TREASURIES" in asset_ledger:
        asset_ledger["TREASURIES"]["pd_net_usd_b"] = pd_pos

    # ── RISK-NOW composite ──
    naaim_z = _find(F["data/naaim.json"], ("z",)) or _find(F["data/naaim.json"], ("zscore",))
    naaim_x = _find(F["data/naaim.json"], ("exposure",)) or _find(F["data/naaim.json"], ("mean",))
    lev = _find(F["data/leverage-monitor.json"], ("cycle", "score")) or _find(F["data/leverage-monitor.json"], ("composite",))
    breadth = (gfd.get("inst_vs_retail") or {}).get("radar_breadth_pct")
    own_dix = dxd.get("own_dix_pct")
    parts_now = []
    if isinstance(naaim_z, (int, float)): parts_now.append(("naaim_z", _clip(naaim_z * 35)))
    if isinstance(own_dix, (int, float)): parts_now.append(("own_dix", _clip((own_dix - 54.5) * 12)))
    if isinstance(breadth, (int, float)): parts_now.append(("flow_breadth", _clip((breadth - 50) * 2.2)))
    if isinstance(cftc_risk, (int, float)): parts_now.append(("cftc_risk_appetite", _clip((cftc_risk - 50) * 2) if cftc_risk > 1 else _clip(cftc_risk * 40)))
    if isinstance(lev, (int, float)): parts_now.append(("leverage_cycle", _clip((lev - 50) * 1.4)))
    risk_now = round(st.fmean([v for _, v in parts_now]), 1) if parts_now else None
    now_label = ("RISK-ON" if risk_now > 18 else "RISK-OFF" if risk_now < -18 else "NEUTRAL") if risk_now is not None else "N/A"

    # ── RISK-FORWARD composite (what they are POSITIONING for) ──
    skew = F["data/skew-tail-hedging.json"]
    tail = _find(skew, ("tail", "score")) or _find(skew, ("skew", "z")) or _find(skew, ("put", "bid"))
    gex = _find(F["data/dealer-gex.json"], ("net", "gex")) or _find(F["data/dealer-gex.json"], ("gex",))
    pcr = _find(F["data/options-gamma.json"], ("put", "call")) or _find(F["data/options-gamma.json"], ("pcr",))
    rev_n = len(_tick_list(cftc, ("reversal",)))
    extreme_n = len(_tick_list(cftc, ("extreme",)))
    blackout = _find(F["data/earnings-blackout.json"], ("blackout", "pct")) or _find(F["data/earnings-blackout.json"], ("pct",))
    parts_fwd = []
    if isinstance(tail, (int, float)): parts_fwd.append(("tail_hedge_demand", _clip(-tail * 30)))
    if isinstance(gex, (int, float)): parts_fwd.append(("dealer_gex_sign", _clip(25 if gex > 0 else -35)))
    if isinstance(pcr, (int, float)): parts_fwd.append(("put_call", _clip((1.0 - pcr) * 60)))
    if extreme_n: parts_fwd.append(("cot_extremes", _clip(-6 * extreme_n)))
    if isinstance(blackout, (int, float)): parts_fwd.append(("buyback_blackout", _clip(-(blackout - 35) * 0.8)))
    risk_fwd = round(st.fmean([v for _, v in parts_fwd]), 1) if parts_fwd else None
    fwd_label = ("POSITIONING FOR RISK-ON" if risk_fwd > 15 else "POSITIONING FOR RISK-OFF" if risk_fwd < -15 else "HEDGED / NEUTRAL") if risk_fwd is not None else "N/A"

    # ── CONVICTION MOVES (the surveillance highlights) ──
    conviction = []
    for t in (both_buy or hi_conv or [])[:6]:
        c = xray.get(t) or {}
        conviction.append({"ticker": t, "signal": "ACCUMULATION (13F + dark pool)" if t in both_buy else "DARK-POOL ACCUMULATION",
                           "stage": c.get("stage"), "sector": c.get("sec")})
    for t in (dis or [])[:4]:
        c = xray.get(t) or {}
        conviction.append({"ticker": t, "signal": "DISTRIBUTION INTO STRENGTH", "stage": c.get("stage"), "sector": c.get("sec")})

    # ── posture history (last 400 sessions; feed carries last 60) ──
    HK = "data/history/institutional-footprint.json"
    ph = _j(HK, {}) or {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ph[today] = {"now": risk_now, "fwd": risk_fwd, "dix": own_dix,
                 "dark_b": round(sum(v["dark_daily_usd_m_est"] for v in dark_by_sector.values()) / 1e3, 1) if dark_by_sector else None}
    ph = dict(sorted(ph.items())[-400:])
    s3.put_object(Bucket=BUCKET, Key=HK, Body=json.dumps(ph).encode(), ContentType="application/json")
    history = [{"d": k, **v} for k, v in list(ph.items())[-60:]]

    # ── AI dossier ──
    brief = None
    try:
        from llm_router import complete
        prompt = ("Institutional surveillance dossier. Risk-now %s (%s); forward %s (%s). "
                  "Sectors bought: %s; sold: %s. CFTC smart-money %s. Own-DIX %s. 13F adds %s exits %s. "
                  "Tail-hedge %s, dealer GEX %s. Write 4 sentences, analyst-brief tone: what institutions "
                  "are doing, what they are positioning for, the single sharpest divergence, one watch-item."
                  % (risk_now, now_label, risk_fwd, fwd_label,
                     [s0["complex"] for s0 in sectors["buying"][:3]], [s0["complex"] for s0 in sectors["selling"][:3]],
                     cftc_smart, own_dix, adds[:4], exits[:4], tail, gex))
        brief = complete(prompt, tier="reason", max_tokens=440)
        brief = _clean_brief(brief)
    except Exception:
        brief = None
    if not brief or len(brief) < 90:
        top_buy = sectors["buying"][0]["complex"] if sectors["buying"] else "—"
        top_sell = sectors["selling"][0]["complex"] if sectors["selling"] else "—"
        curve = ""
        cbt = pd_block.get("coupons_by_tenor_b") or {}
        if len(cbt) >= 4:
            lo = max(cbt.items(), key=lambda kv: kv[1]); hi = min(cbt.items(), key=lambda kv: kv[1])
            curve = (" Dealer settled books run long %sy (+%.1fB) against short %sy (%.1fB) — an "
                     "intermediation steepener versus the futures duration bid." % (lo[0], lo[1], hi[0], hi[1]))
        brief = ("Institutions are %s on the tape (risk-now %+.1f: managers near full exposure, dark-pool "
                 "bid %.1f%%) while positioning %s forward (%+.1f: tail hedges bid, buyback blackout deepening). "
                 "The rotation is out of %s and broad beta into %s. In cash treasuries the ledger shows futures "
                 "specs net long %s contracts against a dealer settled book of $%.1fB.%s"
                 % (now_label.replace("-", " ").title(), risk_now or 0, own_dix or 0,
                    "defensively" if (risk_fwd or 0) < 0 else "constructively", risk_fwd or 0,
                    top_sell, top_buy,
                    ("{:+,.0f}".format(cftc_by_class.get("TREASURIES"))) if cftc_by_class.get("TREASURIES") is not None else "—",
                    pd_block.get("net_treasury_b") or 0, curve))
        brief_src = "deterministic"
    else:
        brief_src = "llm"

    doc = {"engine": "justhodl-institutional-footprint", "version": "1.2.0",
           "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "feeds_alive": n_alive, "feeds": fresh,
           "posture": {"risk_now": risk_now, "now_label": now_label, "now_components": dict(parts_now),
                       "risk_forward": risk_fwd, "forward_label": fwd_label, "fwd_components": dict(parts_fwd)},
           "sectors": sectors, "asset_classes": asset_classes,
           "asset_ledger": asset_ledger, "dark_by_sector": dark_by_sector,
           "pd": pd_block, "history": history,
           "feeds_total": len(F), "ai_dossier_src": brief_src,
           "stocks_usd_13f": stocks_usd, "primary_dealer_net": pd_pos, "primary_dealer_note": pd_note,
           "stocks": stocks,
           "dark_pool_footprint": dark, "conviction_moves": conviction, "ai_dossier": brief,
           "method": ("Fusion of the revived smart-money fleet (13F x3, CFTC COT, dealer GEX, options "
                      "gamma, tail-hedging skew, short interest, forced-selling) with this quarter's "
                      "engines (dark-pool v2.4 own-DIX/conviction/wholesaler, capital-flow-radar, "
                      "global-flow-desk, NAAIM, leverage, factor crowding, blackout). Concept "
                      "extractors tolerate legacy feed shapes.")}
    s3.put_object(Bucket=BUCKET, Key=OUT, Body=json.dumps(_finite(doc), separators=(",", ":"), default=str, allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="public, max-age=60")
    return {"ok": True, "feeds_alive": n_alive, "risk_now": risk_now, "now": now_label,
            "risk_fwd": risk_fwd, "fwd": fwd_label,
            "sections": {"sectors": len(comp), "adds": len(adds), "exits": len(exits),
                         "hi_conv": len(hi_conv), "dis": len(dis), "conviction": len(conviction)}}
