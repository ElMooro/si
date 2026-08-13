"""justhodl-stock-buying v1.0.3 (ops 4651)

Khalid's flagship screener: hunt the LARGEST POSITIVE CHANGE the
market hasn't priced — not the cheapest stock. Institutional
multi-factor composite over the fleet's own stores:

  fundamental-census-matrix (fundam['damentals'] quarters),
  _ma200/closes (SMA~250d, RS, double-bottom, accumulation),
  industry-boom (booming-industry gate + AI-class catalyst),
  deal-scanner (catalyst tape), FMP surprises/estimates
  (revisions + beat streak; warm-cached, key injected by ops).

HARD GATES: price ≤ long-SMA (accumulation zone), EPS up every
quarter (last 4 q/q), dilution ≤ +4%/yr, margin ≥ industry floor.
PILLARS (Khalid's ranks → five-core weights): revisions 25 ·
accel(EPS+rev) 25 · FCF/share 15 · valuation-vs-growth (fwd PEG,
P/E vs industry, FCF yield) 15 · catalyst+RS+volume 20. Quality
(ROIC, margin expansion, backlog) modulates ±10.
Every row carries pillar scores, gate verdicts with reasons, and
why: why.html?symbol=SYM. Missing inputs mark pillars n/a — never
fabricated. Davis double-play framing in doctrine.
"""
import json
import math
import os
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/stock-buying.json"
WARM = "data/warm/blackswan/"
s3 = boto3.client("s3", region_name="us-east-1")
FMP_KEY = os.environ.get("FMP_API_KEY", "")
FMP_BUDGET = {"n": 120}


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def fmp(path, qs=""):
    if not FMP_KEY or FMP_BUDGET["n"] <= 0:
        return None
    FMP_BUDGET["n"] -= 1
    try:
        url = ("https://financialmodelingprep.com/api/v3/%s?%s"
               "apikey=%s" % (path, (qs + "&") if qs else "",
                              FMP_KEY))
        req = urllib.request.Request(
            url, headers={"User-Agent": "justhodl-fleet"})
        time.sleep(0.12)
        with urllib.request.urlopen(req, timeout=14) as h:
            return json.loads(h.read())
    except Exception:
        return None


def warm_fmp(sym, kind, path, qs=""):
    wkey = WARM + "sb_%s_%s.json" % (kind,
                                     re.sub(r"[^A-Z0-9]", "_",
                                            sym))
    c = s3_json(wkey)
    if c is not None:
        try:
            ft = datetime.fromisoformat(c["fetched_at"])
            if (datetime.now(timezone.utc) - ft
                    ).total_seconds() < 64800:
                return c.get("data")
        except Exception:
            pass
    d = fmp(path, qs)
    if d is not None:
        try:
            s3.put_object(Bucket=BUCKET, Key=wkey,
                          Body=json.dumps(
                              {"data": d,
                               "fetched_at": datetime.now(
                                   timezone.utc).isoformat(
                                   timespec="seconds")}).encode(),
                          ContentType="application/json")
        except Exception:
            pass
        return d
    return (c or {}).get("data")


def fnum(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def qseq(row, names):
    """Pull a quarterly sequence (oldest->newest) from whichever
    field name/shape the census uses."""
    for nm in names:
        v = row.get(nm)
        if isinstance(v, list) and len(v) >= 3:
            out = [fnum(x) for x in v]
            out = [x for x in out if x is not None]
            if len(out) >= 3:
                return out[-8:]
        if isinstance(v, dict):
            try:
                ks = sorted(v.keys())
                out = [fnum(v[k]) for k in ks]
                out = [x for x in out if x is not None]
                if len(out) >= 3:
                    return out[-8:]
            except Exception:
                pass
    return None


def growth_seq(vals):
    g = []
    for i in range(1, len(vals)):
        a, b = vals[i], vals[i - 1]
        if b and b != 0:
            g.append((a / abs(b) - 1) * 100.0)
    return g


def load_census():
    """Columnar matrix: {tickers:[...], cols:{name:[aligned]}}
    (shape read from the fleet's own consumers)."""
    k = "data/fundamental-census-matrix.json"
    doc = s3_json(k)
    globals()["_MXP"] = {
        "loaded": isinstance(doc, dict),
        "top_keys": list(doc.keys())[:10]
        if isinstance(doc, dict) else None,
        "n_tickers": len(doc.get("tickers") or [])
        if isinstance(doc, dict) else None,
        "n_cols": len(doc.get("cols") or {})
        if isinstance(doc, dict) else None}
    if isinstance(doc, dict):
        tk = doc.get("tickers") or []
        C = doc.get("cols") or doc.get("metrics") or {}
        if tk and isinstance(C, dict) and C:
            rows = []
            names = list(C.keys())
            for i, t in enumerate(tk):
                r2 = {"symbol": t}
                for nm in names:
                    col = C[nm]
                    if isinstance(col, list) and i < len(col):
                        r2[nm] = col[i]
                rows.append(r2)
            return k, "columnar(%d cols)" % len(names), rows
    return None, None, []


def sma_state(closes):
    if not closes or len(closes) < 60:
        return None
    n = min(250, len(closes))
    sma = sum(closes[-n:]) / n
    last = closes[-1]
    return {"sma_n": n, "sma": round(sma, 2),
            "below": last <= sma,
            "dist_pct": round((last / sma - 1) * 100, 1)}


def rel_strength(closes, spy):
    if not closes or not spy or len(closes) < 64 \
            or len(spy) < 64:
        return None
    r = (closes[-1] / closes[-63] - 1) - (spy[-1] / spy[-63] - 1)
    return round(r * 100, 1)


def double_bottom(closes):
    if not closes or len(closes) < 90:
        return None
    w = closes[-120:]
    lows = []
    for i in range(3, len(w) - 3):
        if w[i] == min(w[i - 3:i + 4]):
            lows.append((i, w[i]))
    for i in range(len(lows) - 1):
        for j in range(i + 1, len(lows)):
            d1, v1 = lows[i]
            d2, v2 = lows[j]
            if 15 <= d2 - d1 <= 100 and v1 > 0:
                if abs(v2 - v1) / v1 <= 0.04 and v2 >= v1 * 0.97:
                    if w[-1] > max(v1, v2) * 1.02:
                        return {"formed": True,
                                "lows": [round(v1, 2),
                                         round(v2, 2)],
                                "sep_days": d2 - d1}
    return {"formed": False}


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


def lambda_handler(event=None, context=None):
    ck, cmode, crows = load_census()
    closes_doc = s3_json("data/_ma200/closes.json") or {}
    ser = closes_doc.get("series") or {}
    spy = ser.get("SPY")
    boom = s3_json("data/industry-boom.json") or {}
    league = {str(x.get("industry", "")).lower():
              fnum(x.get("score") or x.get("boom_score")
                   or x.get("composite")) or 0.0
              for x in boom.get("league") or []
              if isinstance(x, dict)}
    boom_med = sorted(league.values())[len(league) // 2] \
        if league else 0.0
    deals = s3_json("data/deal-scanner.json") or {}
    deal_by_sym = {}
    for d in (deals.get("deals") or []):
        for symk in ("symbol", "ticker"):
            sy = d.get(symk)
            if sy:
                deal_by_sym.setdefault(str(sy).upper(),
                                       []).append(
                    {"class": d.get("event_class")
                     or d.get("class") or "deal",
                     "headline": str(d.get("headline")
                                     or d.get("title"))[:90]})
    field_census = sorted(crows[0].keys()) if crows else []
    rows_out = []
    n_gate = {"below_sma": 0, "eps_seq": 0, "dilution": 0,
              "margin_floor": 0}
    for row in crows:
        sym = str(row.get("symbol") or row.get("ticker")
                  or "").upper()
        if not sym or sym in ("SPY",):
            continue
        name = row.get("name") or row.get("companyName") or ""
        industry = str(row.get("industry")
                       or row.get("Industry") or "").lower()
        sector = row.get("sector") or ""
        closes = ser.get(sym)
        sst = sma_state(closes)
        eps_q = qseq(row, ("eps_q", "epsQuarters", "eps_quarterly",
                           "dilutedEPS_q", "eps_dil_q", "epsQ",
                           "eps_ttm_q", "eps"))
        rev_q = qseq(row, ("revenue_q", "revenueQuarters",
                           "revenue_quarterly", "revQ",
                           "revenue"))
        fcfps_q = qseq(row, ("fcf_ps_q", "fcfPerShare_q",
                             "fcf_q", "freeCashFlowPerShare_q",
                             "fcfps"))
        shares_q = qseq(row, ("shares_q", "sharesDiluted_q",
                              "dilutedShares_q", "shares",
                              "sharesOutstanding_q"))
        margin_now = fnum(row.get("operating_margin")
                          or row.get("opMargin")
                          or row.get("op_margin")
                          or row.get("operatingMarginTTM")
                          or row.get("op_margin_ttm"))
        margin_q = qseq(row, ("op_margin_q", "opMargin_q",
                              "operatingMargin_q"))
        roic = fnum(row.get("roic") or row.get("ROIC")
                    or row.get("roic_ttm")
                    or row.get("roicTTM"))
        pe = fnum(row.get("pe") or row.get("peTTM")
                  or row.get("pe_ttm") or row.get("pe_fwd")
                  or row.get("p_e"))
        fpe = fnum(row.get("forward_pe") or row.get("fwdPE")
                   or row.get("forwardPE")) or pe
        ind_pe = fnum(row.get("industry_pe")
                      or row.get("industryPE"))
        ind_margin = fnum(row.get("industry_margin")
                          or row.get("industryOpMargin"))
        backlog = fnum(row.get("backlog")
                       or row.get("backlog_usd")
                       or row.get("backlogUSD"))
        fcf_yield = fnum(row.get("fcf_yield")
                         or row.get("fcf_yield_pct")
                         or row.get("fcfYield"))
        peg_col = fnum(row.get("peg") or row.get("peg_fwd")
                       or row.get("peg_ratio"))

        gates = {}
        reasons = []
        gates["below_sma"] = bool(sst and sst["below"])
        if not gates["below_sma"]:
            reasons.append("above long-SMA (not in "
                           "accumulation zone)" if sst
                           else "no price history")
        else:
            n_gate["below_sma"] += 1
        g_eps = None
        if eps_q and len(eps_q) >= 5:
            last4 = eps_q[-4:]
            prev4 = eps_q[-5:-1]
            g_eps = all(last4[i] > prev4[i]
                        for i in range(4))
        if g_eps is None:
            sg = fnum(row.get("eps_yoy")
                      or row.get("eps_g_yoy")
                      or row.get("eps_growth_yoy"))
            if sg is not None:
                g_eps = sg > 0
        gates["eps_up_every_q"] = bool(g_eps)
        if g_eps:
            n_gate["eps_seq"] += 1
        elif eps_q:
            reasons.append("EPS not up every quarter (y/y "
                           "by qtr)")
        else:
            reasons.append("no quarterly EPS in census")
        dil = None
        if shares_q and len(shares_q) >= 5 and shares_q[-5]:
            dil = (shares_q[-1] / shares_q[-5] - 1) * 100
        if dil is None:
            dil = fnum(row.get("shares_chg_1y_pct")
                       or row.get("share_count_chg_yoy")
                       or row.get("dilution_1y_pct"))
        gates["dilution_ok"] = (dil is None) or dil <= 4.0
        if dil is not None and dil > 4.0:
            reasons.append("diluted shares +%.1f%%/yr" % dil)
        else:
            n_gate["dilution"] += 1
        gates["margin_floor"] = True
        if margin_now is not None and ind_margin is not None \
                and margin_now < ind_margin * 0.9:
            gates["margin_floor"] = False
            reasons.append("margin below industry")
        else:
            n_gate["margin_floor"] += 1

        pillars = {}
        beats = warm_fmp(sym, "sur", "earnings-surprises/" + sym)\
            if gates["below_sma"] else None
        if isinstance(beats, list) and beats:
            b4 = beats[:4]
            n_beat = sum(1 for b in b4
                         if fnum(b.get("actualEarningResult"))
                         is not None
                         and fnum(b.get("estimatedEarning"))
                         is not None
                         and fnum(b["actualEarningResult"])
                         > fnum(b["estimatedEarning"]))
            mag = []
            for b in b4:
                a, e = fnum(b.get("actualEarningResult")), \
                    fnum(b.get("estimatedEarning"))
                if a is not None and e not in (None, 0):
                    mag.append((a / abs(e) - 1) * 100)
            pillars["revisions_beats"] = round(clamp(
                n_beat * 15 + (sum(mag) / len(mag) if mag
                               else 0)), 1)
        else:
            pillars["revisions_beats"] = None
        acc = None
        if acc is None and not (eps_q and len(eps_q) >= 4):
            sg = fnum(row.get("eps_yoy")
                      or row.get("eps_g_yoy"))
            rg = fnum(row.get("rev_yoy")
                      or row.get("revenue_yoy")
                      or row.get("rev_g_yoy"))
            if sg is not None or rg is not None:
                acc = round(clamp(
                    (clamp(sg or 0, -20, 60))
                    + (clamp(rg or 0, -10, 30))), 1)
        if eps_q and len(eps_q) >= 4:
            ge = growth_seq(eps_q[-4:])
            gr = growth_seq(rev_q[-4:]) if rev_q and \
                len(rev_q) >= 4 else []
            sc = 0.0
            if len(ge) >= 2:
                sc += clamp(ge[-1], -20, 60)
                if ge[-1] > ge[0]:
                    sc += 20
            if len(gr) >= 2:
                sc += clamp(gr[-1], -10, 30)
                if gr[-1] > gr[0]:
                    sc += 15
            acc = round(clamp(sc), 1)
        pillars["accel"] = acc
        f = None
        if not (fcfps_q and len(fcfps_q) >= 5):
            fg2 = fnum(row.get("fcf_yoy")
                       or row.get("fcf_growth_yoy")
                       or row.get("fcfps_yoy"))
            if fg2 is not None:
                f = round(clamp(fg2 + 25, 0, 100), 1)
        if fcfps_q and len(fcfps_q) >= 5 and fcfps_q[-5]:
            fg = (fcfps_q[-1] / abs(fcfps_q[-5]) - 1) * 100
            f = round(clamp(fg + 25, 0, 100), 1)
        pillars["fcf_growth"] = f
        val = 0.0
        vparts = 0
        peg = peg_col
        if peg is not None:
            val += clamp((1.5 - peg) * 60, 0, 60)
            vparts += 1
        if peg is None and fpe and eps_q and len(eps_q) >= 5 \
                and eps_q[-5]:
            gy = (eps_q[-1] / abs(eps_q[-5]) - 1) * 100
            if gy > 0:
                peg = fpe / gy
                val += clamp((1.5 - peg) * 60, 0, 60)
                vparts += 1
        if pe and ind_pe and ind_pe > 0:
            val += clamp((1 - pe / ind_pe) * 60, 0, 25)
            vparts += 1
        if fcf_yield is not None:
            val += clamp(fcf_yield * 4, 0, 15)
            vparts += 1
        pillars["valuation_vs_growth"] = round(val, 1) \
            if vparts else None
        rs = rel_strength(closes, spy)
        db_col = row.get("double_bottom")
        db = ({"formed": bool(db_col), "src": "census"}
              if db_col is not None
              else double_bottom(closes))
        cats = []
        if deal_by_sym.get(sym):
            for d in deal_by_sym[sym][:3]:
                cats.append(d)
        bsc = league.get(industry)
        if bsc is not None and bsc >= boom_med * 1.15:
            cats.append({"class": "booming-industry",
                         "headline": "%s boom score %.0f"
                         % (industry[:28], bsc)})
        if acc and acc >= 60:
            cats.append({"class": "earnings-inflection",
                         "headline": "EPS/revenue growth "
                                     "accelerating"})
        px = 0.0
        if rs is not None:
            px += clamp(50 + rs * 3)
        if db and db.get("formed"):
            px += 25
        if cats:
            px += 15
        pillars["catalyst_rs"] = round(clamp(px), 1) \
            if (rs is not None or cats) else None
        q = 0.0
        if roic is not None:
            q += clamp(roic * 2.5, 0, 6)
        if margin_q and len(margin_q) >= 4 and \
                margin_q[-1] > margin_q[-4]:
            q += 2
        if backlog:
            q += 2
        wts = {"revisions_beats": 0.25, "accel": 0.25,
               "fcf_growth": 0.15,
               "valuation_vs_growth": 0.15,
               "catalyst_rs": 0.20}
        num = den = 0.0
        for k2, w in wts.items():
            v = pillars.get(k2)
            if v is not None:
                num += v * w
                den += w
        score = round((num / den if den else 0) + q, 1)
        all_gates = all(gates.values())
        tier = ("EXPLOSIVE-SETUP" if all_gates and score >= 70
                and cats else
                "SETUP" if all_gates and score >= 55 else
                "WATCH" if gates["below_sma"] and score >= 45
                else "SCREENED")
        rows_out.append({
            "symbol": sym, "name": str(name)[:48],
            "sector": sector, "industry": industry[:36],
            "score": score, "tier": tier,
            "gates": gates, "gate_reasons": reasons[:4],
            "pillars": pillars,
            "peg": round(peg, 2) if peg else None,
            "pe": pe, "roic": roic, "dilution_yr_pct":
                round(dil, 1) if dil is not None else None,
            "backlog": backlog,
            "sma": sst, "rs_3m_vs_spy": rs,
            "double_bottom": db,
            "catalysts": cats[:4],
            "why": "why.html?symbol=" + sym})
    rows_out.sort(key=lambda r: (-(r["tier"] ==
                                   "EXPLOSIVE-SETUP"),
                                 -(r["tier"] == "SETUP"),
                                 -r["score"]))
    payload = {
        "schema_version": 1,
        "engine": "justhodl-stock-buying",
        "as_of": datetime.now(timezone.utc).isoformat(
            timespec="seconds"),
        "census_source": ck, "census_mode": cmode,
        "matrix_probe": globals().get("_MXP"),
        "census_fields_sample": field_census,
        "n_universe": len(crows), "n_scored": len(rows_out),
        "gates_summary": n_gate,
        "fmp_key": bool(FMP_KEY),
        "tiers": {t: sum(1 for r in rows_out
                         if r["tier"] == t)
                  for t in ("EXPLOSIVE-SETUP", "SETUP",
                            "WATCH", "SCREENED")},
        "top": rows_out[:60],
        "doctrine": ("largest POSITIVE CHANGE not yet priced: "
                     "gates(≤long-SMA, EPS up every q, no "
                     "dilution, margin floor) · five-core "
                     "weights(revisions 25/accel 25/FCF 15/"
                     "val-vs-growth 15/catalyst+RS 20) · Davis "
                     "double play: EPS +40% with P/E 12→20 = "
                     "+133%")}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload).encode(),
                  ContentType="application/json")
    return {"n": len(rows_out)}
