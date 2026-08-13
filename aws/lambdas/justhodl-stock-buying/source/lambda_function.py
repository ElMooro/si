"""justhodl-stock-buying v1.0.0 (ops 4650)

Khalid's institutional accumulation screener: buy-zone technicals
(under SMA250, RSI<35, relative strength, double-bottom), hard
quality gates (no dilution, positive EPS), and a 9-factor weighted
composite in his rank order — EPS/rev growth + acceleration, FCF,
valuation-vs-growth (PEG, P/E vs sector), margin edge + expansion,
ROIC, booming-industry score, technical confirmation — plus a
catalyst layer joined from the deal-scanner event taxonomy and the
census backlog column. Every row deep-links why.html?ticker=.
All inputs are in-fleet real stores; missing fields degrade the
factor to neutral and are reported, never faked.
"""
import json
import math
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/stock-buying.json"
s3 = boto3.client("s3", region_name="us-east-1")


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def g(row, *names):
    for n in names:
        v = row.get(n)
        if isinstance(v, (int, float)):
            return float(v)
    return None


def rsi14(vals):
    if len(vals) < 20:
        return None
    gains = losses = 0.0
    for i in range(-14, 0):
        d = vals[i] - vals[i - 1]
        if d >= 0:
            gains += d
        else:
            losses -= d
    if losses == 0:
        return 100.0
    rs = (gains / 14) / (losses / 14)
    return 100 - 100 / (1 + rs)


def double_bottom(vals):
    n = len(vals)
    if n < 60:
        return None
    w = vals[-160:] if n >= 160 else vals
    lows = []
    for i in range(3, len(w) - 3):
        if w[i] == min(w[i - 3:i + 4]):
            lows.append((i, w[i]))
    for a in range(len(lows)):
        for b in range(a + 1, len(lows)):
            i1, l1 = lows[a]
            i2, l2 = lows[b]
            if not (12 <= i2 - i1 <= 90):
                continue
            if abs(l1 - l2) / max(l1, l2) > 0.035:
                continue
            neck = max(w[i1:i2 + 1])
            if (neck - min(l1, l2)) / min(l1, l2) < 0.05:
                continue
            last = w[-1]
            if i2 >= len(w) - 45 and last >= min(l1, l2):
                if last > neck:
                    return "CONFIRMED"
                if last > min(l1, l2) * 1.01:
                    return "FORMING"
    return None


def clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))


def lambda_handler(event=None, context=None):
    cen = s3_json("data/fundamental-census.json") or {}
    clo = s3_json("data/_ma200/closes.json") or {}
    boom = s3_json("data/industry-boom.json") or {}
    deals = s3_json("data/deal-scanner.json") or {}

    crows = cen.get("rows") or cen.get("companies") or []
    cmap = {}
    for r in crows:
        t = r.get("ticker") or r.get("symbol")
        if t:
            cmap[str(t).upper()] = r
    dates = clo.get("dates") or []
    ser = clo.get("series") or {}
    spy = ser.get("SPY")

    boom_rows = boom.get("rows") or boom.get("industries") or []
    boom_by_name = {}
    for b in boom_rows:
        nm = str(b.get("industry") or b.get("name") or "").lower()
        sc = b.get("score")
        if not isinstance(sc, (int, float)):
            sc = b.get("velocity")
        if nm and isinstance(sc, (int, float)):
            boom_by_name[nm] = float(sc)

    drows = deals.get("rows") or deals.get("events") or []
    cat_by_t = {}
    for d in drows:
        t = str(d.get("ticker") or d.get("symbol") or "").upper()
        cls = d.get("event_class") or d.get("class") or \
            d.get("category")
        if t and cls:
            cat_by_t.setdefault(t, [])
            if cls not in cat_by_t[t]:
                cat_by_t[t].append(str(cls))

    sect_pe, sect_om = {}, {}
    for t, r in cmap.items():
        sec = str(r.get("sector") or r.get("industry") or "?")
        pe = g(r, "pe", "pe_ratio", "pe_ttm", "price_earnings")
        om = g(r, "operating_margin_pct")
        if pe and 0 < pe < 300:
            sect_pe.setdefault(sec, []).append(pe)
        if om is not None:
            sect_om.setdefault(sec, []).append(om)

    def med(xs):
        xs = sorted(xs)
        return xs[len(xs) // 2] if xs else None

    sect_pe = {k: med(v) for k, v in sect_pe.items()}
    sect_om = {k: med(v) for k, v in sect_om.items()}

    gate_census = {"universe": 0, "have_prices": 0,
                   "under_sma250": 0, "rsi_lt_35": 0,
                   "no_dilution": 0, "eps_positive": 0,
                   "passed_all": 0}
    out_rows = []
    missing = {}

    for t, r in cmap.items():
        vals = ser.get(t)
        gate_census["universe"] += 1
        if not (isinstance(vals, list) and len(vals) >= 60):
            continue
        vals = [float(x) for x in vals if
                isinstance(x, (int, float))]
        if len(vals) < 60:
            continue
        gate_census["have_prices"] += 1
        last = vals[-1]
        w = min(250, len(vals))
        sma = sum(vals[-w:]) / w
        under = last < sma
        if under:
            gate_census["under_sma250"] += 1
        rsi = rsi14(vals)
        rsi_ok = rsi is not None and rsi < 35
        if rsi_ok:
            gate_census["rsi_lt_35"] += 1
        dil = g(r, "shares_out_yoy_pct", "share_count_yoy_pct",
                "shares_yoy_pct", "diluted_shares_yoy_pct")
        no_dil = dil is None or dil <= 2.5
        if no_dil:
            gate_census["no_dilution"] += 1
        eps_y = g(r, "eps_yoy_pct")
        eps_ok = eps_y is not None and eps_y > 0
        if eps_ok:
            gate_census["eps_positive"] += 1
        if not (under and rsi_ok and no_dil and eps_ok):
            continue
        gate_census["passed_all"] += 1

        rev_y = g(r, "revenue_yoy_pct")
        eps_acc = g(r, "eps_yoy_pct_chg", "eps_yoy_chg_pp",
                    "eps_growth_accel_pp")
        rev_acc = g(r, "revenue_yoy_pct_chg",
                    "revenue_yoy_chg_pp")
        fcfm = g(r, "fcf_margin_pct")
        fcf_y = g(r, "fcf_yield_pct")
        peg = g(r, "peg", "peg_ratio", "forward_peg")
        pe = g(r, "pe", "pe_ratio", "pe_ttm")
        om = g(r, "operating_margin_pct")
        om_chg = g(r, "operating_margin_pct_chg",
                   "op_margin_chg_pp")
        roic = g(r, "roic_pct")
        roic_chg = g(r, "roic_pct_chg")
        backlog = g(r, "backlog_usd", "backlog", "backlog_bn")
        sec = str(r.get("sector") or r.get("industry") or "?")
        pe_med = sect_pe.get(sec)
        om_med = sect_om.get(sec)
        ind = str(r.get("industry") or r.get("sector")
                  or "").lower()
        bsc = None
        for nm, sc in boom_by_name.items():
            if nm and (nm in ind or ind in nm):
                bsc = sc
                break
        rs63 = None
        if spy and len(spy) >= 64 and len(vals) >= 64:
            try:
                rs63 = ((vals[-1] / vals[-64] - 1)
                        - (float(spy[-1]) / float(spy[-64]) - 1)
                        ) * 100
            except Exception:
                rs63 = None
        db = double_bottom(vals)

        def track(name, v):
            if v is None:
                missing[name] = missing.get(name, 0) + 1
                return 0.5
            return None

        f = {}
        f["eps_growth"] = track("eps_growth", eps_y) or \
            clamp(eps_y / 50.0)
        f["rev_growth"] = track("rev_growth", rev_y)
        if f["rev_growth"] is None:
            f["rev_growth"] = clamp(rev_y / 30.0)
        f["accel"] = track("accel", eps_acc)
        if f["accel"] is None:
            f["accel"] = clamp(0.5 + eps_acc / 40.0)
        if rev_acc is not None:
            f["accel"] = clamp(0.6 * f["accel"]
                               + 0.4 * clamp(0.5 + rev_acc / 30.0))
        f["fcf"] = track("fcf", fcfm)
        if f["fcf"] is None:
            f["fcf"] = clamp(fcfm / 25.0)
        if fcf_y is not None:
            f["fcf"] = clamp(0.6 * f["fcf"]
                             + 0.4 * clamp(fcf_y / 8.0))
        v_pe = None
        if pe and pe_med and pe > 0:
            v_pe = clamp((pe_med - pe) / pe_med + 0.5)
        v_peg = None
        if peg and peg > 0:
            v_peg = clamp((1.5 - peg) / 1.5 + 0.25)
        if v_pe is None and v_peg is None:
            missing["valuation"] = missing.get("valuation",
                                              0) + 1
            f["valuation"] = 0.5
        else:
            parts = [x for x in (v_pe, v_peg) if x is not None]
            f["valuation"] = sum(parts) / len(parts)
        f["margin"] = 0.5
        if om is not None and om_med is not None:
            f["margin"] = clamp(0.5 + (om - om_med) / 30.0)
        elif om is not None:
            f["margin"] = clamp(om / 25.0)
        else:
            missing["margin"] = missing.get("margin", 0) + 1
        if om_chg is not None:
            f["margin"] = clamp(0.7 * f["margin"]
                                + 0.3 * clamp(0.5 + om_chg / 8))
        f["roic"] = track("roic", roic)
        if f["roic"] is None:
            f["roic"] = clamp(roic / 25.0)
        if roic_chg is not None:
            f["roic"] = clamp(0.75 * f["roic"]
                              + 0.25 * clamp(0.5 + roic_chg / 8))
        f["boom"] = 0.5 if bsc is None else clamp(bsc / 100.0)
        if bsc is None:
            missing["boom"] = missing.get("boom", 0) + 1
        tech = 0.5
        if rs63 is not None:
            tech = clamp(0.5 + rs63 / 30.0)
        if db == "CONFIRMED":
            tech = clamp(tech + 0.25)
        elif db == "FORMING":
            tech = clamp(tech + 0.12)
        f["technical"] = tech

        W = {"eps_growth": 14, "rev_growth": 12, "accel": 13,
             "fcf": 12, "valuation": 14, "margin": 9,
             "roic": 10, "boom": 8, "technical": 8}
        score = sum(f[k] * W[k] for k in W)
        cats = cat_by_t.get(t, [])
        cat_bonus = min(6, 3 * len(cats))
        if backlog:
            cat_bonus = min(8, cat_bonus + 2)
        score = round(min(100.0, score + cat_bonus), 1)

        out_rows.append({
            "ticker": t,
            "name": r.get("name") or r.get("company"),
            "sector": sec,
            "score": score,
            "last": round(last, 2),
            "sma250_gap_pct": round((last / sma - 1) * 100, 1),
            "rsi14": round(rsi, 1),
            "eps_yoy_pct": eps_y,
            "rev_yoy_pct": rev_y,
            "peg": peg, "pe": pe,
            "sector_pe_med": (round(pe_med, 1)
                              if pe_med else None),
            "roic_pct": roic,
            "op_margin_pct": om,
            "margin_edge_pp": (round(om - om_med, 1)
                               if om is not None
                               and om_med is not None else None),
            "fcf_margin_pct": fcfm,
            "boom_score": bsc,
            "rs_63_pp": (round(rs63, 1)
                         if rs63 is not None else None),
            "double_bottom": db,
            "catalysts": cats[:4],
            "backlog": backlog,
            "factors": {k: round(v, 2) for k, v in f.items()},
            "why": "why.html?ticker=" + t,
        })

    out_rows.sort(key=lambda x: -x["score"])
    payload = {
        "schema_version": "1.0",
        "engine": "justhodl-stock-buying",
        "as_of": datetime.now(timezone.utc).isoformat(
            timespec="seconds"),
        "doctrine": "accumulation screener: buy-zone technicals "
                    "(under SMA250 + RSI<35) gated by quality "
                    "(no dilution, positive EPS), ranked by "
                    "Khalid's 9-factor institutional composite "
                    "with catalyst/backlog bonus; missing fields "
                    "score neutral and are counted, never faked",
        "gate_census": gate_census,
        "missing_factor_counts": missing,
        "n_candidates": len(out_rows),
        "rows": out_rows[:60],
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload).encode(),
                  ContentType="application/json")
    return {"ok": True, "candidates": len(out_rows)}
