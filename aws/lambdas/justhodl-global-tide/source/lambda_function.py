"""
justhodl-global-tide v1.0 — THE global liquidity + risk composite.

Audit 1528/1529 finding: 18 liquidity-family Lambdas produce rich per-CB
detail (boj-detail, china-liquidity, ecb-detail/derived) but the composite
brief (global-liquidity.json) is EMPTY. This engine is the missing roof:

  GLI  — G4 central-bank liquidity in USD, 13w impulse, regime
         (FLOOD / RISING / NEUTRAL / EBBING / DRAIN)
  RISK — global risk dial 0-100 from VIX curve regime, HY OAS, NFCI,
         BoJ carry-unwind risk, eurodollar stress (ESI)
  DIVERGENCE — equities vs liquidity: SPX grinding up while G4 liquidity
         drains = air-pocket setup (the 2021Q4 / 2018Q3 signature)

Inputs: own platform briefs first (boj-detail, china-liquidity, ecb-detail,
ecb-derived, vix-curve) + FRED for the Fed leg + spreads.
Output: data/global-tide.json  ·  Telegram on regime change.
"""
import json, os, time, urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
TG_TOKEN = os.environ.get("TG_TOKEN", "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs")
TG_CHAT = os.environ.get("TG_CHAT", "8678089260")


def _rd(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _tg(msg):
    try:
        data = json.dumps({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
        urllib.request.urlopen(urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data,
            headers={"Content-Type": "application/json"}), timeout=10)
    except Exception:
        pass


_FRED_CACHE = None
def fred(series, n=80):
    """(date,val) list — fred-cache.json first, live API fallback."""
    global _FRED_CACHE
    if _FRED_CACHE is None:
        _FRED_CACHE = _rd("data/fred-cache.json") or {}
    blob = _FRED_CACHE.get("series", _FRED_CACHE).get(series) if isinstance(_FRED_CACHE, dict) else None
    if isinstance(blob, dict):
        obs = blob.get("observations") or blob.get("data") or blob.get("obs")
        if isinstance(obs, list) and obs:
            pts = []
            for o in obs[-n:]:
                try:
                    if isinstance(o, dict):
                        v = o.get("value"); d = o.get("date")
                    else:
                        d, v = o[0], o[1]
                    if v not in (None, ".", ""):
                        pts.append((d, float(v)))
                except Exception:
                    continue
            if pts:
                return pts
    try:
        u = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series}"
             f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={n}")
        d = json.loads(urllib.request.urlopen(u, timeout=20).read())
        pts = [(o["date"], float(o["value"])) for o in d.get("observations", []) if o.get("value") not in (".", None)]
        return list(reversed(pts))
    except Exception:
        return []


def _dig(d, *names):
    """Depth-2 search for the first numeric value whose key contains any name."""
    if not isinstance(d, dict):
        return None
    for k, v in d.items():
        kl = k.lower()
        if any(n in kl for n in names) and isinstance(v, (int, float)):
            return v
    for v in d.values():
        if isinstance(v, dict):
            r = _dig(v, *names)
            if r is not None:
                return r
    return None


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    out = {"engine": "global-tide", "version": "1.1", "generated_at": now.isoformat()}
    prev = _rd("data/global-tide.json")

    # ── 1. FED leg: net liquidity = WALCL − RRP − TGA (USD bn) ──
    fed = {}
    try:
        wal = fred("WALCL", 80)              # Fed total assets, $mn weekly
        rrp = fred("RRPONTSYD", 120)         # ON RRP, $bn daily
        tga = fred("WTREGEN", 80)            # Treasury General Account, $bn weekly
        if wal and rrp and tga:
            w_now, w_13 = wal[-1][1] / 1000, wal[-14][1] / 1000 if len(wal) > 13 else None
            r_now = rrp[-1][1]
            r_13 = rrp[-66][1] if len(rrp) > 65 else rrp[0][1]
            t_now = tga[-1][1] / 1000      # WTREGEN is $mn
            t_13 = (tga[-14][1] if len(tga) > 13 else tga[0][1]) / 1000
            nl_now = round(w_now - r_now - t_now, 0)
            nl_13 = round((w_13 - r_13 - t_13), 0) if w_13 is not None else None
            fed = {"net_liquidity_usd_bn": nl_now, "fed_bs_usd_bn": round(w_now, 0),
                   "rrp_usd_bn": round(r_now, 0), "tga_usd_bn": round(t_now, 0),
                   "impulse_13w_usd_bn": round(nl_now - nl_13, 0) if nl_13 is not None else None,
                   "as_of": wal[-1][0]}
    except Exception as e:
        fed = {"err": str(e)[:60]}
    out["fed"] = fed

    # ── 2. ECB leg (own briefs) ──
    ecb_d = _rd("data/ecb-detail.json")
    ecb_v = _rd("data/ecb-derived.json")
    eurusd = ((ecb_v.get("fx") or {}).get("eurusd")) or 1.08
    bs = ecb_d.get("balance_sheet") or {}
    ecb = {"bs_eur_bn": bs.get("total_assets_eur_bn"),
           "bs_usd_bn": round(bs.get("total_assets_eur_bn", 0) * eurusd, 0) if bs.get("total_assets_eur_bn") else None,
           "chg_6m_pct": bs.get("change_6m_pct"), "qt_pace": bs.get("qt_pace"),
           "injection_score": ecb_d.get("ecb_injection_score"), "stance": ecb_d.get("stance_label"),
           "esi_0_100": ((ecb_v.get("indicators") or {}).get("eurodollar_stress_index") or {}).get("esi_0_100")}
    out["ecb"] = ecb

    # ── 3. BoJ leg (own brief) ──
    bj = _rd("data/boj-detail.json")
    bj_bs = bj.get("balance_sheet") or {}
    usdjpy = _dig(bj.get("usdjpy") or {}, "rate", "spot", "level", "usdjpy") or _dig(bj, "usdjpy")
    bs_jpy_tn = _dig(bj_bs, "total_assets_jpy_tn", "jpy_tn", "total_assets")
    boj = {"injection_score": bj.get("boj_injection_score"), "stance": bj.get("stance_label"),
           "bs_jpy_tn": bs_jpy_tn, "usdjpy": usdjpy,
           "bs_usd_bn": round(bs_jpy_tn * 1000 / usdjpy, 0) if (bs_jpy_tn and usdjpy) else None,
           "bs_chg_6m_pct": _dig(bj_bs, "change_6m", "chg_6m", "6m_pct"),
           "carry_unwind_risk_0_100": _dig(bj.get("carry_unwind_risk") or {}, "score", "risk", "level")}
    out["boj"] = boj

    # ── 4. China leg (own brief) ──
    cn = _rd("data/china-liquidity.json")
    china = {"injection_score": cn.get("pboc_injection_score") or _dig(cn, "injection_score"),
             "stance": cn.get("stance_label") or cn.get("stance"),
             "m2_yoy_pct": _dig(cn, "m2_yoy", "m2_growth"),
             "m1_yoy_pct": _dig(cn, "m1_yoy"),
             "usdcny": _dig(cn, "usdcny", "cny")}
    out["china"] = china

    # ── 5. GLI composite: stock where measurable + direction votes everywhere ──
    g4_usd = [v for v in (fed.get("net_liquidity_usd_bn"), ecb.get("bs_usd_bn"), boj.get("bs_usd_bn")) if v]
    votes, parts = [], []
    if fed.get("impulse_13w_usd_bn") is not None:
        v = max(-2, min(2, fed["impulse_13w_usd_bn"] / 150))
        votes.append(v); parts.append(f"Fed 13w {fed['impulse_13w_usd_bn']:+.0f}bn")
    for name, sc in (("ECB", ecb.get("injection_score")), ("BoJ", boj.get("injection_score")), ("PBoC", china.get("injection_score"))):
        if isinstance(sc, (int, float)):
            votes.append(max(-2, min(2, sc / 2))); parts.append(f"{name} {sc:+g}")
    impulse = round(sum(votes) / len(votes) * 50, 0) if votes else None   # −100..+100
    regime = (None if impulse is None else
              "FLOOD" if impulse >= 60 else "RISING" if impulse >= 20 else
              "DRAIN" if impulse <= -60 else "EBBING" if impulse <= -20 else "NEUTRAL")
    out["gli"] = {"g4_stock_usd_tn": round(sum(g4_usd) / 1000, 2) if g4_usd else None,
                  "n_cb_measured": len(g4_usd), "impulse_score": impulse, "regime": regime,
                  "components": parts}

    # ── 6. RISK dial 0-100 ──
    risk_parts, risk_notes = [], []
    vx = _rd("data/vix-curve.json")
    vix_spot = _dig(vx.get("current") or vx, "vix_spot", "spot", "vix")
    if vix_spot:
        risk_parts.append(min(100, max(0, (vix_spot - 12) * 5)))   # 12→0, 32→100
        risk_notes.append(f"VIX {vix_spot:g}")
    comp_reg = vx.get("composite_regime")
    if isinstance(comp_reg, str):
        risk_notes.append(f"vol regime {comp_reg}")
        if "BACKWARD" in comp_reg.upper() or "STRESS" in comp_reg.upper():
            risk_parts.append(85)
    hy = fred("BAMLH0A0HYM2", 80)
    if hy:
        oas = hy[-1][1]
        risk_parts.append(min(100, max(0, (oas - 2.8) * 28)))      # 2.8%→0, ~6.4%→100
        risk_notes.append(f"HY OAS {oas:.2f}%")
        out["hy_oas_pct"] = round(oas, 2)
        out["hy_oas_3m_chg_bp"] = round((oas - hy[-66][1]) * 100, 0) if len(hy) > 65 else None
    nf = fred("NFCI", 30)
    if nf:
        risk_parts.append(min(100, max(0, (nf[-1][1] + 0.6) * 90)))
        risk_notes.append(f"NFCI {nf[-1][1]:+.2f}")
        out["nfci"] = round(nf[-1][1], 2)
    if isinstance(boj.get("carry_unwind_risk_0_100"), (int, float)):
        risk_parts.append(boj["carry_unwind_risk_0_100"])
        risk_notes.append(f"¥-carry {boj['carry_unwind_risk_0_100']:g}/100")
    if isinstance(ecb.get("esi_0_100"), (int, float)):
        risk_parts.append(ecb["esi_0_100"])
        risk_notes.append(f"ESI {ecb['esi_0_100']:g}")
    grisk = round(sum(risk_parts) / len(risk_parts), 0) if risk_parts else None
    rtier = (None if grisk is None else
             "CRISIS" if grisk >= 75 else "STRESSED" if grisk >= 55 else
             "ELEVATED" if grisk >= 40 else "CALM")
    out["risk"] = {"global_risk_0_100": grisk, "tier": rtier, "components": risk_notes}

    # ── 7. Liquidity↔equity divergence ──
    out["indicators"] = {}
    try:
        spx = fred("SP500", 80)
        if spx and impulse is not None:
            ret60 = round((spx[-1][1] / spx[-44][1] - 1) * 100, 1) if len(spx) > 43 else None
            out["spx_60d_pct"] = ret60
            if ret60 is not None and ret60 > 3 and impulse <= -20:
                sig = "CRITICAL" if impulse <= -60 else "WATCH"
                out["indicators"]["liquidity_equity_divergence"] = {
                    "name": "Liquidity↔Equity Divergence (air-pocket setup)",
                    "spx_60d_pct": ret60, "gli_impulse": impulse, "signal": sig,
                    "interpretation": (f"SPX +{ret60}% over 60d while G4 liquidity impulse {impulse:+.0f} "
                                       "(draining). Equities levitating without the tide — 2018Q3/2021Q4 signature; "
                                       "drawdowns from this setup are fast.")}
    except Exception:
        pass
    if regime == "DRAIN":
        out["indicators"]["global_liquidity_drain"] = {
            "name": "G4 Liquidity Drain", "impulse": impulse, "signal": "WATCH",
            "interpretation": f"All-CB composite impulse {impulse:+.0f} — synchronized drain; beta tailwind gone."}
    if grisk is not None and grisk >= 55:
        out["indicators"]["global_risk_stress"] = {
            "name": "Global Risk Dial", "score": grisk, "signal": "CRITICAL" if grisk >= 75 else "WATCH",
            "interpretation": f"Global risk {grisk:.0f}/100 ({rtier}): " + "; ".join(risk_notes)}

    flashing = [k for k, v in out["indicators"].items() if v.get("signal") in ("WATCH", "CRITICAL")]
    out["n_flashing"] = len(flashing)
    out["headline"] = (f"GLI {regime or '?'} ({impulse:+.0f})" if impulse is not None else "GLI partial") + \
                      (f" · Risk {grisk:.0f}/100 {rtier}" if grisk is not None else "") + \
                      (f" · {len(flashing)} flashing" if flashing else "")
    out["read"] = (f"G4 stock ${out['gli']['g4_stock_usd_tn']}tn ({out['gli']['n_cb_measured']}/4 CBs measured). "
                   if out["gli"]["g4_stock_usd_tn"] else "") + \
                  f"Impulse {impulse:+.0f} → {regime}. " + "; ".join(out["gli"]["components"]) + ". " + \
                  (f"Risk {grisk:.0f}/100 ({rtier}): " + "; ".join(risk_notes) + "." if grisk is not None else "")

    # regime-change Telegram
    pr = (prev.get("gli") or {}).get("regime")
    if regime and pr and regime != pr:
        _tg(f"🌊 <b>GLOBAL TIDE</b> regime: {pr} → <b>{regime}</b>\n{out['headline']}\n{out['read'][:300]}")

    out["duration_s"] = round(time.time() - t0, 1)
    S3.put_object(Bucket=BUCKET, Key="data/global-tide.json", Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[global-tide] {out['headline']} in {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"regime": regime, "risk": grisk, "n_flashing": len(flashing)})}
